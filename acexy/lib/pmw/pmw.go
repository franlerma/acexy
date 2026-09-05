// Acexy - Copyright (C) 2024 - Javinator9889 <dev at javinator9889 dot com>
// This program comes with ABSOLUTELY NO WARRANTY; for details type `show w'.
// This is free software, and you are welcome to redistribute it
// under certain conditions; type `show c' for details.
//
// Package pmw (Parallel MultiWriter) contains an implementation of an "io.Writer" that
// duplicates its writes to all the provided writers, similar to the Unix tee(1) command.
// Writers can be added and removed dynamically after creation.
//
// Each registered writer is wrapped in a small, lazily-allocated bounded buffer drained by
// its own goroutine. Writing to the multiwriter NEVER blocks on a slow client: when a
// client's buffer reaches its byte budget, the oldest queued chunks are dropped (lossy),
// which is the correct behaviour for live video (MPEG-TS). A client is only removed when its
// underlying Write fails or the connection is closed.
//
// The buffer is allocated on demand: a client that consumes fast keeps a near-empty queue and
// does not pre-allocate the full budget.
//
// Example:
//
//	package main
//
//	import (
//		"os"
//		"lib/pmw"
//	)
//
//	func main() {
//		w := pmw.New(os.Stdout)
//		w.Add(os.Stderr)
//		w.Write([]byte("hello\n"))
//		w.Remove(os.Stderr)
//		w.Close()
//	}
package pmw

import (
	"io"
	"sync"
)

// defaultBufferBudget is the default maximum number of bytes buffered per client before
// the oldest chunks are dropped (16 MiB). Not pre-allocated: it is only an upper bound.
const defaultBufferBudget = 16 << 20

// PMultiWriter duplicates its writes to all the provided writers, similar to the Unix
// tee(1) command. Writers can be added and removed dynamically after creation. Each write is
// enqueued into every registered client's buffer without blocking on slow clients.
type PMultiWriter struct {
	mu       sync.Mutex
	buffers  map[io.Writer]*clientBuffer
	bufferBudget int
}

// PMultiWriterError is an error that occurs when writing to multiple writers.
type PMultiWriterError struct {
	Errors  []error
	Writers int
}

// Error returns a string representation of the error.
func (e PMultiWriterError) Error() string {
	s := "errors (" + itoa(len(e.Errors)) + ") when writing to " + itoa(e.Writers) + " writers\n"
	for _, err := range e.Errors {
		s += err.Error() + "\n"
	}
	return s
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	neg := n < 0
	if neg {
		n = -n
	}
	var buf [20]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		i--
		buf[i] = '-'
	}
	return string(buf[i:])
}

// New creates a multiwriter that duplicates its writes to all the provided writers,
// similar to the Unix tee(1) command. Writers can be added and removed dynamically after
// creation.
func New(writers ...io.Writer) *PMultiWriter {
	p := &PMultiWriter{
		buffers:      make(map[io.Writer]*clientBuffer),
		bufferBudget: defaultBufferBudget,
	}
	for _, w := range writers {
		p.Add(w)
	}
	return p
}

// SetBufferSize sets the maximum bytes buffered per client (upper bound, not pre-allocated)
// for writers added after this call. Values <= 0 are ignored (previous/default kept).
func (p *PMultiWriter) SetBufferSize(n int) {
	if n <= 0 {
		return
	}
	p.mu.Lock()
	p.bufferBudget = n
	p.mu.Unlock()
}

// Write enqueues the given bytes into every registered client. It never blocks on a slow
// client; if a client's buffer is full the oldest chunks are dropped. Returns len(p) always
// (a lossy fan-out reports success to the producer, which for live video must not stall).
func (p *PMultiWriter) Write(b []byte) (int, error) {
	p.mu.Lock()
	if len(p.buffers) == 0 {
		p.mu.Unlock()
		return len(b), nil
	}
	for _, cb := range p.buffers {
		cb.push(b)
	}
	p.mu.Unlock()
	return len(b), nil
}

// Add registers a writer to receive copies of every write. Wrapping the writer in a bounded
// lossy buffer is handled internally.
func (p *PMultiWriter) Add(w io.Writer) {
	p.mu.Lock()
	if _, ok := p.buffers[w]; ok {
		p.mu.Unlock()
		return
	}
	var cb *clientBuffer
	cb = newClientBuffer(w, p.bufferBudget, func() { p.removeByBuffer(cb) })
	p.buffers[w] = cb
	p.mu.Unlock()

	go cb.run()
}

// Remove removes a previously added writer from the list of writers. It is idempotent: if the
// writer is not registered (e.g. already auto-removed after a write error) it is a no-op. The
// writer's drain goroutine is signalled to stop; the underlying writer is NOT closed here (its
// lifecycle is owned by the caller / net/http).
func (p *PMultiWriter) Remove(w io.Writer) {
	p.mu.Lock()
	if cb, ok := p.buffers[w]; ok {
		delete(p.buffers, w)
		cb.shutdown()
	}
	p.mu.Unlock()
}

// removeByBuffer removes a client by its buffer, used when a drain goroutine detects a write
// error or closed connection. Must not be called while holding cb.mu.
func (p *PMultiWriter) removeByBuffer(cb *clientBuffer) {
	p.mu.Lock()
	for w, existing := range p.buffers {
		if existing == cb {
			delete(p.buffers, w)
			break
		}
	}
	cb.shutdown()
	p.mu.Unlock()
}

// Close stops all client drain goroutines and closes the underlying writers that implement
// io.Closer.
func (p *PMultiWriter) Close() error {
	p.mu.Lock()
	cbs := make([]*clientBuffer, 0, len(p.buffers))
	for _, cb := range p.buffers {
		cbs = append(cbs, cb)
	}
	p.buffers = make(map[io.Writer]*clientBuffer)
	p.mu.Unlock()

	var errs []error
	for _, cb := range cbs {
		cb.shutdown()
		if c, ok := cb.underlying.(io.Closer); ok {
			if err := c.Close(); err != nil {
				errs = append(errs, err)
			}
		}
	}
	if len(errs) > 0 {
		return PMultiWriterError{Errors: errs, Writers: len(cbs)}
	}
	return nil
}

// clientBuffer is a per-client bounded FIFO of byte chunks drained by a single goroutine.
// The queue is allocated lazily: it only grows while this client is consuming slower than
// the producer, up to the budget, after which the oldest chunks are dropped.
type clientBuffer struct {
	mu         sync.Mutex
	cond       *sync.Cond
	underlying io.Writer
	budget     int
	chunks     [][]byte
	size       int // total bytes currently queued
	stop       bool
	onDead     func()
}

func newClientBuffer(w io.Writer, budget int, onDead func()) *clientBuffer {
	cb := &clientBuffer{
		underlying: w,
		budget:     budget,
		onDead:     onDead,
	}
	if cb.budget <= 0 {
		cb.budget = defaultBufferBudget
	}
	cb.cond = sync.NewCond(&cb.mu)
	return cb
}

// push enqueues a copy of p. Non-blocking. Drops oldest whole chunks when the budget would
// be exceeded; a single chunk larger than the whole budget is dropped entirely. Never splits
// a chunk (avoids tearing mid-TS-packet as much as possible).
func (cb *clientBuffer) push(p []byte) {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	if cb.stop || len(p) == 0 {
		return
	}
	if len(p) > cb.budget {
		return // a single chunk bigger than the whole budget: drop it entirely
	}
	for len(cb.chunks) > 0 && cb.size+len(p) > cb.budget {
		cb.size -= len(cb.chunks[0])
		cb.chunks = cb.chunks[1:]
	}
	// Copy: the producer (io.CopyBuffer) reuses its read buffer between calls.
	chunk := make([]byte, len(p))
	copy(chunk, p)
	cb.chunks = append(cb.chunks, chunk)
	cb.size += len(chunk)
	cb.cond.Signal()
}

// run drains the buffer into the underlying writer. It is the only place that performs a
// blocking write to the socket. On a write error or short write the client is considered dead
// and auto-removed via onDead.
func (cb *clientBuffer) run() {
	for {
		cb.mu.Lock()
		for len(cb.chunks) == 0 && !cb.stop {
			cb.cond.Wait()
		}
		if cb.stop && len(cb.chunks) == 0 {
			cb.mu.Unlock()
			return
		}
		chunk := cb.chunks[0]
		cb.chunks = cb.chunks[1:]
		cb.size -= len(chunk)
		cb.mu.Unlock()

		n, err := cb.underlying.Write(chunk)
		if err != nil || n != len(chunk) {
			if cb.onDead != nil {
				cb.onDead()
			}
			return
		}
	}
}

// shutdown stops the drain goroutine and releases any buffered memory.
func (cb *clientBuffer) shutdown() {
	cb.mu.Lock()
	cb.stop = true
	cb.chunks = nil
	cb.size = 0
	cb.cond.Broadcast()
	cb.mu.Unlock()
}
