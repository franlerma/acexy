package pmw

import (
	"bytes"
	"errors"
	"io"
	"sync"
	"testing"
	"time"
)

// recWriter records every successfully written chunk (thread-safe). Its Write never blocks.
type recWriter struct {
	mu   sync.Mutex
	data [][]byte
}

func newRecWriter() *recWriter { return &recWriter{} }

func (r *recWriter) Write(p []byte) (int, error) {
	cp := append([]byte(nil), p...)
	r.mu.Lock()
	r.data = append(r.data, cp)
	r.mu.Unlock()
	return len(p), nil
}

func (r *recWriter) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.data)
}

// gateWriter blocks each Write on g.allow until it is closed, simulating a slow client whose
// TCP socket does not drain.
type gateWriter struct {
	allow chan struct{}
}

func newGateWriter() *gateWriter { return &gateWriter{allow: make(chan struct{})} }

func (g *gateWriter) Write(p []byte) (int, error) {
	<-g.allow
	return len(p), nil
}

// errWriter always fails, simulating a client whose connection is closed/errored.
type errWriter struct{}

func (errWriter) Write(p []byte) (int, error) { return 0, errors.New("boom") }

func waitFor(t *testing.T, what string, d time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

// clientCount returns the number of currently registered clients, reading the fan-out map
// under the lock. The drain goroutines mutate this map concurrently (auto-removal on write
// error), so it must never be read without the lock.
func clientCount(mw *PMultiWriter) int {
	mw.mu.Lock()
	defer mw.mu.Unlock()
	return len(mw.buffers)
}

// T1: a slow (blocked) client must not block Write nor prevent a fast client from receiving.
func TestSlowClientDoesNotBlockWrite(t *testing.T) {
	slow := newGateWriter()
	fast := newRecWriter()

	mw := New()
	mw.Add(slow)
	mw.Add(fast)
	t.Cleanup(func() { close(slow.allow); mw.Close() })

	payload := []byte("hello-stream")
	wrote := make(chan struct{})
	go func() {
		mw.Write(payload)
		close(wrote)
	}()

	select {
	case <-wrote:
		// ok: Write returned even though the slow client is blocked
	case <-time.After(300 * time.Millisecond):
		t.Fatal("Write blocked on a slow client")
	}

	// The fast client must still have received the data.
	waitFor(t, "fast client to receive data", time.Second, func() bool {
		return fast.count() >= 1
	})
}

// T2: the per-client buffer is lossy — when full, the OLDEST chunks are dropped; a chunk larger
// than the whole budget is dropped entirely; the buffer is not pre-allocated.
func TestClientBufferDropsOldest(t *testing.T) {
	cb := newClientBuffer(io.Discard, 10, nil)

	cb.push(bytes.Repeat([]byte("a"), 6)) // queued: [6]
	if len(cb.chunks) != 1 || cb.size != 6 {
		t.Fatalf("after first push: chunks=%d size=%d, want 1/6", len(cb.chunks), cb.size)
	}
	cb.push(bytes.Repeat([]byte("b"), 6)) // would be 12 > 10 -> drop oldest (a)
	if len(cb.chunks) != 1 || cb.size != 6 {
		t.Fatalf("after second push: chunks=%d size=%d, want 1/6 (oldest dropped)", len(cb.chunks), cb.size)
	}
	if !bytes.Equal(cb.chunks[0], bytes.Repeat([]byte("b"), 6)) {
		t.Fatalf("expected only the newest chunk to remain")
	}
	cb.push(bytes.Repeat([]byte("c"), 20)) // single chunk > budget (10) -> dropped entirely
	if len(cb.chunks) != 1 || cb.size != 6 {
		t.Fatalf("oversized chunk should be dropped, chunks=%d size=%d", len(cb.chunks), cb.size)
	}
	// Sanity: still the 'b' chunk.
	if !bytes.Equal(cb.chunks[0], bytes.Repeat([]byte("b"), 6)) {
		t.Fatalf("expected chunk 'b' to remain")
	}

	cb.shutdown()
	if cb.size != 0 || len(cb.chunks) != 0 {
		t.Fatalf("shutdown should release buffered memory")
	}
}

// T3: concurrent Add/Remove while the fan-out is written must not panic nor deadlock (run under
// -race). Removed writers must eventually be gone from the fan-out.
func TestConcurrentAddRemoveUnderLoad(t *testing.T) {
	mw := New()

	stop := make(chan struct{})
	var wg sync.WaitGroup

	// Writer goroutine hammering the fan-out.
	wg.Add(1)
	go func() {
		defer wg.Done()
		i := 0
		for {
			select {
			case <-stop:
				return
			default:
			}
			mw.Write([]byte{byte(i)})
			i++
		}
	}()

	// Churn clients concurrently.
	for g := 0; g < 4; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				w := newRecWriter()
				mw.Add(w)
				mw.Write([]byte("x"))
				mw.Remove(w)
			}
		}()
	}

	// Let it run briefly, then stop writers and wait for churn goroutines.
	time.Sleep(100 * time.Millisecond)
	close(stop)
	wg.Wait()

	// All churn writers were removed: the fan-out should be empty again.
	if n := clientCount(mw); n != 0 {
		t.Fatalf("expected empty fan-out after removing all clients, got %d", n)
	}
	mw.Close()
}

// T4: a client whose underlying Write fails is auto-removed from the fan-out.
func TestWriteErrorAutoRemovesClient(t *testing.T) {
	mw := New()
	bad := errWriter{}
	mw.Add(bad)

	mw.Write([]byte("data"))

	waitFor(t, "errored client to be auto-removed", time.Second, func() bool {
		return clientCount(mw) == 0
	})

	// Remove of an already-removed client must be a no-op (idempotent, no panic).
	mw.Remove(bad)
	mw.Close()
}
