// Acexy - Copyright (C) 2024 - Javinator9889 <dev at javinator9889 dot com>
// This program comes with ABSOLUTELY NO WARRANTY; for details type `show w'.
// This is free software, and you are welcome to redistribute it
// under certain conditions; type `show c' for details.
package acexy

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"javinator9889/acexy/lib/orchestrator"
	"javinator9889/acexy/lib/pmw"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"sync"
	"time"

	"github.com/google/uuid"
)

// As of how the middleware is defined, we tell Go the structure that should match the HTTP
// response for AceStream: https://docs.acestream.net/developers/start-playback/#using-middleware.
// We are interested in the "playback_url" and the "command_url" fields: The first one
// references the stream, and the second one tells the stream to finish.
type AceStreamResponse struct {
	PlaybackURL       string `json:"playback_url"`
	StatURL           string `json:"stat_url"`
	CommandURL        string `json:"command_url"`
	Infohash          string `json:"infohash"`
	PlaybackSessionID string `json:"playback_session_id"`
	IsLive            int    `json:"is_live"`
	IsEncrypted       int    `json:"is_encrypted"`
	ClientSessionID   int    `json:"client_session_id"`
}

type AceStreamMiddleware struct {
	Response AceStreamResponse `json:"response"`
	Error    string            `json:"error"`
}

type AceStreamCommand struct {
	Response string `json:"response"`
	Error    string `json:"error"`
}

type AcexyStatus struct {
	Clients *uint  `json:"clients,omitempty"`
	Streams *uint  `json:"streams,omitempty"`
	ID      *AceID `json:"stream_id,omitempty"`
	StatURL string `json:"stat_url,omitempty"`
}

// The stream information is stored in a structure referencing the `AceStreamResponse`
// plus some extra information to determine whether we should keep the stream alive or not.
type AceStream struct {
	PlaybackURL string
	StatURL     string
	CommandURL  string
	ID          AceID
}

// ongoingStream holds all per-stream state. Its fields are protected by the global
// Acexy.mutex. Network I/O is NEVER performed while holding that mutex; the fetchDone /
// bootStarting gates make sure a slow engine only blocks the stream it belongs to, not the
// whole proxy.
type ongoingStream struct {
	clients  uint
	done     chan struct{}
	player   *http.Response
	stream   *AceStream
	copier   *Copier
	writers  *pmw.PMultiWriter
	instance *orchestrator.AceStreamInstance // nil if orchestration is disabled

	// fetchDone is non-nil (open channel) only while this stream is being enqueued by a
	// FetchStream goroutine. Concurrent callers wait on it and then re-check the map.
	// fetchDone is closed and set to nil once the fetch completes (success or failure).
	fetchDone chan struct{}

	// playback boot single-flight: only one StartStream goroutine issues the a.middleware.Get
	// for a fresh stream; the rest wait on bootDone.
	bootStarting bool
	bootDone     chan struct{}
	bootErr      error
}

// Structure referencing the AceStream Proxy - this is, ourselves
type Acexy struct {
	Scheme            string        // The scheme to be used when connecting to the AceStream middleware
	Host              string        // The host to be used when connecting to the AceStream middleware
	Port              int           // The port to be used when connecting to the AceStream middleware
	Endpoint          AcexyEndpoint // The endpoint to be used when connecting to the AceStream middleware
	EmptyTimeout      time.Duration // Timeout after which, if no data is written, the stream is closed
	EmptyRetryCount   int           // Number of reconnect attempts when a stream stalls (0 = no retry)
	BufferSize        int           // The buffer size to use when copying the data
	ClientBufferSize  int           // Max bytes buffered per client in the fan-out (lossy upper bound)
	NoResponseTimeout time.Duration // Timeout to wait for a response from the AceStream middleware

	Orchestrator *orchestrator.Orchestrator // nil if dynamic orchestration is disabled

	// Information about ongoing streams
	streams    map[AceID]*ongoingStream
	mutex      *sync.Mutex
	middleware *http.Client
}

type AcexyEndpoint string

// The AceStream API available endpoints
const (
	M3U8_ENDPOINT    AcexyEndpoint = "/ace/manifest.m3u8"
	MPEG_TS_ENDPOINT AcexyEndpoint = "/ace/getstream"
)

// Initializes the Acexy structure
func (a *Acexy) Init() {
	a.streams = make(map[AceID]*ongoingStream)
	a.mutex = &sync.Mutex{}
	// The transport to be used when connecting to the AceStream middleware. We have to tweak it
	// a little bit to avoid compression and to limit the number of connections per host. Otherwise,
	// the AceStream Middleware won't work.
	a.middleware = &http.Client{
		Transport: &http.Transport{
			DisableCompression:    true,
			MaxIdleConns:          10,
			MaxConnsPerHost:       10,
			IdleConnTimeout:       30 * time.Second,
			ResponseHeaderTimeout: a.NoResponseTimeout,
			ExpectContinueTimeout: 1 * time.Second,
		},
	}
}

// Starts a new stream. The stream is enqueued in the AceStream backend, returning a playback
// URL to reproduce it and a command URL to finish it. If the stream is already enqueued,
// the playback URL is returned. A number of clients can be reproducing the same stream at
// the same time through the middleware. When the last client finishes, the stream is removed.
// The stream is identified by the “id“ identifier. Optionally, takes extra parameters to
// customize the stream.
//
// The network I/O (instance acquisition + getstream against the engine) happens OUTSIDE the
// global mutex. A single-flight "starting" placeholder is registered so a second client for the
// same new stream waits and reuses it instead of enqueuing it twice.
func (a *Acexy) FetchStream(aceId AceID, extraParams url.Values) (*AceStream, error) {
	var os *ongoingStream

	// Fast paths and placeholder reservation happen under a short lock.
	for {
		a.mutex.Lock()
		cur, ok := a.streams[aceId]
		if ok && cur.fetchDone == nil {
			// Fully enqueued entry (live or idle-but-registered): reuse it.
			stream := cur.stream
			a.mutex.Unlock()
			return stream, nil
		}
		if ok && cur.fetchDone != nil {
			// Another goroutine is fetching this stream. Wait for it, then re-check.
			done := cur.fetchDone
			a.mutex.Unlock()
			<-done
			continue
		}
		// Not present: we become the starter. Reserve a placeholder and fetch outside the lock.
		os = &ongoingStream{
			clients:   0,
			done:      make(chan struct{}),
			writers:   pmw.New(),
			fetchDone: make(chan struct{}),
		}
		os.writers.SetBufferSize(a.ClientBufferSize)
		a.streams[aceId] = os
		a.mutex.Unlock()
		break
	}

	// ---- Network + instance acquisition OUTSIDE the global mutex ----
	var middlewareResp *AceStreamMiddleware
	var instance *orchestrator.AceStreamInstance
	var err error

	if a.Orchestrator != nil {
		instance, err = a.acquireInstance()
		if err != nil {
			a.failFetch(aceId, os)
			return nil, err
		}
		middlewareResp, err = GetStreamFromInstance(instance, a, aceId, extraParams)
		if err != nil {
			a.Orchestrator.ReleaseInstance(instance)
			a.failFetch(aceId, os)
			slog.Error("Error getting stream from instance", "error", err)
			return nil, err
		}
	} else {
		middlewareResp, err = GetStream(a, aceId, extraParams)
		if err != nil {
			a.failFetch(aceId, os)
			slog.Error("Error getting stream middleware", "error", err)
			return nil, err
		}
	}

	slog.Debug("Middleware Information", "id", aceId, "middleware", middlewareResp)
	stream := &AceStream{
		PlaybackURL: middlewareResp.Response.PlaybackURL,
		StatURL:     middlewareResp.Response.StatURL,
		CommandURL:  middlewareResp.Response.CommandURL,
		ID:          aceId,
	}

	// Finalize under a short lock: attach the result and wake any waiters.
	a.mutex.Lock()
	os.stream = stream
	os.instance = instance
	done := os.fetchDone
	os.fetchDone = nil
	if done != nil {
		close(done)
	}
	a.mutex.Unlock()
	return stream, nil
}

// failFetch removes the "starting" placeholder registered by this FetchStream and wakes any
// waiters. Waiters that wake and find the entry gone become starters themselves and retry.
func (a *Acexy) failFetch(aceId AceID, os *ongoingStream) {
	a.mutex.Lock()
	if cur, ok := a.streams[aceId]; ok && cur == os {
		delete(a.streams, aceId)
	}
	done := os.fetchDone
	os.fetchDone = nil
	if done != nil {
		close(done)
	}
	a.mutex.Unlock()
}

// acquireInstance selects an instance with capacity and reserves it atomically, scaling up or
// waiting for a recycled pool when none is available. The caller (FetchStream / migrateStream)
// must release the reservation via ReleaseInstance if the subsequent request fails.
// Must NOT be called while holding Acexy.mutex.
func (a *Acexy) acquireInstance() (*orchestrator.AceStreamInstance, error) {
	if instance := a.Orchestrator.ReserveInstance(); instance != nil {
		return instance, nil
	}

	if a.Orchestrator.IsRecycling() {
		slog.Info("Pool is recycling, waiting for a healthy instance")
		instance := a.Orchestrator.WaitForInstance(2 * time.Minute)
		if instance == nil {
			return nil, errors.New("timed out waiting for instance after pool recycle")
		}
		a.Orchestrator.ReserveExisting(instance)
		return instance, nil
	}

	if a.Orchestrator.TotalInstances() < a.Orchestrator.MaxReplicas {
		instance, err := a.Orchestrator.ScaleUp()
		if err != nil {
			slog.Error("Failed to scale up", "error", err)
			return nil, fmt.Errorf("failed to scale up new instance: %w", err)
		}
		a.Orchestrator.ReserveExisting(instance)
		return instance, nil
	}

	return nil, errors.New("max replicas reached, no instance available")
}

// freshPlaybackConn closes idle keep-alive connections from a.middleware's pool before opening a
// playback connection. The AceStream engine will NOT serve the stream over a reused keep-alive
// connection (verified in live testing: the first playback of a stream works, but re-resuming it
// after a stop stalls because a.middleware reuses an idle connection from the previous session).
// Only idle connections are affected; active streams keep their dedicated connection.
func (a *Acexy) freshPlaybackConn() {
	if tr, ok := a.middleware.Transport.(*http.Transport); ok {
		tr.CloseIdleConnections()
	}
}

func (a *Acexy) StartStream(stream *AceStream, out io.Writer) error {
	var os *ongoingStream

	a.mutex.Lock()
	var ok bool
	os, ok = a.streams[stream.ID]
	if !ok {
		a.mutex.Unlock()
		slog.Debug("Stream not found", "stream", stream.ID)
		return fmt.Errorf(`stream "%s" not found`, stream.ID)
	}

	// Register the client and count it under the lock.
	os.writers.Add(out)
	os.clients++

	// If the stream is already playing, reuse it (the new client is served by the fan-out).
	if os.player != nil {
		a.logClients("Reusing existing stream", stream.ID, os, true)
		a.mutex.Unlock()
		return nil
	}

	// Playback not started yet: single-flight the boot. Only one StartStream issues the Get.
	if os.bootStarting {
		done := os.bootDone
		a.mutex.Unlock()
		<-done
		a.mutex.Lock()
		os2, ok2 := a.streams[stream.ID]
		if !ok2 {
			a.mutex.Unlock()
			slog.Debug("Stream released while waiting for playback", "stream", stream.ID)
			return fmt.Errorf(`stream "%s" not found`, stream.ID)
		}
		if os2.player != nil {
			a.logClients("Reusing existing stream", stream.ID, os2, false)
			a.mutex.Unlock()
			return nil
		}
		bootErr := os2.bootErr
		a.mutex.Unlock()
		if bootErr != nil {
			a.rollbackClient(stream.ID, out)
			slog.Error("Failed to forward stream", "stream", stream.ID, "error", bootErr)
			return bootErr
		}
		a.logClients("Reusing existing stream", stream.ID, os2, false)
		return nil
	}

	// We are the boot owner. Mark boot in progress and log the new stream start.
	os.bootStarting = true
	os.bootDone = make(chan struct{})
	a.logClients("Started new stream", stream.ID, os, true)
	a.mutex.Unlock()

	// Network OUTSIDE the global mutex: a slow/hung middleware only stalls this stream, not the
	// whole proxy. a.middleware has ResponseHeaderTimeout = NoResponseTimeout, so this is bounded.
	// Force a fresh connection: the engine does not serve over a reused keep-alive connection.
	a.freshPlaybackConn()
	resp, err := a.middleware.Get(stream.PlaybackURL)
	if err != nil {
		slog.Error("Failed to forward stream", "error", err)
		a.mutex.Lock()
		os.bootErr = err
		os.bootStarting = false
		close(os.bootDone)
		a.mutex.Unlock()
		a.rollbackClient(stream.ID, out)
		return err
	}

	// Success: wire the copier and the stream loop under a short lock, then wake the waiters.
	a.mutex.Lock()
	if cur, ok := a.streams[stream.ID]; !ok || cur != os {
		// The stream was released (all clients left) while we were fetching the playback.
		a.mutex.Unlock()
		_ = resp.Body.Close()
		a.rollbackClient(stream.ID, out)
		return fmt.Errorf(`stream "%s" was released during start`, stream.ID)
	}
	idType, id := stream.ID.ID()
	os.copier = &Copier{
		Destination:  os.writers,
		Source:       resp.Body,
		EmptyTimeout: a.EmptyTimeout,
		BufferSize:   a.BufferSize,
		StreamID:     string(idType) + ":" + id,
	}
	go a.runStreamLoop(os, stream)
	os.player = resp
	os.bootStarting = false
	close(os.bootDone)
	a.mutex.Unlock()
	return nil
}

// logClients logs stream lifecycle lines with per-stream and total client counts.
func (a *Acexy) logClients(message string, id AceID, os *ongoingStream, withTotal bool) {
	var totalClients uint
	if withTotal {
		for _, s := range a.streams {
			totalClients += s.clients
		}
	}
	if os.instance != nil {
		if withTotal {
			slog.Info(message, "id", id, "stream_clients", os.clients,
				"total_clients", totalClients, "instance", os.instance.Name)
		} else {
			slog.Info(message, "id", id, "stream_clients", os.clients,
				"instance", os.instance.Name)
		}
		return
	}
	if withTotal {
		slog.Info(message, "id", id, "stream_clients", os.clients, "total_clients", totalClients)
		return
	}
	slog.Info(message, "id", id, "stream_clients", os.clients)
}

// rollbackClient removes a client that was registered in StartStream but whose stream failed to
// boot. If it was the last client, the stream is released (network I/O performed outside the
// lock). Caller must NOT hold Acexy.mutex.
func (a *Acexy) rollbackClient(id AceID, out io.Writer) {
	a.mutex.Lock()
	os, ok := a.streams[id]
	if !ok {
		a.mutex.Unlock()
		return
	}
	os.writers.Remove(out)
	if os.clients > 0 {
		os.clients--
	}
	if os.clients == 0 {
		delete(a.streams, id)
		a.mutex.Unlock()
		a.releaseClosedStream(os)
		return
	}
	a.mutex.Unlock()
}

// runStreamLoop manages the copy lifecycle of a stream.
// It is the single goroutine responsible for: running the Copier, handling stalls,
// retrying, and closing the done channel when the stream ends.
func (a *Acexy) runStreamLoop(os *ongoingStream, stream *AceStream) {
	retries := a.EmptyRetryCount
	for {
		err := os.copier.Copy()

		if !errors.Is(err, ErrStreamStalled) {
			a.logCopyError(err, stream.ID)
			break
		}

		if retries == 0 {
			slog.Warn("Stream stalled, no retries left", "stream", stream.ID)
			break
		}
		retries--

		a.notifyStall(os, stream.ID, retries)
		if err := a.handleStall(os, stream); err != nil {
			slog.Error("Failed to recover stalled stream", "stream", stream.ID, "error", err)
			break
		}
		a.notifyRecovered(os)
	}

	a.closeStreamDone(os, stream.ID)
}

// logCopyError logs a copy error according to its type.
// Closed connection errors are logged at debug level; nil means a normal end and is ignored.
func (a *Acexy) logCopyError(err error, id AceID) {
	if err == nil {
		return
	}
	if errors.Is(err, net.ErrClosed) {
		slog.Debug("Connection closed", "stream", id)
	} else {
		slog.Debug("Failed to copy response body", "stream", id, "error", err)
	}
}

// notifyStall notifies the orchestrator that the stream has stalled and logs the reconnect attempt.
// Only notifies when the orchestrator is active, distinguishing stalls from invalid IDs (see MarkStreamStalled).
func (a *Acexy) notifyStall(os *ongoingStream, id AceID, attemptsLeft int) {
	if a.Orchestrator != nil && os.instance != nil {
		a.Orchestrator.MarkStreamStalled(os.instance)
		slog.Warn("Stream stalled, reconnecting",
			"stream", id,
			"instance", os.instance.Name,
			"attemptsLeft", attemptsLeft,
		)
	} else {
		slog.Warn("Stream stalled, reconnecting", "stream", id, "attemptsLeft", attemptsLeft)
	}
}

// notifyRecovered notifies the orchestrator that the stream has successfully reconnected.
func (a *Acexy) notifyRecovered(os *ongoingStream) {
	if a.Orchestrator != nil && os.instance != nil {
		a.Orchestrator.ResetStreamFailures(os.instance)
	}
}

// handleStall decides how to recover a stalled stream:
// if the instance is Unhealthy it migrates to a healthy one; otherwise it reconnects to the same instance.
func (a *Acexy) handleStall(os *ongoingStream, stream *AceStream) error {
	if a.Orchestrator != nil && os.instance != nil && os.instance.Health == orchestrator.Unhealthy {
		return a.migrateStream(os, stream)
	}
	return a.reconnectStream(os, stream)
}

// migrateStream moves a stream from an unhealthy instance to a healthy one.
// It reserves the target atomically and releases the old instance's reservation.
func (a *Acexy) migrateStream(os *ongoingStream, stream *AceStream) error {
	slog.Warn("Instance unhealthy, migrating stream",
		"stream", stream.ID,
		"oldInstance", os.instance.Name,
	)
	newInstance, err := a.acquireInstance()
	if err != nil {
		return fmt.Errorf("no healthy instance available for migration: %w", err)
	}

	// acquireInstance already reserved the new instance (+1); release the old one's
	// reservation so the accounting moves cleanly across instances.
	if os.instance != nil {
		a.Orchestrator.ReleaseInstance(os.instance)
	}
	os.instance = newInstance

	slog.Info("Stream migrated", "stream", stream.ID, "newInstance", newInstance.Name)
	return a.reconnectStream(os, stream)
}

// reconnectStream opens a new connection to the PlaybackURL and updates the Copier and player.
func (a *Acexy) reconnectStream(os *ongoingStream, stream *AceStream) error {
	if os.instance != nil {
		slog.Info("Reconnecting stream", "stream", stream.ID, "instance", os.instance.Name)
	}
	// Force a fresh connection on reconnect for the same reason as StartStream.
	a.freshPlaybackConn()
	newResp, err := a.middleware.Get(stream.PlaybackURL)
	if err != nil {
		return err
	}

	os.copier.Source = newResp.Body
	a.mutex.Lock()
	if os.player != nil {
		_ = os.player.Body.Close()
	}
	os.player = newResp
	a.mutex.Unlock()
	return nil
}

// closeStreamDone closes the stream's done channel if it has not been closed already.
func (a *Acexy) closeStreamDone(os *ongoingStream, id AceID) {
	if os.instance != nil {
		slog.Debug("Copy done", "stream", id, "instance", os.instance.Name)
	} else {
		slog.Debug("Copy done", "stream", id)
	}
	a.closeDoneOnce(os)
	if os.instance != nil {
		slog.Info("Stream closed", "stream", id, "instance", os.instance.Name)
	} else {
		slog.Info("Stream closed", "stream", id)
	}
}

// closeDoneOnce closes the stream's done channel exactly once (idempotent, safe under the lock).
// Caller must hold Acexy.mutex.
func (a *Acexy) closeDoneOnce(os *ongoingStream) {
	if os == nil {
		return
	}
	select {
	case <-os.done:
	default:
		close(os.done)
	}
}

// Finishes a stream. The stream is removed from the AceStream backend. If the stream is not
// enqueued, an error is returned. If the stream has clients reproducing it, the stream is not
// removed. The stream is identified by the “id“ identifier.
func (a *Acexy) StopStream(stream *AceStream, out io.Writer) error {
	a.mutex.Lock()

	os, ok := a.streams[stream.ID]
	if !ok {
		a.mutex.Unlock()
		slog.Debug("Stream not found", "stream", stream.ID)
		return fmt.Errorf(`stream "%s" not found`, stream.ID)
	}

	// Remove the writer from the list of writers
	os.writers.Remove(out)

	// Unregister the client
	if os.clients > 0 {
		os.clients--
		slog.Info("Client stopped", "stream", stream.ID, "clients", os.clients)
	} else {
		slog.Warn("Stream has no clients", "stream", stream.ID)
	}

	// If this was the last client, detach the stream from the map under the lock and release it
	// (network I/O) OUTSIDE the lock. Deleting under the lock guarantees only one goroutine can
	// win the release, so CloseStream is issued exactly once.
	if os.clients == 0 {
		delete(a.streams, stream.ID)
		a.mutex.Unlock()
		a.releaseClosedStream(os)
		slog.Info("Stream done", "stream", stream.ID)
		return nil
	}

	a.mutex.Unlock()
	return nil
}

// releaseClosedStream tears down a stream that has already been detached from the map (by the
// caller under the lock). All network I/O (CloseStream to the engine, player body close) and the
// done-channel close happen here, OUTSIDE the global mutex.
func (a *Acexy) releaseClosedStream(os *ongoingStream) {
	if os == nil {
		return
	}

	if os.instance != nil {
		a.Orchestrator.ReleaseInstance(os.instance)
		slog.Debug("Instance stream count after release", "instance", os.instance.Name)
	}

	if os.stream != nil {
		slog.Debug("Stopping stream", "stream", os.stream.ID)
		if err := CloseStream(os.stream, a.NoResponseTimeout); err != nil {
			slog.Debug("Error closing stream", "error", err)
		}
	}

	if os.player != nil {
		slog.Debug("Closing player", "stream", os.stream.ID)
		_ = os.player.Body.Close()
	}

	a.mutex.Lock()
	a.closeDoneOnce(os)
	a.mutex.Unlock()
}

// Waits for the stream to finish. The stream is identified by the “id“ identifier. If the stream
// is not enqueued, nil is returned. The function returns a channel that will be closed when the
// stream finishes.
func (a *Acexy) WaitStream(stream *AceStream) <-chan struct{} {
	a.mutex.Lock()
	defer a.mutex.Unlock()

	// Get the ongoing stream
	ongoingStream, ok := a.streams[stream.ID]
	if !ok {
		return nil
	}

	return ongoingStream.done
}

// newControlClient returns an http.Client that must be used for CONTROL requests against the
// AceStream engine (getstream / getstreamfrominstance / close). The engine does NOT tolerate
// reusing keep-alive connections from a shared pool between a control request and the playback
// that follows it, so each call builds a fresh transport (no shared pool). A Connection: close
// header is set by the callers. The timeout bounds how long a hung engine can hold a caller.
func newControlClient(timeout time.Duration) *http.Client {
	transport := &http.Transport{
		DisableCompression: true,
	}
	client := &http.Client{
		Transport: transport,
		Timeout:   timeout,
	}
	return client
}

// Performs a request to the AceStream backend to start a new stream. It uses the Acexy
// structure to get the host and port of the AceStream backend. The stream is identified
// by the “id“ identifier. Optionally, takes extra parameters to customize the stream.
// Returns the response from the AceStream backend. If the request fails, an error is returned.
// If the `AceStreamMiddleware:error` field is not empty, an error is returned.
func GetStream(a *Acexy, aceId AceID, extraParams url.Values) (*AceStreamMiddleware, error) {
	slog.Debug("Getting stream", "id", aceId, "extraParams", extraParams)
	slog.Debug("Acexy Information", "scheme", a.Scheme, "host", a.Host, "port", a.Port)
	req, err := http.NewRequest("GET", a.Scheme+"://"+a.Host+":"+strconv.Itoa(a.Port)+string(a.Endpoint), nil)
	if err != nil {
		return nil, err
	}

	// Add the query parameters. We use a unique PID to identify the client accessing the stream.
	// This prevents errors when multiple streams are accessed at the same time. Because of
	// using the UUID package, we can be sure that the PID is unique.
	pid := uuid.NewString()
	slog.Debug("Temporary PID", "pid", pid, "stream", aceId)
	if extraParams == nil {
		extraParams = req.URL.Query()
	}
	idType, id := aceId.ID()
	extraParams.Set(string(idType), id)
	extraParams.Set("format", "json")
	extraParams.Set("pid", pid)
	// and set the headers
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Connection", "close")
	req.URL.RawQuery = extraParams.Encode()

	slog.Debug("Request URL", "url", req.URL.String())

	// Control request: fresh per-call connection, never a shared pool.
	client := newControlClient(a.NoResponseTimeout)
	defer client.CloseIdleConnections()
	res, err := client.Do(req)
	if err != nil {
		slog.Debug("Error getting stream", "error", err)
		return nil, err
	}
	slog.Debug("Stream response", "statusCode", res.StatusCode, "headers", res.Header, "res", res)
	defer res.Body.Close()

	// Read the response into the body
	body, err := io.ReadAll(res.Body)
	if err != nil {
		slog.Debug("Error reading stream response", "error", err)
		return nil, err
	}

	slog.Debug("Stream response", "response", string(body))
	var response AceStreamMiddleware
	if err := json.Unmarshal(body, &response); err != nil {
		slog.Debug("Error unmarshalling stream response", "error", err)
		return nil, err
	}

	if response.Error != "" {
		slog.Debug("Error in stream response", "error", response.Error)
		return nil, errors.New(response.Error)
	}
	return &response, nil
}

// Closes the stream by performing a request to the AceStream backend. The `stream` parameter
// contains the command URL to send data to the middleware. As of the documentation, it is needed
// to add a "method=stop" query parameter to finish the stream.
//
// controlTimeout bounds the request so a hung engine aborts cleanly instead of blocking forever.
func CloseStream(stream *AceStream, controlTimeout time.Duration) error {
	req, err := http.NewRequest("GET", stream.CommandURL, nil)
	if err != nil {
		return err
	}

	q := req.URL.Query()
	q.Add("method", "stop")
	req.URL.RawQuery = q.Encode()
	req.Header.Set("Connection", "close")

	// Control request: fresh per-call connection, never a shared pool.
	client := newControlClient(controlTimeout)
	defer client.CloseIdleConnections()
	res, err := client.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()

	// Read the response into the body
	body, err := io.ReadAll(res.Body)
	if err != nil {
		slog.Debug("Error reading stream response", "error", err)
		return err
	}

	var response AceStreamCommand
	if err := json.Unmarshal(body, &response); err != nil {
		slog.Debug("Error unmarshalling stream response", "error", err)
		return err
	}

	if response.Error != "" {
		slog.Debug("Error in stream response", "error", response.Error)
		return errors.New(response.Error)
	}
	return nil
}

// Gets the status of a stream. If the `id` parameter is nil, the global status is returned.
// If the stream is not enqueued, an error is returned. The stream is identified by the “id“
// identifier.
func (a *Acexy) GetStatus(id *AceID) (AcexyStatus, error) {
	a.mutex.Lock()
	defer a.mutex.Unlock()

	// Return the global status if no ID is given
	if id == nil {
		streams := uint(len(a.streams))
		return AcexyStatus{Streams: &streams}, nil
	}

	// Check if the stream is already enqueued. An entry whose stream is still nil is a
	// "fetch in progress" placeholder (not yet enqueued), so it is reported as not found.
	if stream, ok := a.streams[*id]; ok && stream.stream != nil {
		return AcexyStatus{
			Clients: &stream.clients,
			ID:      id,
			StatURL: stream.stream.StatURL,
		}, nil
	}

	return AcexyStatus{}, fmt.Errorf(`stream "%s" not found`, id)
}

// GetStreamFromInstance performs a stream request against a specific instance in the pool.
// It is equivalent to GetStream but uses the instance host/port instead of the static backend.
func GetStreamFromInstance(instance *orchestrator.AceStreamInstance, a *Acexy, aceId AceID, extraParams url.Values) (*AceStreamMiddleware, error) {
	slog.Debug("Getting stream from instance", "id", aceId, "host", instance.Host, "port", instance.Port)

	req, err := http.NewRequest("GET", a.Scheme+"://"+instance.Host+":"+strconv.Itoa(instance.Port)+string(a.Endpoint), nil)
	if err != nil {
		return nil, err
	}

	pid := uuid.NewString()
	slog.Debug("Temporary PID", "pid", pid, "stream", aceId)
	if extraParams == nil {
		extraParams = req.URL.Query()
	}
	idType, id := aceId.ID()
	extraParams.Set(string(idType), id)
	extraParams.Set("format", "json")
	extraParams.Set("pid", pid)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Connection", "close")
	req.URL.RawQuery = extraParams.Encode()

	slog.Debug("Request URL", "url", req.URL.String())

	// Control request: fresh per-call connection, never a shared pool.
	client := newControlClient(a.NoResponseTimeout)
	defer client.CloseIdleConnections()
	res, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()

	body, err := io.ReadAll(res.Body)
	if err != nil {
		return nil, err
	}

	var response AceStreamMiddleware
	if err := json.Unmarshal(body, &response); err != nil {
		return nil, err
	}

	if response.Error != "" {
		return nil, errors.New(response.Error)
	}
	return &response, nil
}

// Creates a timeout channel that will be closed after the given timeout
func SetTimeout(timeout time.Duration) chan struct{} {
	// Create a channel that will be closed after the given timeout
	timeoutChan := make(chan struct{})

	go func() {
		time.Sleep(timeout)
		close(timeoutChan)
	}()

	return timeoutChan
}
