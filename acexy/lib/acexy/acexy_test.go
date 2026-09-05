package acexy

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// ---- Test harness helpers -------------------------------------------------

// nilWriter is a fast, distinct io.Writer used as a fake streaming client.
type nilWriter struct{}

func (nilWriter) Write(p []byte) (int, error) { return len(p), nil }

var _ io.Writer = nilWriter{}

// newAcexy builds an Acexy with no orchestrator (static backend) and fresh internal state.
func newAcexy(noResponse time.Duration) *Acexy {
	a := &Acexy{
		Scheme:            "http",
		Endpoint:          MPEG_TS_ENDPOINT,
		EmptyTimeout:      10 * time.Second, // do not stall during tests
		EmptyRetryCount:   0,
		BufferSize:        1 << 16,
		ClientBufferSize:  1 << 20,
		NoResponseTimeout: noResponse,
	}
	a.Init()
	return a
}

// pointBackend points the static backend of a at an httptest server (the fake engine).
func pointBackend(a *Acexy, srv *httptest.Server) {
	u, err := url.Parse(srv.URL)
	if err != nil {
		panic(err)
	}
	a.Host = u.Hostname()
	a.Port, _ = strconv.Atoi(u.Port())
}

func aceStreamJSON(playbackURL, commandURL string) string {
	return fmt.Sprintf(`{"response":{"playback_url":%q,"stat_url":"","command_url":%q,"is_live":1},"error":""}`,
		playbackURL, commandURL)
}

// releaseOnCleanup frees a blocked handler at test teardown so its goroutine exits.
func releaseOnCleanup(t *testing.T, ch chan struct{}) {
	t.Cleanup(func() {
		select {
		case <-ch:
		default:
			close(ch)
		}
	})
}

func waitErr(t *testing.T, what string, ch chan error, d time.Duration) error {
	t.Helper()
	select {
	case err := <-ch:
		return err
	case <-time.After(d):
		t.Fatalf("timed out waiting for %s", what)
		return nil
	}
}

func waitOK(t *testing.T, what string, ch chan struct{}, d time.Duration) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(d):
		t.Fatalf("timed out waiting for %s", what)
	}
}

// ---- T5: a hung playback must not block /status nor another stream ---------

func TestPlaybackHangDoesNotBlockStatusOrOtherStreams(t *testing.T) {
	hangRelease := make(chan struct{})

	// Playback A: never sends a response header (the engine hangs).
	hangSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-hangRelease
	}))
	t.Cleanup(hangSrv.Close)
	// Cleanups run LIFO: register the release AFTER the server so it unblocks the
	// handler goroutine BEFORE httptest.Server.Close tries to wait on it.
	releaseOnCleanup(t, hangRelease)

	// Playback B: healthy, sends a bit of data and finishes.
	okSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, "ok-stream-data")
	}))
	t.Cleanup(okSrv.Close)

	var engine *httptest.Server
	engine = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/close" {
			_, _ = io.WriteString(w, `{"response":"ok","error":""}`)
			return
		}
		id := r.URL.Query().Get("id")
		var pb string
		if id == "hang" {
			pb = hangSrv.URL + "/p"
		} else {
			pb = okSrv.URL + "/p"
		}
		_, _ = io.WriteString(w, aceStreamJSON(pb, engine.URL+"/close"))
	}))
	t.Cleanup(engine.Close)

	a := newAcexy(1500 * time.Millisecond)
	pointBackend(a, engine)

	idHang, _ := NewAceID("hang", "")
	idOK, _ := NewAceID("ok", "")

	// Start stream A whose playback hangs.
	strA, err := a.FetchStream(idHang, url.Values{})
	if err != nil {
		t.Fatalf("FetchStream(A) failed: %v", err)
	}
	aStarted := make(chan error, 1)
	go func() { aStarted <- a.StartStream(strA, nilWriter{}) }()

	// Give A a moment to enter its (network, out-of-lock) playback request.
	time.Sleep(50 * time.Millisecond)

	// /ace/status must keep working while A's playback is hung.
	statusOK := make(chan struct{})
	go func() { _, _ = a.GetStatus(nil); close(statusOK) }()
	waitOK(t, "GetStatus(nil) while a playback is hung", statusOK, 300*time.Millisecond)

	// Another stream on the same proxy must start fine while A is hung.
	strB, err := a.FetchStream(idOK, url.Values{})
	if err != nil {
		t.Fatalf("FetchStream(B) failed: %v", err)
	}
	bStarted := make(chan error, 1)
	go func() { bStarted <- a.StartStream(strB, nilWriter{}) }()
	if err := waitErr(t, "StartStream(B)", bStarted, 500*time.Millisecond); err != nil {
		t.Fatalf("StartStream(B) failed while A was hung: %v", err)
	}

	// A's hung playback must abort (ResponseHeaderTimeout) instead of blocking forever.
	if err := waitErr(t, "StartStream(A) to abort hung playback", aStarted, 3*time.Second); err == nil {
		t.Fatal("expected stream A's playback to abort with a timeout error, got success")
	}

	_ = a.StopStream(strB, nilWriter{})
}

// ---- T6: two concurrent fetches of the SAME new stream must hit getstream once.

func TestConcurrentFetchSingleGetstream(t *testing.T) {
	okSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, "data")
	}))
	t.Cleanup(okSrv.Close)

	var getstreamCount int64
	var engine *httptest.Server
	engine = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/close" {
			_, _ = io.WriteString(w, `{"response":"ok","error":""}`)
			return
		}
		atomic.AddInt64(&getstreamCount, 1)
		_, _ = io.WriteString(w, aceStreamJSON(okSrv.URL+"/p", engine.URL+"/close"))
	}))
	t.Cleanup(engine.Close)

	a := newAcexy(time.Second)
	pointBackend(a, engine)

	id, _ := NewAceID("dup", "")
	results := make(chan struct {
		s   *AceStream
		err error
	}, 2)
	var wg sync.WaitGroup
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			s, err := a.FetchStream(id, url.Values{})
			results <- struct {
				s   *AceStream
				err error
			}{s, err}
		}()
	}
	wg.Wait()
	close(results)

	for res := range results {
		if res.err != nil {
			t.Fatalf("FetchStream failed: %v", res.err)
		}
		if res.s.ID != id {
			t.Fatalf("unexpected stream returned")
		}
	}

	if c := atomic.LoadInt64(&getstreamCount); c != 1 {
		t.Fatalf("expected exactly 1 getstream for concurrent same-ID fetches, got %d", c)
	}
}

// ---- T7: the last client must release the stream (CloseStream) exactly once.

func TestLastClientReleasesExactlyOnce(t *testing.T) {
	release := make(chan struct{})

	// Playback stays open (active stream) until released.
	pbSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "video/MP2T")
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, "start")
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
		<-release
	}))
	t.Cleanup(pbSrv.Close)
	// LIFO: unblock the handler before pbSrv.Close waits on it.
	releaseOnCleanup(t, release)

	var closeCount int64
	var engine *httptest.Server
	engine = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/close" {
			atomic.AddInt64(&closeCount, 1)
			_, _ = io.WriteString(w, `{"response":"ok","error":""}`)
			return
		}
		_, _ = io.WriteString(w, aceStreamJSON(pbSrv.URL+"/p", engine.URL+"/close"))
	}))
	t.Cleanup(engine.Close)

	a := newAcexy(time.Second)
	pointBackend(a, engine)

	id, _ := NewAceID("last", "")
	str, err := a.FetchStream(id, url.Values{})
	if err != nil {
		t.Fatalf("FetchStream failed: %v", err)
	}

	// Two distinct clients on the same stream.
	if err := a.StartStream(str, nilWriter{}); err != nil {
		t.Fatalf("StartStream client1 failed: %v", err)
	}
	if err := a.StartStream(str, io.Discard); err != nil {
		t.Fatalf("StartStream client2 failed: %v", err)
	}

	// Both clients stop concurrently; exactly one must trigger the release.
	var wg sync.WaitGroup
	for _, w := range []io.Writer{nilWriter{}, io.Discard} {
		w := w
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = a.StopStream(str, w)
		}()
	}
	wg.Wait()

	if c := atomic.LoadInt64(&closeCount); c != 1 {
		t.Fatalf("expected exactly 1 CloseStream for last-client release, got %d", c)
	}
}

// ---- T8: a mute engine must abort the control request within the timeout ----

func TestMuteEngineAbortsControlRequest(t *testing.T) {
	release := make(chan struct{})

	engine := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-release // never responds
	}))
	t.Cleanup(engine.Close)
	// LIFO: unblock the handler before engine.Close waits on it.
	releaseOnCleanup(t, release)

	a := newAcexy(300 * time.Millisecond)
	pointBackend(a, engine)

	id, _ := NewAceID("mute", "")
	start := time.Now()
	_, err := a.FetchStream(id, url.Values{})
	elapsed := time.Since(start)
	if err == nil {
		t.Fatal("expected FetchStream to abort against a mute engine, got success")
	}
	if elapsed > 2*time.Second {
		t.Fatalf("FetchStream took too long to abort mute engine: %v", elapsed)
	}
}

// ---- T9: control requests must use Connection: close (fresh, not a shared pool)

func TestControlRequestUsesConnectionClose(t *testing.T) {
	okSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, "data")
	}))
	t.Cleanup(okSrv.Close)

	var sawConnClose int64
	var engine *httptest.Server
	engine = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/close" {
			_, _ = io.WriteString(w, `{"response":"ok","error":""}`)
			return
		}
		if r.Header.Get("Connection") == "close" {
			atomic.AddInt64(&sawConnClose, 1)
		}
		_, _ = io.WriteString(w, aceStreamJSON(okSrv.URL+"/p", engine.URL+"/close"))
	}))
	t.Cleanup(engine.Close)

	a := newAcexy(time.Second)
	pointBackend(a, engine)

	id, _ := NewAceID("conn", "")
	if _, err := a.FetchStream(id, url.Values{}); err != nil {
		t.Fatalf("FetchStream failed: %v", err)
	}
	if c := atomic.LoadInt64(&sawConnClose); c != 1 {
		t.Fatalf("expected the control (getstream) request to carry Connection: close, got %d", c)
	}
}
