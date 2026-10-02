package linkselector

import (
	"bytes"
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestWebPostHeaderTimeoutReturnsFixed502(t *testing.T) {
	handler, err := NewHandler("http://web.example", "http://landing.example", slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatalf("NewHandler: %v", err)
	}
	selectorHandler, ok := handler.(*selector)
	if !ok {
		t.Fatalf("NewHandler returned %T, want *selector", handler)
	}
	readStarted := make(chan struct{})
	selectorHandler.client.Transport = roundTripperFunc(func(request *http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode:    http.StatusOK,
			Header:        http.Header{"Content-Type": {"text/javascript"}, "Content-Security-Policy": {"default-src 'self'"}},
			Body:          &timeoutBody{readStarted: readStarted},
			ContentLength: 100,
			Request:       request,
		}, nil
	})

	request := &http.Request{
		Method:     http.MethodGet,
		URL:        &url.URL{},
		RequestURI: "/main.js",
		Header:     make(http.Header),
		Body:       http.NoBody,
	}
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)

	select {
	case <-readStarted:
	default:
		t.Fatal("selector did not read the body after receiving upstream headers")
	}
	if response.Code != http.StatusBadGateway {
		t.Fatalf("timed-out Web response status = %d, want fixed 502", response.Code)
	}
	if response.Body.String() != landingUnavailable {
		t.Errorf("timed-out Web response body = %q, want fixed unavailable page", response.Body.String())
	}
	wantHeaders := map[string]string{
		"Cache-Control":           "no-store",
		"Referrer-Policy":         "no-referrer",
		"X-Content-Type-Options":  "nosniff",
		"Content-Type":            "text/html; charset=utf-8",
		"Content-Security-Policy": "default-src 'none'; style-src 'unsafe-inline'; base-uri 'none'; form-action 'none'; frame-ancestors 'none'",
	}
	for name, want := range wantHeaders {
		if got := response.Header().Get(name); got != want {
			t.Errorf("timed-out Web response %s = %q, want %q", name, got, want)
		}
	}
	if response.Header().Get("Content-Type") == "text/javascript" || response.Header().Get("Content-Length") == "100" {
		t.Errorf("timed-out Web headers were forwarded: %v", response.Header())
	}
}

type roundTripperFunc func(*http.Request) (*http.Response, error)

func (f roundTripperFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return f(request)
}

type timeoutBody struct {
	readStarted chan struct{}
}

func (b *timeoutBody) Read([]byte) (int, error) {
	close(b.readStarted)
	timeout := time.NewTimer(10 * time.Millisecond)
	defer timeout.Stop()
	<-timeout.C
	return 0, context.DeadlineExceeded
}

func (*timeoutBody) Close() error { return nil }

func TestWebStagedBodyBudgetRejectsWhenSlowReadersHoldCapacity(t *testing.T) {
	handler, err := NewHandler("http://web.example", "http://landing.example", slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatalf("NewHandler: %v", err)
	}
	var bodyReads atomic.Int32
	selectorHandler, ok := handler.(*selector)
	if !ok {
		t.Fatalf("NewHandler returned %T, want *selector", handler)
	}
	selectorHandler.client.Transport = roundTripperFunc(func(request *http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode:    http.StatusOK,
			Header:        http.Header{"Content-Type": {"text/javascript"}},
			Body:          &countingBody{reader: bytes.NewReader([]byte("asset")), reads: &bodyReads},
			ContentLength: maxWebBodyBytes,
			Request:       request,
		}, nil
	})

	writeStarted := make(chan struct{}, 2)
	releaseWrites := make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(releaseWrites) }) }
	defer release()
	request := func() *http.Request {
		return &http.Request{
			Method:     http.MethodGet,
			URL:        &url.URL{},
			RequestURI: "/main.js",
			Header:     make(http.Header),
			Body:       http.NoBody,
		}
	}
	finished := make(chan struct{}, 2)
	for range 2 {
		go func() {
			handler.ServeHTTP(&gatedResponseWriter{
				ResponseRecorder: httptest.NewRecorder(),
				writeStarted:     writeStarted,
				release:          releaseWrites,
			}, request())
			finished <- struct{}{}
		}()
	}
	for range 2 {
		select {
		case <-writeStarted:
		case <-time.After(time.Second):
			t.Fatal("Web response did not reach its blocked downstream write")
		}
	}

	readsBeforeExtraRequest := bodyReads.Load()
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request())
	if response.Code != http.StatusBadGateway {
		t.Errorf("request over staged-body budget status = %d, want fixed 502", response.Code)
	}
	if response.Body.String() != landingUnavailable {
		t.Errorf("request over staged-body budget body = %q, want fixed unavailable page", response.Body.String())
	}
	if got := bodyReads.Load(); got != readsBeforeExtraRequest {
		t.Errorf("upstream body reads for rejected request = %d, want no additional reads", got-readsBeforeExtraRequest)
	}

	release()
	for range 2 {
		select {
		case <-finished:
		case <-time.After(time.Second):
			t.Fatal("slow-reader request did not finish after its write was released")
		}
	}
}

type countingBody struct {
	reader *bytes.Reader
	reads  *atomic.Int32
}

func (b *countingBody) Read(p []byte) (int, error) {
	b.reads.Add(1)
	return b.reader.Read(p)
}

func (*countingBody) Close() error { return nil }

type gatedResponseWriter struct {
	*httptest.ResponseRecorder

	writeStarted chan<- struct{}
	release      <-chan struct{}
}

func (w *gatedResponseWriter) Write(p []byte) (int, error) {
	w.writeStarted <- struct{}{}
	<-w.release
	return w.ResponseRecorder.Write(p)
}
