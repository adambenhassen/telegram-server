package linkselector

import (
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
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
