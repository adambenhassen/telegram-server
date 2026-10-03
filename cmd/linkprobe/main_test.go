package main

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/teagramhq/teagram-server/internal/linklanding"
)

func TestCheckLandingRequiresTheFixedStaticResponse(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		status     int
		body       string
		omitHeader bool
		want       bool
	}{
		{name: "contract", status: http.StatusOK, want: true},
		{name: "bad status", status: http.StatusServiceUnavailable},
		{name: "wrong body", status: http.StatusOK, body: "not the landing page"},
		{name: "missing headers", status: http.StatusOK, omitHeader: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				body, _ := linklanding.StaticBodyForStatus(http.StatusOK)
				if test.body != "" {
					body = test.body
				}
				if !test.omitHeader {
					linklanding.SetSecurityHeaders(w.Header(), body)
				}
				w.WriteHeader(test.status)
				if _, err := fmt.Fprint(w, body); err != nil {
					t.Errorf("write landing body: %v", err)
				}
			}))
			defer server.Close()

			client := &http.Client{}
			if got := checkLanding(client, server.URL+probePath); got != test.want {
				t.Errorf("checkLanding() = %t, want %t", got, test.want)
			}
		})
	}
}

func TestCheckSelectorChecksSyntheticLandingAndWebRoot(t *testing.T) {
	t.Parallel()

	var seenLanding, seenRoot bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case probePath:
			seenLanding = true
			body, _ := linklanding.StaticBodyForStatus(http.StatusOK)
			linklanding.SetSecurityHeaders(w.Header(), body)
			if _, err := fmt.Fprint(w, body); err != nil {
				t.Errorf("write landing body: %v", err)
			}
		case "/":
			seenRoot = true
			w.Header().Set("Content-Type", "text/html; charset=utf-8")
			w.WriteHeader(http.StatusOK)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	if !checkSelector(server.Client(), server.URL) {
		t.Fatal("checkSelector() = false, want true")
	}
	if !seenLanding || !seenRoot {
		t.Fatalf("selector probes: landing=%t root=%t, want both", seenLanding, seenRoot)
	}
}

func TestCheckSelectorFailsWhenWebRootIsUnavailable(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == probePath {
			body, _ := linklanding.StaticBodyForStatus(http.StatusOK)
			linklanding.SetSecurityHeaders(w.Header(), body)
			if _, err := fmt.Fprint(w, body); err != nil {
				t.Errorf("write landing body: %v", err)
			}
			return
		}
		w.Header().Set("Content-Type", "text/html")
		w.WriteHeader(http.StatusBadGateway)
	}))
	defer server.Close()

	if got := checkSelector(server.Client(), server.URL); got {
		t.Fatal("checkSelector() = true, want false")
	}
}
