// Command linkprobe performs the fixed synthetic HTTP checks used by the
// isolated link-edge containers' Docker health checks.
package main

import (
	"context"
	"fmt"
	"io"
	"mime"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/teagramhq/teagram-server/internal/linklanding"
)

const (
	selectorOrigin = "http://127.0.0.1:8081"
	landingOrigin  = "http://127.0.0.1:8082"
	probePath      = "/syntheticname"
	maxProbeBody   = 4096
	contentPolicy  = "default-src 'none'; style-src 'unsafe-inline'; base-uri 'none'; form-action 'none'; frame-ancestors 'none'"
)

func main() {
	if len(os.Args) != 2 {
		fmt.Fprintln(os.Stderr, "usage: linkprobe landing|selector")
		os.Exit(2)
	}

	client := &http.Client{
		Transport: &http.Transport{Proxy: nil},
		Timeout:   3 * time.Second,
	}
	var healthy bool
	switch os.Args[1] {
	case "landing":
		healthy = checkLanding(client, landingOrigin+probePath)
	case "selector":
		healthy = checkSelector(client, selectorOrigin)
	default:
		fmt.Fprintln(os.Stderr, "usage: linkprobe landing|selector")
		os.Exit(2)
	}
	if !healthy {
		fmt.Fprintln(os.Stderr, "link edge health check failed")
		os.Exit(1)
	}
}

func checkSelector(client *http.Client, origin string) bool {
	return checkLanding(client, origin+probePath) && checkWebRoot(client, origin+"/")
}

func checkLanding(client *http.Client, target string) bool {
	response, ok := get(client, target)
	if !ok {
		return false
	}

	body, readErr := io.ReadAll(io.LimitReader(response.Body, maxProbeBody+1))
	closeErr := response.Body.Close()
	expectedBody, supported := linklanding.StaticBodyForStatus(http.StatusOK)
	return readErr == nil && closeErr == nil && response.StatusCode == http.StatusOK &&
		supported &&
		len(body) <= maxProbeBody && string(body) == expectedBody &&
		response.Header.Get("Content-Type") == "text/html; charset=utf-8" &&
		response.Header.Get("Content-Length") == strconv.Itoa(len(expectedBody)) &&
		response.Header.Get("Cache-Control") == "no-store" &&
		response.Header.Get("Referrer-Policy") == "no-referrer" &&
		response.Header.Get("X-Content-Type-Options") == "nosniff" &&
		response.Header.Get("Content-Security-Policy") == contentPolicy
}

func checkWebRoot(client *http.Client, target string) bool {
	response, ok := get(client, target)
	if !ok {
		return false
	}
	closeErr := response.Body.Close()
	contentType, _, parseErr := mime.ParseMediaType(response.Header.Get("Content-Type"))
	return closeErr == nil && parseErr == nil && response.StatusCode == http.StatusOK && contentType == "text/html"
}

func get(client *http.Client, target string) (*http.Response, bool) {
	request, err := http.NewRequestWithContext(context.Background(), http.MethodGet, target, nil)
	if err != nil {
		return nil, false
	}
	response, err := client.Do(request)
	if err != nil {
		return nil, false
	}
	return response, true
}
