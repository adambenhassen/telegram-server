package blob_test

import (
	"bytes"
	"context"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/adambenhassen/telegram-server/internal/blob"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

// TestStoreConformance is the shared behavioral suite entry point. Every backend
// gets the same byte and range assertions so the remote implementation cannot
// quietly acquire different semantics from the local one.
func TestStoreConformance(t *testing.T) {
	t.Parallel()

	type conformanceBackend struct {
		store blob.Store
		seed  func(string, []byte) error
	}
	factories := map[string]func(*testing.T) conformanceBackend{
		"local": func(t *testing.T) conformanceBackend {
			t.Helper()
			l, dir := newLocal(t)
			return conformanceBackend{
				store: l,
				seed: func(key string, body []byte) error {
					filename := filepath.Join(dir, filepath.FromSlash(key))
					if err := os.MkdirAll(filepath.Dir(filename), 0o700); err != nil {
						return err
					}
					return os.WriteFile(filename, body, 0o600)
				},
			}
		},
		"s3": func(t *testing.T) conformanceBackend {
			t.Helper()
			store := newMinioStore(t)
			return conformanceBackend{
				store: store,
				seed: func(key string, body []byte) error {
					return blob.PutRawObjectForTest(context.Background(), store, key, body)
				},
			}
		},
	}
	for name, makeStore := range factories {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			backend := makeStore(t)
			store := backend.store
			ctx := context.Background()
			key := blob.PartsPrefix + "round-trip"
			want := []byte("0123456789abcdef")

			objects := []struct {
				name string
				key  string
				body []byte
			}{
				{name: "round-trip", key: key, body: want},
				{name: "zero-byte", key: blob.PartsPrefix + "zero-byte"},
			}
			for _, object := range objects {
				n, err := store.Put(ctx, object.key, bytes.NewReader(object.body))
				if err != nil {
					t.Fatalf("put %s: %v", object.name, err)
				}
				if n != int64(len(object.body)) {
					t.Fatalf("put %s returned %d bytes, want %d", object.name, n, len(object.body))
				}
			}
			zero, err := store.ReadAt(ctx, objects[1].key, 0, 1)
			if err != nil {
				t.Fatalf("read zero-byte object: %v", err)
			}
			if len(zero) != 0 {
				t.Fatalf("read zero-byte object = %d bytes, want 0", len(zero))
			}

			tests := []struct {
				name          string
				offset, limit int64
				want          []byte
			}{
				{name: "full", offset: 0, limit: int64(len(want)), want: want},
				{name: "middle", offset: 5, limit: 4, want: []byte("5678")},
				{name: "past end", offset: 14, limit: 20, want: []byte("ef")},
				{name: "at end", offset: int64(len(want)), limit: 0, want: nil},
			}
			for _, tt := range tests {
				t.Run(tt.name, func(t *testing.T) {
					got, err := store.ReadAt(ctx, key, tt.offset, tt.limit)
					if err != nil {
						t.Fatalf("read: %v", err)
					}
					if !bytes.Equal(got, tt.want) {
						t.Fatalf("read %q, want %q", got, tt.want)
					}
				})
			}
			if got, err := store.ReadAt(ctx, blob.PartsPrefix+"missing-zero", 0, 0); err != nil || len(got) != 0 {
				t.Fatalf("zero-length missing read = %q, %v; want empty success", got, err)
			}

			if err := store.Remove(ctx, key); err != nil {
				t.Fatalf("remove: %v", err)
			}
			if err := store.Remove(ctx, key); err != nil {
				t.Fatalf("remove missing: %v", err)
			}
			if _, err := store.ReadAt(ctx, key, 0, 1); !errors.Is(err, blob.ErrNotFound) {
				t.Fatalf("read removed = %v, want ErrNotFound", err)
			}

			for key, body := range map[string]string{
				blob.PartsPrefix + "aaa": "a",
				blob.PartsPrefix + "bbb": "bb",
				blob.Key(4242):           "outside",
			} {
				if _, err := store.Put(ctx, key, strings.NewReader(body)); err != nil {
					t.Fatalf("put %s: %v", key, err)
				}
			}
			if _, err := store.Put(ctx, "scope-file", strings.NewReader("scope")); err != nil {
				t.Fatalf("put scope file: %v", err)
			}
			const invalidKey = blob.PartsPrefix + "foo bar"
			if err := backend.seed(invalidKey, nil); err != nil {
				t.Fatalf("seed %s: %v", invalidKey, err)
			}
			entries := map[string]blob.Entry{}
			if err := store.WalkPrefix(ctx, blob.PartsPrefix, func(entry blob.Entry) error {
				if !strings.HasPrefix(entry.Key, blob.PartsPrefix) {
					t.Fatalf("walk yielded %q outside prefix", entry.Key)
				}
				if !entry.Regular || entry.Dir {
					t.Fatalf("walk yielded non-regular entry %+v", entry)
				}
				entries[entry.Key] = entry
				return nil
			}); err != nil {
				t.Fatalf("walk: %v", err)
			}
			if len(entries) != 4 {
				t.Fatalf("walk yielded %d entries, want 4: %+v", len(entries), entries)
			}
			for key, wantSize := range map[string]int64{
				blob.PartsPrefix + "aaa":       1,
				blob.PartsPrefix + "bbb":       2,
				blob.PartsPrefix + "zero-byte": 0,
				invalidKey:                     0,
			} {
				if entries[key].Size != wantSize {
					t.Fatalf("walk %q size = %d, want %d", key, entries[key].Size, wantSize)
				}
			}
			if err := store.WalkPrefix(ctx, "", func(blob.Entry) error {
				t.Fatal("empty prefix invoked callback")
				return nil
			}); err != nil {
				t.Fatalf("empty prefix: %v", err)
			}
			if err := store.WalkPrefix(ctx, "scope-file", func(blob.Entry) error { return nil }); err == nil {
				t.Fatal("prefix naming a file succeeded")
			}
		})
	}
}

type minioHarness struct {
	once      sync.Once
	endpoint  string
	namespace string
	err       error
}

var sharedMinio minioHarness

func newMinioStore(t *testing.T) *blob.S3 {
	t.Helper()
	endpoint := sharedMinioEndpoint(t)
	prefix := "conformance/" + sharedMinio.namespace + "/" + strings.NewReplacer("/", "-", "_", "-").Replace(t.Name()) + "/"
	store, err := blob.NewS3(blob.S3Config{
		Endpoint:          endpoint,
		Bucket:            "blob-conformance",
		Prefix:            prefix,
		Region:            "us-east-1",
		AccessKeyID:       "minioadmin",
		SecretAccessKey:   "minioadmin",
		AllowInsecureHTTP: true,
		OperationTimeout:  10 * time.Second,
		MaxAttempts:       3,
	})
	if err != nil {
		t.Fatalf("new MinIO store: %v", err)
	}
	return store
}

func sharedMinioEndpoint(t *testing.T) string {
	t.Helper()
	sharedMinio.once.Do(func() {
		ctx := context.Background()
		container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
			ContainerRequest: testcontainers.ContainerRequest{
				Image:        "minio/minio:latest",
				Name:         "tg-test-s3",
				ExposedPorts: []string{"9000/tcp"},
				Env: map[string]string{
					"MINIO_ROOT_USER":     "minioadmin",
					"MINIO_ROOT_PASSWORD": "minioadmin",
				},
				Cmd:        []string{"server", "/data"},
				WaitingFor: wait.ForHTTP("/minio/health/live").WithPort("9000/tcp"),
			},
			Started: true,
			Reuse:   true,
		})
		if err != nil {
			sharedMinio.err = err
			return
		}
		host, err := container.Host(ctx)
		if err != nil {
			sharedMinio.err = err
			return
		}
		port, err := container.MappedPort(ctx, "9000/tcp")
		if err != nil {
			sharedMinio.err = err
			return
		}
		sharedMinio.endpoint = "http://" + net.JoinHostPort(host, port.Port())
		sharedMinio.namespace = "run-" + strconv.FormatInt(time.Now().UnixNano(), 36)

		admin, err := blob.NewS3(blob.S3Config{
			Endpoint:          sharedMinio.endpoint,
			Bucket:            "blob-conformance",
			Prefix:            "conformance/",
			Region:            "us-east-1",
			AccessKeyID:       "minioadmin",
			SecretAccessKey:   "minioadmin",
			AllowInsecureHTTP: true,
			OperationTimeout:  10 * time.Second,
			MaxAttempts:       3,
		})
		if err != nil {
			sharedMinio.err = err
			return
		}
		if err := blob.CreateBucketForTest(ctx, admin); err != nil {
			sharedMinio.err = err
		}
	})
	if sharedMinio.err != nil {
		t.Fatalf("MinIO harness: %v", sharedMinio.err)
	}
	return sharedMinio.endpoint
}

func writeS3Error(w http.ResponseWriter, status int, code string) {
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(status)
	if _, err := io.WriteString(w, fmt.Sprintf("<Error><Code>%s</Code></Error>", code)); err != nil {
		return
	}
}

func TestS3RejectsInvalidKeysBeforeNetwork(t *testing.T) {
	t.Parallel()
	var requests int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		http.Error(w, "unexpected request", http.StatusInternalServerError)
	}))
	t.Cleanup(server.Close)
	store, err := blob.NewS3(blob.S3Config{
		Endpoint:          server.URL,
		Bucket:            "test-bucket",
		Prefix:            "tenant/",
		AccessKeyID:       "access",
		SecretAccessKey:   "secret",
		AllowInsecureHTTP: true,
		MaxAttempts:       1,
	})
	if err != nil {
		t.Fatalf("new S3 store: %v", err)
	}
	ctx := context.Background()
	if _, err := store.Put(ctx, "../escape", strings.NewReader("x")); !errors.Is(err, blob.ErrInvalidKey) {
		t.Fatalf("hostile put = %v, want ErrInvalidKey", err)
	}
	if _, err := store.ReadAt(ctx, "a//b", 0, 1); !errors.Is(err, blob.ErrInvalidKey) {
		t.Fatalf("hostile read = %v, want ErrInvalidKey", err)
	}
	if err := store.Remove(ctx, "a/./b"); !errors.Is(err, blob.ErrInvalidKey) {
		t.Fatalf("hostile remove = %v, want ErrInvalidKey", err)
	}
	if err := store.WalkPrefix(ctx, "../", func(blob.Entry) error { return nil }); !errors.Is(err, blob.ErrInvalidKey) {
		t.Fatalf("hostile walk = %v, want ErrInvalidKey", err)
	}
	if requests != 0 {
		t.Fatalf("hostile keys issued %d requests", requests)
	}
}

func TestS3ReadAtMapsOnlyMissingObjectToErrNotFound(t *testing.T) {
	t.Parallel()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch path.Base(r.URL.Path) {
		case "missing":
			writeS3Error(w, http.StatusNotFound, "NoSuchKey")
		case "bucket":
			writeS3Error(w, http.StatusNotFound, "NoSuchBucket")
		case "denied":
			w.Header().Set("Content-Type", "application/xml")
			w.WriteHeader(http.StatusForbidden)
			if _, err := io.WriteString(w, "<Error><Code>AccessDenied</Code><Message>secret-value</Message></Error>"); err != nil {
				return
			}
		default:
			writeS3Error(w, http.StatusNotFound, "NoSuchKey")
		}
	}))
	t.Cleanup(server.Close)
	store, err := blob.NewS3(blob.S3Config{
		Endpoint:          server.URL,
		Bucket:            "test-bucket",
		Prefix:            "tenant/",
		AccessKeyID:       "access",
		SecretAccessKey:   "secret-value",
		AllowInsecureHTTP: true,
		MaxAttempts:       1,
	})
	if err != nil {
		t.Fatalf("new S3 store: %v", err)
	}
	for _, test := range []struct {
		name     string
		caseName string
		missing  bool
	}{
		{name: "missing object", caseName: "missing", missing: true},
		{name: "missing bucket", caseName: "bucket"},
		{name: "permission denied", caseName: "denied"},
	} {
		t.Run(test.name, func(t *testing.T) {
			key := "parts/" + test.caseName
			got, err := store.ReadAt(context.Background(), key, 0, 1)
			if test.missing {
				if !errors.Is(err, blob.ErrNotFound) {
					t.Fatalf("read error = %v, want ErrNotFound", err)
				}
				return
			}
			if err == nil || errors.Is(err, blob.ErrNotFound) {
				t.Fatalf("read error = %v, want non-missing error", err)
			}
			for _, forbidden := range []string{"secret-value", "test-bucket", server.URL} {
				if strings.Contains(err.Error(), forbidden) {
					t.Fatalf("read error %q contains %q", err, forbidden)
				}
			}
			if got != nil {
				t.Fatalf("read returned %q with error", got)
			}
		})
	}
}

func TestS3PutRetriesWithKnownLength(t *testing.T) {
	t.Parallel()
	var attempts int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts++
		if r.ContentLength != int64(len(payload)) {
			t.Errorf("Content-Length = %d, want %d", r.ContentLength, len(payload))
		}
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("read body: %v", err)
		}
		if string(body) != payload {
			t.Errorf("body = %q, want %q", body, payload)
		}
		if attempts < 3 {
			writeS3Error(w, http.StatusServiceUnavailable, "SlowDown")
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(server.Close)
	store, err := blob.NewS3(blob.S3Config{
		Endpoint:          server.URL,
		Bucket:            "test-bucket",
		Prefix:            "tenant/",
		AccessKeyID:       "access",
		SecretAccessKey:   "secret",
		AllowInsecureHTTP: true,
		MaxAttempts:       3,
		OperationTimeout:  time.Second,
	})
	if err != nil {
		t.Fatalf("new S3 store: %v", err)
	}
	n, err := store.Put(context.Background(), "parts/retry", strings.NewReader(payload))
	if err != nil {
		t.Fatalf("put: %v", err)
	}
	if n != int64(len(payload)) {
		t.Fatalf("put returned %d bytes, want %d", n, len(payload))
	}
	if attempts != 3 {
		t.Fatalf("got %d attempts, want 3", attempts)
	}
}

func TestS3ReadAtRetriesWithoutRequestBody(t *testing.T) {
	t.Parallel()
	var attempts int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts++
		if attempts == 1 {
			writeS3Error(w, http.StatusServiceUnavailable, "SlowDown")
			return
		}
		w.WriteHeader(http.StatusPartialContent)
		if _, err := io.WriteString(w, "x"); err != nil {
			return
		}
	}))
	t.Cleanup(server.Close)
	store, err := blob.NewS3(blob.S3Config{
		Endpoint:          server.URL,
		Bucket:            "test-bucket",
		Prefix:            "tenant/",
		AccessKeyID:       "access",
		SecretAccessKey:   "secret",
		AllowInsecureHTTP: true,
		MaxAttempts:       2,
		OperationTimeout:  time.Second,
	})
	if err != nil {
		t.Fatalf("new S3 store: %v", err)
	}
	got, err := store.ReadAt(context.Background(), "parts/retry-read", 0, 1)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if string(got) != "x" || attempts != 2 {
		t.Fatalf("read = %q after %d attempts, want x after 2", got, attempts)
	}
}

func TestS3RefusesUnsizedPutWithoutNetwork(t *testing.T) {
	t.Parallel()
	var requests int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(server.Close)
	store, err := blob.NewS3(blob.S3Config{
		Endpoint:          server.URL,
		Bucket:            "test-bucket",
		Prefix:            "tenant/",
		AccessKeyID:       "access",
		SecretAccessKey:   "secret",
		AllowInsecureHTTP: true,
		MaxAttempts:       1,
	})
	if err != nil {
		t.Fatalf("new S3 store: %v", err)
	}
	if _, err := store.Put(context.Background(), "parts/unsized", &unsizedReader{r: strings.NewReader(payload)}); err == nil {
		t.Fatal("unsized reader accepted")
	}
	if requests != 0 {
		t.Fatalf("unsized reader issued %d requests", requests)
	}
}

func TestS3ReadAtLimitsResponseBodyToRequestedWindow(t *testing.T) {
	t.Parallel()
	var gotRange string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotRange = r.Header.Get("Range")
		w.Header().Set("Content-Length", strconv.Itoa(1<<20))
		w.WriteHeader(http.StatusPartialContent)
		if _, err := io.CopyN(w, bytes.NewReader(bytes.Repeat([]byte("x"), 1<<20)), 1<<20); err != nil {
			return
		}
	}))
	t.Cleanup(server.Close)
	store, err := blob.NewS3(blob.S3Config{
		Endpoint:          server.URL,
		Bucket:            "test-bucket",
		Prefix:            "tenant/",
		AccessKeyID:       "access",
		SecretAccessKey:   "secret",
		AllowInsecureHTTP: true,
		MaxAttempts:       1,
	})
	if err != nil {
		t.Fatalf("new S3 store: %v", err)
	}
	got, err := store.ReadAt(context.Background(), "parts/bounded", 7, 4)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if len(got) != 4 {
		t.Fatalf("read %d bytes, want 4", len(got))
	}
	if gotRange != "bytes=7-10" {
		t.Fatalf("Range = %q, want bytes=7-10", gotRange)
	}
}

func TestS3ReadAtBoundsUnexpectedFullResponse(t *testing.T) {
	t.Parallel()
	const (
		bodyBytes = 1 << 20
		chunkSize = 1 << 10
		chunkWait = time.Millisecond
	)
	var sent atomic.Int64
	var gotRange string
	var rangeMu sync.Mutex
	done := make(chan struct{})
	var doneOnce sync.Once
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer doneOnce.Do(func() { close(done) })
		rangeMu.Lock()
		gotRange = r.Header.Get("Range")
		rangeMu.Unlock()
		w.Header().Set("Content-Length", strconv.Itoa(bodyBytes))
		w.WriteHeader(http.StatusOK)
		flusher, ok := w.(http.Flusher)
		if !ok {
			return
		}
		chunk := bytes.Repeat([]byte("x"), chunkSize)
		for sent.Load() < bodyBytes {
			n, err := w.Write(chunk)
			sent.Add(int64(n))
			if err != nil {
				return
			}
			flusher.Flush()
			time.Sleep(chunkWait)
		}
	}))
	t.Cleanup(server.Close)
	store, err := blob.NewS3(blob.S3Config{
		Endpoint:          server.URL,
		Bucket:            "test-bucket",
		Prefix:            "tenant/",
		AccessKeyID:       "access",
		SecretAccessKey:   "secret",
		AllowInsecureHTTP: true,
		MaxAttempts:       1,
	})
	if err != nil {
		t.Fatalf("new S3 store: %v", err)
	}
	if _, err := store.ReadAt(context.Background(), "parts/full-response", 0, 4); err == nil {
		t.Fatal("full response accepted for ranged read")
	}
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("server did not observe the response connection closing")
	}
	rangeMu.Lock()
	got := gotRange
	rangeMu.Unlock()
	if got != "bytes=0-3" {
		t.Fatalf("Range = %q, want bytes=0-3", got)
	}
	if got := sent.Load(); got >= bodyBytes/2 {
		t.Fatalf("unexpected full response sent %d bytes, want less than %d", got, bodyBytes/2)
	}
}

func TestS3ZeroLengthReadMakesNoRequest(t *testing.T) {
	t.Parallel()
	var requests int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		w.WriteHeader(http.StatusInternalServerError)
	}))
	t.Cleanup(server.Close)
	store, err := blob.NewS3(blob.S3Config{
		Endpoint:          server.URL,
		Bucket:            "test-bucket",
		Prefix:            "tenant/",
		AccessKeyID:       "access",
		SecretAccessKey:   "secret",
		AllowInsecureHTTP: true,
		MaxAttempts:       1,
	})
	if err != nil {
		t.Fatalf("new S3 store: %v", err)
	}
	got, err := store.ReadAt(context.Background(), "parts/empty", 123, 0)
	if err != nil {
		t.Fatalf("zero-length read: %v", err)
	}
	if len(got) != 0 || requests != 0 {
		t.Fatalf("zero-length read returned %d bytes and made %d requests", len(got), requests)
	}
}

type unsizedReader struct {
	r io.Reader
}

func (r *unsizedReader) Read(p []byte) (int, error) { return r.r.Read(p) }

func TestS3RefusesCrossHostRedirect(t *testing.T) {
	t.Parallel()
	var redirected int
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		redirected++
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(target.Close)
	source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Location", target.URL)
		w.WriteHeader(http.StatusTemporaryRedirect)
	}))
	t.Cleanup(source.Close)
	store, err := blob.NewS3(blob.S3Config{
		Endpoint:          source.URL,
		Bucket:            "test-bucket",
		Prefix:            "tenant/",
		AccessKeyID:       "access",
		SecretAccessKey:   "secret",
		AllowInsecureHTTP: true,
		MaxAttempts:       1,
	})
	if err != nil {
		t.Fatalf("new S3 store: %v", err)
	}
	if _, err := store.Put(context.Background(), "parts/redirect", strings.NewReader(payload)); err == nil {
		t.Fatal("cross-host redirect accepted")
	}
	if redirected != 0 {
		t.Fatalf("redirect target received %d requests", redirected)
	}
}

func TestS3PlaintextRequiresOptInAndWarns(t *testing.T) {
	t.Parallel()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK) }))
	t.Cleanup(server.Close)
	base := blob.S3Config{
		Endpoint:        server.URL,
		Bucket:          "test-bucket",
		Prefix:          "tenant/",
		AccessKeyID:     "access",
		SecretAccessKey: "secret-value",
	}
	if _, err := blob.NewS3(base); err == nil {
		t.Fatal("plaintext endpoint accepted without opt-in")
	}
	var logbuf strings.Builder
	base.AllowInsecureHTTP = true
	base.Logger = slog.New(slog.NewTextHandler(&logbuf, nil))
	if _, err := blob.NewS3(base); err != nil {
		t.Fatalf("plaintext opt-in rejected: %v", err)
	}
	if !strings.Contains(logbuf.String(), "plaintext") {
		t.Fatalf("warning = %q, want plaintext warning", logbuf.String())
	}
	if strings.Contains(logbuf.String(), "secret-value") {
		t.Fatalf("warning contains secret: %q", logbuf.String())
	}
}

func TestS3UsesConfiguredCABundle(t *testing.T) {
	t.Parallel()
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.ContentLength != int64(len(payload)) {
			t.Errorf("Content-Length = %d, want %d", r.ContentLength, len(payload))
		}
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(server.Close)
	caPath := filepath.Join(t.TempDir(), "ca.pem")
	caPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw})
	if err := os.WriteFile(caPath, caPEM, 0o600); err != nil {
		t.Fatalf("write CA bundle: %v", err)
	}
	store, err := blob.NewS3(blob.S3Config{
		Endpoint:        server.URL,
		Bucket:          "test-bucket",
		Prefix:          "tenant/",
		AccessKeyID:     "access",
		SecretAccessKey: "secret",
		CAPath:          caPath,
		MaxAttempts:     1,
	})
	if err != nil {
		t.Fatalf("new S3 store: %v", err)
	}
	if _, err := store.Put(context.Background(), "parts/tls", strings.NewReader(payload)); err != nil {
		t.Fatalf("put over configured CA: %v", err)
	}
}

func TestS3WalkPrefixPagesAndConfinesEntries(t *testing.T) {
	t.Parallel()
	server := newListingServer(t)
	store, err := blob.NewS3(blob.S3Config{
		Endpoint:          server.URL,
		Bucket:            "test-bucket",
		Prefix:            "tenant/",
		AccessKeyID:       "access",
		SecretAccessKey:   "secret",
		AllowInsecureHTTP: true,
		MaxAttempts:       1,
	})
	if err != nil {
		t.Fatalf("new S3 store: %v", err)
	}
	var got []blob.Entry
	if err := store.WalkPrefix(context.Background(), blob.PartsPrefix, func(e blob.Entry) error {
		got = append(got, e)
		return nil
	}); err != nil {
		t.Fatalf("walk: %v", err)
	}
	if len(got) != 2 || got[0].Key != "parts/aaa" || got[1].Key != "parts/bbb" {
		t.Fatalf("walk entries = %+v, want two ordered entries", got)
	}
	for _, entry := range got {
		if !strings.HasPrefix(entry.Key, blob.PartsPrefix) || !entry.Regular || entry.Dir || entry.Size <= 0 || entry.ModTime.IsZero() {
			t.Fatalf("entry %+v is not a confined regular object", entry)
		}
	}
	if len(server.queries) != 2 || server.queries[1].Get("continuation-token") != "next-page" {
		t.Fatalf("list queries = %v, want a continuation-token second page", server.queries)
	}
}

func TestS3WalkPrefixEmptyAndFileScope(t *testing.T) {
	t.Parallel()
	server := newListingServer(t)
	store, err := blob.NewS3(blob.S3Config{
		Endpoint:          server.URL,
		Bucket:            "test-bucket",
		Prefix:            "tenant/",
		AccessKeyID:       "access",
		SecretAccessKey:   "secret",
		AllowInsecureHTTP: true,
		MaxAttempts:       1,
	})
	if err != nil {
		t.Fatalf("new S3 store: %v", err)
	}
	if err := store.WalkPrefix(context.Background(), "", func(blob.Entry) error {
		t.Fatal("empty prefix invoked callback")
		return nil
	}); err != nil {
		t.Fatalf("empty prefix: %v", err)
	}
	if len(server.queries) != 0 {
		t.Fatalf("empty prefix issued %d list requests", len(server.queries))
	}
	server.files["tenant/ab"] = true
	if err := store.WalkPrefix(context.Background(), "ab", func(blob.Entry) error { return nil }); err == nil {
		t.Fatal("prefix naming an object succeeded")
	}
}

func TestS3WalkPrefixSeparatorFreeDoesNotWiden(t *testing.T) {
	t.Parallel()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodHead {
			writeS3Error(w, http.StatusNotFound, "NoSuchKey")
			return
		}
		w.Header().Set("Content-Type", "application/xml")
		w.WriteHeader(http.StatusOK)
		if _, err := io.WriteString(w, `<ListBucketResult><IsTruncated>false</IsTruncated><Contents><Key>tenant/abacus/object</Key><LastModified>2026-08-30T00:00:00Z</LastModified><Size>1</Size></Contents></ListBucketResult>`); err != nil {
			return
		}
	}))
	t.Cleanup(server.Close)
	store, err := blob.NewS3(blob.S3Config{
		Endpoint:          server.URL,
		Bucket:            "test-bucket",
		Prefix:            "tenant/",
		AccessKeyID:       "access",
		SecretAccessKey:   "secret",
		AllowInsecureHTTP: true,
		MaxAttempts:       1,
	})
	if err != nil {
		t.Fatalf("new S3 store: %v", err)
	}
	var callbacks int
	if err := store.WalkPrefix(context.Background(), "ab", func(blob.Entry) error {
		callbacks++
		return nil
	}); err == nil {
		t.Fatal("separator-free walk widened to a neighboring prefix")
	}
	if callbacks != 0 {
		t.Fatalf("widened walk invoked callback %d times", callbacks)
	}
}

func TestS3WalkPrefixTreatsEmptyHead404AsMissingObject(t *testing.T) {
	t.Parallel()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodHead {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.WriteHeader(http.StatusOK)
		if _, err := io.WriteString(w, `<ListBucketResult><IsTruncated>false</IsTruncated></ListBucketResult>`); err != nil {
			return
		}
	}))
	t.Cleanup(server.Close)
	store, err := blob.NewS3(blob.S3Config{
		Endpoint:          server.URL,
		Bucket:            "test-bucket",
		Prefix:            "tenant/",
		AccessKeyID:       "access",
		SecretAccessKey:   "secret",
		AllowInsecureHTTP: true,
		MaxAttempts:       1,
	})
	if err != nil {
		t.Fatalf("new S3 store: %v", err)
	}
	if err := store.WalkPrefix(context.Background(), "parts", func(blob.Entry) error { return nil }); err != nil {
		t.Fatalf("walk with empty HEAD 404: %v", err)
	}
}

func TestS3WalkPrefixUsesPerPageDeadlines(t *testing.T) {
	t.Parallel()
	const (
		pages    = 4
		pageWait = 40 * time.Millisecond
	)
	var page int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			writeS3Error(w, http.StatusNotFound, "NoSuchKey")
			return
		}
		current := page
		page++
		time.Sleep(pageWait)
		w.Header().Set("Content-Type", "application/xml")
		w.WriteHeader(http.StatusOK)
		truncated := current+1 < pages
		token := ""
		if truncated {
			token = fmt.Sprintf("<NextContinuationToken>page-%d</NextContinuationToken>", current+1)
		}
		body := fmt.Sprintf(`<ListBucketResult><IsTruncated>%t</IsTruncated>%s</ListBucketResult>`, truncated, token)
		if _, err := io.WriteString(w, body); err != nil {
			return
		}
	}))
	t.Cleanup(server.Close)
	store, err := blob.NewS3(blob.S3Config{
		Endpoint:          server.URL,
		Bucket:            "test-bucket",
		Prefix:            "tenant/",
		AccessKeyID:       "access",
		SecretAccessKey:   "secret",
		AllowInsecureHTTP: true,
		MaxAttempts:       1,
		OperationTimeout:  100 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("new S3 store: %v", err)
	}
	var entries int
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := store.WalkPrefix(ctx, blob.PartsPrefix, func(blob.Entry) error {
		entries++
		return nil
	}); err != nil {
		t.Fatalf("walk across %d pages: %v", pages, err)
	}
	if entries != 0 {
		t.Fatalf("walk yielded %d entries, want 0", entries)
	}
	if page != pages {
		t.Fatalf("walk requested %d pages, want %d", page, pages)
	}
}

type listingServer struct {
	*httptest.Server

	mu      sync.Mutex
	queries []url.Values
	files   map[string]bool
	page    int
}

func newListingServer(t *testing.T) *listingServer {
	t.Helper()
	server := &listingServer{files: make(map[string]bool)}
	server.Server = httptest.NewServer(http.HandlerFunc(server.serveHTTP))
	t.Cleanup(server.Close)
	return server
}

func (s *listingServer) serveHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodHead {
		s.mu.Lock()
		exists := s.files[strings.TrimPrefix(r.URL.Path, "/test-bucket/")]
		s.mu.Unlock()
		if exists {
			w.WriteHeader(http.StatusOK)
			return
		}
		writeS3Error(w, http.StatusNotFound, "NoSuchKey")
		return
	}
	if r.Method != http.MethodGet || r.URL.Query().Get("list-type") != "2" {
		writeS3Error(w, http.StatusNotFound, "NoSuchKey")
		return
	}
	s.mu.Lock()
	s.queries = append(s.queries, r.URL.Query())
	page := s.page
	s.page++
	s.mu.Unlock()
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(http.StatusOK)
	if page == 0 {
		if _, err := io.WriteString(w, `<ListBucketResult><IsTruncated>true</IsTruncated><NextContinuationToken>next-page</NextContinuationToken><Contents><Key>tenant/parts/aaa</Key><LastModified>2026-08-30T00:00:00Z</LastModified><Size>3</Size></Contents></ListBucketResult>`); err != nil {
			return
		}
		return
	}
	if _, err := io.WriteString(w, `<ListBucketResult><IsTruncated>false</IsTruncated><Contents><Key>tenant/parts/bbb</Key><LastModified>2026-08-30T00:00:01Z</LastModified><Size>4</Size></Contents></ListBucketResult>`); err != nil {
		return
	}
}
