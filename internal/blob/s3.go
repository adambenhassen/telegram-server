package blob

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/pem"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
)

const (
	// DefaultS3OperationTimeout bounds one S3 operation, including its bounded
	// retries. Callers can use a shorter timeout when the backend is local to the
	// process or a longer one when it is across a private network.
	DefaultS3OperationTimeout = 30 * time.Second
	// DefaultS3MaxAttempts bounds the number of requests one operation can make.
	DefaultS3MaxAttempts = 3

	s3UnsignedPayload = "UNSIGNED-PAYLOAD"
	s3Service         = "s3"
	s3MaxAttempts     = 10
	s3MaxErrorBytes   = 16 * 1024
	s3MaxDrainBytes   = 16 * 1024
	s3MaxListBytes    = 16 * 1024 * 1024
	s3ListPageSize    = 1000
)

var (
	errS3Operation       = errors.New("object store operation failed")
	errS3InvalidConfig   = errors.New("invalid object store configuration")
	errS3InvalidWindow   = errors.New("invalid blob read window")
	errS3Redirect        = errors.New("object store redirect refused")
	errS3UnscopedPrefix  = errors.New("object store prefix has no containment scope")
	errS3MissingListPage = errors.New("object store listing stopped without a continuation token")
)

// S3Config describes an S3-compatible endpoint. This implementation is not
// wired into server configuration; the follow-up selection ticket owns that
// construction. Tests and future callers can construct it directly.
type S3Config struct {
	Endpoint string
	Bucket   string
	Prefix   string
	Region   string

	AccessKeyID     string
	SecretAccessKey string

	// CAPath points to a PEM bundle for a private store certificate authority.
	// TLS verification is always enabled; there is intentionally no
	// verification-bypass setting.
	CAPath string
	// AllowInsecureHTTP is an explicit loopback/compose escape hatch. It is
	// rejected by default and emits a warning when enabled.
	AllowInsecureHTTP bool

	OperationTimeout time.Duration
	MaxAttempts      int
	Logger           *slog.Logger
}

// S3 stores blobs in an S3-compatible object store using path-style requests.
// The configured prefix is applied to every object key and is stripped from
// entries returned by WalkPrefix, keeping the Store contract relative to the
// backend root.
type S3 struct {
	endpoint *url.URL
	bucket   string
	prefix   string
	region   string
	access   string
	secret   string

	client           *http.Client
	operationTimeout time.Duration
	maxAttempts      int
}

// NewS3 validates cfg and constructs an S3-compatible blob store. It does not
// contact the endpoint or create a bucket. Startup validation and backend
// selection belong to the follow-up configuration ticket.
func NewS3(cfg S3Config) (*S3, error) {
	endpoint, err := url.Parse(cfg.Endpoint)
	if err != nil || endpoint.Scheme == "" || endpoint.Host == "" || endpoint.User != nil ||
		endpoint.RawQuery != "" || endpoint.Fragment != "" || endpoint.RawPath != "" ||
		(endpoint.Path != "" && endpoint.Path != "/") {
		return nil, errS3InvalidConfig
	}
	if endpoint.Scheme != "https" {
		if endpoint.Scheme != "http" || !cfg.AllowInsecureHTTP {
			return nil, errS3InvalidConfig
		}
		logger := cfg.Logger
		if logger == nil {
			logger = slog.Default()
		}
		logger.Warn("blob S3 endpoint is using plaintext HTTP")
	}
	if !validS3Bucket(cfg.Bucket) {
		return nil, errS3InvalidConfig
	}
	prefix, err := normalizeS3Prefix(cfg.Prefix)
	if err != nil {
		return nil, errS3InvalidConfig
	}
	if cfg.AccessKeyID == "" || cfg.SecretAccessKey == "" {
		return nil, errS3InvalidConfig
	}
	region := cfg.Region
	if region == "" {
		region = "us-east-1"
	}
	timeout := cfg.OperationTimeout
	if timeout == 0 {
		timeout = DefaultS3OperationTimeout
	}
	if timeout < 0 {
		return nil, errS3InvalidConfig
	}
	attempts := cfg.MaxAttempts
	if attempts == 0 {
		attempts = DefaultS3MaxAttempts
	}
	if attempts < 1 || attempts > s3MaxAttempts {
		return nil, errS3InvalidConfig
	}

	roots, err := loadS3Roots(cfg.CAPath)
	if err != nil {
		return nil, errS3InvalidConfig
	}
	transport := &http.Transport{
		Proxy:       http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{Timeout: 10 * time.Second}).DialContext,
		TLSClientConfig: &tls.Config{
			MinVersion: tls.VersionTLS12,
			RootCAs:    roots,
		},
	}
	client := &http.Client{
		Transport: transport,
		Timeout:   timeout,
	}
	endpointHost := endpoint.Host
	endpointScheme := endpoint.Scheme
	client.CheckRedirect = func(req *http.Request, _ []*http.Request) error {
		if req.URL.Scheme != endpointScheme || !strings.EqualFold(req.URL.Host, endpointHost) {
			return errS3Redirect
		}
		return nil
	}
	endpoint.Path = ""

	return &S3{
		endpoint:         endpoint,
		bucket:           cfg.Bucket,
		prefix:           prefix,
		region:           region,
		access:           cfg.AccessKeyID,
		secret:           cfg.SecretAccessKey,
		client:           client,
		operationTimeout: timeout,
		maxAttempts:      attempts,
	}, nil
}

// OperationTimeout reports the configured bound for one S3 operation,
// including its bounded retries.
func (s *S3) OperationTimeout() time.Duration { return s.operationTimeout }

func loadS3Roots(path string) (*x509.CertPool, error) {
	roots, err := x509.SystemCertPool()
	if err != nil || roots == nil {
		roots = x509.NewCertPool()
	}
	if path == "" {
		return roots, nil
	}
	b, err := os.ReadFile(path) //nolint:gosec // the CA path is explicit configuration
	if err != nil {
		return nil, err
	}
	for len(b) > 0 {
		block, rest := pem.Decode(b)
		if block == nil {
			return nil, errS3InvalidConfig
		}
		b = rest
		if block.Type != "CERTIFICATE" || !roots.AppendCertsFromPEM(pem.EncodeToMemory(block)) {
			return nil, errS3InvalidConfig
		}
	}
	return roots, nil
}

func validS3Bucket(bucket string) bool {
	if bucket == "" || len(bucket) > maxKeyBytes {
		return false
	}
	for i := range bucket {
		c := bucket[i]
		if (c < 'a' || c > 'z') && (c < 'A' || c > 'Z') &&
			(c < '0' || c > '9') && c != '.' && c != '-' && c != '_' {
			return false
		}
	}
	return true
}

func normalizeS3Prefix(prefix string) (string, error) {
	if prefix == "" {
		return "", nil
	}
	if !strings.HasSuffix(prefix, "/") {
		prefix += "/"
	}
	if err := validateS3Prefix(prefix); err != nil {
		return "", err
	}
	return prefix, nil
}

// validateS3Prefix accepts a containment prefix's one trailing separator while
// reusing the package-wide key validator for every actual segment.
func validateS3Prefix(prefix string) error {
	if prefix == "" {
		return nil
	}
	if !strings.HasSuffix(prefix, "/") {
		return ValidateKey(prefix)
	}
	trimmed := strings.TrimSuffix(prefix, "/")
	if trimmed == "" {
		return ErrInvalidKey
	}
	return ValidateKey(trimmed)
}

func (s *S3) scopedKey(key string) (string, error) {
	// The caller's key is validated before the configured prefix is applied.
	// This ordering is deliberate: a malformed key cannot be made to look safe
	// by concatenating it under a trusted namespace.
	if err := ValidateKey(key); err != nil {
		return "", err
	}
	remote := s.prefix + key
	if !strings.HasPrefix(remote, s.prefix) || ValidateKey(remote) != nil {
		return "", ErrInvalidKey
	}
	return remote, nil
}

func (s *S3) scopedWalkPrefix(prefix string) (string, error) {
	if err := validateS3Prefix(prefix); err != nil {
		return "", err
	}
	remote := s.prefix + prefix
	if !strings.HasPrefix(remote, s.prefix) || len(remote) > maxKeyBytes {
		return "", ErrInvalidKey
	}
	if strings.HasSuffix(remote, "/") {
		if err := validateS3Prefix(remote); err != nil {
			return "", ErrInvalidKey
		}
	} else if err := ValidateKey(remote); err != nil {
		return "", ErrInvalidKey
	}
	return remote, nil
}

type s3Body struct {
	reader     io.Reader
	seeker     io.Seeker
	start      int64
	size       int64
	replayable bool
}

// inspectS3Body obtains a request length without consuming or buffering the
// reader. A seekable reader is rewound to its original position; a reader with
// Size or Len supplies its already-known length and is sent once because it
// cannot be replayed after a transient response.
func inspectS3Body(r io.Reader) (*s3Body, error) {
	if r == nil {
		return nil, errS3Operation
	}
	if seeker, ok := r.(io.Seeker); ok {
		start, err := seeker.Seek(0, io.SeekCurrent)
		if err != nil {
			return nil, errS3Operation
		}
		end, err := seeker.Seek(0, io.SeekEnd)
		if err != nil || end < start {
			return nil, errS3Operation
		}
		if _, err := seeker.Seek(start, io.SeekStart); err != nil {
			return nil, errS3Operation
		}
		return &s3Body{reader: r, seeker: seeker, start: start, size: end - start, replayable: true}, nil
	}
	if sized, ok := r.(interface{ Size() int64 }); ok {
		size := sized.Size()
		if size < 0 {
			return nil, errS3Operation
		}
		return &s3Body{reader: r, size: size}, nil
	}
	if sized, ok := r.(interface{ Len() int }); ok {
		size := sized.Len()
		if size < 0 {
			return nil, errS3Operation
		}
		return &s3Body{reader: r, size: int64(size)}, nil
	}
	return nil, errS3Operation
}

func (b *s3Body) rewind() error {
	if !b.replayable {
		return errS3Operation
	}
	_, err := b.seeker.Seek(b.start, io.SeekStart)
	if err != nil {
		return errS3Operation
	}
	return nil
}

// Put stores r under key and returns its known length. The request carries that
// length directly, so a large assembled object is streamed to the transport
// and is never buffered just to determine Content-Length. Same-key writes use
// the remote store's last-writer-wins semantics and may replace an existing
// object.
func (s *S3) Put(ctx context.Context, key string, r io.Reader) (n int64, retErr error) {
	remote, err := s.scopedKey(key)
	if err != nil {
		return 0, err
	}
	body, err := inspectS3Body(r)
	if err != nil {
		return 0, s.operationError("put", err)
	}
	opCtx, cancel := context.WithTimeout(ctx, s.operationTimeout)
	defer cancel()
	resp, err := s.doRequest(opCtx, http.MethodPut, s.objectPath(remote), "", body, nil)
	if err != nil {
		return 0, s.operationError("put", err)
	}
	defer func() {
		if closeErr := resp.Body.Close(); closeErr != nil && retErr == nil {
			retErr = s.operationError("put", closeErr)
			n = 0
		}
	}()
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		if err := drainBody(resp.Body); err != nil {
			return 0, s.operationError("put", err)
		}
		return 0, s.operationError("put", nil)
	}
	if err := drainBody(resp.Body); err != nil {
		return 0, s.operationError("put", err)
	}
	return body.size, nil
}

// ReadAt returns at most limit bytes beginning at offset. S3 is always asked
// for the exact byte range, and the response reader is independently limited so
// a non-conforming endpoint cannot make this process consume a whole object.
func (s *S3) ReadAt(ctx context.Context, key string, offset, limit int64) (data []byte, retErr error) {
	remote, err := s.scopedKey(key)
	if err != nil {
		return nil, err
	}
	if offset < 0 || limit < 0 || (limit > 0 && offset > math.MaxInt64-(limit-1)) {
		return nil, s.operationError("read", errS3InvalidWindow)
	}
	if limit == 0 {
		return []byte{}, nil
	}
	end := offset + limit - 1
	opCtx, cancel := context.WithTimeout(ctx, s.operationTimeout)
	defer cancel()
	resp, err := s.doRequest(opCtx, http.MethodGet, s.objectPath(remote), "", nil,
		http.Header{"Range": []string{fmt.Sprintf("bytes=%d-%d", offset, end)}})
	if err != nil {
		return nil, s.operationError("read", err)
	}
	defer func() {
		if closeErr := resp.Body.Close(); closeErr != nil && retErr == nil {
			retErr = s.operationError("read", closeErr)
			data = nil
		}
	}()
	if resp.StatusCode == http.StatusRequestedRangeNotSatisfiable {
		if err := drainBody(resp.Body); err != nil {
			return nil, s.operationError("read", err)
		}
		return []byte{}, nil
	}
	if resp.StatusCode == http.StatusNotFound {
		code, codeErr := readS3ErrorCode(resp.Body)
		if codeErr != nil {
			return nil, s.operationError("read", codeErr)
		}
		if isMissingObjectCode(code) {
			return nil, ErrNotFound
		}
		return nil, s.operationError("read", nil)
	}
	if resp.StatusCode != http.StatusPartialContent {
		if err := drainBody(resp.Body); err != nil {
			return nil, s.operationError("read", err)
		}
		return nil, s.operationError("read", nil)
	}
	data, readErr := io.ReadAll(io.LimitReader(resp.Body, limit))
	if readErr != nil {
		return nil, s.operationError("read", errS3Operation)
	}
	return data, nil
}

// Remove deletes key. A missing object is the documented no-op, while a
// missing bucket or any other failure remains an operation error.
func (s *S3) Remove(ctx context.Context, key string) (retErr error) {
	remote, err := s.scopedKey(key)
	if err != nil {
		return err
	}
	opCtx, cancel := context.WithTimeout(ctx, s.operationTimeout)
	defer cancel()
	resp, err := s.doRequest(opCtx, http.MethodDelete, s.objectPath(remote), "", nil, nil)
	if err != nil {
		return s.operationError("remove", err)
	}
	defer func() {
		if closeErr := resp.Body.Close(); closeErr != nil && retErr == nil {
			retErr = s.operationError("remove", closeErr)
		}
	}()
	if resp.StatusCode == http.StatusNotFound {
		code, codeErr := readS3ErrorCode(resp.Body)
		if codeErr != nil {
			return s.operationError("remove", codeErr)
		}
		if isMissingObjectCode(code) {
			return nil
		}
		return s.operationError("remove", nil)
	}
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		if err := drainBody(resp.Body); err != nil {
			return s.operationError("remove", err)
		}
		return s.operationError("remove", nil)
	}
	if err := drainBody(resp.Body); err != nil {
		return s.operationError("remove", err)
	}
	return nil
}

// WalkPrefix pages ListObjectsV2 results and invokes fn for each object without
// retaining the bucket listing. A separator-free prefix gets the same
// containment treatment as Local: an exact object at that name is a file and
// fails closed; otherwise only its slash-delimited children are listed.
func (s *S3) WalkPrefix(ctx context.Context, prefix string, fn func(Entry) error) error {
	if prefix == "" {
		return nil
	}
	remotePrefix, err := s.scopedWalkPrefix(prefix)
	if err != nil {
		return err
	}
	if fn == nil {
		return s.operationError("walk", errS3UnscopedPrefix)
	}
	listPrefix := remotePrefix
	if !strings.Contains(prefix, "/") && !strings.HasSuffix(prefix, "/") {
		exact, err := s.scopedKey(prefix)
		if err != nil {
			return err
		}
		probeCtx, probeCancel := context.WithTimeout(ctx, s.operationTimeout)
		exists, err := s.objectExists(probeCtx, exact)
		probeCancel()
		if err != nil {
			return s.operationError("walk", err)
		}
		if exists {
			return s.operationError("walk", errS3UnscopedPrefix)
		}
		listPrefix = remotePrefix + "/"
	}

	var token string
	for {
		query := encodeS3Query([]s3Query{
			{key: "list-type", value: "2"},
			{key: "prefix", value: listPrefix},
			{key: "max-keys", value: strconv.Itoa(s3ListPageSize)},
			{key: "continuation-token", value: token},
		})
		pageCtx, pageCancel := context.WithTimeout(ctx, s.operationTimeout)
		truncated, next, pageErr := s.listPage(pageCtx, query, listPrefix, prefix, fn)
		pageCancel()
		if pageErr != nil {
			return pageErr
		}
		if !truncated {
			return nil
		}
		if next == "" {
			return s.operationError("walk", errS3MissingListPage)
		}
		token = next
	}
}

func (s *S3) objectExists(ctx context.Context, remote string) (exists bool, retErr error) {
	resp, err := s.doRequest(ctx, http.MethodHead, s.objectPath(remote), "", nil, nil)
	if err != nil {
		return false, err
	}
	defer func() {
		if closeErr := resp.Body.Close(); closeErr != nil && retErr == nil {
			retErr = closeErr
			exists = false
		}
	}()
	if resp.StatusCode == http.StatusNotFound {
		code, codeErr := readS3ErrorCode(resp.Body)
		if codeErr != nil {
			// HEAD responses for a missing object commonly have no body at
			// all. The subsequent ListObjects request still distinguishes a
			// missing bucket from an empty prefix.
			if errors.Is(codeErr, io.EOF) {
				return false, nil
			}
			return false, codeErr
		}
		if isMissingObjectCode(code) {
			return false, nil
		}
		return false, errS3Operation
	}
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		if drainErr := drainBody(resp.Body); drainErr != nil {
			return false, drainErr
		}
		return false, errS3Operation
	}
	if err := drainBody(resp.Body); err != nil {
		return false, err
	}
	return true, nil
}

type s3ListObject struct {
	Key          string `xml:"Key"`
	Size         int64  `xml:"Size"`
	LastModified string `xml:"LastModified"`
}

type s3ListPage struct {
	truncated bool
	next      string
}

func (s *S3) listPage(ctx context.Context, query, remotePrefix, localPrefix string, fn func(Entry) error) (truncated bool, next string, err error) {
	resp, err := s.doRequest(ctx, http.MethodGet, s.bucketPath(), query, nil, nil)
	if err != nil {
		return false, "", s.operationError("walk", err)
	}
	defer func() {
		if closeErr := resp.Body.Close(); closeErr != nil && err == nil {
			err = s.operationError("walk", closeErr)
		}
	}()
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		return false, "", s.operationError("walk", nil)
	}
	decoder := xml.NewDecoder(io.LimitReader(resp.Body, s3MaxListBytes))
	var count int
	var page s3ListPage
	for {
		token, tokenErr := decoder.Token()
		if errors.Is(tokenErr, io.EOF) {
			break
		}
		if tokenErr != nil {
			return false, "", s.operationError("walk", tokenErr)
		}
		start, ok := token.(xml.StartElement)
		if !ok {
			continue
		}
		switch start.Name.Local {
		case "IsTruncated":
			if decodeErr := decoder.DecodeElement(&page.truncated, &start); decodeErr != nil {
				return false, "", s.operationError("walk", decodeErr)
			}
		case "NextContinuationToken":
			if decodeErr := decoder.DecodeElement(&page.next, &start); decodeErr != nil {
				return false, "", s.operationError("walk", decodeErr)
			}
		case "Contents":
			count++
			if count > s3ListPageSize {
				return false, "", s.operationError("walk", errS3Operation)
			}
			var object s3ListObject
			if decodeErr := decoder.DecodeElement(&object, &start); decodeErr != nil {
				return false, "", s.operationError("walk", decodeErr)
			}
			if object.Size < 0 || !strings.HasPrefix(object.Key, remotePrefix) {
				return false, "", s.operationError("walk", errS3Operation)
			}
			localKey, ok := strings.CutPrefix(object.Key, s.prefix)
			if !ok || !strings.HasPrefix(localKey, localPrefix) || ValidateKey(localKey) != nil {
				return false, "", s.operationError("walk", errS3Operation)
			}
			modTime, parseErr := time.Parse(time.RFC3339Nano, object.LastModified)
			if parseErr != nil {
				return false, "", s.operationError("walk", parseErr)
			}
			if callbackErr := fn(Entry{
				Key:     localKey,
				Regular: true,
				Size:    object.Size,
				ModTime: modTime,
			}); callbackErr != nil {
				return false, "", callbackErr
			}
		default:
			// Keep the document root open so its child elements can be
			// streamed. Unknown nested elements are not part of the
			// ListObjectsV2 contract this store consumes and can be skipped as
			// one subtree.
			if start.Name.Local != "ListBucketResult" {
				if skipErr := decoder.Skip(); skipErr != nil {
					return false, "", s.operationError("walk", skipErr)
				}
			}
		}
	}
	return page.truncated, page.next, nil
}

func (s *S3) operationError(op string, err error) error {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return fmt.Errorf("blob s3 %s: %w", op, err)
	}
	return fmt.Errorf("blob s3 %s: %w", op, errS3Operation)
}

func (s *S3) objectPath(key string) string {
	return "/" + awsURIEncode(s.bucket, true) + "/" + awsURIEncode(key, true)
}

func (s *S3) bucketPath() string {
	return "/" + awsURIEncode(s.bucket, true) + "/"
}

type s3Query struct {
	key   string
	value string
}

func encodeS3Query(params []s3Query) string {
	type encoded struct {
		key, value string
	}
	parts := make([]encoded, 0, len(params))
	for _, p := range params {
		if p.value == "" && p.key == "continuation-token" {
			continue
		}
		parts = append(parts, encoded{key: awsURIEncode(p.key, false), value: awsURIEncode(p.value, false)})
	}
	sort.Slice(parts, func(i, j int) bool {
		if parts[i].key == parts[j].key {
			return parts[i].value < parts[j].value
		}
		return parts[i].key < parts[j].key
	})
	values := make([]string, len(parts))
	for i, p := range parts {
		values[i] = p.key + "=" + p.value
	}
	return strings.Join(values, "&")
}

func canonicalS3Query(raw string) string {
	values, err := url.ParseQuery(raw)
	if err != nil {
		return raw
	}
	params := make([]s3Query, 0)
	for key, list := range values {
		for _, value := range list {
			params = append(params, s3Query{key: key, value: value})
		}
	}
	return encodeS3Query(params)
}

func awsURIEncode(value string, preserveSlash bool) string {
	const hexDigits = "0123456789ABCDEF"
	var b strings.Builder
	for i := range value {
		c := value[i]
		if (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') ||
			(c >= '0' && c <= '9') || c == '-' || c == '_' || c == '.' || c == '~' ||
			(preserveSlash && c == '/') {
			b.WriteByte(c)
			continue
		}
		b.WriteByte('%')
		b.WriteByte(hexDigits[c>>4])
		b.WriteByte(hexDigits[c&0x0f])
	}
	return b.String()
}

func (s *S3) requestURL(requestPath, query string) string {
	u := *s.endpoint
	u.Path = requestPath
	u.RawPath = ""
	u.RawQuery = query
	return u.String()
}

func (s *S3) doRequest(ctx context.Context, method, requestPath, query string, body *s3Body, headers http.Header) (*http.Response, error) {
	attempts := s.maxAttempts
	if body != nil && !body.replayable {
		attempts = 1
	}
	for attempt := range attempts {
		if attempt > 0 && body != nil {
			if err := body.rewind(); err != nil {
				return nil, err
			}
		}
		var reader io.Reader
		if body != nil {
			reader = body.reader
		}
		req, err := http.NewRequestWithContext(ctx, method, s.requestURL(requestPath, query), reader)
		if err != nil {
			return nil, errS3Operation
		}
		if body != nil {
			req.ContentLength = body.size
		}
		for key, values := range headers {
			for _, value := range values {
				req.Header.Add(key, value)
			}
		}
		payloadHash := emptyPayloadHash
		if body != nil {
			payloadHash = s3UnsignedPayload
		}
		s.signRequest(req, payloadHash)
		resp, err := s.client.Do(req)
		if err != nil {
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			if attempt+1 == attempts {
				return nil, errS3Operation
			}
			if err := waitS3Retry(ctx, attempt); err != nil {
				return nil, err
			}
			continue
		}
		if retryableS3Status(resp.StatusCode) && attempt+1 < attempts {
			if err := discardAndClose(resp.Body); err != nil {
				return nil, errS3Operation
			}
			if err := waitS3Retry(ctx, attempt); err != nil {
				return nil, err
			}
			continue
		}
		return resp, nil
	}
	return nil, errS3Operation
}

func (s *S3) signRequest(req *http.Request, payloadHash string) {
	now := time.Now().UTC()
	date := now.Format("20060102")
	amzDate := now.Format("20060102T150405Z")
	req.Header.Set("X-Amz-Date", amzDate)
	req.Header.Set("X-Amz-Content-Sha256", payloadHash)
	req.Host = req.URL.Host
	canonicalHeaders := "host:" + strings.TrimSpace(req.Host) + "\n" +
		"x-amz-content-sha256:" + payloadHash + "\n" +
		"x-amz-date:" + amzDate + "\n"
	signedHeaders := "host;x-amz-content-sha256;x-amz-date"
	canonicalRequest := req.Method + "\n" + req.URL.EscapedPath() + "\n" +
		canonicalS3Query(req.URL.RawQuery) + "\n" + canonicalHeaders + "\n" +
		signedHeaders + "\n" + payloadHash
	credentialScope := date + "/" + s.region + "/" + s3Service + "/aws4_request"
	stringToSign := "AWS4-HMAC-SHA256\n" + amzDate + "\n" + credentialScope + "\n" +
		hexDigest([]byte(canonicalRequest))
	signingKey := hmacDigest([]byte("AWS4"+s.secret), date)
	signingKey = hmacDigest(signingKey, s.region)
	signingKey = hmacDigest(signingKey, s3Service)
	signingKey = hmacDigest(signingKey, "aws4_request")
	signature := hex.EncodeToString(hmacDigest(signingKey, stringToSign))
	req.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential="+s.access+"/"+
		credentialScope+", SignedHeaders="+signedHeaders+", Signature="+signature)
}

var emptyPayloadHash = hexDigest(nil)

func hexDigest(value []byte) string {
	digest := sha256.Sum256(value)
	return hex.EncodeToString(digest[:])
}

func hmacDigest(key []byte, value string) []byte {
	h := hmac.New(sha256.New, key)
	_, _ = h.Write([]byte(value))
	return h.Sum(nil)
}

func retryableS3Status(status int) bool {
	return status == http.StatusRequestTimeout || status == http.StatusTooManyRequests || status >= 500
}

func waitS3Retry(ctx context.Context, attempt int) error {
	delay := 25 * time.Millisecond * time.Duration(1<<min(attempt, 5))
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func drainBody(body io.Reader) error {
	_, readErr := io.Copy(io.Discard, io.LimitReader(body, s3MaxDrainBytes))
	return readErr
}

func discardAndClose(body io.ReadCloser) error {
	readErr := drainBody(body)
	closeErr := body.Close()
	if readErr != nil {
		return readErr
	}
	return closeErr
}

func readS3ErrorCode(body io.Reader) (string, error) {
	data, readErr := io.ReadAll(io.LimitReader(body, s3MaxErrorBytes))
	if readErr != nil {
		return "", readErr
	}
	var result struct {
		Code string `xml:"Code"`
	}
	if err := xml.Unmarshal(data, &result); err != nil {
		return "", err
	}
	return result.Code, nil
}

func isMissingObjectCode(code string) bool {
	switch code {
	case "NoSuchKey", "NoSuchObject", "NotFound":
		return true
	default:
		return false
	}
}
