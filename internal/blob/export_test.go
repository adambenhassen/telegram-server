package blob

import (
	"bytes"
	"context"
	"net/http"
)

// CreateBucketForTest provisions the bucket used by the integration harness
// without expanding the production Store API with lifecycle operations.
func CreateBucketForTest(ctx context.Context, s *S3) error {
	opCtx, cancel := context.WithTimeout(ctx, s.operationTimeout)
	defer cancel()
	resp, err := s.doRequest(opCtx, http.MethodPut, s.bucketPath(), "", nil, nil)
	if err != nil {
		return err
	}
	defer func() {
		_ = resp.Body.Close() //nolint:errcheck // test bucket provisioning cleanup
	}()
	return drainBody(resp.Body)
}

// PutRawObjectForTest seeds a test bucket with a key that the public Store API
// deliberately rejects, so the conformance suite can exercise objects created
// by an outside actor.
func PutRawObjectForTest(ctx context.Context, s *S3, key string, data []byte) error {
	body, err := inspectS3Body(bytes.NewReader(data))
	if err != nil {
		return err
	}
	opCtx, cancel := context.WithTimeout(ctx, s.operationTimeout)
	defer cancel()
	resp, err := s.doRequest(opCtx, http.MethodPut, s.objectPath(s.prefix+key), "", body, nil)
	if err != nil {
		return err
	}
	defer func() {
		_ = resp.Body.Close() //nolint:errcheck // test object provisioning cleanup
	}()
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		if err := drainBody(resp.Body); err != nil {
			return err
		}
		return s.operationError("put", nil)
	}
	return drainBody(resp.Body)
}
