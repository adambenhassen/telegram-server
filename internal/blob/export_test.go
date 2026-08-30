package blob

import (
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
