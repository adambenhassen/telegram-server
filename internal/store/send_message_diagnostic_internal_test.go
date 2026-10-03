package store

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5"
)

func TestSendMessageDiagnosticRetainsOnlyItsBackendHandle(t *testing.T) {
	diagnostic := NewSendMessageDiagnosticForTesting()
	ctx := ContextWithSendMessageDiagnosticForTesting(context.Background(), diagnostic)
	if got := sendMessageDiagnosticFromContext(ctx); got != diagnostic {
		t.Fatal("context did not retain its diagnostic handle")
	}

	backend := &pgx.Conn{}
	diagnostic.captureBackend(backend)
	if got := diagnostic.BackendConnForTesting(); got != backend {
		t.Fatal("diagnostic did not retain the observed backend handle")
	}

	diagnostic.ClearBackendForTesting()
	if got := diagnostic.BackendConnForTesting(); got != nil {
		t.Fatal("diagnostic retained the backend handle after clearing")
	}
}
