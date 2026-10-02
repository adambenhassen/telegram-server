package api

import (
	"context"
	"testing"
	"time"
)

func TestChatCompletionContextIgnoresPeerCancelAndKeepsDeadline(t *testing.T) {
	t.Parallel()

	deadline := time.Now().Add(250 * time.Millisecond)
	rpcCtx, cancelRPC := context.WithDeadline(context.Background(), deadline)
	defer cancelRPC()
	serverCtx, cancelServer := context.WithCancel(t.Context())
	defer cancelServer()
	completionCtx, cancelCompletion := chatCompletionContext(serverCtx, rpcCtx)
	defer cancelCompletion()

	if got, ok := completionCtx.Deadline(); !ok || !got.Equal(deadline) {
		t.Fatalf("completion deadline = %s, %t; want %s", got, ok, deadline)
	}
	cancelRPC()
	if err := completionCtx.Err(); err != nil {
		t.Fatalf("peer cancellation cancelled committed completion: %v", err)
	}

	select {
	case <-completionCtx.Done():
		if completionCtx.Err() != context.DeadlineExceeded {
			t.Fatalf("completion deadline error = %v, want deadline exceeded", completionCtx.Err())
		}
	case <-time.After(time.Until(deadline) + time.Second):
		t.Fatal("completion did not retain the original RPC deadline")
	}
}

func TestChatCompletionContextStopsOnServerShutdown(t *testing.T) {
	t.Parallel()

	rpcCtx, cancelRPC := context.WithCancel(t.Context())
	defer cancelRPC()
	serverCtx, cancelServer := context.WithCancel(t.Context())
	completionCtx, cancelCompletion := chatCompletionContext(serverCtx, rpcCtx)
	defer cancelCompletion()

	cancelRPC()
	if err := completionCtx.Err(); err != nil {
		t.Fatalf("peer cancellation cancelled committed completion: %v", err)
	}
	cancelServer()
	select {
	case <-completionCtx.Done():
		if completionCtx.Err() != context.Canceled {
			t.Fatalf("server shutdown error = %v, want canceled", completionCtx.Err())
		}
	case <-time.After(time.Second):
		t.Fatal("server shutdown did not cancel committed completion")
	}
}
