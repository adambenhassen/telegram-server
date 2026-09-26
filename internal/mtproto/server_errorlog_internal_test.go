package mtproto

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/gotd/td/exchange"
)

type serverErrorLogRecord struct {
	message string
	attrs   map[string]any
}

type serverErrorLogSink struct {
	records []serverErrorLogRecord
}

func (*serverErrorLogSink) Enabled(context.Context, slog.Level) bool { return true }

func (s *serverErrorLogSink) Handle(_ context.Context, record slog.Record) error {
	attrs := make(map[string]any)
	record.Attrs(func(attr slog.Attr) bool {
		attrs[attr.Key] = attr.Value.Any()
		return true
	})
	s.records = append(s.records, serverErrorLogRecord{message: record.Message, attrs: attrs})
	return nil
}

func (s *serverErrorLogSink) WithAttrs([]slog.Attr) slog.Handler { return s }
func (s *serverErrorLogSink) WithGroup(string) slog.Handler      { return s }

func TestConnectionFailureLogsUseSampledSafeCategories(t *testing.T) {
	sink := &serverErrorLogSink{}
	server := New(exchange.PrivateKey{}, 2, NewMemoryAuthKeyStore(), nil, slog.New(sink))
	const rawDetail = "UNTRUSTED-ERROR-DETAIL"
	tests := []struct {
		name     string
		err      error
		category string
	}{
		{name: "key persistence", err: errors.Join(errAuthKeyPersistenceFailure, errors.New(rawDetail)), category: "key_persistence"},
		{name: "exchange", err: errors.Join(errAuthKeyExchangeFailure, errors.New(rawDetail)), category: "exchange"},
		{name: "request handling", err: errors.Join(errRequestHandlingFailure, errors.New(rawDetail)), category: "request_handling"},
	}

	for _, test := range tests {
		server.logConnectionFailure(test.err)
	}
	server.logConnectionFailure(tests[0].err)
	if len(sink.records) != len(tests) {
		t.Fatalf("records after sampled repeats = %d, want %d", len(sink.records), len(tests))
	}

	for i, test := range tests {
		record := sink.records[i]
		if record.attrs["category"] != test.category {
			t.Errorf("%s category = %v, want %q", test.name, record.attrs["category"], test.category)
		}
		if record.attrs["suppressed"] != int64(0) {
			t.Errorf("first %s suppressed count = %v, want 0", test.name, record.attrs["suppressed"])
		}
		if strings.Contains(record.message+fmt.Sprint(record.attrs), rawDetail) {
			t.Errorf("%s log exposed raw error details: %+v", test.name, record)
		}
	}

	server.keyPersistenceErrorLog.last.Store(time.Now().Add(-preAuthLogInterval).UnixNano())
	server.logConnectionFailure(tests[0].err)
	if len(sink.records) != len(tests)+1 {
		t.Fatalf("records after persistence sample window = %d, want %d", len(sink.records), len(tests)+1)
	}
	if got := sink.records[len(sink.records)-1].attrs["suppressed"]; got != int64(1) {
		t.Fatalf("key persistence suppressed count = %v, want 1", got)
	}
}

func TestConnectionFailureLogsAuthKeyLookupWithSampledCategory(t *testing.T) {
	sink := &serverErrorLogSink{}
	server := New(exchange.PrivateKey{}, 2, NewMemoryAuthKeyStore(), nil, slog.New(sink))
	const rawDetail = "UNTRUSTED-LOOKUP-ERROR"
	err := errors.Join(errAuthKeyLookupFailure, errors.New(rawDetail))

	server.logConnectionFailure(err)
	server.logConnectionFailure(err)
	if len(sink.records) != 1 {
		t.Fatalf("records after sampled lookup repeats = %d, want 1", len(sink.records))
	}
	if record := sink.records[0]; record.message != "auth key lookup failed" || record.attrs["category"] != "auth_key_lookup" {
		t.Fatalf("lookup failure record = %+v, want fixed auth_key_lookup category", record)
	}
	if strings.Contains(sink.records[0].message+fmt.Sprint(sink.records[0].attrs), rawDetail) {
		t.Fatalf("lookup log exposed raw error details: %+v", sink.records[0])
	}

	server.authKeyLookupErrorLog.last.Store(time.Now().Add(-preAuthLogInterval).UnixNano())
	server.logConnectionFailure(err)
	if len(sink.records) != 2 {
		t.Fatalf("records after lookup sample window = %d, want 2", len(sink.records))
	}
	if got := sink.records[1].attrs["suppressed"]; got != int64(1) {
		t.Fatalf("lookup suppressed count = %v, want 1", got)
	}
}
