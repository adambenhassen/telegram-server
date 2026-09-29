package api_test

import (
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/gotd/td/bin"
	"github.com/gotd/td/tg"

	"github.com/adambenhassen/telegram-server/internal/api"
)

func encodedUnhandledBody(t *testing.T, request bin.Encoder) *bin.Buffer {
	t.Helper()
	var body bin.Buffer
	if err := request.Encode(&body); err != nil {
		t.Fatalf("encode request: %v", err)
	}
	return &body
}

func bodyForConstructor(id uint32) *bin.Buffer {
	var body bin.Buffer
	body.PutID(id)
	return &body
}

func TestUnhandledLogsEachDistinctMethodDuringInterval(t *testing.T) {
	t.Parallel()
	h := &captureHandler{}
	log := slog.New(h)
	conn := unhandledConn()
	conn.SetClock(&testClock{now: time.Now()})
	conn.SetLog(log)

	a := registerDeviceBody(t)
	b := encodedUnhandledBody(t, &tg.ContactsResolveUsernameRequest{Username: "private-user-marker"})
	for i, body := range []*bin.Buffer{a, a, b} {
		err := api.UnhandledForTest(log, conn, body)
		if err == nil {
			t.Fatalf("call %d returned no error", i+1)
		}
		if rpc := mustRPCError(t, err); rpc.Code != 400 || rpc.Message != "INPUT_METHOD_INVALID" {
			t.Fatalf("call %d returned %d %s, want 400 INPUT_METHOD_INVALID", i+1, rpc.Code, rpc.Message)
		}
	}
	if len(h.records) != 2 {
		t.Fatalf("records = %d, want one for each method in the A, A, B burst", len(h.records))
	}
	want := []map[string]string{
		{"type_id": "0xec86017a", "method": "account.registerDevice#ec86017a"},
		{"type_id": "0x725afbbc", "method": "contacts.resolveUsername#725afbbc"},
	}
	for i, record := range h.records {
		got := attrs(record)
		for key, value := range want[i] {
			if got[key] != value {
				t.Errorf("record %d %s = %q, want %q", i, key, got[key], value)
			}
		}
		if got["suppressed"] != "0" {
			t.Errorf("record %d suppressed = %q, want 0", i, got["suppressed"])
		}
	}
	conn.FlushUnimplementedLog()
	if len(h.records) != 3 {
		t.Fatalf("records after quiet flush = %d, want 2 method lines and one suppression summary", len(h.records))
	}
	if got := attrs(h.records[2])["suppressed"]; got != "1" {
		t.Errorf("quiet flush suppressed = %q, want the repeated A call", got)
	}
	assertUnhandledLogsOmit(t, h, "private-user-marker")
}

func TestGatedUnhandledLogsEachDistinctMethodDuringInterval(t *testing.T) {
	t.Parallel()
	h := &captureHandler{}
	log := slog.New(h)
	conn, _ := gatedConn()
	conn.SetClock(&testClock{now: time.Now()})
	conn.SetLog(log)

	ids := []uint32{tg.AccountRegisterDeviceRequestTypeID, tg.AccountRegisterDeviceRequestTypeID, tg.MessagesImportChatInviteRequestTypeID}
	for i, id := range ids {
		req := provisionalBody(t)
		req.Buf = bodyForConstructor(id)
		if i == 2 {
			req.Buf = encodedUnhandledBody(t, &tg.MessagesImportChatInviteRequest{Hash: "private-invite-marker"})
		}
		err := api.GatedUnhandledForTest(log, conn, req)
		if err == nil {
			t.Fatalf("call %d returned no error", i+1)
		}
		if rpc := mustRPCError(t, err); rpc.Code != 401 || rpc.Message != "AUTH_KEY_UNREGISTERED" {
			t.Fatalf("call %d returned %d %s, want 401 AUTH_KEY_UNREGISTERED", i+1, rpc.Code, rpc.Message)
		}
	}
	if len(h.records) != 2 {
		t.Fatalf("records = %d, want one for each provisional method", len(h.records))
	}
	for i, want := range []string{"account.registerDevice#ec86017a", "messages.importChatInvite#de91436e"} {
		if got := attrs(h.records[i])["method"]; got != want {
			t.Errorf("record %d method = %q, want %q", i, got, want)
		}
	}
	conn.FlushUnimplementedLog()
	if len(h.records) != 3 {
		t.Fatalf("records after quiet flush = %d, want 2 method lines and one suppression summary", len(h.records))
	}
	if got := attrs(h.records[2])["suppressed"]; got != "1" {
		t.Errorf("quiet flush suppressed = %q, want the repeated provisional call", got)
	}
	assertUnhandledLogsOmit(t, h, "private-invite-marker")
}

func TestUnhandledDistinctNameTrackingIsBounded(t *testing.T) {
	t.Parallel()
	methods := make([]struct {
		id   uint32
		name string
	}, 0, 34)
	seen := map[string]bool{}
	for id, name := range tg.TypesMap() {
		if !strings.Contains(name, ".") || seen[name] {
			continue
		}
		seen[name] = true
		methods = append(methods, struct {
			id   uint32
			name string
		}{id: id, name: name})
	}
	sort.Slice(methods, func(i, j int) bool { return methods[i].id < methods[j].id })
	if len(methods) < 34 {
		t.Fatalf("pinned schema has %d distinct method names, want at least 34", len(methods))
	}
	methods = methods[:34]

	h := &captureHandler{}
	log := slog.New(h)
	conn := unhandledConn()
	clock := &testClock{now: time.Now()}
	conn.SetClock(clock)
	for i := range methods {
		if err := api.UnhandledForTest(log, conn, bodyForConstructor(methods[i].id)); err == nil {
			t.Fatalf("method %q returned no error", methods[i].name)
		}
	}
	for range 2 {
		for _, method := range methods[32:34] {
			if err := api.UnhandledForTest(log, conn, bodyForConstructor(method.id)); err == nil {
				t.Fatal("overflow method returned no error")
			}
		}
	}
	if len(h.records) != 33 {
		t.Fatalf("records before interval elapsed = %d, want 32 distinct names plus one overflow sample", len(h.records))
	}
	clock.Advance(11 * time.Second)
	if err := api.UnhandledForTest(log, conn, bodyForConstructor(methods[33].id)); err == nil {
		t.Fatal("overflow method returned no error after interval")
	}
	if len(h.records) != 34 {
		t.Fatalf("records after interval elapsed = %d, want one overflow sample", len(h.records))
	}
	last := attrs(h.records[len(h.records)-1])
	if last["method"] != methods[33].name {
		t.Errorf("overflow method = %q, want %q", last["method"], methods[33].name)
	}
	if want := fmt.Sprintf("%#x", methods[33].id); last["type_id"] != want {
		t.Errorf("overflow constructor ID = %q, want %q", last["type_id"], want)
	}
	if last["suppressed"] != "5" {
		t.Errorf("overflow sample suppressed = %q, want 5", last["suppressed"])
	}
}

func assertUnhandledLogsOmit(t *testing.T, h *captureHandler, secret string) {
	t.Helper()
	allowed := map[string]bool{
		"type_id": true, "method": true, "error_code": true, "error": true, "suppressed": true,
	}
	for i, record := range h.records {
		if strings.Contains(record.Message, secret) {
			t.Errorf("record %d message contains request data", i)
		}
		for key, value := range attrs(record) {
			if !allowed[key] {
				t.Errorf("record %d has unexpected field %q", i, key)
			}
			if strings.Contains(key, secret) || strings.Contains(value, secret) {
				t.Errorf("record %d field %q contains request data", i, key)
			}
		}
	}
}
