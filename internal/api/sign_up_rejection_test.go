package api_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"log/slog"
	"net/netip"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/gotd/td/bin"
	"github.com/gotd/td/tg"
	"github.com/jackc/pgx/v5"

	"github.com/teagramhq/teagram-server/internal/api"
	"github.com/teagramhq/teagram-server/internal/config"
	"github.com/teagramhq/teagram-server/internal/mtproto"
	"github.com/teagramhq/teagram-server/internal/store"
)

func sampledSignUpRejectionLogs(h *captureHandler) []slog.Record {
	var records []slog.Record
	for _, record := range h.records {
		if record.Message == "auth.signUp rejected" {
			records = append(records, record)
		}
	}
	return records
}

func assertSignUpRejectionLog(t *testing.T, h *captureHandler, reason, mode string, suppressed string) slog.Record {
	t.Helper()
	var matches []slog.Record
	for _, record := range sampledSignUpRejectionLogs(h) {
		fields := attrs(record)
		if fields["reason"] == reason && fields["suppressed"] == suppressed {
			matches = append(matches, record)
		}
	}
	if len(matches) != 1 {
		t.Fatalf("captured %d sampled logs for reason %q, want 1; records = %#v", len(matches), reason, sampledSignUpRejectionLogs(h))
	}
	record := matches[0]
	if record.Level != slog.LevelWarn {
		t.Errorf("level = %v, want %v", record.Level, slog.LevelWarn)
	}
	fields := attrs(record)
	want := map[string]string{
		"reason":            reason,
		"registration_mode": mode,
		"suppressed":        suppressed,
	}
	if len(fields) != len(want) {
		t.Errorf("fields = %#v, want only %#v", fields, want)
	}
	for key, value := range want {
		if fields[key] != value {
			t.Errorf("field %q = %q, want %q", key, fields[key], value)
		}
	}
	return record
}

func signUpRequest(handle, hash, firstName, lastName string) *tg.AuthSignUpRequest {
	return &tg.AuthSignUpRequest{
		PhoneNumber:   handle,
		PhoneCodeHash: hash,
		FirstName:     firstName,
		LastName:      lastName,
	}
}

func newLoggedSignUpRunner(
	t *testing.T,
	s *store.Store,
	keyID [8]byte,
	mode config.RegistrationMode,
	limit store.RateLimitConfig,
) (*api.SignUpTestRunner, *captureHandler) {
	t.Helper()
	if keyID != [8]byte{} {
		if err := s.SaveAuthKey(context.Background(), mtproto.AuthKeyIDInt64(keyID), make([]byte, 256)); err != nil {
			t.Fatalf("save auth key: %v", err)
		}
	}
	logs := &captureHandler{}
	runner := api.NewSignUpTestRunner(
		s,
		keyID,
		netip.MustParseAddr("203.0.113.211"),
		limit,
		mode,
		slog.New(logs),
	)
	return runner, logs
}

func TestSignUpRejectionLogsClassifyEveryFailure(t *testing.T) {
	t.Parallel()
	s := openStore(t)
	keyNumber := byte(1)
	newRunner := func(mode config.RegistrationMode, limit store.RateLimitConfig) (*api.SignUpTestRunner, *captureHandler) {
		keyID := [8]byte{keyNumber, 0x41, 0x52, 0x63, 0x74, 0x85, 0x96, 0xa7}
		keyNumber++
		return newLoggedSignUpRunner(t, s, keyID, mode, limit)
	}
	validRequest := func(handle, hash string) *tg.AuthSignUpRequest {
		return signUpRequest(handle, hash, "First", "Last")
	}

	t.Run("malformed request", func(t *testing.T) {
		runner, logs := newRunner(config.RegistrationOpen, store.RateLimitConfig{})
		if _, err := runner.SignUpBody(&bin.Buffer{}); err == nil {
			t.Fatal("malformed request accepted")
		}
		assertSignUpRejectionLog(t, logs, "malformed_request", "open", "0")
	})

	t.Run("closed mode", func(t *testing.T) {
		runner, logs := newRunner(config.RegistrationClosed, store.RateLimitConfig{})
		_, err := runner.SignUp(validRequest("closedhandle", "hash"))
		if !isInputRequestInvalid(err) {
			t.Fatalf("closed mode error = %v, want INPUT_REQUEST_INVALID", err)
		}
		assertSignUpRejectionLog(t, logs, "registration_mode_unavailable", "closed", "0")
	})

	t.Run("unknown mode", func(t *testing.T) {
		const mode = config.RegistrationMode("unknown-mode")
		runner, logs := newRunner(mode, store.RateLimitConfig{})
		_, err := runner.SignUp(validRequest("unknownhandle", "hash"))
		if !isInputRequestInvalid(err) {
			t.Fatalf("unknown mode error = %v, want INPUT_REQUEST_INVALID", err)
		}
		assertSignUpRejectionLog(t, logs, "registration_mode_unavailable", string(mode), "0")
	})

	t.Run("rate limited", func(t *testing.T) {
		limit := store.RateLimitConfig{Limit: 1, Window: time.Hour}
		runner, logs := newRunner(config.RegistrationOpen, limit)
		if _, err := runner.SignUp(validRequest("ratehandle", "missing-hash-one")); signUpRPCMessage(err) != "PHONE_CODE_INVALID" {
			t.Fatalf("first request error = %v, want PHONE_CODE_INVALID", err)
		}
		_, err := runner.SignUp(validRequest("ratehandle", "missing-hash-two"))
		if !isFloodWait(err) {
			t.Fatalf("second request error = %v, want FLOOD_WAIT", err)
		}
		assertSignUpRejectionLog(t, logs, "rate_limited", "open", "0")
	})

	t.Run("invalid handle", func(t *testing.T) {
		runner, logs := newRunner(config.RegistrationOpen, store.RateLimitConfig{})
		_, err := runner.SignUp(validRequest("bad!handle", "hash"))
		if signUpRPCMessage(err) != "USERNAME_INVALID" {
			t.Fatalf("invalid handle error = %v, want USERNAME_INVALID", err)
		}
		assertSignUpRejectionLog(t, logs, "handle_invalid", "open", "0")
	})

	t.Run("reserved handle", func(t *testing.T) {
		runner, logs := newRunner(config.RegistrationOpen, store.RateLimitConfig{})
		_, err := runner.SignUp(validRequest("me", "hash"))
		if signUpRPCMessage(err) != "USERNAME_INVALID" {
			t.Fatalf("reserved handle error = %v, want USERNAME_INVALID", err)
		}
		assertSignUpRejectionLog(t, logs, "handle_invalid", "open", "0")
	})

	t.Run("invalid first name", func(t *testing.T) {
		runner, logs := newRunner(config.RegistrationOpen, store.RateLimitConfig{})
		_, err := runner.SignUp(signUpRequest("validfirst", "hash", "bad\x00name", "Last"))
		if signUpRPCMessage(err) != "FIRSTNAME_INVALID" {
			t.Fatalf("invalid first name error = %v, want FIRSTNAME_INVALID", err)
		}
		assertSignUpRejectionLog(t, logs, "first_name_invalid", "open", "0")
	})

	t.Run("invalid last name", func(t *testing.T) {
		runner, logs := newRunner(config.RegistrationOpen, store.RateLimitConfig{})
		_, err := runner.SignUp(signUpRequest("validlast", "hash", "First", "bad\x00name"))
		if signUpRPCMessage(err) != "LASTNAME_INVALID" {
			t.Fatalf("invalid last name error = %v, want LASTNAME_INVALID", err)
		}
		assertSignUpRejectionLog(t, logs, "last_name_invalid", "open", "0")
	})

	t.Run("invalid code", func(t *testing.T) {
		runner, logs := newRunner(config.RegistrationOpen, store.RateLimitConfig{})
		_, err := runner.SignUp(validRequest("validcode", "missing-hash"))
		if signUpRPCMessage(err) != "PHONE_CODE_INVALID" {
			t.Fatalf("invalid code error = %v, want PHONE_CODE_INVALID", err)
		}
		assertSignUpRejectionLog(t, logs, "code_invalid", "open", "0")
	})

	t.Run("invalid invite", func(t *testing.T) {
		const handle = "invitehandle"
		invite, _, err := s.IssueInvite(context.Background(), handle)
		if err != nil {
			t.Fatal(err)
		}
		hash, _, err := s.IssueCodeForUsername(context.Background(), handle)
		if err != nil {
			t.Fatal(err)
		}
		if err := s.SetCodeForUsername(context.Background(), handle, hash, "invalid-invite-value"); err != nil {
			t.Fatal(err)
		}
		runner, logs := newRunner(config.RegistrationInvite, store.RateLimitConfig{})
		_, err = runner.SignUp(validRequest(handle, hash))
		if signUpRPCMessage(err) != "INVITE_HASH_INVALID" {
			t.Fatalf("invalid invite error = %v, want INVITE_HASH_INVALID", err)
		}
		record := assertSignUpRejectionLog(t, logs, "invite_invalid", "invite", "0")
		assertLogOmits(t, []slog.Record{record}, strconv.FormatInt(invite.ID, 10), "invalid-invite-value")
	})

	t.Run("occupied handle", func(t *testing.T) {
		const handle = "occupiedsignup"
		user, err := s.CreateUsernameUser(context.Background(), handle, "Existing", "User")
		if err != nil {
			t.Fatal(err)
		}
		if err := s.ClaimUsername(context.Background(), user.ID, handle); err != nil {
			t.Fatal(err)
		}
		hash, _, err := s.IssueCodeForUsername(context.Background(), handle)
		if err != nil {
			t.Fatal(err)
		}
		runner, logs := newRunner(config.RegistrationOpen, store.RateLimitConfig{})
		_, err = runner.SignUp(validRequest(handle, hash))
		if signUpRPCMessage(err) != "USERNAME_OCCUPIED" {
			t.Fatalf("occupied handle error = %v, want USERNAME_OCCUPIED", err)
		}
		assertSignUpRejectionLog(t, logs, "handle_occupied", "open", "0")
	})

	t.Run("ineligible session key", func(t *testing.T) {
		keyID := [8]byte{keyNumber, 0x41, 0x52, 0x63, 0x74, 0x85, 0x96, 0xa7}
		keyNumber++
		if err := s.SaveAuthKey(context.Background(), mtproto.AuthKeyIDInt64(keyID), make([]byte, 256)); err != nil {
			t.Fatal(err)
		}
		user, err := s.CreateUsernameUser(context.Background(), "sessionowner", "Session", "Owner")
		if err != nil {
			t.Fatal(err)
		}
		if err := s.BindAuthKeyUser(context.Background(), mtproto.AuthKeyIDInt64(keyID), user.ID); err != nil {
			t.Fatal(err)
		}
		logs := &captureHandler{}
		runner := api.NewSignUpTestRunner(s, keyID, netip.MustParseAddr("203.0.113.211"), store.RateLimitConfig{}, config.RegistrationOpen, slog.New(logs))
		_, err = runner.SignUp(validRequest("validsession", "hash"))
		if signUpRPCMessage(err) != "SESSION_STATE_INVALID" {
			t.Fatalf("ineligible key error = %v, want SESSION_STATE_INVALID", err)
		}
		assertSignUpRejectionLog(t, logs, "session_state_invalid", "open", "0")
	})

	t.Run("missing session key", func(t *testing.T) {
		runner, logs := newLoggedSignUpRunner(t, s, [8]byte{}, config.RegistrationOpen, store.RateLimitConfig{})
		_, err := runner.SignUp(validRequest("missingkey", "hash"))
		if signUpRPCMessage(err) != "SESSION_STATE_INVALID" {
			t.Fatalf("missing key error = %v, want SESSION_STATE_INVALID", err)
		}
		assertSignUpRejectionLog(t, logs, "session_state_invalid", "open", "0")
	})

	t.Run("internal failure retains existing error report", func(t *testing.T) {
		runner, logs := newRunner(config.RegistrationOpen, store.RateLimitConfig{})
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		_, err := runner.SignUpWithContext(ctx, validRequest("internalfailure", "hash"))
		if signUpRPCMessage(err) != "INTERNAL" {
			t.Fatalf("internal failure error = %v, want INTERNAL", err)
		}
		assertSignUpRejectionLog(t, logs, "internal", "open", "0")
		foundUnsampledError := false
		for _, record := range logs.records {
			if record.Message == "sign up: inspect auth key" {
				foundUnsampledError = true
			}
		}
		if !foundUnsampledError {
			t.Error("unexpected storage failure lost its existing unsampled error record")
		}
	})
}

func TestSignUpRejectionSamplingIsIndependentAndRedactsRequestData(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s, dsn := openStoreDSN(t)
	const (
		userIDBase   = int64(987654320)
		inviteIDBase = int64(876543210)
	)
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connect to set test sequences: %v", err)
	}
	t.Cleanup(func() {
		if err := conn.Close(ctx); err != nil {
			t.Errorf("close sequence connection: %v", err)
		}
	})
	for _, setup := range []struct {
		query string
		value int64
	}{
		{query: "SELECT setval(pg_get_serial_sequence('users', 'id')::regclass, $1, true)", value: userIDBase},
		{query: "SELECT setval(pg_get_serial_sequence('registration_invites', 'id')::regclass, $1, true)", value: inviteIDBase},
	} {
		var got int64
		if err := conn.QueryRow(ctx, setup.query, setup.value).Scan(&got); err != nil {
			t.Fatalf("set test sequence: %v", err)
		}
	}

	keyID := [8]byte{0x12, 0x34, 0x56, 0x78, 0x9a, 0xbc, 0xde, 0xf0}
	keyIDInt := mtproto.AuthKeyIDInt64(keyID)
	if err := s.SaveAuthKey(ctx, keyIDInt, make([]byte, 256)); err != nil {
		t.Fatalf("save auth key: %v", err)
	}
	clientAddr := netip.MustParseAddr("203.0.113.211")
	logs := &captureHandler{}
	runner := api.NewSignUpTestRunner(s, keyID, clientAddr, store.RateLimitConfig{}, config.RegistrationOpen, slog.New(logs))
	const (
		handle         = "DistinctiveHandle73"
		firstName      = "DistinctiveFirstName"
		lastName       = "DistinctiveLastName"
		codeHash       = "DistinctivePhoneCodeHash_92a1"
		inviteHandle   = "DistinctiveInvite73"
		phoneCodeValue = "DistinctiveInviteValue_7755"
	)
	request := signUpRequest(handle, codeHash, firstName, lastName)
	_, err = runner.SignUp(request)
	rpcErr := mustRPCError(t, err)
	if rpcErr.Code != 400 || rpcErr.Message != "PHONE_CODE_INVALID" {
		t.Fatalf("invalid code error = %v, want PHONE_CODE_INVALID", err)
	}
	first := assertSignUpRejectionLog(t, logs, "code_invalid", "open", "0")
	if got := attrs(first)["suppressed"]; got != "0" {
		t.Fatalf("first code rejection suppressed = %q, want 0", got)
	}

	for attempt := range 2 {
		if _, err := runner.SignUp(request); signUpRPCMessage(err) != "PHONE_CODE_INVALID" {
			t.Fatalf("repeated invalid code %d error = %v, want PHONE_CODE_INVALID", attempt+1, err)
		}
	}
	if got := len(sampledSignUpRejectionLogs(logs)); got != 1 {
		t.Fatalf("sampled %d code rejection records before the interval, want 1", got)
	}
	secondKeyID := [8]byte{0x21, 0x43, 0x65, 0x87, 0xa9, 0xcb, 0xed, 0xf1}
	if err := s.SaveAuthKey(ctx, mtproto.AuthKeyIDInt64(secondKeyID), make([]byte, 256)); err != nil {
		t.Fatalf("save second auth key: %v", err)
	}
	owner, err := s.CreateUsernameUser(ctx, "redactionowner", "Owner", "Account")
	if err != nil {
		t.Fatalf("create session owner: %v", err)
	}
	if owner.ID != userIDBase+1 {
		t.Fatalf("session owner id = %d, want %d", owner.ID, userIDBase+1)
	}
	if err := s.BindAuthKeyUser(ctx, keyIDInt, owner.ID); err != nil {
		t.Fatalf("bind session key: %v", err)
	}
	_, err = runner.SignUp(request)
	if signUpRPCMessage(err) != "SESSION_STATE_INVALID" {
		t.Fatalf("bound key error = %v, want SESSION_STATE_INVALID", err)
	}
	assertSignUpRejectionLog(t, logs, "session_state_invalid", "open", "0")
	runner.SetAuthKeyIDForTest(secondKeyID)

	invite, validInviteSecret, err := s.IssueInvite(ctx, inviteHandle)
	if err != nil {
		t.Fatalf("issue invite: %v", err)
	}
	inviteCodeHash, _, err := s.IssueCodeForUsername(ctx, strings.ToLower(inviteHandle))
	if err != nil {
		t.Fatalf("issue invite code: %v", err)
	}
	if err := s.SetCodeForUsername(ctx, strings.ToLower(inviteHandle), inviteCodeHash, phoneCodeValue); err != nil {
		t.Fatalf("set invite code value: %v", err)
	}
	runner.SetRegistrationModeForTest(config.RegistrationInvite)
	_, err = runner.SignUp(signUpRequest(inviteHandle, inviteCodeHash, firstName, lastName))
	if signUpRPCMessage(err) != "INVITE_HASH_INVALID" {
		t.Fatalf("invalid invite error = %v, want INVITE_HASH_INVALID", err)
	}
	assertSignUpRejectionLog(t, logs, "invite_invalid", "invite", "0")

	runner.AdvanceClockForTest(11 * time.Second)
	runner.SetRegistrationModeForTest(config.RegistrationOpen)
	if _, err := runner.SignUp(request); signUpRPCMessage(err) != "PHONE_CODE_INVALID" {
		t.Fatalf("invalid code after sample interval = %v, want PHONE_CODE_INVALID", err)
	}
	assertSignUpRejectionLog(t, logs, "code_invalid", "open", "2")

	handleLower := strings.ToLower(handle)
	handleDigest := sha256.Sum256([]byte(handleLower))
	inviteHandleLower := strings.ToLower(inviteHandle)
	inviteHandleDigest := sha256.Sum256([]byte(inviteHandleLower))
	inviteDigest := sha256.Sum256([]byte(validInviteSecret))
	phoneCodeDigest := sha256.Sum256([]byte(phoneCodeValue))
	ipBucket, ok := store.IPBucketKey(clientAddr)
	if !ok {
		t.Fatal("client address did not produce a sign-up rate-limit bucket")
	}
	keyHexBytes := hex.EncodeToString(keyID[:])
	keyHexInt := strconv.FormatInt(keyIDInt, 16)
	secondKeyIDInt := mtproto.AuthKeyIDInt64(secondKeyID)
	assertLogOmits(t, sampledSignUpRejectionLogs(logs),
		handle,
		handleLower,
		strings.ToUpper(handle),
		hex.EncodeToString(handleDigest[:]),
		firstName,
		lastName,
		codeHash,
		inviteHandle,
		inviteHandleLower,
		strings.ToUpper(inviteHandle),
		hex.EncodeToString(inviteHandleDigest[:]),
		inviteCodeHash,
		phoneCodeValue,
		validInviteSecret,
		hex.EncodeToString(inviteDigest[:]),
		hex.EncodeToString(phoneCodeDigest[:]),
		strconv.FormatInt(invite.ID, 10),
		clientAddr.String(),
		ipBucket.String(),
		strconv.FormatInt(keyIDInt, 10),
		keyHexBytes,
		keyHexInt,
		strconv.FormatInt(secondKeyIDInt, 10),
		hex.EncodeToString(secondKeyID[:]),
		strconv.FormatInt(secondKeyIDInt, 16),
		strconv.FormatInt(owner.ID, 10),
		"phone code invalid",
	)
}

func assertLogOmits(t *testing.T, records []slog.Record, values ...string) {
	t.Helper()
	var text strings.Builder
	for _, record := range records {
		text.WriteString(record.Message)
		record.Attrs(func(attr slog.Attr) bool {
			text.WriteString(attr.Key)
			text.WriteString("=")
			text.WriteString(attr.Value.String())
			text.WriteString(" ")
			return true
		})
	}
	for _, value := range values {
		if value != "" && strings.Contains(text.String(), value) {
			t.Errorf("sampled rejection log contains sensitive value %q: %q", value, text.String())
		}
	}
}
