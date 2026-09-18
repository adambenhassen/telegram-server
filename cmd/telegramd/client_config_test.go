package main

import (
	"bytes"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestRunClientConfigIsIndependentAndAtomicOnWriterFailure(t *testing.T) {
	t.Setenv("TG_POSTGRES_DSN", "")
	t.Setenv("TG_AUTHKEY_ENC_KEY", "")
	t.Setenv("TG_AUTHKEY_ENC_KEY_FILE", "")
	t.Setenv("TG_ADVERTISE_ADDR", "mtproto.example.com:443")
	t.Setenv("TG_DC_ID", "2")
	keyPath := filepath.Join(t.TempDir(), "server-key.pem")
	t.Setenv("TG_RSA_KEY_PATH", keyPath)

	var stdout bytes.Buffer
	if err := runCommand([]string{"client-config"}, slog.New(slog.DiscardHandler), &stdout, &stdout); err != nil {
		t.Fatalf("client-config: %v", err)
	}
	if stdout.Len() == 0 || !strings.Contains(stdout.String(), `"rsa_spki":"`) {
		t.Fatalf("client-config output = %q", stdout.String())
	}
	if strings.Contains(stdout.String(), "PRIVATE KEY") {
		t.Fatalf("client-config output contains private-key material: %q", stdout.String())
	}
	first := bytes.Clone(stdout.Bytes())
	if _, err := os.Stat(keyPath); err != nil {
		t.Fatalf("client key was not persisted: %v", err)
	}

	stdout.Reset()
	if err := runCommand([]string{"client-config"}, slog.New(slog.DiscardHandler), &stdout, &stdout); err != nil {
		t.Fatalf("second client-config: %v", err)
	}
	if !bytes.Equal(stdout.Bytes(), first) {
		t.Fatalf("client-config output changed for the same key:\nfirst:  %s\nsecond: %s", first, stdout.Bytes())
	}

	stdout.Reset()
	if err := runCommand([]string{"client-config"}, slog.New(slog.DiscardHandler), &failWriter{}, &stdout); err == nil {
		t.Fatal("client-config succeeded after stdout write failure")
	}
	if stdout.Len() != 0 {
		t.Fatalf("stderr after stdout failure = %q", stdout.String())
	}
}

func TestRunCommandClientConfigHelpDoesNotLoadIdentity(t *testing.T) {
	keyPath := filepath.Join(t.TempDir(), "must-not-be-created.pem")
	t.Setenv("TG_RSA_KEY_PATH", keyPath)
	var stdout, stderr bytes.Buffer
	if err := runCommand([]string{"client-config", "--help"}, slog.New(slog.DiscardHandler), &stdout, &stderr); err != nil {
		t.Fatalf("client-config help: %v", err)
	}
	if !strings.Contains(stdout.String(), "telegramd client-config") {
		t.Fatalf("help = %q", stdout.String())
	}
	if stderr.Len() != 0 {
		t.Fatalf("help stderr = %q", stderr.String())
	}
	if _, err := os.Stat(keyPath); !os.IsNotExist(err) {
		t.Fatalf("help created RSA key, stat error = %v", err)
	}
}

type failWriter struct{}

func (*failWriter) Write([]byte) (int, error) { return 0, os.ErrPermission }
