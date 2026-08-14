package config_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/adambenhassen/telegram-server/internal/config"
)

func TestBootstrapPasswordBytes_FromEnv(t *testing.T) {
	cfg := config.Config{
		BootstrapUsername: "operator",
		BootstrapPassword: "secure-password-123",
	}
	password, err := cfg.BootstrapPasswordBytes()
	if err != nil {
		t.Fatalf("BootstrapPasswordBytes: %v", err)
	}
	if string(password) != "secure-password-123" {
		t.Errorf("password = %q, want %q", password, "secure-password-123")
	}
}

func TestBootstrapPasswordBytes_FromEnv_Trimmed(t *testing.T) {
	cfg := config.Config{
		BootstrapUsername: "operator",
		BootstrapPassword: " secure-password-123 \n",
	}
	password, err := cfg.BootstrapPasswordBytes()
	if err != nil {
		t.Fatalf("BootstrapPasswordBytes: %v", err)
	}
	if string(password) != "secure-password-123" {
		t.Errorf("password = %q, want trimmed", password)
	}
}

func TestBootstrapPasswordBytes_FromFile(t *testing.T) {
	tmp := filepath.Join(t.TempDir(), "password.txt")
	if err := os.WriteFile(tmp, []byte("secure-file-passwd\n"), 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	cfg := config.Config{
		BootstrapUsername:     "operator",
		BootstrapPasswordFile: tmp,
	}
	password, err := cfg.BootstrapPasswordBytes()
	if err != nil {
		t.Fatalf("BootstrapPasswordBytes: %v", err)
	}
	if string(password) != "secure-file-passwd" {
		t.Errorf("password = %q, want %q", password, "secure-file-passwd")
	}
}

func TestBootstrapPasswordBytes_FileNotFound(t *testing.T) {
	cfg := config.Config{
		BootstrapUsername:     "operator",
		BootstrapPasswordFile: "/nonexistent/path/password.txt",
	}
	_, err := cfg.BootstrapPasswordBytes()
	if err == nil {
		t.Error("expected error for non-existent file")
	}
}

func TestBootstrapPasswordBytes_TooShort(t *testing.T) {
	cfg := config.Config{
		BootstrapUsername: "operator",
		BootstrapPassword: "short",
	}
	_, err := cfg.BootstrapPasswordBytes()
	if err == nil {
		t.Fatal("expected error for short password")
	}
	if !strings.Contains(err.Error(), "TG_BOOTSTRAP_PASSWORD") {
		t.Errorf("error %q does not name TG_BOOTSTRAP_PASSWORD", err)
	}
}

func TestBootstrapPasswordBytes_FileTooShort(t *testing.T) {
	tmp := filepath.Join(t.TempDir(), "password.txt")
	if err := os.WriteFile(tmp, []byte("short"), 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	cfg := config.Config{
		BootstrapUsername:     "operator",
		BootstrapPasswordFile: tmp,
	}
	_, err := cfg.BootstrapPasswordBytes()
	if err == nil {
		t.Fatal("expected error for short password from file")
	}
	if !strings.Contains(err.Error(), "TG_BOOTSTRAP_PASSWORD_FILE") {
		t.Errorf("error %q does not name TG_BOOTSTRAP_PASSWORD_FILE", err)
	}
}

func TestBootstrapPasswordBytes_EmptyFile(t *testing.T) {
	tmp := filepath.Join(t.TempDir(), "password.txt")
	if err := os.WriteFile(tmp, []byte(""), 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	cfg := config.Config{
		BootstrapUsername:     "operator",
		BootstrapPasswordFile: tmp,
	}
	_, err := cfg.BootstrapPasswordBytes()
	if err == nil {
		t.Fatal("expected error for empty password file")
	}
}
