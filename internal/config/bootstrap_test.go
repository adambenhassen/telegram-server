package config_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/adambenhassen/telegram-server/internal/config"
)

func TestBootstrapPasswordBytes_FromEnv(t *testing.T) {
	cfg := config.Config{
		BootstrapUsername: "operator",
		BootstrapPassword: "secret",
	}
	password, err := cfg.BootstrapPasswordBytes()
	if err != nil {
		t.Fatalf("BootstrapPasswordBytes: %v", err)
	}
	if string(password) != "secret" {
		t.Errorf("password = %q, want %q", password, "secret")
	}
}

func TestBootstrapPasswordBytes_FromFile(t *testing.T) {
	tmp := filepath.Join(t.TempDir(), "password.txt")
	if err := os.WriteFile(tmp, []byte("file-password\n"), 0o600); err != nil {
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
	if string(password) != "file-password" {
		t.Errorf("password = %q, want %q", password, "file-password")
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
