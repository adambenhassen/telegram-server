package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/adambenhassen/telegram-server/internal/catalog"
)

func TestBuildCommandProducesCanonicalEnglishArtifactOffline(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	sourcePath := filepath.Join(dir, "lang.strings")
	artifactPath := filepath.Join(dir, "english.catalog.json")
	raw := []byte("\"key\" = \"value\";\n")
	if err := os.WriteFile(sourcePath, raw, 0o600); err != nil {
		t.Fatalf("write source: %v", err)
	}
	sum := sha256.Sum256(raw)
	var stdout, stderr bytes.Buffer
	if err := run([]string{
		"build",
		"--input", sourcePath,
		"--output", artifactPath,
		"--source-url", "https://example.test/tdesktop/lang.strings",
		"--source-revision", "revision",
		"--source-sha256", hex.EncodeToString(sum[:]),
	}, &stdout, &stderr); err != nil {
		t.Fatalf("build: %v", err)
	}
	data, err := os.ReadFile(artifactPath)
	if err != nil {
		t.Fatalf("read artifact: %v", err)
	}
	artifact, err := catalog.ParseArtifact(data)
	if err != nil {
		t.Fatalf("parse artifact: %v", err)
	}
	if artifact.LanguageCode != catalog.LanguageEnglish || len(artifact.Entries) != 1 || stderr.Len() != 0 {
		t.Fatalf("artifact = %+v, stderr = %q", artifact, stderr.String())
	}
}

func TestBuildCommandRejectsNonEnglishInputWithFixedDiagnostic(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	sourcePath := filepath.Join(dir, "lang.strings")
	raw := []byte("\"key\" = \"value\";\n")
	if err := os.WriteFile(sourcePath, raw, 0o600); err != nil {
		t.Fatalf("write source: %v", err)
	}
	sum := sha256.Sum256(raw)
	err := run([]string{
		"build",
		"--input", sourcePath,
		"--output", filepath.Join(dir, "artifact.json"),
		"--language-code", "de",
		"--source-url", "https://example.test/tdesktop/lang.strings",
		"--source-revision", "revision",
		"--source-sha256", hex.EncodeToString(sum[:]),
	}, &bytes.Buffer{}, &bytes.Buffer{})
	if err == nil || !strings.Contains(err.Error(), catalog.ErrEnglishOnly.Error()) {
		t.Fatalf("error = %v, want fixed English-only diagnostic", err)
	}
}
