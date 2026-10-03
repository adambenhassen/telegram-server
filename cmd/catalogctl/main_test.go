package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/teagramhq/teagram-server/internal/catalog"
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

func TestBuildCommandAcceptsUpstreamBlockHeaderOffline(t *testing.T) {
	t.Parallel()
	sourcePath := filepath.Join("..", "..", "internal", "catalog", "testdata", "tdesktop_header.strings")
	raw, err := os.ReadFile(sourcePath)
	if err != nil {
		t.Fatalf("read header fixture: %v", err)
	}
	sum := sha256.Sum256(raw)
	checksum := hex.EncodeToString(sum[:])
	var stdout, stderr bytes.Buffer
	if err := run([]string{
		"build", "--input", sourcePath, "--output", "-",
		"--source-url", "https://example.test/tdesktop/lang.strings",
		"--source-revision", "revision", "--source-sha256", checksum,
	}, &stdout, &stderr); err != nil {
		t.Fatalf("build unchanged source: %v", err)
	}
	artifact, err := catalog.ParseArtifact(stdout.Bytes())
	if err != nil {
		t.Fatalf("parse artifact: %v", err)
	}
	if stderr.Len() != 0 || artifact.Source.SHA256 != checksum || len(artifact.Entries) != 2 ||
		artifact.Entries[0].Key != "lng_language_name" || artifact.Entries[0].Value != "English" ||
		artifact.Entries[1].Key != "lng_switch_to_this" || artifact.Entries[1].Value != "Continue in English" {
		t.Fatalf("artifact = %+v, stderr = %q", artifact, stderr.String())
	}
}

func TestBuildCommandRejectsUnterminatedBlockWithoutWritingArtifact(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	sourcePath := filepath.Join(dir, "lang.strings")
	artifactPath := filepath.Join(dir, "artifact.json")
	raw := []byte("\"key\" = \"value\";\n/* PRIVATE_TRANSLATION")
	if err := os.WriteFile(sourcePath, raw, 0o600); err != nil {
		t.Fatalf("write source: %v", err)
	}
	sum := sha256.Sum256(raw)
	var stdout, stderr bytes.Buffer
	err := run([]string{
		"build", "--input", sourcePath, "--output", artifactPath,
		"--source-url", "https://example.test/tdesktop/lang.strings",
		"--source-revision", "revision", "--source-sha256", hex.EncodeToString(sum[:]),
	}, &stdout, &stderr)
	if !errors.Is(err, catalog.ErrMalformedEntry) || strings.Contains(err.Error(), "PRIVATE_TRANSLATION") || stdout.Len() != 0 || stderr.Len() != 0 {
		t.Fatalf("build error = %v, stdout = %q, stderr = %q", err, stdout.String(), stderr.String())
	}
	if _, err := os.Stat(artifactPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("invalid source wrote an artifact: %v", err)
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

func TestPublishUsageDoesNotPrintDSNFromEnvironment(t *testing.T) {
	const sentinel = "SENTINEL_PASSWORD"
	t.Setenv("TG_POSTGRES_DSN", "dsn-"+sentinel)
	for _, args := range [][]string{
		{"publish", "-h"},
		{"publish", "--unknown-flag"},
	} {
		name := strings.Join(args[1:], "-")
		t.Run(name, func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			err := run(args, &stdout, &stderr)
			if err == nil {
				t.Fatal("publish unexpectedly succeeded")
			}
			if strings.Contains(stderr.String(), sentinel) || strings.Contains(err.Error(), sentinel) {
				t.Fatalf("publish usage exposed the environment DSN: stderr=%q error=%q", stderr.String(), err)
			}
		})
	}
}

func TestPublishRejectsUntrackedArtifact(t *testing.T) {
	t.Parallel()
	repo := initCatalogTestRepo(t)
	if err := os.WriteFile(filepath.Join(repo, "catalog.json"), []byte("not an artifact"), 0o600); err != nil {
		t.Fatalf("write artifact: %v", err)
	}
	assertPublishRejectsArtifactPath(t, repo, "catalog.json")
}

func TestPublishRejectsArtifactOutsideRepository(t *testing.T) {
	t.Parallel()
	repo := initCatalogTestRepo(t)
	outsideDir := t.TempDir()
	outsideArtifact := filepath.Join(outsideDir, "catalog.json")
	if err := os.WriteFile(outsideArtifact, []byte("not an artifact"), 0o600); err != nil {
		t.Fatalf("write artifact: %v", err)
	}
	assertPublishRejectsArtifactPath(t, repo, outsideArtifact)
}

func TestPublishRejectsArtifactSymlinkOutsideRepository(t *testing.T) {
	t.Parallel()
	repo := initCatalogTestRepo(t)
	outsideDir := t.TempDir()
	outsideArtifact := filepath.Join(outsideDir, "catalog.json")
	if err := os.WriteFile(outsideArtifact, []byte("not an artifact"), 0o600); err != nil {
		t.Fatalf("write artifact: %v", err)
	}
	if err := os.Symlink(outsideArtifact, filepath.Join(repo, "catalog.json")); err != nil {
		t.Fatalf("symlink artifact: %v", err)
	}
	assertPublishRejectsArtifactPath(t, repo, "catalog.json")
}

func TestResolveReviewedArtifactMatchesCommittedBytes(t *testing.T) {
	repo, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatalf("resolve repository: %v", err)
	}
	artifactPath, relativePath, err := resolveReviewedArtifact(repo, "go.mod")
	if err != nil {
		t.Fatalf("resolve tracked artifact: %v", err)
	}
	data, err := os.ReadFile(artifactPath)
	if err != nil {
		t.Fatalf("read tracked artifact: %v", err)
	}
	commit, err := exec.CommandContext(t.Context(), "git", "-C", repo, "rev-parse", "HEAD").Output() // #nosec G204 -- repo is the current test repository.
	if err != nil {
		t.Fatalf("read HEAD: %v", err)
	}
	commitSHA := strings.TrimSpace(string(commit))
	if err := requireArtifactAtCommit(repo, commitSHA, relativePath, data); err != nil {
		t.Fatalf("validate committed artifact: %v", err)
	}
	modified := append(append([]byte(nil), data...), '\n')
	if err := requireArtifactAtCommit(repo, commitSHA, relativePath, modified); err == nil {
		t.Fatal("modified artifact matched reviewed commit bytes")
	}
}

func initCatalogTestRepo(t *testing.T) string {
	t.Helper()
	repo := t.TempDir()
	cmd := exec.CommandContext(t.Context(), "git", "init", "--quiet", repo) // #nosec G204 -- repo is an isolated t.TempDir path.
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("initialize test repository: %v: %s", err, output)
	}
	return repo
}

func assertPublishRejectsArtifactPath(t *testing.T, repo, artifact string) {
	t.Helper()
	var stdout, stderr bytes.Buffer
	err := run([]string{
		"publish",
		"--artifact", artifact,
		"--repo", repo,
		"--dsn", "postgres://unused:unused@127.0.0.1:1/catalog",
	}, &stdout, &stderr)
	if err == nil || !strings.Contains(err.Error(), "artifact") {
		t.Fatalf("publish error = %v, want artifact path rejection", err)
	}
}
