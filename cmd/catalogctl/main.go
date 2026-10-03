// Command catalogctl builds reviewed language artifacts offline and publishes
// them through the write-only catalog path.
package main

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/teagramhq/teagram-server/internal/catalog"
	"github.com/teagramhq/teagram-server/internal/catalogpublish"
)

func main() {
	if err := run(os.Args[1:], os.Stdout, os.Stderr); err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(args []string, stdout, stderr io.Writer) error {
	if len(args) == 0 {
		return errors.New("usage: catalogctl build|publish")
	}
	switch args[0] {
	case "build":
		return runBuild(args[1:], stdout, stderr)
	case "publish":
		return runPublish(args[1:], stdout, stderr)
	default:
		return fmt.Errorf("unknown catalog command %q", args[0])
	}
}

func runBuild(args []string, stdout, stderr io.Writer) error {
	flags := flag.NewFlagSet("build", flag.ContinueOnError)
	flags.SetOutput(stderr)
	input := flags.String("input", "", "path to tdesktop lang.strings")
	output := flags.String("output", "", "path for the canonical artifact, or -")
	languageCode := flags.String("language-code", catalog.LanguageEnglish, "source language code")
	sourceURL := flags.String("source-url", "", "pinned upstream source URL")
	sourceRevision := flags.String("source-revision", "", "pinned upstream source revision")
	sourceSHA256 := flags.String("source-sha256", "", "sha256 of the supplied source bytes")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if *languageCode != catalog.LanguageEnglish {
		return catalog.ErrEnglishOnly
	}
	if *input == "" || *output == "" {
		return errors.New("usage: catalogctl build --input <lang.strings> --output <artifact> --source-url <url> --source-revision <sha> --source-sha256 <sha256>")
	}
	raw, err := os.ReadFile(*input)
	if err != nil {
		return errors.New("catalog: unable to read source file")
	}
	artifact, err := catalog.BuildEnglish(raw, catalog.Source{
		URL:      *sourceURL,
		Revision: *sourceRevision,
		SHA256:   *sourceSHA256,
	})
	if err != nil {
		return err
	}
	data, err := artifact.CanonicalBytes()
	if err != nil {
		return err
	}
	if *output == "-" {
		if _, err := stdout.Write(data); err != nil {
			return errors.New("catalog: unable to write artifact")
		}
		return nil
	}
	if err := os.WriteFile(*output, data, 0o600); err != nil { // #nosec G703 -- output is the operator-selected artifact destination.
		return errors.New("catalog: unable to write artifact")
	}
	return nil
}

func runPublish(args []string, stdout, stderr io.Writer) error {
	flags := flag.NewFlagSet("publish", flag.ContinueOnError)
	flags.SetOutput(stderr)
	artifactPath := flags.String("artifact", "", "path to a reviewed catalog artifact")
	repo := flags.String("repo", ".", "reviewed repository worktree")
	dsn := flags.String("dsn", "", "Postgres DSN, or TG_POSTGRES_DSN")
	if err := flags.Parse(args); err != nil {
		return err
	}
	dsnValue := *dsn
	if dsnValue == "" {
		dsnValue = os.Getenv("TG_POSTGRES_DSN")
	}
	if *artifactPath == "" || dsnValue == "" {
		return errors.New("usage: catalogctl publish --artifact <artifact> [--repo <repo>] [--dsn <dsn>]")
	}
	resolvedArtifact, artifactRelativePath, err := resolveReviewedArtifact(*repo, *artifactPath)
	if err != nil {
		return err
	}
	if err := requireCleanRepo(*repo); err != nil {
		return err
	}
	reviewedCommit, err := reviewedCommit(*repo)
	if err != nil {
		return err
	}
	data, err := os.ReadFile(resolvedArtifact) // #nosec G304 -- path is canonicalized and constrained to a tracked file in the reviewed worktree.
	if err != nil {
		return errors.New("catalog: unable to read artifact")
	}
	if err := requireArtifactAtCommit(*repo, reviewedCommit, artifactRelativePath, data); err != nil {
		return err
	}
	artifact, err := catalog.ParseArtifact(data)
	if err != nil {
		return err
	}
	result, err := catalogpublish.Publish(context.Background(), dsnValue, artifact, reviewedCommit, catalogpublish.PublishOptions{})
	if err != nil {
		return err
	}
	if result.Changed {
		_, err = fmt.Fprintf(stdout, "published version %d\n", result.NewVersion)
	} else {
		_, err = fmt.Fprintf(stdout, "unchanged version %d\n", result.NewVersion)
	}
	if err != nil {
		return errors.New("catalog: unable to write result")
	}
	return nil
}

func resolveReviewedArtifact(repo, artifactPath string) (string, string, error) {
	if filepath.IsAbs(artifactPath) || artifactPath == "" {
		return "", "", errors.New("catalog: artifact must be a tracked repository-relative path")
	}
	repoRoot, err := filepath.Abs(repo)
	if err != nil {
		return "", "", errors.New("catalog: unable to resolve reviewed source tree")
	}
	repoRoot, err = filepath.EvalSymlinks(repoRoot)
	if err != nil {
		return "", "", errors.New("catalog: unable to resolve reviewed source tree")
	}
	cleanPath := filepath.Clean(artifactPath)
	artifactPathAbs := filepath.Join(repoRoot, cleanPath)
	lexicalRelative, err := filepath.Rel(repoRoot, artifactPathAbs)
	if err != nil || escapesDirectory(lexicalRelative) {
		return "", "", errors.New("catalog: artifact is outside reviewed source tree")
	}
	resolvedArtifact, err := filepath.EvalSymlinks(artifactPathAbs)
	if err != nil {
		return "", "", errors.New("catalog: unable to resolve artifact")
	}
	resolvedRelative, err := filepath.Rel(repoRoot, resolvedArtifact)
	if err != nil || escapesDirectory(resolvedRelative) || resolvedRelative == "." {
		return "", "", errors.New("catalog: artifact is outside reviewed source tree")
	}
	info, err := os.Stat(resolvedArtifact)
	if err != nil || !info.Mode().IsRegular() {
		return "", "", errors.New("catalog: artifact is not a regular file")
	}
	for _, relativePath := range []string{lexicalRelative, resolvedRelative} {
		if err := requireTrackedArtifact(repoRoot, relativePath); err != nil {
			return "", "", err
		}
	}
	return resolvedArtifact, filepath.ToSlash(resolvedRelative), nil
}

func escapesDirectory(path string) bool {
	return path == ".." || strings.HasPrefix(path, ".."+string(filepath.Separator))
}

func requireTrackedArtifact(repo, relativePath string) error {
	cmd := exec.CommandContext(context.Background(), "git", "-C", repo, "--literal-pathspecs", "ls-files", "--error-unmatch", "--", relativePath) // #nosec G204,G703 -- repo and path are arguments, not shell input.
	if err := cmd.Run(); err != nil {
		return errors.New("catalog: artifact must be tracked in reviewed source tree")
	}
	return nil
}

func requireArtifactAtCommit(repo, commit, relativePath string, data []byte) error {
	cmd := exec.CommandContext(context.Background(), "git", "-C", repo, "cat-file", "blob", commit+":"+relativePath) // #nosec G204,G703 -- repository and object path are arguments, not shell input.
	committed, err := cmd.Output()
	if err != nil || !bytes.Equal(committed, data) {
		return errors.New("catalog: artifact does not match reviewed source commit")
	}
	return nil
}

func requireCleanRepo(repo string) error {
	cmd := exec.CommandContext(context.Background(), "git", "-C", repo, "status", "--porcelain", "--untracked-files=all") // #nosec G204,G703 -- repo is the operator-selected worktree and no shell is involved.
	var output bytes.Buffer
	cmd.Stdout = &output
	if err := cmd.Run(); err != nil {
		return errors.New("catalog: unable to inspect reviewed source tree")
	}
	if output.Len() != 0 {
		return errors.New("catalog: reviewed source tree is dirty")
	}
	return nil
}

func reviewedCommit(repo string) (string, error) {
	cmd := exec.CommandContext(context.Background(), "git", "-C", repo, "rev-parse", "HEAD") // #nosec G204,G703 -- repo is the operator-selected worktree and no shell is involved.
	output, err := cmd.Output()
	if err != nil {
		return "", errors.New("catalog: unable to read reviewed source commit")
	}
	commit := strings.TrimSpace(string(output))
	if commit == "" || strings.ContainsAny(commit, " \t\r\n") {
		return "", errors.New("catalog: invalid reviewed source commit")
	}
	return commit, nil
}
