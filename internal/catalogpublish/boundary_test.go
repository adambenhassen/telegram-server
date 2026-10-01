package catalogpublish_test

import (
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestRuntimePackagesDoNotDependOnCatalogPublisher(t *testing.T) {
	repoRoot, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatalf("resolve repository root: %v", err)
	}
	cmd := exec.CommandContext(t.Context(), "go", "list", "-deps", "./cmd/telegramd", "./internal/api", "./internal/mtproto", "./internal/admin")
	cmd.Dir = repoRoot
	output, err := cmd.Output()
	if err != nil {
		t.Fatalf("list runtime dependencies: %v", err)
	}
	for dependency := range strings.FieldsSeq(string(output)) {
		if dependency == "github.com/adambenhassen/telegram-server/internal/catalogpublish" {
			t.Fatal("runtime dependency graph reaches the catalog publisher")
		}
	}
}
