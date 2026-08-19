package store_test

import (
	"go/ast"
	"go/parser"
	"go/printer"
	"go/token"
	"io/fs"
	"path/filepath"
	"strings"
	"testing"
)

// TestPartPrefixHasOneBoundedWriter pins the premise the orphan pass's
// temporary class rests on: "a Put that started before the cutoff is not still
// running". That holds because every writer whose bytes land under the parts
// prefix hands Put a payload of at most MaxPartBytes already in memory, and the
// one writer that can genuinely run long — assembly, streaming a whole file out
// of its parts — writes under an assembled key, which the disjointness test in
// internal/blob shows can never fall under the prefix.
//
// The premise is a fact about the set of callers, so it is pinned as one: this
// census reds when a third Put appears anywhere in the tree, which is the
// moment somebody has to say which prefix it writes to and whether it is
// bounded. A staged or streamed write landing under parts/ would make an aged
// temporary there something other than an abandoned write, and the pass would
// unlink bytes a caller is still handing over.
func TestPartPrefixHasOneBoundedWriter(t *testing.T) {
	t.Parallel()

	// key expression at each call site, by file, for the callers that exist.
	want := map[string]string{
		"internal/api/media.go":     "blob.Key(file.ID)",
		"internal/store/uploads.go": "key",
	}

	fset := token.NewFileSet()
	got := map[string]string{}
	root := filepath.Join("..", "..")
	err := filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		switch {
		case d.IsDir():
			// Vendored, generated and non-Go trees hold no writers of ours.
			if name := d.Name(); name == ".git" || name == "node_modules" || name == "bin" {
				return fs.SkipDir
			}
			return nil
		case !strings.HasSuffix(p, ".go") || strings.HasSuffix(p, "_test.go"):
			return nil
		}
		f, err := parser.ParseFile(fset, p, nil, 0)
		if err != nil {
			return err
		}
		ast.Inspect(f, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			sel, ok := call.Fun.(*ast.SelectorExpr)
			if !ok || sel.Sel.Name != "Put" || len(call.Args) != 3 {
				return true
			}
			// blob.Store.Put is (ctx, key, io.Reader); anything else named Put
			// with three arguments is caught here too, which is the safe
			// direction for a census.
			var b strings.Builder
			if err := printer.Fprint(&b, fset, call.Args[1]); err != nil {
				t.Errorf("print key expression in %s: %v", p, err)
				return false
			}
			rel, err := filepath.Rel(root, p)
			if err != nil {
				t.Errorf("relative path for %s: %v", p, err)
				return false
			}
			got[filepath.ToSlash(rel)] = b.String()
			return true
		})
		return nil
	})
	if err != nil {
		t.Fatalf("walk source tree: %v", err)
	}

	for file, key := range want {
		k, ok := got[file]
		if !ok {
			t.Errorf("no blob write found in %s; the census is stale, not the invariant", file)
			continue
		}
		if k != key {
			t.Errorf("%s writes to %s, census says %s — say which prefix it lands under", file, k, key)
		}
	}
	for file, key := range got {
		if _, ok := want[file]; !ok {
			t.Errorf("new blob writer in %s (key %s): if it lands under %q it must be bounded, "+
				"because the orphan pass unlinks an aged temporary there as an abandoned write",
				file, key, "parts/")
		}
	}
}
