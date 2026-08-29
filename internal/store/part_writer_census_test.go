package store_test

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/printer"
	"go/token"
	"io/fs"
	"path/filepath"
	"slices"
	"sort"
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

	// Every blob write in the tree, by file, as the key expression it hands
	// Put. The census counts call sites rather than files: a second write
	// added to a file already listed here is the likely shape of the change
	// this pin exists to catch — a streaming part writer lands in
	// internal/store/uploads.go, which is already a writer — so a file whose
	// entry merely still matches must not read as unchanged.
	want := map[string][]string{
		"internal/api/media.go":     {"blob.Key(file.ID)"},
		"internal/store/uploads.go": {"key"},
	}

	fset := token.NewFileSet()
	got := map[string][]string{}
	where := map[string][]string{}
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
		rel, err := filepath.Rel(root, p)
		if err != nil {
			return err
		}
		rel = filepath.ToSlash(rel)
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
				t.Errorf("print key expression in %s: %v", rel, err)
				return false
			}
			got[rel] = append(got[rel], b.String())
			where[rel] = append(where[rel], fmt.Sprintf("%s:%d writes %s",
				rel, fset.Position(call.Pos()).Line, b.String()))
			return true
		})
		return nil
	})
	if err != nil {
		t.Fatalf("walk source tree: %v", err)
	}

	for file, keys := range want {
		found, ok := got[file]
		if !ok {
			t.Errorf("no blob write found in %s; the census is stale, not the invariant", file)
			continue
		}
		sort.Strings(found)
		sort.Strings(keys)
		if !slices.Equal(found, keys) {
			t.Errorf("%s writes %v, census says %v — for each one, say which prefix it lands under "+
				"and whether it is bounded:\n\t%s",
				file, found, keys, strings.Join(where[file], "\n\t"))
		}
	}
	for file, keys := range got {
		if _, ok := want[file]; !ok {
			t.Errorf("new blob writer in %s (keys %v): if it lands under %q it must be bounded, "+
				"because the orphan pass unlinks an aged temporary there as an abandoned write",
				file, keys, "parts/")
		}
	}
}
