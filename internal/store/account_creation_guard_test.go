package store_test

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

// TestAccountCreationPathsStayBehindAdmission keeps unauthenticated account
// creation out of production code. Store.CreateUser remains available as a
// fixture constructor, but no production caller may invoke it; the old
// username signup method and its auth-key binding path must stay deleted.
func TestAccountCreationPathsStayBehindAdmission(t *testing.T) {
	t.Parallel()

	root := filepath.Join("..", "..")
	fset := token.NewFileSet()
	var violations []string
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			switch entry.Name() {
			case ".git", "bin", "node_modules":
				return fs.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}

		file, err := parser.ParseFile(fset, path, nil, 0)
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		rel = filepath.ToSlash(rel)
		ast.Inspect(file, func(node ast.Node) bool {
			switch n := node.(type) {
			case *ast.CallExpr:
				selector, ok := n.Fun.(*ast.SelectorExpr)
				if !ok || selector.Sel.Name != "CreateUser" {
					return true
				}
				// This is the Store.CreateUser implementation's call to the
				// generated SQL query, not a production caller of Store.CreateUser.
				if rel == "internal/store/users.go" {
					if receiver, ok := selector.X.(*ast.Ident); ok && receiver.Name == "qtx" {
						return true
					}
				}
				violations = append(violations, fmt.Sprintf("%s:%d calls CreateUser", rel, fset.Position(n.Pos()).Line))
			case *ast.Ident:
				if n.Name == "SignUpUsernameUser" || n.Name == "BindAuthKeyUserForSignUp" {
					violations = append(violations, fmt.Sprintf("%s:%d references %s", rel, fset.Position(n.Pos()).Line, n.Name))
				}
			}
			return true
		})
		return nil
	})
	if err != nil {
		t.Fatalf("walk source tree: %v", err)
	}
	sort.Strings(violations)
	if len(violations) != 0 {
		t.Fatalf("account-creation path escaped the admission boundary:\n\t%s", strings.Join(violations, "\n\t"))
	}
}
