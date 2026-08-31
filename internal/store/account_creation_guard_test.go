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

// TestAccountCreationPathsStayBehindAdmission keeps the username account
// creation sequence in one production function. The other allowed primitive
// calls are existing phone-account fixtures, bootstrap, authentication, and
// username-management paths; adding a new call site requires naming it here.
func TestAccountCreationPathsStayBehindAdmission(t *testing.T) {
	t.Parallel()

	root := filepath.Join("..", "..")
	fset := token.NewFileSet()
	var violations []string
	seenAdmissionCalls := map[string]bool{}
	allowed := map[string]map[string]map[string]bool{
		"internal/api/auth.go": {
			"handleSignInPhone": {"BindAuthKeyUser": true},
		},
		"internal/store/admission.go": {
			"AdmitUsername": {
				"CreateUsernameUser": true,
				"ClaimUsername":      true,
				"BindAuthKeyUser":    true,
			},
		},
		// Bootstrap is the pre-port account path and must remain unaffected.
		"internal/store/bootstrap.go": {
			"BootstrapAccount": {"CreateUsernameUser": true, "ClaimUsername": true},
		},
		"internal/store/authkeys.go": {
			"BindAuthKeyUser": {"BindAuthKeyUser": true},
		},
		// These Store methods are test fixtures or account/channel username
		// management, not unauthenticated account issuance.
		"internal/store/users.go": {
			"CreateUser":           {"CreateUser": true},
			"CreateUsernameUser":   {"CreateUsernameUser": true},
			"UpdateUsername":       {"ClaimUsername": true},
			"ClaimUsername":        {"ClaimUsername": true},
			"ClaimChannelUsername": {"ClaimUsername": true},
		},
		"internal/store/channels.go": {
			"EditChannelUsername": {"ClaimUsername": true},
		},
	}

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
		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil {
				continue
			}
			function := fn.Name.Name
			ast.Inspect(fn.Body, func(node ast.Node) bool {
				call, ok := node.(*ast.CallExpr)
				if !ok {
					return true
				}
				selector, ok := call.Fun.(*ast.SelectorExpr)
				if !ok {
					return true
				}
				name := selector.Sel.Name
				if !trackedPrimitive(name) {
					return true
				}
				if rel == "internal/store/admission.go" && function == "AdmitUsername" {
					seenAdmissionCalls[name] = true
				}
				if !allowedCall(allowed, rel, function, name) {
					violations = append(violations, fmt.Sprintf(
						"%s:%d %s in %s is outside the allowlist",
						rel, fset.Position(call.Pos()).Line, name, function,
					))
				}
				return true
			})
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk source tree: %v", err)
	}
	for _, name := range []string{"CreateUsernameUser", "ClaimUsername", "BindAuthKeyUser"} {
		if !seenAdmissionCalls[name] {
			violations = append(violations, "admission.go:AdmitUsername does not call "+name)
		}
	}
	if len(violations) != 0 {
		sort.Strings(violations)
		t.Fatalf("account-creation path escaped the admission boundary:\n\t%s", strings.Join(violations, "\n\t"))
	}
}

func trackedPrimitive(name string) bool {
	switch name {
	case "CreateUsernameUser", "ClaimUsername", "BindAuthKeyUser", "CreateUser":
		return true
	default:
		return false
	}
}

func allowedCall(allowed map[string]map[string]map[string]bool, file, function, primitive string) bool {
	return allowed[file][function][primitive]
}
