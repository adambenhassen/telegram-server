package config_test

import (
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"runtime"
	"testing"
)

func TestHandshakeReadTimeoutUsesGotdDefault(t *testing.T) {
	_, testFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed")
	}

	configFile := filepath.Join(filepath.Dir(testFile), "config.go")
	file, err := parser.ParseFile(token.NewFileSet(), configFile, nil, 0)
	if err != nil {
		t.Fatalf("parse config.go: %v", err)
	}

	var initializer ast.Expr
	for _, decl := range file.Decls {
		gen, ok := decl.(*ast.GenDecl)
		if !ok || gen.Tok != token.CONST {
			continue
		}
		for _, spec := range gen.Specs {
			values, ok := spec.(*ast.ValueSpec)
			if !ok {
				continue
			}
			for i, name := range values.Names {
				if name.Name == "handshakeReadTimeout" && i < len(values.Values) {
					initializer = values.Values[i]
				}
			}
		}
	}

	selector, ok := initializer.(*ast.SelectorExpr)
	if !ok {
		t.Fatalf("handshakeReadTimeout initializer = %T, want exchange.DefaultTimeout", initializer)
	}
	packageName, ok := selector.X.(*ast.Ident)
	if !ok || packageName.Name != "exchange" || selector.Sel.Name != "DefaultTimeout" {
		t.Fatalf("handshakeReadTimeout initializer is not exchange.DefaultTimeout")
	}
}
