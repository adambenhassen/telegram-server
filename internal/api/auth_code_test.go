package api_test

import (
	"testing"

	"github.com/adambenhassen/telegram-server/internal/api"
)

func TestIsGeneratedCode(t *testing.T) {
	tests := []struct {
		code string
		want bool
	}{
		{code: "00000", want: true},
		{code: "12345", want: true},
		{code: "99999", want: true},
		{code: "1234", want: false},
		{code: "123456", want: false},
		{code: "12a45", want: false},
		{code: "１２３４５", want: false},
	}
	for _, tc := range tests {
		t.Run(tc.code, func(t *testing.T) {
			if got := api.IsGeneratedCodeForTest(tc.code); got != tc.want {
				t.Fatalf("isGeneratedCode(%q) = %v, want %v", tc.code, got, tc.want)
			}
		})
	}
}
