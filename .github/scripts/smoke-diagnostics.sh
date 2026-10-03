smoke_diagnostic_output_indent() (
  set -euo pipefail

  local fixture_dir indent
  fixture_dir=$(mktemp -d "${RUNNER_TEMP:-${TMPDIR:-/tmp}}/smoke-diagnostics-indent.XXXXXX")
  trap 'rm -rf -- "$fixture_dir"' EXIT

  cat >"$fixture_dir/go.mod" <<'EOF'
module smoke-diagnostics-indent

go 1.27.1
EOF
  cat >"$fixture_dir/fixture_test.go" <<'EOF'
package fixture

import "testing"

func TestDiagnosticIndent(t *testing.T) {
	t.Errorf("SMOKE_DIAGNOSTIC_INDENT_SENTINEL")
}
EOF

  indent=$(cd -- "$fixture_dir" && {
    set +o pipefail
    go test -json -count=1 -run '^TestDiagnosticIndent$' . 2>&1 | jq -Rr '
      fromjson?
      | select(.Action == "output" and .Test == "TestDiagnosticIndent")
      | .Output // empty
      | select(type == "string" and test("^[ ]+[a-z0-9_]+\\.go:[1-9][0-9]{0,5}: SMOKE_DIAGNOSTIC_INDENT_SENTINEL\\n$"))
      | capture("^(?<indent> +)")
      | .indent
    ' 2>/dev/null | head -n 1
  })
  if [[ ! "$indent" =~ ^\ +$ ]]; then
    return 1
  fi

  printf '%s' "$indent"
)

smoke_scenario_is_known() {
  local candidate="$1" scenario
  for scenario in "${SMOKE_SCENARIOS[@]}"; do
    if [[ "$candidate" == "$scenario" ]]; then
      return 0
    fi
  done
  return 1
}

smoke_generate_command_token() {
  python3 -c 'import secrets; print(secrets.token_hex(32))'
}

smoke_emit_raw_output() {
  local json_file="$1" command_token="$2" jq_status=0
  if [[ ! "$command_token" =~ ^[0-9a-f]{64}$ ]]; then
    return 1
  fi

  printf '::stop-commands::%s\n' "$command_token"
  jq -Rr '
    fromjson?
    | select(.Action == "output")
    | .Output // empty
  ' "$json_file" 2>/dev/null || jq_status=$?
  printf '::%s::\n' "$command_token"
  return "$jq_status"
}

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
SMOKE_DIAGNOSTICS_ROOT="${SMOKE_DIAGNOSTICS_ROOT:-$(cd -- "$script_dir/../.." && pwd)}"
SMOKE_E2E_PACKAGE="github.com/teagramhq/teagram-server/test/e2e"
source "$script_dir/smoke-scenarios.sh"

report_smoke_failure_diagnostics() {
  local status="${1:-1}" input="${2:--}" indent="${SMOKE_OUTPUT_INDENT:-}"
  local scenario
  local -a args=(
    --status "$status"
    --package "$SMOKE_E2E_PACKAGE"
    --root "$SMOKE_DIAGNOSTICS_ROOT"
    --indent "$indent"
  )
  if [[ ! "$indent" =~ ^\ +$ ]]; then
    indent=$(smoke_diagnostic_output_indent 2>/dev/null) || indent=""
    args[7]="$indent"
  fi
  for scenario in "${SMOKE_SCENARIOS[@]}"; do
    args+=(--scenario "$scenario")
  done
  python3 "$script_dir/smoke-diagnostics.py" "${args[@]}" "$input"
}
