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

report_smoke_failure_diagnostics() {
  local status="${1:-1}" output failed_tests scenario_json locations
  local checked_out_commit indent scenario test_name basename source_line
  local failed_parent=0 unknown_failed=0 reported=0
  local -A first_basename=() first_line=() first_seen=()
  local -a tracked_paths=()

  output=$(cat)
  if [[ "$status" == "0" ]]; then
    return 0
  fi

  checked_out_commit=$(git -C "$SMOKE_DIAGNOSTICS_ROOT" rev-parse HEAD 2>/dev/null) || checked_out_commit=""
  if [[ ! "$checked_out_commit" =~ ^[0-9a-f]{40}$ ]]; then
    checked_out_commit="unavailable"
  fi

  failed_tests=$(jq -Rr --arg package "$SMOKE_E2E_PACKAGE" '
    fromjson?
    | select(.Package == $package and .Action == "fail")
    | .Test // empty
  ' <<<"$output" 2>/dev/null) || failed_tests=""

  while IFS= read -r test_name; do
    case "$test_name" in
      TestSmoke)
        failed_parent=1
        ;;
      TestSmoke/*)
        scenario="${test_name#TestSmoke/}"
        if ! smoke_scenario_is_known "$scenario"; then
          unknown_failed=1
        fi
        ;;
    esac
  done <<<"$failed_tests"

  indent="${SMOKE_OUTPUT_INDENT:-}"
  if [[ ! "$indent" =~ ^\ +$ ]]; then
    indent=$(smoke_diagnostic_output_indent 2>/dev/null) || indent=""
  fi

  if [[ -n "$indent" ]]; then
    scenario_json=$(printf '%s\n' "${SMOKE_SCENARIOS[@]}" | jq -Rsc 'split("\n")[:-1]')
    locations=$(jq -Rr --arg package "$SMOKE_E2E_PACKAGE" --argjson scenarios "$scenario_json" --arg indent "$indent" '
      def safe_output($output):
        ($output | endswith("\n"))
        and (($output | .[0:-1] | contains("\n")) | not)
        and (($output | contains("\r")) | not)
        and (($output | contains("\u001b")) | not)
        and (($output | contains("::")) | not)
        and (($output | test("(?i)%0[ad]")) | not);

      fromjson?
      | select(.Package == $package and .Action == "output")
      | .Test as $test
      | select(($scenarios | map("TestSmoke/" + .) | index($test)) != null)
      | .Output as $output
      | select(($output | type) == "string" and safe_output($output))
      | ($output | .[0:-1]) as $line
      | select($line | test("^" + $indent + "[a-z0-9_]+\\.go:[1-9][0-9]{0,5}: "))
      | ($line | capture("^" + $indent + "(?<basename>[a-z0-9_]+\\.go):(?<line>[1-9][0-9]{0,5}): "))
      | [$test, .basename, .line]
      | @tsv
    ' <<<"$output" 2>/dev/null) || locations=""
  else
    locations=""
  fi

  while IFS=$'\t' read -r test_name basename source_line; do
    [[ -n "$basename" ]] || continue
    scenario="${test_name#TestSmoke/}"
    if [[ -z "${first_seen[$scenario]+present}" ]]; then
      first_seen["$scenario"]=1
      first_basename["$scenario"]="$basename"
      first_line["$scenario"]="$source_line"
    fi
  done <<<"$locations"

  mapfile -d '' -t tracked_paths < <(git -C "$SMOKE_DIAGNOSTICS_ROOT" ls-tree -r -z --name-only HEAD -- test/e2e)

  for scenario in "${SMOKE_SCENARIOS[@]}"; do
    if ! grep -Fqx -- "TestSmoke/$scenario" <<<"$failed_tests"; then
      continue
    fi

    local resolved_path="" resolved_count=0 candidate_path line_count numeric_line numeric_line_count
    basename="${first_basename[$scenario]:-}"
    source_line="${first_line[$scenario]:-}"
    if [[ -n "$basename" && -n "$source_line" ]]; then
      for candidate_path in "${tracked_paths[@]}"; do
        if [[ "$candidate_path" =~ ^test/e2e/[a-z0-9_]+\.go$ && "${candidate_path##*/}" == "$basename" ]]; then
          resolved_path="$candidate_path"
          ((resolved_count += 1))
        fi
      done
    fi

    if [[ "$resolved_count" -eq 1 ]]; then
      line_count=$(git -C "$SMOKE_DIAGNOSTICS_ROOT" show "HEAD:$resolved_path" 2>/dev/null | awk 'END { print NR }') || line_count=""
      if [[ "$line_count" =~ ^[0-9]+$ ]]; then
        numeric_line=$((10#$source_line))
        numeric_line_count=$((10#$line_count))
        if ((numeric_line <= numeric_line_count)); then
          printf '::error file=%s,line=%s::TestSmoke/%s failed (category: scenario-failure; location: %s:%s; checked-out commit: %s; details redacted)\n' \
            "$resolved_path" "$source_line" "$scenario" "$resolved_path" "$source_line" "$checked_out_commit"
          reported=1
          continue
        fi
      fi
    fi

    printf '::error::TestSmoke/%s failed (category: scenario-failure; location-unavailable; checked-out commit: %s; details redacted)\n' \
      "$scenario" "$checked_out_commit"
    reported=1
  done

  if [[ "$unknown_failed" -eq 1 || ( "$reported" -eq 0 && "$failed_parent" -eq 1 ) ]]; then
    echo "::error::TestSmoke failed (category: suite-failure; details redacted)"
  elif [[ "$reported" -eq 0 ]]; then
    echo "::error::TestSmoke failed (category: execution-failure; details redacted)"
  fi
}

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
SMOKE_DIAGNOSTICS_ROOT="$(cd -- "$script_dir/../.." && pwd)"
SMOKE_E2E_PACKAGE="github.com/adambenhassen/telegram-server/test/e2e"
source "$script_dir/smoke-scenarios.sh"
