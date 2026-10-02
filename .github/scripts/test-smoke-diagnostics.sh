#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
source "$script_dir/smoke-diagnostics.sh"

SMOKE_OUTPUT_INDENT=$(smoke_diagnostic_output_indent) || {
  printf 'hosted smoke output indentation fixture failed\n' >&2
  exit 1
}
export SMOKE_OUTPUT_INDENT

checked_out_commit=$(git -C "$SMOKE_DIAGNOSTICS_ROOT" rev-parse HEAD)
if [[ ! "$checked_out_commit" =~ ^[0-9a-f]{40}$ ]]; then
  printf 'checked-out commit validation failed\n' >&2
  exit 1
fi

while IFS= read -r declared_scenario; do
  [[ -n "$declared_scenario" ]] || continue
  if ! smoke_scenario_is_known "$declared_scenario"; then
    printf 'smoke scenario list does not cover every declared subtest\n' >&2
    exit 1
  fi
done < <(sed -nE 's/^[[:space:]]*t\.Run\("([^\"]+)".*/\1/p' "$SMOKE_DIAGNOSTICS_ROOT/test/e2e/smoke_test.go")

scenario_failure='dialog-filters'
canary='private-runtime-assertion-canary-9472'
valid_body="${SMOKE_OUTPUT_INDENT}smoke_test.go:29: assertion value $canary"$'\n'

json_event() {
  local action="$1" test_name="$2" body="${3:-}"
  jq -cn --arg package "$SMOKE_E2E_PACKAGE" --arg action "$action" \
    --arg test "$test_name" --arg output "$body" \
    '{Package:$package,Action:$action,Test:$test,Output:$output}'
}

failure_fixture() {
  local test_name="$1" body="${2:-}" extra="${3:-}"
  {
    if [[ "$extra" == 'with-output' ]]; then
      json_event output "$test_name" "$body"
      printf '\n'
    fi
    json_event fail "TestSmoke/$scenario_failure"
    printf '\n'
    json_event fail TestSmoke
  }
}

expected_location() {
  printf '::error file=test/e2e/smoke_test.go,line=29::TestSmoke/%s failed (category: scenario-failure; location: test/e2e/smoke_test.go:29; checked-out commit: %s; details redacted)' \
    "$scenario_failure" "$checked_out_commit"
}

expected_unavailable() {
  printf '::error::TestSmoke/%s failed (category: scenario-failure; location-unavailable; checked-out commit: %s; details redacted)' \
    "$scenario_failure" "$checked_out_commit"
}

assert_case() {
  local name="$1" fixture="$2" expected="$3" forbidden="$4" actual
  actual=$(report_smoke_failure_diagnostics 1 <<<"$fixture")
  if [[ "$actual" != "$expected" ]]; then
    printf 'unexpected smoke diagnostic for verifier case: %s\n' "$name" >&2
    exit 1
  fi
  if [[ -n "$forbidden" && "$actual" == *"$forbidden"* ]]; then
    printf 'smoke verifier case exposed fixture bytes: %s\n' "$name" >&2
    exit 1
  fi
}

assert_case valid-location \
  "$(failure_fixture "TestSmoke/$scenario_failure" "$valid_body" with-output)" \
  "$(expected_location)" "$canary"

for invalid_path in 'untracked_secret.go:29' '../smoke_test.go:29' '/tmp/smoke_test.go:29'; do
  body="${SMOKE_OUTPUT_INDENT}${invalid_path}: $canary"$'\n'
  assert_case invalid-path "$(failure_fixture "TestSmoke/$scenario_failure" "$body" with-output)" \
    "$(expected_unavailable)" "$canary"
done

for invalid_line in 0 999999 1000000; do
  body="${SMOKE_OUTPUT_INDENT}smoke_test.go:$invalid_line: $canary"$'\n'
  assert_case "invalid-line-$invalid_line" "$(failure_fixture "TestSmoke/$scenario_failure" "$body" with-output)" \
    "$(expected_unavailable)" "$canary"
done

cross_scenario=$(json_event output TestSmoke/basic-group "$valid_body")
cross_scenario+=$'\n'
cross_scenario+=$(json_event fail "TestSmoke/$scenario_failure")
cross_scenario+=$'\n'
cross_scenario+=$(json_event fail TestSmoke)
assert_case cross-scenario "$cross_scenario" "$(expected_unavailable)" "$canary"

parent_location=$(json_event output TestSmoke "$valid_body")
parent_location+=$'\n'
parent_location+=$(json_event fail "TestSmoke/$scenario_failure")
parent_location+=$'\n'
parent_location+=$(json_event fail TestSmoke)
assert_case parent-location "$parent_location" "$(expected_unavailable)" "$canary"

first_candidate=$(json_event output "TestSmoke/$scenario_failure" \
  "${SMOKE_OUTPUT_INDENT}unknown_tracked.go:1: $canary"$'\n')
first_candidate+=$'\n'
first_candidate+=$(json_event output "TestSmoke/$scenario_failure" "$valid_body")
first_candidate+=$'\n'
first_candidate+=$(json_event fail "TestSmoke/$scenario_failure")
first_candidate+=$'\n'
first_candidate+=$(json_event fail TestSmoke)
assert_case first-eligible-location "$first_candidate" "$(expected_unavailable)" "$canary"

for unsafe_kind in esc-osc workflow-marker percent-newline carriage-return embedded-newline; do
  case "$unsafe_kind" in
    esc-osc) unsafe_body="${valid_body%$'\n'}"$'\033]8;;https://example.invalid\007\n' ;;
    workflow-marker) unsafe_body="${valid_body%$'\n'} ::error file=/tmp/forged.go,line=1::fake"$'\n' ;;
    percent-newline) unsafe_body="${valid_body%$'\n'} encoded%0Aworkflow-command"$'\n' ;;
    carriage-return) unsafe_body="${valid_body%$'\n'}"$'\r\n' ;;
    embedded-newline) unsafe_body="${SMOKE_OUTPUT_INDENT}smoke_test.go:"$'\n'"29: $canary"$'\n' ;;
  esac
  assert_case "$unsafe_kind" "$(failure_fixture "TestSmoke/$scenario_failure" "$unsafe_body" with-output)" \
    "$(expected_unavailable)" "$canary"
done

non_json_fixture="$canary ${SMOKE_OUTPUT_INDENT}smoke_test.go:29: raw input"$'\n'
non_json_fixture+=$(failure_fixture "TestSmoke/$scenario_failure")
assert_case non-json-line "$non_json_fixture" "$(expected_unavailable)" "$canary"

unknown_scenario='runtime-secret-scenario-9472'
unknown_fixture=$(json_event output "TestSmoke/$unknown_scenario" "$valid_body")
unknown_fixture+=$'\n'
unknown_fixture+=$(json_event fail "TestSmoke/$unknown_scenario")
unknown_fixture+=$'\n'
unknown_fixture+=$(json_event fail TestSmoke)
unknown_expected='::error::TestSmoke failed (category: suite-failure; details redacted)'
assert_case unknown-scenario "$unknown_fixture" "$unknown_expected" "$unknown_scenario"
passing_fixture=$(json_event pass "TestSmoke/$scenario_failure" "$canary")
passing_diagnostics=$(report_smoke_failure_diagnostics 0 <<<"$passing_fixture")
if [[ -n "$passing_diagnostics" ]]; then
  printf 'passing smoke fixture produced diagnostics\n' >&2
  exit 1
fi

printf 'smoke diagnostic verifier fixtures passed\n'
