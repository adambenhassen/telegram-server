#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
source "$script_dir/smoke-diagnostics.sh"

SMOKE_OUTPUT_INDENT=$(smoke_diagnostic_output_indent) || {
  printf 'hosted smoke output indentation fixture failed\n' >&2
  exit 1
}
export SMOKE_OUTPUT_INDENT

source_root="$SMOKE_DIAGNOSTICS_ROOT"
source_commit=$(git -C "$source_root" rev-parse HEAD)
if [[ ! "$source_commit" =~ ^[0-9a-f]{40}$ ]]; then
  printf 'checked-out commit validation failed\n' >&2
  exit 1
fi

while IFS= read -r declared_scenario; do
  [[ -n "$declared_scenario" ]] || continue
  if ! smoke_scenario_is_known "$declared_scenario"; then
    printf 'smoke scenario list does not cover every declared subtest\n' >&2
    exit 1
  fi
done < <(sed -nE 's/^[[:space:]]*t\.Run\("([^\"]+)".*/\1/p' "$source_root/test/e2e/smoke_test.go")

fixture_root=$(mktemp -d "${RUNNER_TEMP:-${TMPDIR:-/tmp}}/smoke-diagnostics-repo.XXXXXX")
trap 'rm -rf -- "$fixture_root"' EXIT
mkdir -p "$fixture_root/test/e2e"

write_fixture_source() {
  cat >"$fixture_root/test/e2e/smoke_test.go" <<'EOF'
package e2e

import "testing"

func TestSmoke(t *testing.T) {
    t.Run("dialog-filters", func(t *testing.T) {
        t.Errorf("[assert:dialog-filters.direct-check] fixture")
    })
    waitForSmokeOnline(t, "dialog-filters.helper-call-one")
    waitForSmokeOnline(t, "dialog-filters.helper-call-two")
    t.Errorf("legacy fixture")
}

func waitForSmokeOnline(t *testing.T, callsite string) {
    t.Helper()
    t.Errorf("[assert:dialog-filters.helper-check] fixture")
}

// legacy fixture line 18
// legacy fixture line 19
// legacy fixture line 20
// legacy fixture line 21
// legacy fixture line 22
// legacy fixture line 23
// legacy fixture line 24
// legacy fixture line 25
// legacy fixture line 26
// legacy fixture line 27
// legacy fixture line 28
// legacy fixture line 29
EOF
}

fixture_commit() {
  git -C "$fixture_root" add -- test/e2e/smoke_test.go
  local tree commit
  tree=$(git -C "$fixture_root" write-tree)
  commit=$(printf 'tree %s\n\nfixture\n' "$tree" | git -C "$fixture_root" hash-object -w -t commit --stdin)
  printf '%s\n' "$commit" >"$fixture_root/.git/HEAD"
}

git init --quiet "$fixture_root"
write_fixture_source
fixture_commit
SMOKE_DIAGNOSTICS_ROOT="$fixture_root"
checked_out_commit=$(git -C "$fixture_root" rev-parse HEAD)
if [[ ! "$checked_out_commit" =~ ^[0-9a-f]{40}$ ]]; then
  printf 'diagnostic fixture commit validation failed\n' >&2
  exit 1
fi

scenario_failure='dialog-filters'
canary='private-runtime-assertion-canary-9472'
direct_id='dialog-filters.direct-check'
callsite_one='dialog-filters.helper-call-one'
callsite_two='dialog-filters.helper-call-two'
helper_check='dialog-filters.helper-check'

json_event() {
  local action="$1" test_name="$2" body="${3:-}"
  jq -cn --arg package "$SMOKE_E2E_PACKAGE" --arg action "$action" \
    --arg test "$test_name" --arg output "$body" \
    '{Package:$package,Action:$action,Test:$test,Output:$output}'
}

failure_fixture() {
  local failed_test="$1" output_test="${2:-$1}" body="${3:-}"
  {
    if [[ -n "$body" ]]; then
      json_event output "$output_test" "$body"
    fi
    json_event fail "$failed_test"
    if [[ "$failed_test" == TestSmoke/* ]]; then
      json_event fail TestSmoke
    fi
    json_event fail ''
  }
}

expected_unavailable() {
  printf '::error::TestSmoke/%s failed (category: scenario-failure; location-unavailable; checked-out commit: %s; details redacted)' \
    "$scenario_failure" "$checked_out_commit"
}

expected_legacy_location() {
  printf '::error file=test/e2e/smoke_test.go,line=29::TestSmoke/%s failed (category: scenario-failure; location: test/e2e/smoke_test.go:29; checked-out commit: %s; details redacted)' \
    "$scenario_failure" "$checked_out_commit"
}

expected_direct_assertion() {
  printf '::error file=test/e2e/smoke_test.go,line=7::TestSmoke/%s failed (category: assertion; ID: %s; location: test/e2e/smoke_test.go:7; checked-out commit: %s; details redacted)' \
    "$scenario_failure" "$direct_id" "$checked_out_commit"
}

expected_helper_assertion() {
  printf '::error file=test/e2e/smoke_test.go,line=16::TestSmoke/%s failed (category: assertion; ID: %s/%s; location: test/e2e/smoke_test.go:16; helper-call: test/e2e/smoke_test.go:9; checked-out commit: %s; details redacted)' \
    "$scenario_failure" "$callsite_one" "$helper_check" "$checked_out_commit"
}

expected_helper_call() {
  local id="$1"
  printf '::error file=test/e2e/smoke_test.go,line=9::TestSmoke/%s failed (category: helper-call; ID: %s; location: test/e2e/smoke_test.go:9; checked-out commit: %s; details redacted)' \
    "$scenario_failure" "$id" "$checked_out_commit"
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

direct_body="${SMOKE_OUTPUT_INDENT}smoke_test.go:7: [assert:${direct_id}] runtime state ${canary}"$'\n'
assert_case valid-fixed-assertion \
  "$(failure_fixture "TestSmoke/$scenario_failure" "TestSmoke/$scenario_failure" "$direct_body")" \
  "$(expected_direct_assertion)" "$canary"

multiline_direct_body="${SMOKE_OUTPUT_INDENT}smoke_test.go:7: [assert:${direct_id}] runtime state ${canary}"$'\n'"continued state ${canary}"$'\n'
assert_case multiline-runtime-body \
  "$(failure_fixture "TestSmoke/$scenario_failure" "TestSmoke/$scenario_failure" "$multiline_direct_body")" \
  "$(expected_direct_assertion)" "$canary"

paired_body="${SMOKE_OUTPUT_INDENT}smoke_test.go:9: [assert:${callsite_one}/${helper_check}] runtime state ${canary}"$'\n'
assert_case paired-helper-identities \
  "$(failure_fixture "TestSmoke/$scenario_failure" "TestSmoke/$scenario_failure" "$paired_body")" \
  "$(expected_helper_assertion)" "$canary"

bare_callsite_body="${SMOKE_OUTPUT_INDENT}smoke_test.go:9: [assert:${callsite_one}] runtime state ${canary}"$'\n'
assert_case bare-callsite-is-helper-call \
  "$(failure_fixture "TestSmoke/$scenario_failure" "TestSmoke/$scenario_failure" "$bare_callsite_body")" \
  "$(expected_helper_call "$callsite_one")" "$canary"

bare_check_body="${SMOKE_OUTPUT_INDENT}smoke_test.go:9: [assert:${helper_check}] runtime state ${canary}"$'\n'
assert_case bare-helper-check-is-helper-call \
  "$(failure_fixture "TestSmoke/$scenario_failure" "TestSmoke/$scenario_failure" "$bare_check_body")" \
  "$(expected_helper_call "$helper_check")" "$canary"

legacy_body="${SMOKE_OUTPUT_INDENT}smoke_test.go:29: legacy assertion value ${canary}"$'\n'
assert_case legacy-location-fallback \
  "$(failure_fixture "TestSmoke/$scenario_failure" "TestSmoke/$scenario_failure" "$legacy_body")" \
  "$(expected_legacy_location)" "$canary"

for invalid_path in 'untracked_secret.go:29' '../smoke_test.go:29' '/tmp/smoke_test.go:29'; do
  body="${SMOKE_OUTPUT_INDENT}${invalid_path}: ${canary}"$'\n'
  assert_case invalid-path "$(failure_fixture "TestSmoke/$scenario_failure" "TestSmoke/$scenario_failure" "$body")" \
    "$(expected_unavailable)" "$canary"
done

for invalid_line in 0 999999 1000000; do
  body="${SMOKE_OUTPUT_INDENT}smoke_test.go:${invalid_line}: ${canary}"$'\n'
  assert_case "invalid-line-${invalid_line}" "$(failure_fixture "TestSmoke/$scenario_failure" "TestSmoke/$scenario_failure" "$body")" \
    "$(expected_unavailable)" "$canary"
done

for forged_kind in runtime-value continuation-line; do
  case "$forged_kind" in
    runtime-value)
      forged_body="${SMOKE_OUTPUT_INDENT}smoke_test.go:29: value [assert:${direct_id}] ${canary}"$'\n'
      fixture=$(failure_fixture "TestSmoke/$scenario_failure" "TestSmoke/$scenario_failure" "$forged_body")
      ;;
    continuation-line)
      fixture=$(failure_fixture "TestSmoke/$scenario_failure" "TestSmoke/$scenario_failure" "$legacy_body")
      fixture+=$'\n'
      fixture+=$(json_event output "TestSmoke/$scenario_failure" "${SMOKE_OUTPUT_INDENT}[assert:${direct_id}] ${canary}"$'\n')
      fixture+=$'\n'
      fixture+=$(json_event fail "TestSmoke/$scenario_failure")
      fixture+=$'\n'
      fixture+=$(json_event fail TestSmoke)
      fixture+=$'\n'
      fixture+=$(json_event fail '')
      ;;
  esac
  assert_case "$forged_kind-id" "$fixture" "$(expected_unavailable)" "$canary"
done

cross_scenario_body="${SMOKE_OUTPUT_INDENT}smoke_test.go:7: [assert:basic-group.direct-check] ${canary}"$'\n'
assert_case cross-scenario-id \
  "$(failure_fixture "TestSmoke/$scenario_failure" "TestSmoke/$scenario_failure" "$cross_scenario_body")" \
  "$(expected_unavailable)" "$canary"

unknown_id_body="${SMOKE_OUTPUT_INDENT}smoke_test.go:7: [assert:dialog-filters.stale-check] ${canary}"$'\n'
assert_case stale-id \
  "$(failure_fixture "TestSmoke/$scenario_failure" "TestSmoke/$scenario_failure" "$unknown_id_body")" \
  "$(expected_unavailable)" "$canary"

duplicate_marker_body="${SMOKE_OUTPUT_INDENT}smoke_test.go:7: [assert:${direct_id}] output [assert:${direct_id}] ${canary}"$'\n'
assert_case duplicate-runtime-marker \
  "$(failure_fixture "TestSmoke/$scenario_failure" "TestSmoke/$scenario_failure" "$duplicate_marker_body")" \
  "$(expected_unavailable)" "$canary"

two_ids=$(json_event output "TestSmoke/$scenario_failure" "$direct_body")
two_ids+=$'\n'
two_ids+=$(json_event output "TestSmoke/$scenario_failure" "$bare_callsite_body")
two_ids+=$'\n'
two_ids+=$(json_event fail "TestSmoke/$scenario_failure")
two_ids+=$'\n'
two_ids+=$(json_event fail TestSmoke)
two_ids+=$'\n'
two_ids+=$(json_event fail '')
assert_case multiple-failed-ids "$two_ids" "$(expected_unavailable)" "$canary"

nested_body="${SMOKE_OUTPUT_INDENT}smoke_test.go:7: [assert:${direct_id}] ${canary}"$'\n'
assert_case nested-scenario-id \
  "$(failure_fixture "TestSmoke/$scenario_failure/nested" "TestSmoke/$scenario_failure/nested" "$nested_body")" \
  "$(expected_unavailable)" "$canary"

duplicate_source_case() {
  printf '\n// duplicate metadata: %s\n' "$direct_id" >>"$fixture_root/test/e2e/smoke_test.go"
  fixture_commit
  checked_out_commit=$(git -C "$fixture_root" rev-parse HEAD)
  assert_case duplicate-source-literal \
    "$(failure_fixture "TestSmoke/$scenario_failure" "TestSmoke/$scenario_failure" "$direct_body")" \
    "$(expected_unavailable)" "$canary"
  write_fixture_source
  fixture_commit
  checked_out_commit=$(git -C "$fixture_root" rev-parse HEAD)
}
duplicate_source_case

printf '\n// dirty source tree\n' >>"$fixture_root/test/e2e/smoke_test.go"
assert_case dirty-source-tree \
  "$(failure_fixture "TestSmoke/$scenario_failure" "TestSmoke/$scenario_failure" "$direct_body")" \
  "$(expected_unavailable)" "$canary"
git -C "$fixture_root" restore -- test/e2e/smoke_test.go

printf 'package e2e\n' >"$fixture_root/test/e2e/untracked.go"
assert_case untracked-source-tree \
  "$(failure_fixture "TestSmoke/$scenario_failure" "TestSmoke/$scenario_failure" "$direct_body")" \
  "$(expected_unavailable)" "$canary"
rm -- "$fixture_root/test/e2e/untracked.go"

for unsafe_kind in escape workflow-marker percent-newline carriage-return embedded-newline; do
  case "$unsafe_kind" in
    escape) unsafe_body="${SMOKE_OUTPUT_INDENT}smoke_test.go:29: ${canary}"$'\033[31m\n' ;;
    workflow-marker) unsafe_body="${SMOKE_OUTPUT_INDENT}smoke_test.go:29: ::error file=/tmp/forged.go,line=1::fake ${canary}"$'\n' ;;
    percent-newline) unsafe_body="${SMOKE_OUTPUT_INDENT}smoke_test.go:29: encoded%0Aworkflow-command ${canary}"$'\n' ;;
    carriage-return) unsafe_body="${SMOKE_OUTPUT_INDENT}smoke_test.go:29: ${canary}"$'\r\n' ;;
    embedded-newline) unsafe_body="${SMOKE_OUTPUT_INDENT}smoke_test.go:"$'\n'"29: ${canary}"$'\n' ;;
  esac
  assert_case "$unsafe_kind" "$(failure_fixture "TestSmoke/$scenario_failure" "TestSmoke/$scenario_failure" "$unsafe_body")" \
    "$(expected_unavailable)" "$canary"
done

unknown_scenario='runtime-secret-scenario-9472'
unknown_fixture=$(json_event output "TestSmoke/$unknown_scenario" "$direct_body")
unknown_fixture+=$'\n'
unknown_fixture+=$(json_event fail "TestSmoke/$unknown_scenario")
unknown_fixture+=$'\n'
unknown_fixture+=$(json_event fail TestSmoke)
unknown_fixture+=$'\n'
unknown_fixture+=$(json_event fail '')
unknown_expected="::error::TestSmoke failed (category: suite-failure; checked-out commit: $checked_out_commit; details redacted)"
assert_case unknown-scenario "$unknown_fixture" "$unknown_expected" "$unknown_scenario"

for execution_failure in race timeout build-failure; do
  case "$execution_failure" in
    race)
      failed_stream=$(json_event output "TestSmoke/$scenario_failure" 'WARNING: DATA RACE')
      failed_stream+=$'\n'
      failed_stream+=$(json_event fail "TestSmoke/$scenario_failure")
      failed_stream+=$'\n'
      failed_stream+=$(json_event fail TestSmoke)
      failed_stream+=$'\n'
      failed_stream+=$(json_event fail '')
      ;;
    timeout)
      failed_stream=$(json_event output "TestSmoke/$scenario_failure" 'panic: test timed out after 15m')
      failed_stream+=$'\n'
      failed_stream+=$(json_event fail "TestSmoke/$scenario_failure")
      failed_stream+=$'\n'
      failed_stream+=$(json_event fail TestSmoke)
      failed_stream+=$'\n'
      failed_stream+=$(json_event fail '')
      ;;
    build-failure)
      failed_stream=$(json_event output '' '# github.com/example/build-error')
      failed_stream+=$'\n'
      failed_stream+=$(json_event fail '')
      ;;
  esac
  expected_execution_failure="::error::E2E suite failed (category: execution-failure; checked-out commit: $checked_out_commit; details redacted)"
  assert_case "$execution_failure" "$failed_stream" "$expected_execution_failure" "$canary"
done

truncated_stream=$(json_event fail "TestSmoke/$scenario_failure")
expected_execution_failure="::error::E2E suite failed (category: execution-failure; checked-out commit: $checked_out_commit; details redacted)"
assert_case truncated-json "$truncated_stream" "$expected_execution_failure" "$canary"

outside_failure=$(json_event fail TestOther)
outside_failure+=$'\n'
outside_failure+=$(json_event fail '')
expected_suite_failure="::error::E2E suite failed (category: suite-failure; checked-out commit: $checked_out_commit; details redacted)"
assert_case failure-outside-smoke "$outside_failure" "$expected_suite_failure" "$canary"

non_json_fixture="${canary} ${direct_body}"$'\n'
non_json_fixture+=$(failure_fixture "TestSmoke/$scenario_failure")
expected_execution_failure="::error::E2E suite failed (category: execution-failure; checked-out commit: $checked_out_commit; details redacted)"
assert_case non-json-input "$non_json_fixture" "$expected_execution_failure" "$canary"

passing_fixture=$(json_event pass "TestSmoke/$scenario_failure" "$canary")
passing_diagnostics=$(report_smoke_failure_diagnostics 0 <<<"$passing_fixture")
if [[ -n "$passing_diagnostics" ]]; then
  printf 'passing smoke fixture produced diagnostics\n' >&2
  exit 1
fi

raw_text="::error file=/tmp/forged.go,line=1::${canary} ::stop-commands::attacker"$'\n'
raw_json="$fixture_root/raw-output.json"
json_event output TestOther "$raw_text" >"$raw_json"
command_token=$(smoke_generate_command_token)
if [[ ! "$command_token" =~ ^[0-9a-f]{64}$ ]]; then
  printf 'command suppression token was not unpredictable hex\n' >&2
  exit 1
fi
raw_passthrough=$(smoke_emit_raw_output "$raw_json" "$command_token")
expected_raw_passthrough="::stop-commands::$command_token"$'\n'"$raw_text"$'\n'"::$command_token::"
if [[ "$raw_passthrough" != "$expected_raw_passthrough" ]]; then
  printf 'raw output command suppression boundaries were incorrect\n' >&2
  exit 1
fi

mock_bin="$fixture_root/mock-bin"
runner_temp="$fixture_root/runner-temp"
mkdir -p "$mock_bin" "$runner_temp"
cat >"$mock_bin/go" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
printf '%s\n' "$@" >>"$MOCK_GO_ARGS"
cat "$MOCK_GO_JSON"
exit "$MOCK_GO_STATUS"
EOF
chmod +x "$mock_bin/go"

mock_json="$fixture_root/mock-go.json"
mock_args="$fixture_root/mock-go.args"
mock_raw_body="::error file=/tmp/forged.go,line=1::${canary} ::stop-commands::attacker"$'\n'
mock_failure=$(json_event output "TestSmoke/$scenario_failure" "$direct_body")
mock_failure+=$'\n'
mock_failure+=$(json_event output "TestSmoke/$scenario_failure" "$mock_raw_body")
mock_failure+=$'\n'
mock_failure+=$(json_event fail "TestSmoke/$scenario_failure")
mock_failure+=$'\n'
mock_failure+=$(json_event fail TestSmoke)
mock_failure+=$'\n'
mock_failure+=$(json_event fail '')
printf '%s\n' "$mock_failure" >"$mock_json"
: >"$mock_args"

if output=$(PATH="$mock_bin:$PATH" RUNNER_TEMP="$runner_temp" \
  SMOKE_DIAGNOSTICS_ROOT="$fixture_root" SMOKE_OUTPUT_INDENT="$SMOKE_OUTPUT_INDENT" \
  MOCK_GO_ARGS="$mock_args" MOCK_GO_JSON="$mock_json" MOCK_GO_STATUS=37 \
  bash "$script_dir/run-e2e-diagnostics.sh" 2>&1); then
  result_status=0
else
  result_status=$?
fi
if [[ "$result_status" -ne 37 ]]; then
  printf 'E2E wrapper changed go test exit status\n' >&2
  exit 1
fi
expected_args=$'test\n-race\n-count=1\n-timeout\n15m\n-json\n'"$SMOKE_E2E_PACKAGE"
if [[ "$(cat "$mock_args")" != "$expected_args" ]]; then
  printf 'E2E invocation flags or package selection changed\n' >&2
  exit 1
fi
stop_line=$(grep -E '^::stop-commands::[0-9a-f]{64}$' <<<"$output" || true)
resume_line=$(grep -E '^::[0-9a-f]{64}::$' <<<"$output" || true)
token_from_line="${stop_line#::stop-commands::}"
if [[ -z "$stop_line" || "$resume_line" != "::$token_from_line::" || "$output" != *"$mock_raw_body"* ]]; then
  printf 'E2E raw output was not enclosed by command suppression\n' >&2
  exit 1
fi
post_resume="${output#*"$resume_line"$'\n'}"
annotation=$(grep '^::error' <<<"$post_resume" || true)
if [[ "$annotation" != "$(expected_direct_assertion)" || "$annotation" == *"$canary"* ]]; then
  printf 'E2E annotation exposed runtime text or lost assertion mapping\n' >&2
  exit 1
fi
if find "$runner_temp" -maxdepth 1 -type f -name 'e2e-test-json.*' -print -quit | grep -q .; then
  printf 'E2E JSON temporary file remained after the step\n' >&2
  exit 1
fi

outside_json=$(json_event fail TestOther)
outside_json+=$'\n'
outside_json+=$(json_event fail '')
printf '%s\n' "$outside_json" >"$mock_json"
: >"$mock_args"
if output=$(PATH="$mock_bin:$PATH" RUNNER_TEMP="$runner_temp" \
  SMOKE_DIAGNOSTICS_ROOT="$fixture_root" SMOKE_OUTPUT_INDENT="$SMOKE_OUTPUT_INDENT" \
  MOCK_GO_ARGS="$mock_args" MOCK_GO_JSON="$mock_json" MOCK_GO_STATUS=23 \
  bash "$script_dir/run-e2e-diagnostics.sh" 2>&1); then
  result_status=0
else
  result_status=$?
fi
if [[ "$result_status" -ne 23 || "$output" != *"$expected_suite_failure"* || "$output" == *'::error file='* ]]; then
  printf 'E2E wrapper lost a nonzero exit without a smoke location\n' >&2
  exit 1
fi

printf 'smoke diagnostic verifier fixtures passed\n'
