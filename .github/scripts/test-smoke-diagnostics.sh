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

scenario_failure='peer-disconnect'
canary='private-runtime-assertion-canary-9472'
direct_id='peer-disconnect.initial-state'
callsite_one='peer-disconnect.message-blocker-clear-confirm'
callsite_two='peer-disconnect.chat-blocker-clear-confirm'
owner_lock_state='peer-disconnect.owner-lock-state'
owner_lock_inspection='peer-disconnect.owner-lock-inspection'
short_callsite='peer-disconnect.message-blocker-clear-confirm-short'
short_helper_check='peer-disconnect.owner-lock-state-short'

write_fixture_source() {
  cat >"$fixture_root/test/e2e/smoke_test.go" <<'EOF'
package e2e

import "testing"

func TestSmoke(t *testing.T) {
EOF
  for ((fixture_line = 1; fixture_line <= 71; fixture_line++)); do
    printf '    // fixture filler\n' >>"$fixture_root/test/e2e/smoke_test.go"
  done
  cat >>"$fixture_root/test/e2e/smoke_test.go" <<'EOF'
    t.Run("peer-disconnect", func(t *testing.T) {
        t.Parallel()
        testSmokePeerDisconnect(t)
    })
    t.Run("other-scenario", func(t *testing.T) {
        testSmokeOther(t)
    })
}
EOF

  cat >"$fixture_root/test/e2e/rpc_disconnect_smoke_test.go" <<'EOF'
package e2e

import "testing"

func testSmokePeerDisconnect(t *testing.T) {
    t.Helper()
    t.Fatalf("[assert:peer-disconnect.initial-state] initial state: %v", "fixture")
    waitForSmokeOwnerLock(t, nil, nil, false, "peer-disconnect.message-blocker-clear-confirm")
    waitForSmokeOwnerLock(t, nil, nil, false, "peer-disconnect.chat-blocker-clear-confirm")
    lockSmokeOwner(t, "peer-disconnect.message-owner-lock")
    holdSmokeOwnerLock(t)
    waitForSmokeOnline(t, "peer-disconnect.online-caller")
    legacySmokeCheck(t)
}

func waitForSmokeOwnerLock(t *testing.T, ctx any, lock any, wantBlocked bool, callsiteID string) {
    t.Helper()
    smokeOwnerLockCount(t, ctx, lock, callsiteID)
    if false {
        t.Fatalf("[assert:%s/peer-disconnect.owner-lock-state] owner lock state %t", callsiteID, wantBlocked)
    }
}

func smokeOwnerLockCount(t *testing.T, ctx any, lock any, callsiteID string) int {
    t.Helper()
    t.Fatalf("[assert:%s/peer-disconnect.owner-lock-inspection] inspect owner lock", callsiteID)
    _ = ctx
    _ = lock
    return 0
}

func holdSmokeOwnerLock(t *testing.T) {
    t.Helper()
    smokeOwnerLockCount(t, nil, nil, "peer-disconnect.inner-hold-probe")
}

func lockSmokeOwner(t *testing.T, callsiteID string) {
    t.Helper()
    t.Fatalf("[assert:%s/peer-disconnect.owner-lock-acquire] acquire owner lock", callsiteID)
    smokeOwnerLockCount(t, nil, nil, "peer-disconnect.lockSmokeOwner-internal-callsite")
    t.Cleanup(func() {
        smokeOwnerLockCount(t, nil, nil, "peer-disconnect.cleanup-closure-callsite")
    })
}

func waitForSmokeOnline(t *testing.T, callsiteID string) {
    t.Helper()
    t.Fatalf("[assert:%s/peer-disconnect.online-state-mismatch] online state", callsiteID)
}

func legacySmokeCheck(t *testing.T) {
    t.Helper()
    t.Fatalf("[assert:peer-disconnect.legacy-helper-check] legacy helper check")
}

func testSmokeOther(t *testing.T) {
    t.Helper()
}

func TestOutside(t *testing.T) {
    testSmokeOther(t)
}
EOF
}

fixture_commit() {
  git -C "$fixture_root" add -- test/e2e
  local tree commit identity_headers
  tree=$(git -C "$fixture_root" write-tree)
  identity_headers=$(git -C "$source_root" cat-file commit "$source_commit" | awk '
    /^author / { author++; print }
    /^committer / { committer++; print }
    END { if (author != 1 || committer != 1) exit 1 }
  ') || {
    printf 'diagnostic fixture source commit lacks valid identity headers\n' >&2
    return 1
  }
  commit=$(
    {
      printf 'tree %s\n' "$tree"
      printf '%s\n' "$identity_headers"
      printf '\nfixture\n'
    } | git -C "$fixture_root" hash-object -w -t commit --stdin
  )
  printf '%s\n' "$commit" >"$fixture_root/.git/HEAD"
}

replace_fixture_text() {
  python3 - "$1" "$2" "$3" <<'PY'
from pathlib import Path
import sys

path = Path(sys.argv[1])
source = path.read_text(encoding="utf-8")
old, new = sys.argv[2], sys.argv[3]
if old not in source:
    raise SystemExit(f"fixture replacement was not found in {path.name}")
path.write_text(source.replace(old, new, 1), encoding="utf-8")
PY
}

rpc_fixture="$fixture_root/test/e2e/rpc_disconnect_smoke_test.go"
smoke_fixture="$fixture_root/test/e2e/smoke_test.go"

apply_mutation() {
  local mutation="$1"
  case "$mutation" in
    none) ;;
    root-no-helper)
      replace_fixture_text "$rpc_fixture" \
        $'func testSmokePeerDisconnect(t *testing.T) {\n    t.Helper()' \
        $'func testSmokePeerDisconnect(t *testing.T) {\n    _ = t'
      ;;
    helper-no-helper)
      replace_fixture_text "$rpc_fixture" \
        $'func waitForSmokeOwnerLock(t *testing.T, ctx any, lock any, wantBlocked bool, callsiteID string) {\n    t.Helper()' \
        $'func waitForSmokeOwnerLock(t *testing.T, ctx any, lock any, wantBlocked bool, callsiteID string) {\n    _ = t'
      ;;
    check-helper-no-helper)
      replace_fixture_text "$rpc_fixture" \
        $'func smokeOwnerLockCount(t *testing.T, ctx any, lock any, callsiteID string) int {\n    t.Helper()' \
        $'func smokeOwnerLockCount(t *testing.T, ctx any, lock any, callsiteID string) int {\n    _ = t'
      ;;
    root-missing)
      replace_fixture_text "$rpc_fixture" \
        'func testSmokePeerDisconnect(t *testing.T)' \
        'func testSmokePeerDisconnectMissing(t *testing.T)'
      ;;
    root-duplicate)
      cat >>"$rpc_fixture" <<'EOF'

func testSmokePeerDisconnect(t *testing.T) {
    t.Helper()
}
EOF
      ;;
    root-method-collision)
      cat >>"$rpc_fixture" <<'EOF'

type fixtureReceiver struct{}

func (fixtureReceiver) testSmokePeerDisconnect(t *testing.T) {
    t.Helper()
}
EOF
      ;;
    helper-missing)
      replace_fixture_text "$rpc_fixture" \
        'func waitForSmokeOwnerLock(t *testing.T, ctx any, lock any, wantBlocked bool, callsiteID string)' \
        'func waitForSmokeOwnerLockMissing(t *testing.T, ctx any, lock any, wantBlocked bool, callsiteID string)'
      ;;
    helper-duplicate)
      cat >>"$rpc_fixture" <<'EOF'

func waitForSmokeOwnerLock(t *testing.T, ctx any, lock any, wantBlocked bool, callsiteID string) {
    t.Helper()
}
EOF
      ;;
    helper-method-collision)
      cat >>"$rpc_fixture" <<'EOF'

type fixtureReceiver struct{}

func (fixtureReceiver) waitForSmokeOwnerLock(t *testing.T, ctx any, lock any, wantBlocked bool, callsiteID string) {
    t.Helper()
}
EOF
      ;;
    check-helper-missing)
      replace_fixture_text "$rpc_fixture" \
        'func smokeOwnerLockCount(t *testing.T, ctx any, lock any, callsiteID string) int' \
        'func smokeOwnerLockCountMissing(t *testing.T, ctx any, lock any, callsiteID string) int'
      ;;
    check-helper-duplicate)
      cat >>"$rpc_fixture" <<'EOF'

func smokeOwnerLockCount(t *testing.T, ctx any, lock any, callsiteID string) int {
    t.Helper()
    return 0
}
EOF
      ;;
    check-helper-method-collision)
      cat >>"$rpc_fixture" <<'EOF'

type fixtureReceiver struct{}

func (fixtureReceiver) smokeOwnerLockCount(t *testing.T, ctx any, lock any, callsiteID string) int {
    t.Helper()
    return 0
}
EOF
      ;;
    testsmoke-missing)
      replace_fixture_text "$smoke_fixture" 'func TestSmoke(t *testing.T)' 'func TestSmokeMissing(t *testing.T)'
      ;;
    testsmoke-duplicate)
      cat >>"$smoke_fixture" <<'EOF'

func TestSmoke(t *testing.T) {
    t.Helper()
}
EOF
      ;;
    closure-zero-call)
      replace_fixture_text "$smoke_fixture" "        testSmokePeerDisconnect(t)" ""
      ;;
    closure-two-calls)
      replace_fixture_text "$smoke_fixture" \
        "        testSmokePeerDisconnect(t)" \
        $'        testSmokePeerDisconnect(t)\n        testSmokePeerDisconnect(t)'
      ;;
    duplicate-scenario-run)
      replace_fixture_text "$smoke_fixture" \
        $'    })\n    t.Run("other-scenario"' \
        $'    })\n    t.Run("peer-disconnect", func(t *testing.T) {\n        testSmokePeerDisconnect(t)\n    })\n    t.Run("other-scenario"'
      ;;
    scenario-pattern-mismatch)
      replace_fixture_text "$smoke_fixture" \
        't.Run("peer-disconnect", func(t *testing.T) {' \
        't.Run(smokeScenarioName(), func(t *testing.T) {'
      ;;
    unsupported-callsite)
      replace_fixture_text "$rpc_fixture" \
        '    waitForSmokeOwnerLock(t, nil, nil, false, "peer-disconnect.message-blocker-clear-confirm")' \
        $'    waitForSmokeOwnerLock(\n        t,\n        nil,\n        nil,\n        false,\n        "peer-disconnect.message-blocker-clear-confirm",\n    )'
      ;;
    root-signature-mismatch)
      replace_fixture_text "$smoke_fixture" \
        'func TestSmoke(t *testing.T)' 'func TestSmoke(t testing.TB)'
      ;;
    second-root-caller)
      cat >>"$rpc_fixture" <<'EOF'

func TestOtherRootCaller(t *testing.T) {
    testSmokePeerDisconnect(t)
}
EOF
      ;;
    inner-literal-hop)
      replace_fixture_text "$rpc_fixture" \
        'smokeOwnerLockCount(t, ctx, lock, callsiteID)' \
        'smokeOwnerLockCount(t, ctx, lock, "peer-disconnect.inner-forwarding-literal")'
      ;;
    inner-expression-hop)
      replace_fixture_text "$rpc_fixture" \
        'smokeOwnerLockCount(t, ctx, lock, callsiteID)' \
        'smokeOwnerLockCount(t, ctx, lock, strings.Clone(callsiteID))'
      ;;
    two-hop-chain)
      replace_fixture_text "$rpc_fixture" \
        '    smokeOwnerLockCount(t, ctx, lock, callsiteID)' \
        '    smokeOwnerLockMiddle(t, ctx, lock, callsiteID)'
      cat >>"$rpc_fixture" <<'EOF'

func smokeOwnerLockMiddle(t *testing.T, ctx any, lock any, callsiteID string) {
    t.Helper()
    smokeOwnerLockCount(t, ctx, lock, callsiteID)
}
EOF
      ;;
    duplicate-callsite-literal)
      cat >>"$rpc_fixture" <<'EOF'

func duplicateMetadata(t *testing.T) {
    _ = "peer-disconnect.message-blocker-clear-confirm"
}
EOF
      ;;
    duplicate-check-literal)
      cat >>"$rpc_fixture" <<'EOF'

func duplicateMetadata(t *testing.T) {
    _ = "peer-disconnect.owner-lock-state"
}
EOF
      ;;
    duplicate-direct-literal)
      cat >>"$rpc_fixture" <<'EOF'

func duplicateMetadata(t *testing.T) {
    _ = "peer-disconnect.initial-state"
}
EOF
      ;;
    dirty-second-file)
      printf '\n// dirty source tree\n' >>"$rpc_fixture"
      ;;
    *)
      printf 'unknown fixture mutation: %s\n' "$mutation" >&2
      exit 1
      ;;
  esac
}

git init --quiet "$fixture_root"
write_fixture_source
fixture_commit
checked_out_commit=$(git -C "$fixture_root" rev-parse HEAD)
if [[ ! "$checked_out_commit" =~ ^[0-9a-f]{40}$ ]]; then
  printf 'diagnostic fixture commit validation failed\n' >&2
  exit 1
fi

SMOKE_SCENARIOS=(peer-disconnect)
SMOKE_DIAGNOSTICS_ROOT="$fixture_root"

wrapper_script_dir="$fixture_root/wrapper-scripts"
mock_bin="$fixture_root/mock-bin"
runner_temp="$fixture_root/runner-temp"
mkdir -p "$wrapper_script_dir" "$mock_bin" "$runner_temp"
cp "$script_dir/run-e2e-diagnostics.sh" "$script_dir/smoke-diagnostics.sh" \
  "$script_dir/smoke-diagnostics.py" "$wrapper_script_dir/"
printf 'SMOKE_SCENARIOS=(peer-disconnect)\n' >"$wrapper_script_dir/smoke-scenarios.sh"
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

expected_helper_assertion() {
  local callsite="$1" callsite_line="$2" check="$3" check_line="$4"
  printf '::error file=test/e2e/rpc_disconnect_smoke_test.go,line=%s::TestSmoke/%s failed (category: assertion; ID: %s/%s; location: test/e2e/rpc_disconnect_smoke_test.go:%s; helper-call: test/e2e/rpc_disconnect_smoke_test.go:%s; checked-out commit: %s; details redacted)' \
    "$check_line" "$scenario_failure" "$callsite" "$check" \
    "$check_line" "$callsite_line" "$checked_out_commit"
}

expected_direct_assertion() {
  local assertion_line="$1"
  printf '::error file=test/e2e/rpc_disconnect_smoke_test.go,line=%s::TestSmoke/%s failed (category: assertion; ID: %s; location: test/e2e/rpc_disconnect_smoke_test.go:%s; checked-out commit: %s; details redacted)' \
    "$assertion_line" "$scenario_failure" "$direct_id" "$assertion_line" "$checked_out_commit"
}

expected_helper_call() {
  local identifier="$1" callsite_line="$2"
  printf '::error file=test/e2e/rpc_disconnect_smoke_test.go,line=%s::TestSmoke/%s failed (category: helper-call; ID: %s; location: test/e2e/rpc_disconnect_smoke_test.go:%s; checked-out commit: %s; details redacted)' \
    "$callsite_line" "$scenario_failure" "$identifier" "$callsite_line" "$checked_out_commit"
}

expected_legacy_helper_check() {
  local caller_line="$1"
  printf '::error file=test/e2e/rpc_disconnect_smoke_test.go,line=%s::TestSmoke/%s failed (category: helper-call; ID: peer-disconnect.legacy-helper-check; location: test/e2e/rpc_disconnect_smoke_test.go:%s; checked-out commit: %s; details redacted)' \
    "$caller_line" "$scenario_failure" "$caller_line" "$checked_out_commit"
}

assert_case() {
  local name="$1" fixture="$2" expected="$3" actual output result_status \
    stop_line resume_line token_from_line post_resume wrapper_diagnostics
  local canary_event
  canary_event=$(json_event output "TestSmoke/$scenario_failure" "untrusted runtime detail ${canary}")
  fixture="${canary_event}"$'\n'"${fixture}"
  if [[ "$fixture" != *"$canary"* ]]; then
    printf 'smoke verifier case omitted its redaction canary: %s\n' "$name" >&2
    exit 1
  fi

  actual=$(report_smoke_failure_diagnostics 37 <<<"$fixture")
  if [[ "$actual" != "$expected" ]]; then
    printf 'unexpected smoke diagnostic for verifier case: %s\n' "$name" >&2
    exit 1
  fi
  if [[ "$actual" == *"$canary"* ]]; then
    printf 'smoke verifier case exposed fixture bytes: %s\n' "$name" >&2
    exit 1
  fi

  printf '%s\n' "$fixture" >"$mock_json"
  : >"$mock_args"
  if output=$(PATH="$mock_bin:$PATH" RUNNER_TEMP="$runner_temp" \
    SMOKE_DIAGNOSTICS_ROOT="$fixture_root" SMOKE_OUTPUT_INDENT="$SMOKE_OUTPUT_INDENT" \
    MOCK_GO_ARGS="$mock_args" MOCK_GO_JSON="$mock_json" MOCK_GO_STATUS=37 \
    bash "$wrapper_script_dir/run-e2e-diagnostics.sh" 2>&1); then
    result_status=0
  else
    result_status=$?
  fi
  if [[ "$result_status" -ne 37 ]]; then
    printf 'E2E wrapper changed go test exit status in verifier case: %s\n' "$name" >&2
    exit 1
  fi
  expected_args=$'test\n-race\n-count=1\n-timeout\n15m\n-json\n'"$SMOKE_E2E_PACKAGE"
  if [[ "$(cat "$mock_args")" != "$expected_args" ]]; then
    printf 'E2E invocation flags or package selection changed: %s\n' "$name" >&2
    exit 1
  fi
  stop_line=$(grep -E '^::stop-commands::[0-9a-f]{64}$' <<<"$output" || true)
  resume_line=$(grep -E '^::[0-9a-f]{64}::$' <<<"$output" || true)
  token_from_line="${stop_line#::stop-commands::}"
  if [[ -z "$stop_line" || "$resume_line" != "::$token_from_line::" ]]; then
    printf 'E2E raw output lost command suppression: %s\n' "$name" >&2
    exit 1
  fi
  post_resume="${output#*"$resume_line"$'\n'}"
  wrapper_diagnostics=$(grep '^::error' <<<"$post_resume" || true)
  if [[ "$wrapper_diagnostics" != "$actual" || "$wrapper_diagnostics" == *"$canary"* ]]; then
    printf 'E2E wrapper changed or exposed the diagnostic: %s\n' "$name" >&2
    exit 1
  fi
  if find "$runner_temp" -maxdepth 1 -type f -name 'e2e-test-json.*' -print -quit | grep -q .; then
    printf 'E2E JSON temporary file remained after verifier case: %s\n' "$name" >&2
    exit 1
  fi
}

line_for_text() {
  local path="$1" text="$2" result
  result=$(grep -nF -- "$text" "$path" | cut -d: -f1)
  if [[ -z "$result" || "$result" == *$'\n'* ]]; then
    printf 'fixture line was missing or ambiguous: %s\n' "$text" >&2
    exit 1
  fi
  printf '%s' "$result"
}

commit_fixture() {
  fixture_commit
  checked_out_commit=$(git -C "$fixture_root" rev-parse HEAD)
}

assert_unavailable_case() {
  local name="$1" token="$2" runtime_location="$3" mutation="${4:-none}"
  write_fixture_source
  apply_mutation "$mutation"
  commit_fixture
  local body fixture
  body="${SMOKE_OUTPUT_INDENT}${runtime_location}: [assert:${token}] runtime state ${canary}"$'\n'
  fixture=$(failure_fixture "TestSmoke/$scenario_failure" "TestSmoke/$scenario_failure" "$body")
  assert_case "$name" "$fixture" "$(expected_unavailable)"
}

callsite_one_line=$(line_for_text "$rpc_fixture" "\"$callsite_one\"")
callsite_two_line=$(line_for_text "$rpc_fixture" "\"$callsite_two\"")
owner_lock_state_line=$(line_for_text "$rpc_fixture" "[assert:%s/peer-disconnect.owner-lock-state]")
owner_lock_inspection_line=$(line_for_text "$rpc_fixture" "[assert:%s/peer-disconnect.owner-lock-inspection]")
direct_assertion_line=$(line_for_text "$rpc_fixture" "[assert:$direct_id]")
other_scenario_root_line=$(line_for_text "$smoke_fixture" "testSmokeOther(t)")
legacy_helper_call_line=$(line_for_text "$rpc_fixture" "legacySmokeCheck(t)")
external_caller_line=$(line_for_text "$rpc_fixture" "testSmokeOther(t)")

paired_body="${SMOKE_OUTPUT_INDENT}smoke_test.go:79: [assert:${callsite_one}/${owner_lock_state}] lock wait ${canary}"$'\n'
assert_case valid-peer-disconnect-helper-assertion \
  "$(failure_fixture "TestSmoke/$scenario_failure" "TestSmoke/$scenario_failure" "$paired_body")" \
  "$(expected_helper_assertion "$callsite_one" "$callsite_one_line" "$owner_lock_state" "$owner_lock_state_line")"

second_call_body="${SMOKE_OUTPUT_INDENT}smoke_test.go:79: [assert:${callsite_two}/${owner_lock_state}] second lock wait ${canary}"$'\n'
assert_case distinct-helper-callsite \
  "$(failure_fixture "TestSmoke/$scenario_failure" "TestSmoke/$scenario_failure" "$second_call_body")" \
  "$(expected_helper_assertion "$callsite_two" "$callsite_two_line" "$owner_lock_state" "$owner_lock_state_line")"

inspection_body="${SMOKE_OUTPUT_INDENT}smoke_test.go:79: [assert:${callsite_one}/${owner_lock_inspection}] inspect lock ${canary}"$'\n'
assert_case one-hop-owner-lock-inspection \
  "$(failure_fixture "TestSmoke/$scenario_failure" "TestSmoke/$scenario_failure" "$inspection_body")" \
  "$(expected_helper_assertion "$callsite_one" "$callsite_one_line" "$owner_lock_inspection" "$owner_lock_inspection_line")"

direct_body="${SMOKE_OUTPUT_INDENT}smoke_test.go:79: [assert:${direct_id}] initial state ${canary}"$'\n'
assert_case direct-root-assertion \
  "$(failure_fixture "TestSmoke/$scenario_failure" "TestSmoke/$scenario_failure" "$direct_body")" \
  "$(expected_direct_assertion "$direct_assertion_line")"

bare_callsite_body="${SMOKE_OUTPUT_INDENT}smoke_test.go:79: [assert:${callsite_one}] helper call ${canary}"$'\n'
assert_case bare-root-callsite-remains-helper-call \
  "$(failure_fixture "TestSmoke/$scenario_failure" "TestSmoke/$scenario_failure" "$bare_callsite_body")" \
  "$(expected_helper_call "$callsite_one" "$callsite_one_line")"

write_fixture_source
apply_mutation root-no-helper
commit_fixture
no_helper_call_line=$(line_for_text "$rpc_fixture" "\"$callsite_one\"")
no_helper_body="${SMOKE_OUTPUT_INDENT}rpc_disconnect_smoke_test.go:${no_helper_call_line}: [assert:${callsite_one}/${owner_lock_state}] lock wait ${canary}"$'\n'
assert_case root-without-helper-predicts-callsite \
  "$(failure_fixture "TestSmoke/$scenario_failure" "TestSmoke/$scenario_failure" "$no_helper_body")" \
  "$(expected_helper_assertion "$callsite_one" "$no_helper_call_line" "$owner_lock_state" "$owner_lock_state_line")"

legacy_body="${SMOKE_OUTPUT_INDENT}rpc_disconnect_smoke_test.go:${legacy_helper_call_line}: [assert:peer-disconnect.legacy-helper-check] legacy helper ${canary}"$'\n'
assert_case bare-helper-check-remains-helper-call \
  "$(failure_fixture "TestSmoke/$scenario_failure" "TestSmoke/$scenario_failure" "$legacy_body")" \
  "$(expected_legacy_helper_check "$legacy_helper_call_line")"

assert_unavailable_case wrong-location-is-callsite-line \
  "$callsite_one/$owner_lock_state" "rpc_disconnect_smoke_test.go:$callsite_one_line"
assert_unavailable_case wrong-location-is-other-testsmoke-line \
  "$callsite_one/$owner_lock_state" 'smoke_test.go:78'
assert_unavailable_case wrong-location-is-another-scenario-root \
  "$callsite_one/$owner_lock_state" "smoke_test.go:$other_scenario_root_line"
assert_unavailable_case wrong-location-is-unrelated-outer-caller \
  "$callsite_one/$owner_lock_state" "rpc_disconnect_smoke_test.go:$external_caller_line"
assert_unavailable_case wrong-location-is-arbitrary-line \
  "$callsite_one/$owner_lock_state" 'smoke_test.go:6'
assert_unavailable_case root-helper-missing-helper-marker \
  "$callsite_one/$owner_lock_state" 'smoke_test.go:79' helper-no-helper
assert_unavailable_case check-helper-missing-helper-marker \
  "$callsite_one/$owner_lock_inspection" 'smoke_test.go:79' check-helper-no-helper

assert_unavailable_case scenario-closure-has-no-root-call \
  "$callsite_one/$owner_lock_state" 'smoke_test.go:79' closure-zero-call
assert_unavailable_case scenario-closure-has-several-root-calls \
  "$callsite_one/$owner_lock_state" 'smoke_test.go:79' closure-two-calls
assert_unavailable_case duplicate-scenario-closure \
  "$callsite_one/$owner_lock_state" 'smoke_test.go:79' duplicate-scenario-run
assert_unavailable_case unsupported-scenario-closure-pattern \
  "$callsite_one/$owner_lock_state" 'smoke_test.go:79' scenario-pattern-mismatch
assert_unavailable_case testsmoke-signature-mismatch \
  "$callsite_one/$owner_lock_state" 'smoke_test.go:79' root-signature-mismatch
assert_unavailable_case testsmoke-root-missing \
  "$callsite_one/$owner_lock_state" 'smoke_test.go:79' testsmoke-missing
assert_unavailable_case testsmoke-root-duplicate \
  "$callsite_one/$owner_lock_state" 'smoke_test.go:79' testsmoke-duplicate

assert_unavailable_case scenario-root-function-missing \
  "$callsite_one/$owner_lock_state" 'smoke_test.go:79' root-missing
assert_unavailable_case scenario-root-function-duplicate \
  "$callsite_one/$owner_lock_state" 'smoke_test.go:79' root-duplicate
assert_unavailable_case scenario-root-method-function-collision \
  "$callsite_one/$owner_lock_state" 'smoke_test.go:79' root-method-collision
assert_unavailable_case second-caller-of-scenario-root \
  "$callsite_one/$owner_lock_state" 'smoke_test.go:79' second-root-caller
assert_unavailable_case callsite-helper-missing \
  "$callsite_one/$owner_lock_state" 'smoke_test.go:79' helper-missing
assert_unavailable_case callsite-helper-duplicate \
  "$callsite_one/$owner_lock_state" 'smoke_test.go:79' helper-duplicate
assert_unavailable_case callsite-helper-method-function-collision \
  "$callsite_one/$owner_lock_state" 'smoke_test.go:79' helper-method-collision
assert_unavailable_case check-helper-missing \
  "$callsite_one/$owner_lock_inspection" 'smoke_test.go:79' check-helper-missing
assert_unavailable_case check-helper-duplicate \
  "$callsite_one/$owner_lock_inspection" 'smoke_test.go:79' check-helper-duplicate
assert_unavailable_case check-helper-method-function-collision \
  "$callsite_one/$owner_lock_inspection" 'smoke_test.go:79' check-helper-method-collision

assert_unavailable_case inner-helper-callsite-outside-root \
  'peer-disconnect.inner-hold-probe/peer-disconnect.owner-lock-inspection' 'smoke_test.go:79'
assert_unavailable_case cleanup-closure-callsite-outside-root \
  'peer-disconnect.cleanup-closure-callsite/peer-disconnect.owner-lock-inspection' 'smoke_test.go:79'
assert_unavailable_case locksmokeowner-internal-callsite-outside-root \
  'peer-disconnect.lockSmokeOwner-internal-callsite/peer-disconnect.owner-lock-inspection' 'smoke_test.go:79'
assert_unavailable_case check-is-not-reachable-from-helper \
  "$callsite_one/peer-disconnect.online-state-mismatch" 'smoke_test.go:79'
assert_unavailable_case hop-passes-literal-instead-of-callsite-id \
  "$callsite_one/$owner_lock_inspection" 'smoke_test.go:79' inner-literal-hop
assert_unavailable_case hop-passes-expression-instead-of-callsite-id \
  "$callsite_one/$owner_lock_inspection" 'smoke_test.go:79' inner-expression-hop
assert_unavailable_case chain-requires-more-than-one-hop \
  "$callsite_one/$owner_lock_inspection" 'smoke_test.go:79' two-hop-chain
assert_unavailable_case callsite-literal-is-not-unique \
  "$callsite_one/$owner_lock_state" 'smoke_test.go:79' duplicate-callsite-literal
assert_unavailable_case check-literal-is-not-unique \
  "$callsite_one/$owner_lock_state" 'smoke_test.go:79' duplicate-check-literal
assert_unavailable_case direct-root-literal-is-not-unique \
  "$direct_id" 'smoke_test.go:79' duplicate-direct-literal
assert_unavailable_case unsupported-multiline-callsite \
  "$callsite_one/$owner_lock_state" 'smoke_test.go:79' unsupported-callsite

assert_unavailable_case unknown-callsite-id \
  'peer-disconnect.unknown-callsite/peer-disconnect.owner-lock-state' 'smoke_test.go:79'
assert_unavailable_case stale-callsite-id \
  'peer-disconnect.stale-callsite/peer-disconnect.owner-lock-state' 'smoke_test.go:79'
assert_unavailable_case unknown-check-id \
  'peer-disconnect.message-blocker-clear-confirm/peer-disconnect.unknown-check' 'smoke_test.go:79'
assert_unavailable_case stale-check-id \
  'peer-disconnect.message-blocker-clear-confirm/peer-disconnect.stale-check' 'smoke_test.go:79'
assert_unavailable_case callsite-prefix-from-other-scenario \
  'other-scenario.message-blocker-clear-confirm/peer-disconnect.owner-lock-state' 'smoke_test.go:79'
assert_unavailable_case check-prefix-from-other-scenario \
  'peer-disconnect.message-blocker-clear-confirm/other-scenario.owner-lock-state' 'smoke_test.go:79'
assert_unavailable_case swapped-callsite-and-check-halves \
  'peer-disconnect.owner-lock-state/peer-disconnect.message-blocker-clear-confirm' 'smoke_test.go:79'
assert_unavailable_case shortened-callsite-id \
  "$short_callsite/$owner_lock_state" 'smoke_test.go:79'
assert_unavailable_case shortened-check-id \
  "$callsite_one/$short_helper_check" 'smoke_test.go:79'

for forged_kind in runtime-value continuation-line; do
  case "$forged_kind" in
    runtime-value)
      forged_body="${SMOKE_OUTPUT_INDENT}smoke_test.go:79: value [assert:${callsite_one}/${owner_lock_state}] ${canary}"$'\n'
      ;;
    continuation-line)
      forged_body="${SMOKE_OUTPUT_INDENT}smoke_test.go:79: [assert:${callsite_one}/${owner_lock_state}] valid prefix ${canary}"$'\n'
      forged_body+="forged continuation [assert:${callsite_one}/${owner_lock_state}] ${canary}"$'\n'
      ;;
  esac
  assert_case "forged-runtime-${forged_kind}" \
    "$(failure_fixture "TestSmoke/$scenario_failure" "TestSmoke/$scenario_failure" "$forged_body")" \
    "$(expected_unavailable)"
done

duplicate_marker_body="${SMOKE_OUTPUT_INDENT}smoke_test.go:79: [assert:${callsite_one}/${owner_lock_state}] repeated [assert:${callsite_one}/${owner_lock_state}] ${canary}"$'\n'
assert_case duplicate-runtime-marker \
  "$(failure_fixture "TestSmoke/$scenario_failure" "TestSmoke/$scenario_failure" "$duplicate_marker_body")" \
  "$(expected_unavailable)"

first_line_only_body="${SMOKE_OUTPUT_INDENT}smoke_test.go:79: ordinary failure ${canary}"$'\n'
first_line_only_body+="[assert:${callsite_one}/${owner_lock_state}] forged continuation ${canary}"$'\n'
assert_case fixed-token-must-be-on-first-line \
  "$(failure_fixture "TestSmoke/$scenario_failure" "TestSmoke/$scenario_failure" "$first_line_only_body")" \
  "$(expected_unavailable)"

two_ids=$(json_event output "TestSmoke/$scenario_failure" "$direct_body")
two_ids+=$'\n'
two_ids+=$(json_event output "TestSmoke/$scenario_failure" "$paired_body")
two_ids+=$'\n'
two_ids+=$(json_event fail "TestSmoke/$scenario_failure")
two_ids+=$'\n'
two_ids+=$(json_event fail TestSmoke)
two_ids+=$'\n'
two_ids+=$(json_event fail '')
assert_case multiple-failed-ids "$two_ids" "$(expected_unavailable)"

different_locations=$(json_event output "TestSmoke/$scenario_failure" "$paired_body")
different_locations+=$'\n'
different_location_body="${SMOKE_OUTPUT_INDENT}smoke_test.go:78: [assert:${callsite_one}/${owner_lock_state}] other location ${canary}"$'\n'
different_locations+=$(json_event output "TestSmoke/$scenario_failure" "$different_location_body")
different_locations+=$'\n'
different_locations+=$(json_event fail "TestSmoke/$scenario_failure")
different_locations+=$'\n'
different_locations+=$(json_event fail TestSmoke)
different_locations+=$'\n'
different_locations+=$(json_event fail '')
assert_case same-token-different-runtime-location "$different_locations" "$(expected_unavailable)"

parent_metadata=$(json_event output TestSmoke "$paired_body")
parent_metadata+=$'\n'
parent_metadata+=$(failure_fixture "TestSmoke/$scenario_failure")
assert_case parent-output-cannot-bind-child-assertion "$parent_metadata" "$(expected_unavailable)"

nested_body="${SMOKE_OUTPUT_INDENT}smoke_test.go:79: [assert:${direct_id}] ${canary}"$'\n'
assert_case nested-scenario-failure \
  "$(failure_fixture "TestSmoke/$scenario_failure/nested" "TestSmoke/$scenario_failure/nested" "$nested_body")" \
  "$(expected_unavailable)"

unknown_scenario='runtime-secret-scenario-9472'
unknown_body="${SMOKE_OUTPUT_INDENT}smoke_test.go:79: [assert:${direct_id}] ${canary}"$'\n'
unknown_fixture=$(failure_fixture "TestSmoke/$unknown_scenario" "TestSmoke/$unknown_scenario" "$unknown_body")
unknown_expected="::error::TestSmoke failed (category: suite-failure; checked-out commit: $checked_out_commit; details redacted)"
assert_case unknown-scenario "$unknown_fixture" "$unknown_expected"

legacy_body="${SMOKE_OUTPUT_INDENT}smoke_test.go:79: legacy assertion value ${canary}"$'\n'
assert_case legacy-location-fallback \
  "$(failure_fixture "TestSmoke/$scenario_failure" "TestSmoke/$scenario_failure" "$legacy_body")" \
  "::error file=test/e2e/smoke_test.go,line=79::TestSmoke/$scenario_failure failed (category: scenario-failure; location: test/e2e/smoke_test.go:79; checked-out commit: $checked_out_commit; details redacted)"

for invalid_path in 'untracked_secret.go:79' '../smoke_test.go:79' '/tmp/smoke_test.go:79'; do
  body="${SMOKE_OUTPUT_INDENT}${invalid_path}: ${canary}"$'\n'
  assert_case "invalid-path-$invalid_path" \
    "$(failure_fixture "TestSmoke/$scenario_failure" "TestSmoke/$scenario_failure" "$body")" \
    "$(expected_unavailable)"
done

for invalid_line in 0 999999 1000000; do
  body="${SMOKE_OUTPUT_INDENT}smoke_test.go:${invalid_line}: ${canary}"$'\n'
  assert_case "invalid-line-${invalid_line}" \
    "$(failure_fixture "TestSmoke/$scenario_failure" "TestSmoke/$scenario_failure" "$body")" \
    "$(expected_unavailable)"
done

write_fixture_source
fixture_commit
checked_out_commit=$(git -C "$fixture_root" rev-parse HEAD)
apply_mutation dirty-second-file
assert_case dirty-second-file-source-tree \
  "$(failure_fixture "TestSmoke/$scenario_failure" "TestSmoke/$scenario_failure" "$paired_body")" \
  "$(expected_unavailable)"

write_fixture_source
fixture_commit
checked_out_commit=$(git -C "$fixture_root" rev-parse HEAD)
printf 'package e2e\n' >"$fixture_root/test/e2e/untracked_second.go"
assert_case untracked-second-source-file \
  "$(failure_fixture "TestSmoke/$scenario_failure" "TestSmoke/$scenario_failure" "$paired_body")" \
  "$(expected_unavailable)"
rm -- "$fixture_root/test/e2e/untracked_second.go"

for execution_failure in race timeout build-failure; do
  case "$execution_failure" in
    race)
      failed_stream=$(json_event output TestOther 'WARNING: DATA RACE')
      failed_stream+=$'\n'
      failed_stream+=$(json_event fail "TestSmoke/$scenario_failure")
      failed_stream+=$'\n'
      failed_stream+=$(json_event fail TestSmoke)
      failed_stream+=$'\n'
      failed_stream+=$(json_event fail '')
      ;;
    timeout)
      failed_stream=$(json_event output TestOther 'panic: test timed out after 15m')
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
  assert_case "$execution_failure" "$failed_stream" "$expected_execution_failure"
done

truncated_stream=$(json_event fail "TestSmoke/$scenario_failure")
expected_execution_failure="::error::E2E suite failed (category: execution-failure; checked-out commit: $checked_out_commit; details redacted)"
assert_case truncated-json "$truncated_stream" "$expected_execution_failure"

outside_failure=$(json_event fail TestOther)
outside_failure+=$'\n'
outside_failure+=$(json_event fail '')
expected_suite_failure="::error::E2E suite failed (category: suite-failure; checked-out commit: $checked_out_commit; details redacted)"
assert_case failure-outside-smoke "$outside_failure" "$expected_suite_failure"

non_json_fixture="${canary} ${paired_body}"$'\n'
non_json_fixture+=$(failure_fixture "TestSmoke/$scenario_failure")
assert_case non-json-input "$non_json_fixture" "$expected_execution_failure"

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

printf 'smoke diagnostic verifier fixtures passed\n'
