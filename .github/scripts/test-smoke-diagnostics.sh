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
probe_go_version=$(sed -nE 's/^go ([0-9]+\.[0-9]+(\.[0-9]+)?)$/\1/p' "$source_root/go.mod")
if [[ ! "$probe_go_version" =~ ^[0-9]+\.[0-9]+(\.[0-9]+)?$ ]]; then
  printf 'Go probe version could not be read from go.mod\n' >&2
  exit 1
fi

require_literal() {
  local path="$1" literal="$2"
  if ! grep -Fq -- "$literal" "$path"; then
    printf 'diagnostic profile command or caller drifted: %s\n' "$path" >&2
    exit 1
  fi
}

require_literal "$source_root/.github/workflows/ci.yml" \
  "go test -json -count=1 -timeout 2m -v ./test/e2e -run '^TestSmoke' 2>&1"
require_literal "$source_root/.github/workflows/ci.yml" \
  'report_smoke_failure_diagnostics "$status" smoke <<<"$output" || true'
require_literal "$script_dir/run-e2e-diagnostics.sh" \
  'go test -race -count=1 -timeout 15m -json "$SMOKE_E2E_PACKAGE"'
require_literal "$script_dir/run-e2e-diagnostics.sh" \
  'report_smoke_failure_diagnostics "$status" full-suite "$json_file" || true'

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
probe_root="$fixture_root/probes"
mkdir -p "$probe_root"

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
    root-callsite-in-cleanup-closure)
      replace_fixture_text "$rpc_fixture" \
        '    waitForSmokeOwnerLock(t, nil, nil, false, "peer-disconnect.message-blocker-clear-confirm")' \
        $'    t.Cleanup(func() {\n        waitForSmokeOwnerLock(t, nil, nil, false, "peer-disconnect.message-blocker-clear-confirm")\n    })'
      ;;
    direct-root-check-in-cleanup-closure)
      replace_fixture_text "$rpc_fixture" \
        '    t.Fatalf("[assert:peer-disconnect.initial-state] initial state: %v", "fixture")' \
        $'    t.Cleanup(func() {\n        t.Fatalf("[assert:peer-disconnect.initial-state] initial state: %v", "fixture")\n    })'
      ;;
    helper-check-in-cleanup-closure)
      replace_fixture_text "$rpc_fixture" \
        '        t.Fatalf("[assert:%s/peer-disconnect.owner-lock-state] owner lock state %t", callsiteID, wantBlocked)' \
        $'        t.Cleanup(func() {\n            t.Fatalf("[assert:%s/peer-disconnect.owner-lock-state] owner lock state %t", callsiteID, wantBlocked)\n        })'
      ;;
    inspection-check-in-cleanup-closure)
      replace_fixture_text "$rpc_fixture" \
        '    t.Fatalf("[assert:%s/peer-disconnect.owner-lock-inspection] inspect owner lock", callsiteID)' \
        $'    t.Cleanup(func() {\n        t.Fatalf("[assert:%s/peer-disconnect.owner-lock-inspection] inspect owner lock", callsiteID)\n    })'
      ;;
    goroutine-check-hop)
      replace_fixture_text "$rpc_fixture" \
        '    smokeOwnerLockCount(t, ctx, lock, callsiteID)' \
        '    go smokeOwnerLockCount(t, ctx, lock, callsiteID)'
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
cat >"$wrapper_script_dir/report-failure.sh" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
source "$script_dir/smoke-diagnostics.sh"
if [[ -n "${MOCK_SCENARIOS+x}" ]]; then
  IFS=',' read -r -a SMOKE_SCENARIOS <<<"$MOCK_SCENARIOS"
fi
if [[ "${MOCK_STDIN:-false}" == true ]]; then
  report_smoke_failure_diagnostics "$MOCK_GO_STATUS" "$MOCK_PROFILE" || true
elif [[ "${MOCK_OMIT_PROFILE:-false}" == true ]]; then
  report_smoke_failure_diagnostics "$MOCK_GO_STATUS" -- "$MOCK_GO_JSON" || true
else
  report_smoke_failure_diagnostics "$MOCK_GO_STATUS" "$MOCK_PROFILE" "$MOCK_GO_JSON" || true
fi
exit "$MOCK_GO_STATUS"
EOF
chmod +x "$wrapper_script_dir/report-failure.sh"
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
  local name="$1" fixture="$2" expected="$3" profile="${4:-smoke}" \
    test_status="${5:-37}" actual output result_status \
    stop_line resume_line token_from_line suppressed_output post_resume \
    wrapper_diagnostics diagnostics status
  local canary_event
  canary_event=$(json_event output "TestSmoke/$scenario_failure" \
    "untrusted runtime detail ${canary} ::error file=/tmp/forged.go,line=1::forged ::stop-commands::attacker")
  fixture="${canary_event}"$'\n'"${fixture}"
  if [[ "$fixture" != *"$canary"* ]]; then
    printf 'smoke verifier case omitted its redaction canary: %s\n' "$name" >&2
    exit 1
  fi

  actual=$(report_smoke_failure_diagnostics "$test_status" "$profile" <<<"$fixture")
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
    MOCK_GO_ARGS="$mock_args" MOCK_GO_JSON="$mock_json" MOCK_GO_STATUS="$test_status" \
    bash "$wrapper_script_dir/run-e2e-diagnostics.sh" 2>&1); then
    result_status=0
  else
    result_status=$?
  fi
  if [[ "$result_status" -ne "$test_status" ]]; then
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
  suppressed_output="${output#*"$stop_line"$'\n'}"
  suppressed_output="${suppressed_output%%"$resume_line"*}"
  if [[ "$suppressed_output" != *"$canary"* \
    || "$suppressed_output" != *'::error file=/tmp/forged.go,line=1::forged ::stop-commands::attacker'* ]]; then
    printf 'E2E raw output injection fixture escaped command suppression: %s\n' "$name" >&2
    exit 1
  fi
  post_resume="${output#*"$resume_line"$'\n'}"
  wrapper_diagnostics=$(grep '^::error' <<<"$post_resume" || true)
  if [[ "$wrapper_diagnostics" != "$actual" || "$post_resume" == *"$canary"* ]]; then
    printf 'E2E wrapper changed or exposed the diagnostic: %s\n' "$name" >&2
    exit 1
  fi
  if find "$runner_temp" -maxdepth 1 -type f -name 'e2e-test-json.*' -print -quit | grep -q .; then
    printf 'E2E JSON temporary file remained after verifier case: %s\n' "$name" >&2
    exit 1
  fi

  for status in 1 2; do
    if output=$(PATH="$mock_bin:$PATH" RUNNER_TEMP="$runner_temp" \
      SMOKE_DIAGNOSTICS_ROOT="$fixture_root" SMOKE_OUTPUT_INDENT="$SMOKE_OUTPUT_INDENT" \
      MOCK_GO_JSON="$mock_json" MOCK_GO_STATUS="$status" MOCK_PROFILE="$profile" \
      bash "$wrapper_script_dir/report-failure.sh" 2>&1); then
      result_status=0
    else
      result_status=$?
    fi
    if [[ "$result_status" -ne "$status" ]]; then
      printf 'reporter fixture changed the injected test status: %s (status %s)\n' \
        "$name" "$status" >&2
      exit 1
    fi
    diagnostics=$(grep '^::error' <<<"$output" || true)
    if [[ "$diagnostics" != "$expected" || "$output" == *"$canary"* ]]; then
      printf 'reporter fixture wrapper changed or exposed output: %s (status %s)\n' \
        "$name" "$status" >&2
      exit 1
    fi
  done
}

expected_execution_failure() {
  printf '::error::E2E suite failed (category: execution-failure; reason: %s; checked-out commit: %s; details redacted)' \
    "$1" "$checked_out_commit"
}

assert_execution_input_case() {
  local name="$1" profile="$2" reason="$3" input_path="$4" \
    require_canary="${5:-yes}" expected status actual output result_status diagnostics
  expected=$(expected_execution_failure "$reason")
  if [[ "$require_canary" == yes ]] && ! grep -qF -- "$canary" "$input_path"; then
    printf 'execution-failure fixture omitted its redaction canary: %s\n' "$name" >&2
    exit 1
  fi

  actual=$(report_smoke_failure_diagnostics 37 "$profile" "$input_path")
  if [[ "$actual" != "$expected" || "$actual" == *"$canary"* \
    || "$actual" == *'file='* || "$actual" == *'line='* ]]; then
    printf 'unexpected execution-failure diagnostic: %s\n' "$name" >&2
    exit 1
  fi

  if [[ "$profile" == full-suite && -s "$input_path" ]]; then
    local fixture
    fixture=$(cat -- "$input_path")
    assert_case "$name-full-suite-wrapper" "$fixture" "$expected" full-suite
    return
  fi

  for status in 1 2; do
    actual=$(report_smoke_failure_diagnostics "$status" "$profile" "$input_path")
    if [[ "$actual" != "$expected" || "$actual" == *"$canary"* \
      || "$actual" == *'file='* || "$actual" == *'line='* ]]; then
      printf 'execution-failure status fixture changed its diagnostic: %s (status %s)\n' \
        "$name" "$status" >&2
      exit 1
    fi

    if output=$(PATH="$mock_bin:$PATH" RUNNER_TEMP="$runner_temp" \
      SMOKE_DIAGNOSTICS_ROOT="$fixture_root" SMOKE_OUTPUT_INDENT="$SMOKE_OUTPUT_INDENT" \
      MOCK_GO_JSON="$input_path" MOCK_GO_STATUS="$status" MOCK_PROFILE="$profile" \
      bash "$wrapper_script_dir/report-failure.sh" 2>&1); then
      result_status=0
    else
      result_status=$?
    fi
    if [[ "$result_status" -ne "$status" ]]; then
      printf 'reporter changed the injected test status: %s (status %s)\n' \
        "$name" "$status" >&2
      exit 1
    fi
    diagnostics=$(grep '^::error' <<<"$output" || true)
    if [[ "$diagnostics" != "$expected" || "$output" == *"$canary"* \
      || "$diagnostics" == *'file='* || "$diagnostics" == *'line='* ]]; then
      printf 'reporter wrapper changed or exposed the diagnostic: %s (status %s)\n' \
        "$name" "$status" >&2
      exit 1
    fi
  done
}

assert_execution_stdin_case() {
  local name="$1" profile="$2" reason="$3" input_path="$4" \
    expected status actual output result_status diagnostics
  expected=$(expected_execution_failure "$reason")
  if ! grep -qF -- "$canary" "$input_path"; then
    printf 'execution-failure stdin fixture omitted its redaction canary: %s\n' "$name" >&2
    exit 1
  fi

  for status in 1 2; do
    actual=$(PYTHONUTF8=1 report_smoke_failure_diagnostics "$status" "$profile" <"$input_path")
    if [[ "$actual" != "$expected" || "$actual" == *"$canary"* \
      || "$actual" == *'file='* || "$actual" == *'line='* ]]; then
      printf 'unexpected execution-failure stdin diagnostic: %s (status %s)\n' \
        "$name" "$status" >&2
      exit 1
    fi

    if output=$(PYTHONUTF8=1 PATH="$mock_bin:$PATH" RUNNER_TEMP="$runner_temp" \
      SMOKE_DIAGNOSTICS_ROOT="$fixture_root" SMOKE_OUTPUT_INDENT="$SMOKE_OUTPUT_INDENT" \
      MOCK_GO_JSON="$input_path" MOCK_GO_STATUS="$status" MOCK_PROFILE="$profile" \
      MOCK_STDIN=true bash "$wrapper_script_dir/report-failure.sh" <"$input_path" 2>&1); then
      result_status=0
    else
      result_status=$?
    fi
    if [[ "$result_status" -ne "$status" ]]; then
      printf 'reporter changed the injected stdin test status: %s (status %s)\n' \
        "$name" "$status" >&2
      exit 1
    fi
    diagnostics=$(grep '^::error' <<<"$output" || true)
    if [[ "$diagnostics" != "$expected" || "$output" == *"$canary"* \
      || "$diagnostics" == *'file='* || "$diagnostics" == *'line='* ]]; then
      printf 'reporter stdin wrapper changed or exposed output: %s (status %s)\n' \
        "$name" "$status" >&2
      exit 1
    fi
  done
}

assert_reporter_exception_case() {
  local name="$1" actual expected
  if ! actual=$(python3 - "$script_dir/smoke-diagnostics.py" "$fixture_root" "$mock_json" <<'PY'
import contextlib
import io
import runpy
import sys

script_path, root, input_path = sys.argv[1:]
namespace = runpy.run_path(script_path, run_name="smoke_diagnostics")
module_globals = namespace["main"].__globals__
module_globals["sys"].argv = [
    script_path,
    "--status",
    "37",
    "--package",
    "github.com/teagramhq/teagram-server/test/e2e",
    "--root",
    root,
    "--profile",
    "smoke",
    "--scenario",
    "peer-disconnect",
    input_path,
]

def fail_report(*_args):
    raise RuntimeError("fixture exception")

module_globals["report_failure"] = fail_report
captured = io.StringIO()
with contextlib.redirect_stdout(captured):
    status = namespace["main"]()
if status != 0:
    raise SystemExit("reporter exception changed the test status")
sys.stdout.write(captured.getvalue())
PY
  ); then
    printf 'reporter exception fixture could not run: %s\n' "$name" >&2
    exit 1
  fi
  expected=$(expected_execution_failure unknown)
  if [[ "$actual" != "$expected" ]]; then
    printf 'unexpected execution-failure diagnostic: %s\n' "$name" >&2
    exit 1
  fi
}

write_probe_module() {
  local name="$1" module_path="$2"
  mkdir -p "$probe_root/$name"
  cat >"$probe_root/$name/go.mod" <<EOF
module $module_path

go $probe_go_version
EOF
}

write_probe_module timeout example.com/smoke-timeout-probe
cat >"$probe_root/timeout/timeout_test.go" <<'EOF'
package fixture

import (
	"testing"
	"time"
)

func TestSmoke(t *testing.T) {
	time.Sleep(10 * time.Second)
}
EOF

write_probe_module race example.com/smoke-race-probe
cat >"$probe_root/race/race_test.go" <<'EOF'
package fixture

import (
	"testing"
)

var raceValue int

func TestRace(t *testing.T) {
	done := make(chan struct{})
	go func() {
		raceValue = 1
		close(done)
	}()
	raceValue = 2
	<-done
}
EOF

write_probe_module build example.com/smoke-build-probe
cat >"$probe_root/build/build_test.go" <<'EOF'
package fixture

import "testing"

func TestBuildFailure(t *testing.T) {
	missingDiagnosticProbeSymbol()
}
EOF

run_probe() {
  local name="$1" status=0
  shift
  if (cd -- "$probe_root/$name" && go test "$@" .) >"$probe_root/$name.json" 2>&1; then
    status=0
  else
    status=$?
  fi
  if [[ "$status" -eq 0 ]]; then
    printf 'Go diagnostic probe unexpectedly passed: %s\n' "$name" >&2
    exit 1
  fi
}

run_probe timeout -json -count=1 -timeout 1s -run '^TestSmoke$'
run_probe race -race -json -count=1 -run '^TestRace$'
run_probe build -json -count=1 -timeout 1m -run '^TestBuildFailure$'

probe_attributions=$(python3 - "$probe_root/timeout.json" "$probe_root/race.json" "$probe_root/build.json" <<'PY'
import json
import re
import sys
from pathlib import Path


def events(path):
    result = []
    for line in Path(path).read_text(encoding="utf-8").splitlines():
        if line.strip():
            result.append(json.loads(line))
    if not result or any(not isinstance(event, dict) for event in result):
        raise SystemExit("Go diagnostic probe emitted an unexpected event stream")
    return result


timeout_events = events(sys.argv[1])
timeout_output = [
    event
    for event in timeout_events
    if event.get("Action") == "output"
    and "panic: test timed out after" in event.get("Output", "")
]
if (
    len(timeout_output) != 1
    or timeout_output[0].get("Test") != "TestSmoke"
    or timeout_output[0].get("Output") != "panic: test timed out after 1s\n"
):
    raise SystemExit("Go timeout event shape drifted")

race_events = events(sys.argv[2])
race_output = [
    event
    for event in race_events
    if event.get("Action") == "output" and event.get("Output") == "WARNING: DATA RACE\n"
]
race_companion = [
    event
    for event in race_events
    if event.get("Action") == "output"
    and event.get("Test") == "TestRace"
    and isinstance(event.get("Output"), str)
    and "race detected during execution of test" in event["Output"]
]
if (
    len(race_output) != 1
    or race_output[0].get("Test") != "TestRace"
    or len(race_companion) != 1
    or re.fullmatch(
        r" +testing\.go:[1-9][0-9]*: race detected during execution of test\n",
        race_companion[0]["Output"],
    )
    is None
):
    raise SystemExit("Go race event shape drifted")

build_events = events(sys.argv[3])
build_diagnostics = [
    event for event in build_events if event.get("Action") in {"build-output", "build-fail"}
]
if not build_diagnostics or not any(event.get("Action") == "build-fail" for event in build_diagnostics):
    raise SystemExit("Go build event shape drifted")
if any(
    not isinstance(event.get("ImportPath"), str)
    or "Package" in event
    or "Test" in event
    or ("Output" in event and not isinstance(event["Output"], str))
    for event in build_diagnostics
):
    raise SystemExit("Go build event shape drifted")
terminal_failures = [
    event
    for event in build_events
    if event.get("Action") == "fail"
    and event.get("Package") == "example.com/smoke-build-probe"
    and event.get("Test", "") == ""
]
if (
    len(terminal_failures) != 1
    or not isinstance(terminal_failures[0].get("FailedBuild"), str)
    or not terminal_failures[0]["FailedBuild"]
):
    raise SystemExit("Go FailedBuild event shape drifted")

print(timeout_output[0]["Test"])
print(race_output[0]["Test"])
PY
)
timeout_test="${probe_attributions%%$'\n'*}"
race_test="${probe_attributions#*$'\n'}"
if [[ "$timeout_test" != TestSmoke || "$race_test" != TestRace ]]; then
  printf 'Go diagnostic probe attribution drifted\n' >&2
  exit 1
fi

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
assert_unavailable_case root-callsite-in-cleanup-closure \
  "$callsite_one/$owner_lock_state" 'smoke_test.go:79' root-callsite-in-cleanup-closure
assert_unavailable_case direct-root-check-in-cleanup-closure \
  "$direct_id" 'smoke_test.go:79' direct-root-check-in-cleanup-closure
assert_unavailable_case helper-check-in-cleanup-closure \
  "$callsite_one/$owner_lock_state" 'smoke_test.go:79' helper-check-in-cleanup-closure
assert_unavailable_case inspection-check-in-cleanup-closure \
  "$callsite_one/$owner_lock_inspection" 'smoke_test.go:79' inspection-check-in-cleanup-closure
assert_unavailable_case goroutine-check-hop \
  "$callsite_one/$owner_lock_inspection" 'smoke_test.go:79' goroutine-check-hop

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

execution_canary_event=$(json_event output TestOther \
  "untrusted runtime detail ${canary} ::error file=/tmp/forged.go,line=1::forged ::stop-commands::attacker")
package_terminal_fail=$(json_event fail '')

go_build_event() {
  local action="$1" import_path="$2" body="$3"
  jq -cn --arg action "$action" --arg import_path "$import_path" --arg output "$body" \
    '{Action:$action,ImportPath:$import_path,Output:$output}'
}

failed_build_terminal() {
  jq -cn --arg package "$SMOKE_E2E_PACKAGE" --arg failed_build "$1" \
    '{Package:$package,Action:"fail",FailedBuild:$failed_build}'
}

write_execution_case() {
  local name="$1" profile="$2" reason="$3" stream="$4" \
    prepend_canary="${5:-yes}" require_canary="${6:-yes}" input_path
  input_path="$fixture_root/$name.json"
  if [[ "$prepend_canary" == yes ]]; then
    stream="${execution_canary_event}"$'\n'"$stream"
  fi
  printf '%s\n' "$stream" >"$input_path"
  assert_execution_input_case "$name" "$profile" "$reason" "$input_path" "$require_canary"
}

timeout_smoke=$(json_event output "$timeout_test" $'panic: test timed out after 2m0s\n')
timeout_full=$(json_event output "$timeout_test" $'panic: test timed out after 15m0s\n')

smoke_timeout_stream="$timeout_smoke"$'\n'"$(failure_fixture "TestSmoke/$scenario_failure")"
write_execution_case timeout-smoke smoke timeout-signature "$smoke_timeout_stream"
full_timeout_stream="$timeout_full"$'\n'"$(failure_fixture "TestSmoke/$scenario_failure")"
write_execution_case timeout-full-suite full-suite timeout-signature "$full_timeout_stream"

race_event=$(json_event output "$race_test" $'WARNING: DATA RACE\n')
race_companion_event=$(json_event output "$race_test" \
  $'    testing.go:1865: race detected during execution of test\n')
race_stream="$race_event"$'\n'"$race_companion_event"$'\n'"$(failure_fixture "TestSmoke/$scenario_failure")"
write_execution_case race-full-suite full-suite race-signature "$race_stream"

build_events_stream=$(go_build_event build-output example.com/build-probe "compiler output ${canary}")
build_events_stream+=$'\n'
build_events_stream+=$(go_build_event build-fail example.com/build-probe "build details ${canary}")
build_stream="$build_events_stream"$'\n'
build_stream+=$(failed_build_terminal "private build value ${canary}")
write_execution_case build-failure full-suite build-failure-signature "$build_stream"

# The concrete regression: an output event is followed by a line truncated in
# the middle of a JSON object. Cutting the same input on the previous newline is
# an incomplete stream instead.
truncated_line='{"Package":"github.com/teagramhq/teagram-server/test/e2e","Action":"fail","Test":'
write_execution_case mid-line-truncated-json smoke invalid-stream "$truncated_line"
write_execution_case line-boundary-truncated-json smoke incomplete-stream ''

missing_terminal_stream=$(json_event fail "TestSmoke/$scenario_failure")
write_execution_case missing-package-terminal smoke incomplete-stream "$missing_terminal_stream"
empty_input="$fixture_root/empty.json"
: >"$empty_input"
assert_execution_input_case empty-stream smoke incomplete-stream "$empty_input" no

timeout_incomplete_stream="$timeout_full"$'\n'"$(json_event fail "TestSmoke/$scenario_failure")"
write_execution_case incomplete-before-signature full-suite incomplete-stream "$timeout_incomplete_stream"

non_json_input="$fixture_root/non-json-first-line.json"
printf '::error file=/tmp/forged.go,line=1::%s ::stop-commands::attacker\n%s\n%s\n' \
  "$canary" "$execution_canary_event" "$package_terminal_fail" >"$non_json_input"
assert_execution_input_case non-json-command-injection smoke invalid-stream "$non_json_input"

wrong_package=$(jq -cn --arg output "private runtime ${canary}" \
  '{Package:"example.com/wrong-package",Action:"output",Test:"TestOther",Output:$output}')
write_execution_case wrong-package smoke invalid-stream "$wrong_package"

unknown_action=$(json_event mystery TestOther "private runtime ${canary}")
write_execution_case unknown-action smoke invalid-stream "$unknown_action"

non_string_test=$(jq -cn --arg package "$SMOKE_E2E_PACKAGE" --arg output "$canary" \
  '{Package:$package,Action:"output",Test:7,Output:$output}')
write_execution_case non-string-test smoke invalid-stream "$non_string_test"

non_string_output=$(jq -cn --arg package "$SMOKE_E2E_PACKAGE" \
  '{Package:$package,Action:"output",Test:"TestOther",Output:7}')
write_execution_case non-string-output smoke invalid-stream "$non_string_output"

non_string_failed_build=$(jq -cn --arg package "$SMOKE_E2E_PACKAGE" \
  '{Package:$package,Action:"fail",FailedBuild:7}')
write_execution_case non-string-failed-build smoke invalid-stream "$non_string_failed_build"

json_array='[]'
write_execution_case json-array smoke invalid-stream "$json_array"

bad_build_event=$(jq -cn '{Action:"build-fail",Package:"example.com/build-probe",ImportPath:"example.com/build-probe"}')
write_execution_case build-event-with-package smoke invalid-stream "$bad_build_event"
bad_build_event=$(jq -cn '{Action:"build-output",Test:"TestBuild",ImportPath:"example.com/build-probe",Output:"secret"}')
write_execution_case build-event-with-test smoke invalid-stream "$bad_build_event"
bad_build_event=$(jq -cn '{Action:"build-fail",Output:"missing import path"}')
write_execution_case build-event-without-import-path smoke invalid-stream "$bad_build_event"

invalid_utf8_input="$fixture_root/invalid-utf8.json"
printf '%s\n' "$execution_canary_event" >"$invalid_utf8_input"
printf '{"Package":"github.com/teagramhq/teagram-server/test/e2e","Action":"output","Test":"TestOther","Output":"\377"}\n' >>"$invalid_utf8_input"
printf '%s\n%s\n%s\n' "$timeout_full" "$(json_event fail "TestSmoke/$scenario_failure")" "$package_terminal_fail" >>"$invalid_utf8_input"
assert_execution_input_case invalid-utf8 smoke invalid-stream "$invalid_utf8_input"

forged_timeout_cases=(
  "noise panic: test timed out after 15m0s"
  "panic: test timed out after 15m0s with trailing text"
  "panic: test timed out after 2m0s"
  $'panic: test timed out after 15m0s\ncontinuation'
)
for index in "${!forged_timeout_cases[@]}"; do
  forged_event=$(json_event output "$timeout_test" "${forged_timeout_cases[$index]}")
  forged_stream="$forged_event"$'\n'"$(failure_fixture "TestSmoke/$scenario_failure")"
  write_execution_case "forged-timeout-$index" full-suite unknown "$forged_stream"
done

repeated_timeout_stream="$timeout_full"$'\n'"$timeout_full"$'\n'"$(failure_fixture "TestSmoke/$scenario_failure")"
write_execution_case repeated-timeout full-suite unknown "$repeated_timeout_stream"

split_timeout_stream=$(json_event output "$timeout_test" 'panic: test timed out after')
split_timeout_stream+=$'\n'
split_timeout_stream+=$(json_event output "$timeout_test" '15m0s')
split_timeout_stream+=$'\n'
split_timeout_stream+=$(failure_fixture "TestSmoke/$scenario_failure")
write_execution_case split-timeout-continuation full-suite unknown "$split_timeout_stream"

race_smoke_stream="$race_event"$'\n'"$(failure_fixture "TestSmoke/$scenario_failure")"
write_execution_case race-in-smoke smoke unknown "$race_smoke_stream"

midline_race=$(json_event output "$race_test" 'text before WARNING: DATA RACE\n')
midline_race+=$'\n'
midline_race+=$(failure_fixture "TestSmoke/$scenario_failure")
write_execution_case midline-race full-suite unknown "$midline_race"

race_timeout_stream="$race_event"$'\n'"$timeout_full"$'\n'"$(failure_fixture "TestSmoke/$scenario_failure")"
write_execution_case overlapping-race-timeout full-suite unknown "$race_timeout_stream"

build_timeout_stream="$build_events_stream"$'\n'"$timeout_full"$'\n'"$(failed_build_terminal "private build value ${canary}")"
write_execution_case overlapping-build-timeout full-suite unknown "$build_timeout_stream"

valid_assertion_event=$(json_event output "TestSmoke/$scenario_failure" \
  "${SMOKE_OUTPUT_INDENT}smoke_test.go:79: [assert:${direct_id}] assertion ${canary}"$'\n')
stdin_invalid_utf8_input="$fixture_root/stdin-invalid-utf8.json"
printf '%s\n' "$valid_assertion_event" >"$stdin_invalid_utf8_input"
python3 - "$stdin_invalid_utf8_input" "$SMOKE_E2E_PACKAGE" <<'PY'
import json
import sys

path, package = sys.argv[1:]
event = {
    "Package": package,
    "Action": "output",
    "Test": "TestOther",
    "Output": "invalid-utf8-sentinel",
}
line = json.dumps(event, separators=(",", ":")).encode("utf-8")
line = line.replace(b"invalid-utf8-sentinel", b"\xff", 1)
with open(path, "ab") as stream:
    stream.write(line + b"\n")
PY
printf '%s\n' "$(failure_fixture "TestSmoke/$scenario_failure")" >>"$stdin_invalid_utf8_input"
assert_execution_stdin_case stdin-invalid-utf8 smoke invalid-stream "$stdin_invalid_utf8_input"
forged_assertion_stream="$valid_assertion_event"$'\n'"$(json_event output "$timeout_test" 'panic: test timed out after 15m0s in test output')"$'\n'"$(failure_fixture "TestSmoke/$scenario_failure")"
write_execution_case forged-signature-with-assertion full-suite unknown "$forged_assertion_stream"

invalid_timeout_stream='not-json'
invalid_timeout_stream+=$'\n'
invalid_timeout_stream+="$timeout_full"
invalid_timeout_stream+=$'\n'
invalid_timeout_stream+=$(failure_fixture "TestSmoke/$scenario_failure")
write_execution_case invalid-before-signature smoke invalid-stream "$invalid_timeout_stream"

terminal_build=$(failed_build_terminal "private build value ${canary}")
build_failure_event=$(go_build_event build-fail example.com/build-probe "${canary}")
build_signature_stream="$build_failure_event"$'\n'"$terminal_build"
write_execution_case build-failure-signature full-suite build-failure-signature "$build_signature_stream"

timeout_status_pass="$timeout_full"$'\n'"$(json_event pass TestSmoke)"$'\n'"$(json_event pass '')"
write_execution_case status-without-failure-event full-suite status-without-failure-event "$timeout_status_pass"

stream_unavailable="$fixture_root/no-such-directory/input.json"
assert_execution_input_case unavailable-input smoke stream-unavailable "$stream_unavailable" no
assert_reporter_exception_case reporter-read-exception

configuration_input="$fixture_root/configuration-canary.json"
printf '%s\n%s\n' "$execution_canary_event" "$(json_event pass '')" >"$configuration_input"
configuration_bad_input="$fixture_root/configuration-invalid.json"
printf '%s\n' 'not-json' >"$configuration_bad_input"

assert_configuration_case() {
  local name="$1" scenario_csv="$2" profile="$3" omit_profile="$4" \
    input_path="$5" alternate_input="${6:-}" expected actual alternate \
    status output result_status diagnostics scenario_value
  expected=$(expected_execution_failure invalid-configuration)

  if [[ -n "$scenario_csv" ]]; then
    IFS=',' read -r -a SMOKE_SCENARIOS <<<"$scenario_csv"
  else
    SMOKE_SCENARIOS=()
  fi
  if [[ "$omit_profile" == true ]]; then
    actual=$(report_smoke_failure_diagnostics 1 -- "$input_path")
  else
    actual=$(report_smoke_failure_diagnostics 1 "$profile" "$input_path")
  fi
  if [[ "$actual" != "$expected" || "$actual" == *"$canary"* \
    || "$actual" == *'file='* || "$actual" == *'line='* ]]; then
    printf 'invalid configuration did not fail closed: %s\n' "$name" >&2
    exit 1
  fi
  if [[ -n "$alternate_input" ]]; then
    if [[ "$omit_profile" == true ]]; then
      alternate=$(report_smoke_failure_diagnostics 1 -- "$alternate_input")
    else
      alternate=$(report_smoke_failure_diagnostics 1 "$profile" "$alternate_input")
    fi
    if [[ "$alternate" != "$actual" ]]; then
      printf 'invalid configuration read its input stream: %s\n' "$name" >&2
      exit 1
    fi
  fi

  for status in 1 2; do
    if [[ "${SMOKE_SCENARIOS[*]}" == *' '* ]]; then
      scenario_value=$(IFS=,; printf '%s' "${SMOKE_SCENARIOS[*]}")
    else
      scenario_value="$scenario_csv"
    fi
    if output=$(PATH="$mock_bin:$PATH" RUNNER_TEMP="$runner_temp" \
      SMOKE_DIAGNOSTICS_ROOT="$fixture_root" SMOKE_OUTPUT_INDENT="$SMOKE_OUTPUT_INDENT" \
      MOCK_GO_JSON="$input_path" MOCK_GO_STATUS="$status" MOCK_PROFILE="$profile" \
      MOCK_SCENARIOS="$scenario_value" MOCK_OMIT_PROFILE="$omit_profile" \
      bash "$wrapper_script_dir/report-failure.sh" 2>&1); then
      result_status=0
    else
      result_status=$?
    fi
    if [[ "$result_status" -ne "$status" ]]; then
      printf 'invalid reporter configuration changed test status: %s\n' "$name" >&2
      exit 1
    fi
    diagnostics=$(grep '^::error' <<<"$output" || true)
    if [[ "$diagnostics" != "$expected" || "$output" == *"$canary"* \
      || "$diagnostics" == *'file='* || "$diagnostics" == *'line='* ]]; then
      printf 'invalid configuration wrapper exposed input or changed output: %s\n' "$name" >&2
      exit 1
    fi
  done
  SMOKE_SCENARIOS=(peer-disconnect)
}

assert_configuration_case duplicate-scenarios 'peer-disconnect,peer-disconnect' smoke false \
  "$configuration_input" "$configuration_bad_input"
assert_configuration_case invalid-scenario-grammar 'peer_disconnect' smoke false \
  "$configuration_input" "$configuration_bad_input"
assert_configuration_case unknown-profile peer-disconnect unknown false \
  "$configuration_input" "$configuration_bad_input"
assert_configuration_case missing-profile peer-disconnect '' true \
  "$configuration_input" "$fixture_root/no-such-directory/missing.json"

invalid_config_missing_scenarios="$fixture_root/missing-scenarios.json"
printf '%s\n' "$execution_canary_event" >"$invalid_config_missing_scenarios"
assert_configuration_case missing-scenarios '' smoke false \
  "$invalid_config_missing_scenarios" "$configuration_bad_input"

passing_fixture="$execution_canary_event"$'\n'"$(json_event pass "TestSmoke/$scenario_failure")"$'\n'"$(json_event pass TestSmoke)"$'\n'"$(json_event pass '')"
passing_path="$fixture_root/passing.json"
printf '%s\n' "$passing_fixture" >"$passing_path"
passing_diagnostics=$(report_smoke_failure_diagnostics 0 smoke "$passing_path")
if [[ -n "$passing_diagnostics" ]]; then
  printf 'passing smoke fixture produced diagnostics\n' >&2
  exit 1
fi

outside_failure=$(json_event fail TestOther)
outside_failure+=$'\n'
outside_failure+=$(json_event fail '')
expected_suite_failure="::error::E2E suite failed (category: suite-failure; checked-out commit: $checked_out_commit; details redacted)"
assert_case failure-outside-smoke "$outside_failure" "$expected_suite_failure"

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
