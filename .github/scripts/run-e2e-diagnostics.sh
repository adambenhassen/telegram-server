#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
source "$script_dir/smoke-diagnostics.sh"

json_file=$(mktemp "${RUNNER_TEMP:-${TMPDIR:-/tmp}}/e2e-test-json.XXXXXX")
trap 'rm -f -- "$json_file"' EXIT

status=0
go test -race -count=1 -timeout 15m -json "$SMOKE_E2E_PACKAGE" >"$json_file" 2>&1 || status=$?

command_token=$(smoke_generate_command_token 2>/dev/null) || command_token=""
if [[ "$command_token" =~ ^[0-9a-f]{64}$ ]]; then
  passthrough_status=0
  smoke_emit_raw_output "$json_file" "$command_token" || passthrough_status=$?
fi

if [[ "$status" -ne 0 ]]; then
  report_smoke_failure_diagnostics "$status" "$json_file" || true
  exit "$status"
fi

if ! jq -e -Rrs --arg package "$SMOKE_E2E_PACKAGE" '
  split("\n")
  | map(select(length > 0) | try fromjson catch null) as $events
  | ($events | length) > 0
    and all($events[];
      type == "object"
      and .Package == $package
      and (.Action | type) == "string"
      and (.Action as $action | (["start", "run", "pause", "cont", "output", "pass", "bench", "fail", "skip"] | index($action)) != null)
      and (.Test == null or (.Test | type) == "string")
      and (.Output == null or (.Output | type) == "string")
    )
    and (($events[-1].Test // "") == "")
    and ($events[-1].Action == "pass")
' "$json_file" >/dev/null 2>&1; then
  checked_out_commit=$(git rev-parse HEAD 2>/dev/null) || checked_out_commit="unavailable"
  if [[ ! "$checked_out_commit" =~ ^[0-9a-f]{40}$ ]]; then
    checked_out_commit="unavailable"
  fi
  echo "::error::E2E suite did not report a complete passing JSON stream (category: execution-failure; checked-out commit: $checked_out_commit; details redacted)"
  exit 1
fi
