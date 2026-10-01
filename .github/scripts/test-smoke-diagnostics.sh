#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
source "$script_dir/smoke-diagnostics.sh"

probe='diagnostic-probe-code-1234 object-content-private'
package='github.com/adambenhassen/telegram-server/test/e2e'
fixture=$(jq -nc --arg package "$package" --arg output "$probe" \
  '{Package:$package,Action:"fail",Test:"TestSmoke/dialog-filters",Output:$output}')
fixture+=$'\n'
fixture+=$(jq -nc --arg package "$package" --arg output "$probe" \
  '{Package:$package,Action:"fail",Test:"TestSmoke",Output:$output}')

diagnostics=$(report_smoke_failure_diagnostics <<<"$fixture")
expected='::error::TestSmoke/dialog-filters failed (category: scenario-failure; details redacted)'
if [ "$diagnostics" != "$expected" ]; then
  printf 'unexpected sanitized diagnostic\n' >&2
  exit 1
fi
if [[ "$diagnostics" == *"$probe"* ]]; then
  printf 'raw smoke output was exposed\n' >&2
  exit 1
fi

printf 'smoke failure diagnostics are sanitized\n'
