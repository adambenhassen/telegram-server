report_smoke_failure_diagnostics() {
  local output failed_tests reported=0
  output=$(cat)
  if ! failed_tests=$(jq -Rr '
    fromjson?
    | select(.Package == "github.com/adambenhassen/telegram-server/test/e2e"
        and .Action == "fail")
    | .Test // empty
  ' <<<"$output"); then
    failed_tests=""
  fi

  for scenario in one-to-one saved-messages basic-group channel contacts-search username-registration; do
    if grep -Fqx -- "TestSmoke/$scenario" <<<"$failed_tests"; then
      echo "::error::TestSmoke/$scenario failed (category: scenario-failure; details redacted)"
      reported=1
    fi
  done
  if [ "$reported" -eq 0 ]; then
    if grep -Fqx -- 'TestSmoke' <<<"$failed_tests"; then
      echo "::error::TestSmoke failed (category: suite-failure; details redacted)"
    else
      echo "::error::TestSmoke failed (category: execution-failure; details redacted)"
    fi
  fi
}
