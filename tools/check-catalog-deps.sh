#!/usr/bin/env bash
set -euo pipefail

if rg -n 'catalogpublish' cmd/telegramd internal/api internal/mtproto internal/admin; then
  echo "runtime catalog packages must not import catalogpublish" >&2
  exit 1
fi

deps="$(go list -deps ./cmd/telegramd ./internal/api ./internal/mtproto ./internal/admin)"
if printf '%s\n' "$deps" | rg -n '/internal/catalogpublish$'; then
  echo "runtime dependency graph reaches catalogpublish" >&2
  exit 1
fi

echo "catalog dependency boundary ok"
