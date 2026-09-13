#!/usr/bin/env bash
set -euo pipefail
repo="$(cd "$(dirname "$0")/.." && pwd)"
: "${HERMES_SOURCE:?Set HERMES_SOURCE to the Hermes source checkout used by your runtime}"
if [ ! -f "$HERMES_SOURCE/scripts/run_tests.sh" ]; then
  printf '%s\n' 'HERMES_SOURCE does not contain the canonical Hermes test runner' >&2
  exit 1
fi
exec bash "$HERMES_SOURCE/scripts/run_tests.sh" "$repo/tests/native" --file-retries 0 --import-mode=importlib "$@"
