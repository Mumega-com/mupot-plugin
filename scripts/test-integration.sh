#!/usr/bin/env bash
set -euo pipefail

plugin_root="$(cd "$(dirname "$0")/.." && pwd)"
: "${MUPOT_SERVER_SOURCE:?Set MUPOT_SERVER_SOURCE to the pinned Mupot server checkout}"
: "${HERMES_SOURCE:?Set HERMES_SOURCE to the pinned Hermes source checkout}"
: "${HERMES_PYTHON:?Set HERMES_PYTHON to the Python interpreter used for native tests}"

readonly expected_server_head="80001a11c29d93a5dd83f09f87eeaff92514f851"
actual_server_head="$(git -C "$MUPOT_SERVER_SOURCE" rev-parse HEAD)"
if [ "$actual_server_head" != "$expected_server_head" ]; then
  printf 'Mupot server head mismatch: expected %s, got %s\n' \
    "$expected_server_head" "$actual_server_head" >&2
  exit 1
fi
if ! git -C "$MUPOT_SERVER_SOURCE" diff --quiet -- \
  || ! git -C "$MUPOT_SERVER_SOURCE" diff --cached --quiet --; then
  printf '%s\n' 'Mupot server checkout must have a clean tracked tree' >&2
  exit 1
fi
if [ ! -x "$MUPOT_SERVER_SOURCE/node_modules/.bin/vitest" ]; then
  printf '%s\n' 'Mupot server checkout is missing its pinned Vitest installation' >&2
  exit 1
fi
if [ ! -f "$HERMES_SOURCE/scripts/run_tests.sh" ]; then
  printf '%s\n' 'HERMES_SOURCE does not contain the canonical Hermes test runner' >&2
  exit 1
fi
if [ ! -x "$HERMES_PYTHON" ]; then
  printf '%s\n' 'HERMES_PYTHON must be executable' >&2
  exit 1
fi

cd "$MUPOT_SERVER_SOURCE"
env \
  MUPOT_PLUGIN_SOURCE="$plugin_root" \
  MUPOT_SERVER_SOURCE="$MUPOT_SERVER_SOURCE" \
  HERMES_SOURCE="$HERMES_SOURCE" \
  HERMES_PYTHON="$HERMES_PYTHON" \
  "$MUPOT_SERVER_SOURCE/node_modules/.bin/vitest" run \
  "$plugin_root/tests/integration/server_routine_envelope.test.ts" \
  --root "$plugin_root" \
  --config "$MUPOT_SERVER_SOURCE/vitest.config.ts" \
  --cache=false \
  --reporter=verbose
