#!/usr/bin/env bash
set -euo pipefail

repo="$(cd "$(dirname "$0")/.." && pwd)"
tmp="$(mktemp -d)"
python="${PYTHON:-python3}"
cleanup() {
  cd /
  rm -rf "$tmp"
}
trap cleanup EXIT

ln -s "$repo" "$tmp/plugin"
cd "$tmp"
"$python" -m pytest plugin/tests -q
"$python" -m unittest -q plugin.tests.test_operator
"$python" -m py_compile \
  plugin/__init__.py \
  plugin/mupot_operator.py \
  plugin/schemas.py \
  plugin/tools.py

# Regression: starting Python from the plugin directory must not shadow the
# standard-library `operator` module.
cd "$repo"
"$python" -c 'import operator; assert hasattr(operator, "itemgetter")'
