#!/usr/bin/env bash
set -euo pipefail
repo="$(cd "$(dirname "$0")/.." && pwd)"
: "${HERMES_SOURCE:?Set HERMES_SOURCE to the Hermes source checkout used by your runtime}"
if [ ! -f "$HERMES_SOURCE/scripts/run_tests.sh" ]; then
  printf '%s\n' 'HERMES_SOURCE does not contain the canonical Hermes test runner' >&2
  exit 1
fi

# Local release checkouts can contain a Hermes venv with pytest but without
# the native test extras.  When the caller supplies the validated runtime
# interpreter, keep Hermes as the working tree while using that interpreter
# explicitly; this also prevents this plugin's top-level tools.py from
# shadowing Hermes's tools package.
if [ -n "${HERMES_PYTHON:-}" ]; then
  if [ ! -x "$HERMES_PYTHON" ] || ! "$HERMES_PYTHON" -c 'import pytest' 2>/dev/null; then
    printf '%s\n' 'HERMES_PYTHON must be an executable Python with pytest' >&2
    exit 1
  fi
  runtime_site=""
  for runtime_python in "$HERMES_SOURCE/.venv/bin/python" "$HERMES_SOURCE/venv/bin/python"; do
    if [ -x "$runtime_python" ]; then
      candidate="$($runtime_python -c 'import sysconfig; print(sysconfig.get_paths()["purelib"])')"
      if [ -d "$candidate" ]; then
        runtime_site="$candidate"
        break
      fi
    fi
  done
  host_sites="$($HERMES_PYTHON -c 'import site; print(":".join([*site.getsitepackages(), site.getusersitepackages()]))')"
  python_path="$HERMES_SOURCE:$host_sites"
  if [ -n "$runtime_site" ]; then
    python_path="$python_path:$runtime_site"
  fi
  cd "$HERMES_SOURCE"
  exec env -i \
    PATH="$PATH" \
    HOME="$HOME" \
    PYTHONPATH="$python_path" \
    PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 \
    TZ=UTC \
    LANG=C.UTF-8 \
    LC_ALL=C.UTF-8 \
    PYTHONHASHSEED=0 \
    PYTHONUTF8=1 \
    "$HERMES_PYTHON" "$HERMES_SOURCE/scripts/run_tests_parallel.py" \
    "$repo/tests/native" --file-retries 0 --import-mode=importlib \
    -p pytest_asyncio.plugin "$@"
fi

exec bash "$HERMES_SOURCE/scripts/run_tests.sh" "$repo/tests/native" --file-retries 0 --import-mode=importlib "$@"
