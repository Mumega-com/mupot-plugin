"""Load this plugin package when the native Hermes test runner owns the cwd."""
import importlib.util
import sys
from pathlib import Path

root = Path(__file__).resolve().parents[2]
if "plugin" not in sys.modules:
    spec = importlib.util.spec_from_file_location("plugin", root / "__init__.py",
                                                 submodule_search_locations=[str(root)])
    module = importlib.util.module_from_spec(spec)
    sys.modules["plugin"] = module
    spec.loader.exec_module(module)
