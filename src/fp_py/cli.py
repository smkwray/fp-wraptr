"""Compatibility module for `python -m fp_py.cli`."""

from fppy.cli import main

if __name__ == "__main__":
    raise SystemExit(main())
