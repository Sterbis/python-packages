#!/usr/bin/env python

import sys
from pathlib import Path

REPOSITORY_DIR_PATH = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPOSITORY_DIR_PATH / "hooks" / "shared"))

from run_script import run_scripts_in_dir

GIT_HOOK_NAME = Path(__file__).stem
SCRIPTS_DIR_PATH = REPOSITORY_DIR_PATH / "hooks" / GIT_HOOK_NAME
SCRIPTS = [
    "run_unittests.py",
]


def main():
    success = run_scripts_in_dir(SCRIPTS_DIR_PATH, SCRIPTS)
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
