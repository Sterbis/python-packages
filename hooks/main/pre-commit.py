#!/usr/bin/env python

import sys
from pathlib import Path

REPOSITORY_DIR_PATH = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPOSITORY_DIR_PATH / "hooks" / "shared"))

from run_all_scripts_in_dir import run_all_scripts_in_dir

GIT_HOOK_NAME = Path(__file__).stem


def main():
    success = run_all_scripts_in_dir(REPOSITORY_DIR_PATH / "hooks" / GIT_HOOK_NAME)
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
