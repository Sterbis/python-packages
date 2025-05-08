import subprocess
import sys
from collections.abc import Sequence
from pathlib import Path

REPOSITORY_DIR_PATH = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPOSITORY_DIR_PATH / "hooks" / "shared"))

from gitrepository import GitRepository


def run_black_formatter(files: Sequence[str | Path]) -> None:
    subprocess.run(["black", *files], check=True)


def main():
    repository = GitRepository(REPOSITORY_DIR_PATH)
    staged_files = [
        file for file in repository.get_staged_files() if file.suffix == ".py"
    ]
    if staged_files:
        run_black_formatter(staged_files)
        repository.add(*staged_files)


if __name__ == "__main__":
    main()
