import subprocess
import sys
from pathlib import Path

REPOSITORY_DIR_PATH = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPOSITORY_DIR_PATH / "hooks" / "shared"))

from gitrepository import GitRepository


MODULE_PATH = REPOSITORY_DIR_PATH / "sqldatabase"
OUTPUT_DIR_PATH = REPOSITORY_DIR_PATH / "docs"
DOCUMENTATION_FORMAT = "google"


def generate_documentation(
    *modules: str | Path,
    output_dir_path: str | Path,
    documentation_format: str | None = None
) -> None:
    args = ["pdoc", *modules, "--output-dir", output_dir_path]
    if documentation_format is not None:
        args += ["--docformat", documentation_format]
    subprocess.run(
        args,
        check=True,
    )


def main():
    repository = GitRepository(REPOSITORY_DIR_PATH)
    staged_files = repository.get_staged_files(MODULE_PATH)
    if len(staged_files):
        generate_documentation(
            MODULE_PATH,
            output_dir_path=OUTPUT_DIR_PATH,
            documentation_format=DOCUMENTATION_FORMAT,
        )
        repository.add(OUTPUT_DIR_PATH)


if __name__ == "__main__":
    main()
