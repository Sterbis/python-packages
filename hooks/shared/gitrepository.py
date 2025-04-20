import subprocess
from pathlib import Path
from typing import List


class GitRepository:
    def __init__(self, path: str | Path = "."):
        self.path = Path(path).resolve()

    def run_git_command(
        self, command: List[str], capture_output: bool = True
    ) -> subprocess.CompletedProcess:
        command = ["git"] + command
        print(f"Running git command: {' '.join(command)}")
        process = subprocess.run(
            command,
            cwd=self.path,
            text=True,
            capture_output=capture_output,
            check=False,
        )
        if process.returncode != 0:
            print(f"Git command failed: {' '.join(command)}")
            if process.stderr:
                print(f"Error: {process.stderr.strip()}")
        return process

    def _get_files(
        self, command: list[str], dir_path: str | Path | None = None
    ) -> list[Path]:
        command = command.copy()
        if dir_path:
            command.append(str(dir_path))
        process = self.run_git_command(command)
        if process.returncode != 0:
            return []
        return [self.path / file for file in process.stdout.strip().splitlines()]

    def get_staged_files(self, dir_path: str | Path | None = None) -> list[Path]:
        return self._get_files(["diff", "--cached", "--name-only"], dir_path)

    def get_unstaged_files(self, dir_path: str | Path | None = None) -> list[Path]:
        return self._get_files(["diff", "--name-only"], dir_path)

    def get_changed_files(self, dir_path: str | Path | None = None) -> list[Path]:
        return self._get_files(["diff", "HEAD", "--name-only"], dir_path)

    def get_tracked_files(self, dir_path: str | Path | None = None) -> list[Path]:
        return self._get_files(["ls-files"], dir_path)

    def get_untracked_files(self, dir_path: str | Path | None = None) -> list[Path]:
        return self._get_files(["ls-files", "--others", "--exclude-standard"], dir_path)

    def add(self, *files: str | Path) -> bool:
        process = self.run_git_command(
            ["add"] + list(map(str, files)), capture_output=False
        )
        return process.returncode == 0

    def commit(self, message: str) -> bool:
        process = self.run_git_command(["commit", "-m", message], capture_output=False)
        return process.returncode == 0


if __name__ == "__main__":
    repository = GitRepository()
    print("Staged files:")
    for staged_file in repository.get_staged_files():
        print(f"    {staged_file}")
