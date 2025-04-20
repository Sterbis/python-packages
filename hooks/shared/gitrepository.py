import subprocess
from pathlib import Path
from typing import List


class GitRepository:
    def __init__(self, path: str | Path = "."):
        self.path = Path(path).resolve()

    def run_git_command(
        self, command: List[str], capture_output: bool = True
    ) -> subprocess.CompletedProcess:
        process = subprocess.run(
            ["git"] + command,
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

    def get_staged_files(self) -> List[Path]:
        process = self.run_git_command(["diff", "--cached", "--name-only"])
        staged_files = []
        if process.returncode == 0:
            for file in process.stdout.strip().splitlines():
                staged_files.append(self.path / file)
        return staged_files

    def add(self, *files: str) -> bool:
        process = self.run_git_command(["add"] + list(files), capture_output=False)
        return process.returncode == 0

    def commit(self, message: str) -> bool:
        process = self.run_git_command(["commit", "-m", message], capture_output=False)
        return process.returncode == 0


if __name__ == "__main__":
    repository = GitRepository()
    print("Staged files:")
    for staged_file in repository.get_staged_files():
        print(staged_file)
