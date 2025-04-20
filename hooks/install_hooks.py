import os
import traceback
from pathlib import Path

REPOSITORY_DIR_PATH = Path(__file__).parent.parent
GIT_HOOKS_DIR_PATH = REPOSITORY_DIR_PATH / ".git" / "hooks"
GIT_HOOK_SCRIPTS_DIR_PATH = REPOSITORY_DIR_PATH / "hooks" / "main"


def install_hooks():
    for script_file_path in GIT_HOOK_SCRIPTS_DIR_PATH.glob("*.py"):
        git_hook_name = script_file_path.stem
        symlink_path = GIT_HOOKS_DIR_PATH / git_hook_name

        if symlink_path.exists() or symlink_path.is_symlink():
            print(f"Removing existing git-hook: {symlink_path}")
            symlink_path.unlink()

        print(f"Creating git-hook symlink: {symlink_path} -> {script_file_path}")
        os.symlink(script_file_path, symlink_path)


if __name__ == "__main__":
    try:
        install_hooks()
        print("Git-hooks installed successfully.")
    except OSError:
        traceback.print_exc()
        print("Failed to install git-hooks. Please run this script as administrator.")
    input("Press Enter to exit...")
