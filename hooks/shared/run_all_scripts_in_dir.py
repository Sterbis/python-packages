import subprocess
import sys
from pathlib import Path


def run_all_scripts_in_dir(dir_path: str | Path) -> bool:
    success = True
    for script_file_path in sorted(Path(dir_path).glob("*.py")):
        print(f"Running script: {script_file_path.name}")
        process = subprocess.run([sys.executable, script_file_path], check=False)
        if process.returncode != 0:
            print(f"Script failed: {script_file_path.name}")
            success = False
    return success
