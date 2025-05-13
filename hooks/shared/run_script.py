import subprocess
import sys
from pathlib import Path


def run_script(path: str | Path) -> bool:
    path = Path(path)
    print(f"Running script: {path.name}")
    process = subprocess.run([sys.executable, path], check=False)
    if process.returncode != 0:
        print(f"Script failed: {path.name}")
    return process.returncode == 0


def run_scripts_in_dir(dir_path: str | Path, scripts: list[str]) -> bool:
    success = True
    for script in scripts:
        script_file_path = Path(dir_path) / script
        script_success = run_script(script_file_path)
        if script_success is False:
            success = False
    return success


def run_all_scripts_in_dir(dir_path: str | Path) -> bool:
    success = True
    for script_file_path in sorted(Path(dir_path).glob("*.py")):
        result = run_script(script_file_path)
        if result is False:
            success = False
    return success
