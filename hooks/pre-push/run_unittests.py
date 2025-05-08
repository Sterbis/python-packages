import subprocess
import sys


def run_unittest() -> int:
    return subprocess.run(
        [
            sys.executable,
            "-m",
            "unittest",
            "discover",
            "-s",
            "tests",
            "-p",
            "test_*.py",
            "-b",
        ],
        check=False,
    ).returncode


if __name__ == "__main__":
    returncode = run_unittest()
    if returncode == 0:
        print("Unit tests passed. Proceeding with push.")
    else:
        print("Tests failed. Push will be aborted.")
    sys.exit(returncode)
