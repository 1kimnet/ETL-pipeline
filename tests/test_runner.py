#!/usr/bin/env python3
"""Test runner script for ETL pipeline tests.

Usage:
    python tests/test_runner.py              # Run all tests
    python tests/test_runner.py unit         # Run unit tests only
    python tests/test_runner.py integration  # Run integration tests only
    python tests/test_runner.py e2e          # Run end-to-end tests only
"""
import sys
import subprocess
from pathlib import Path


def run_tests(test_type="all"):
    """Run tests based on type."""
    cmd = ["python", "-m", "pytest"]

    if test_type == "unit":
        cmd.extend(["-m", "unit", "tests/unit/"])
    elif test_type == "integration":
        cmd.extend(["-m", "integration", "tests/integration/"])
    elif test_type == "e2e":
        cmd.extend(["-m", "e2e", "tests/e2e/"])
    elif test_type == "all":
        cmd.append("tests/")
    else:
        print(f"Unknown test type: {test_type}")
        sys.exit(1)

    # Add coverage if available
    try:
        subprocess.run(["python", "-c", "import coverage"],
                       check=True, capture_output=True)
        cmd.extend(["--cov=etl", "--cov-report=html", "--cov-report=term"])
    except subprocess.CalledProcessError:
        pass  # Coverage not available

    print(f"Running {test_type} tests...")
    result = subprocess.run(cmd)
    sys.exit(result.returncode)


if __name__ == "__main__":
    test_type = sys.argv[1] if len(sys.argv) > 1 else "all"
    run_tests(test_type)
