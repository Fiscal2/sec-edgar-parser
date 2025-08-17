#!/usr/bin/env python3
"""
Test runner script for SEC EDGAR Parser
Run different types of tests easily:
- python3 run_tests.py          # Run all tests
- python3 run_tests.py unit     # Run only unit tests
- python3 run_tests.py integration  # Run only integration tests
- python3 run_tests.py fast     # Run tests excluding slow ones
"""

import sys
import subprocess
import os

def run_tests(test_type="all"):
    """Run tests based on type"""
    
    if test_type == "unit":
        print("Running unit tests only")
        cmd = ["python3", "-m", "pytest", "-m", "not integration and not slow", "-v"]
    elif test_type == "integration":
        print("Running integration tests only")
        cmd = ["python3", "-m", "pytest", "-m", "integration", "-v"]
    else:
        print("Running all tests")
        cmd = ["python3", "-m", "pytest", "-v"]
    
    try:
        result = subprocess.run(cmd, cwd=os.getcwd())
        return result.returncode
    except KeyboardInterrupt:
        print("\nTests interrupted by user")
        return 1
    except Exception as e:
        print(f"Error running tests: {e}")
        return 1

if __name__ == "__main__":
    test_type = sys.argv[1] if len(sys.argv) > 1 else "all"
    
    print(f"SEC EDGAR Parser Test Runner")
    print(f"Test type: {test_type}")
    print("-" * 50)
    
    exit_code = run_tests(test_type)
    
    if exit_code == 0:
        print("\nAll tests passed")
    else:
        print(f"\nTests failed with exit code {exit_code}")
    
    sys.exit(exit_code)
