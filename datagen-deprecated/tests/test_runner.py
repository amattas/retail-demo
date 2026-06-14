#!/usr/bin/env python3
"""
Test runner script for retail data generator tests.

This script provides convenient commands for running different test suites:
- Unit tests
- Integration tests
- Performance tests
- Property-based tests (Hypothesis)
- Coverage reports

Usage:
    python tests/test_runner.py --unit
    python tests/test_runner.py --integration
    python tests/test_runner.py --all
    python tests/test_runner.py --coverage
"""

import argparse
import os
import subprocess
import sys
from pathlib import Path


def run_command(command: list, description: str) -> bool:
    """Run a command and return success status."""
    print(f"\n{'=' * 60}")
    print(f"Running: {description}")
    print(f"{'=' * 60}")

    try:
        env = dict(**os.environ)
        env.setdefault("PYTEST_DISABLE_PLUGIN_AUTOLOAD", "1")
        subprocess.run(command, check=True, capture_output=False, env=env)
        print(f"\n✅ {description} completed successfully")
        return True
    except subprocess.CalledProcessError as e:
        print(f"\n❌ {description} failed with exit code {e.returncode}")
        return False


def run_unit_tests() -> bool:
    """Run unit tests."""
    command = [
        "python",
        "-m",
        "pytest",
        "tests/unit/",
        "-v",
        "--tb=short",
        "-x",  # Stop on first failure
    ]
    return run_command(command, "Unit Tests")


def run_integration_tests() -> bool:
    """Run integration tests."""
    command = [
        "python",
        "-m",
        "pytest",
        "tests/integration/",
        "-v",
        "--tb=short",
        "-m",
        "integration",
        "-x",
    ]
    return run_command(command, "Integration Tests")


def run_performance_tests() -> bool:
    """Run performance tests."""
    command = [
        "python",
        "-m",
        "pytest",
        "tests/",
        "-v",
        "--tb=short",
        "-m",
        "slow",
        "-x",
    ]
    return run_command(command, "Performance Tests")


def run_property_tests() -> bool:
    """Run property-based tests with Hypothesis."""
    command = [
        "python",
        "-m",
        "pytest",
        "tests/",
        "-v",
        "--tb=short",
        "--hypothesis-show-statistics",
        "-k",
        "property_based",
        "-x",
    ]
    return run_command(command, "Property-Based Tests (Hypothesis)")


def run_all_tests() -> bool:
    """Run all tests."""
    command = ["python", "-m", "pytest", "tests/", "-v", "--tb=short"]
    return run_command(command, "All Tests")


def run_with_coverage() -> bool:
    """Run tests with coverage report."""
    commands = [
        # Run tests with coverage
        [
            "python",
            "-m",
            "pytest",
            "tests/",
            "--cov=src/retail_datagen",
            "--cov-report=term-missing",
            "--cov-report=html:htmlcov",
            "--cov-fail-under=80",
        ],
        # Generate coverage badge (if coverage-badge is installed)
        ["coverage-badge", "-o", "coverage.svg", "-f"],
    ]

    success = True
    for i, command in enumerate(commands):
        desc = "Tests with Coverage" if i == 0 else "Coverage Badge Generation"
        if not run_command(command, desc):
            if i == 0:  # Coverage is critical
                success = False
            # Badge generation failure is not critical

    if success:
        print("\n📊 Coverage report generated in htmlcov/index.html")

    return success


def run_linting() -> bool:
    """Run code linting checks."""
    commands = [
        # Ruff linting
        ["python", "-m", "ruff", "check", "src/", "tests/"],
        # Type checking with mypy
        ["python", "-m", "mypy", "src/retail_datagen"],
    ]

    success = True
    for i, command in enumerate(commands):
        desc = "Ruff Linting" if i == 0 else "MyPy Type Checking"
        if not run_command(command, desc):
            success = False

    return success


def run_formatting_check() -> bool:
    """Check code formatting."""
    command = ["python", "-m", "ruff", "format", "--check", "src/", "tests/"]
    return run_command(command, "Code Formatting Check")


def run_security_scan() -> bool:
    """Run security scanning (requires bandit)."""
    command = [
        "python",
        "-m",
        "bandit",
        "-r",
        "src/",
        "-f",
        "json",
        "-o",
        "bandit-report.json",
    ]
    return run_command(command, "Security Scan (Bandit)")


def validate_test_setup() -> bool:
    """Validate that test environment is properly set up."""
    print("Validating test environment...")

    # Check that required packages are installed
    required_packages = ["pytest", "pytest-cov", "hypothesis", "pandas", "pydantic"]

    missing_packages = []
    for package in required_packages:
        try:
            __import__(package.replace("-", "_"))
        except ImportError:
            missing_packages.append(package)

    if missing_packages:
        print(f"❌ Missing required packages: {', '.join(missing_packages)}")
        print("Install with: pip install -e .[dev]")
        return False

    # Check test directory structure
    required_dirs = ["tests/unit", "tests/integration"]

    missing_dirs = []
    for directory in required_dirs:
        if not Path(directory).exists():
            missing_dirs.append(directory)

    if missing_dirs:
        print(f"❌ Missing test directories: {', '.join(missing_dirs)}")
        return False

    print("✅ Test environment validation passed")
    return True


def main():
    """Main entry point."""
    # Enforce Python 3.11+ for native pytest runs
    parser = argparse.ArgumentParser(
        description="Test runner for retail data generator",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s --unit                 # Run unit tests only
  %(prog)s --integration         # Run integration tests only
  %(prog)s --performance         # Run performance tests only
  %(prog)s --property            # Run property-based tests only
  %(prog)s --all                 # Run all tests
  %(prog)s --coverage            # Run tests with coverage report
  %(prog)s --lint                # Run linting checks
  %(prog)s --format-check        # Check code formatting
  %(prog)s --security            # Run security scan
  %(prog)s --validate            # Validate test environment
  %(prog)s --ci                  # Run full CI pipeline
        """,
    )

    parser.add_argument("--unit", action="store_true", help="Run unit tests")
    parser.add_argument(
        "--integration", action="store_true", help="Run integration tests"
    )
    parser.add_argument(
        "--performance", action="store_true", help="Run performance tests"
    )
    parser.add_argument(
        "--property", action="store_true", help="Run property-based tests"
    )
    parser.add_argument("--all", action="store_true", help="Run all tests")
    parser.add_argument(
        "--coverage", action="store_true", help="Run tests with coverage"
    )
    parser.add_argument("--lint", action="store_true", help="Run linting checks")
    parser.add_argument(
        "--format-check", action="store_true", help="Check code formatting"
    )
    parser.add_argument("--security", action="store_true", help="Run security scan")
    parser.add_argument(
        "--validate", action="store_true", help="Validate test environment"
    )
    parser.add_argument("--ci", action="store_true", help="Run full CI pipeline")

    args = parser.parse_args()

    # If no specific option provided, show help
    if not any(vars(args).values()):
        parser.print_help()
        return 1

    # Always validate first (except for validate command itself)
    if not args.validate and not validate_test_setup():
        return 1

    success = True

    # Run requested test suites
    if args.validate:
        success &= validate_test_setup()

    if args.unit:
        success &= run_unit_tests()

    if args.integration:
        success &= run_integration_tests()

    if args.performance:
        success &= run_performance_tests()

    if args.property:
        success &= run_property_tests()

    if args.all:
        success &= run_all_tests()

    if args.coverage:
        success &= run_with_coverage()

    if args.lint:
        success &= run_linting()

    if args.format_check:
        success &= run_formatting_check()

    if args.security:
        success &= run_security_scan()

    if args.ci:
        # Full CI pipeline
        print("\n🚀 Running full CI pipeline...")
        success &= run_formatting_check()
        success &= run_linting()
        success &= run_unit_tests()
        success &= run_integration_tests()
        success &= run_with_coverage()
        success &= run_security_scan()

    # Print final results
    print("\n" + "=" * 60)
    if success:
        print("🎉 All tests and checks passed!")
        return 0
    else:
        print("💥 Some tests or checks failed!")
        return 1


if __name__ == "__main__":
    sys.exit(main())
