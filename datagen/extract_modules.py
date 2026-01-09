"""
Systematic extraction script for modularizing fact_generator.py
"""

import re
from pathlib import Path


def extract_imports_section(lines: list[str]) -> tuple[list[str], int]:
    """Extract all imports from the beginning of the file"""
    imports = []
    last_import_idx = 0

    # Skip module docstring
    i = 0
    if lines[0].strip().startswith('"""') or lines[0].strip().startswith("'''"):
        # Find end of docstring
        quote = '"""' if '"""' in lines[0] else "'''"
        i = 1
        while i < len(lines) and quote not in lines[i]:
            i += 1
        i += 1  # Skip the closing quote line

    # Now extract imports
    while i < len(lines):
        line = lines[i].strip()
        if line.startswith("import ") or line.startswith("from "):
            imports.append(lines[i])
            last_import_idx = i
            # Handle multi-line imports
            while i < len(lines) and "(" in lines[i] and ")" not in lines[i]:
                i += 1
                imports.append(lines[i])
                last_import_idx = i
        elif line and not line.startswith("#"):
            # Hit non-import, non-comment line
            break
        i += 1

    return imports, last_import_idx + 1


def find_class_or_function(
    lines: list[str], name: str, start_idx: int = 0
) -> tuple[int, int]:
    """Find start and end indices for a class or top-level function"""
    # Find start
    start = None
    for i in range(start_idx, len(lines)):
        # Match class or function definition
        if re.match(rf"^class {re.escape(name)}[\(:< ]", lines[i]) or re.match(
            rf"^def {re.escape(name)}\(", lines[i]
        ):
            start = i
            break

    if start is None:
        return None, None

    # Find end - next top-level definition or EOF
    end = len(lines)
    for i in range(start + 1, len(lines)):
        if re.match(r"^(class |def |@)", lines[i]):
            end = i
            break

    return start, end


def find_method_in_class(
    lines: list[str], method_name: str, class_start: int, class_end: int
) -> tuple[int, int]:
    """Find a method within a class"""
    # Find method start
    start = None
    for i in range(class_start, class_end):
        if re.match(rf"^    def {re.escape(method_name)}\(", lines[i]):
            start = i
            break

    if start is None:
        return None, None

    # Find method end - next method at same indent level or class end
    end = class_end
    for i in range(start + 1, class_end):
        line = lines[i]
        # Skip empty lines and highly indented lines (method body)
        if not line.strip():
            continue
        # Check for next method (same indent as 'def')
        if re.match(r"^    def ", line) or re.match(r"^    @", line):
            end = i
            break

    return start, end


# Read source
source_path = Path("src/retail_datagen/generators/fact_generator.py")
with open(source_path) as f:
    lines = f.readlines()

# Extract imports
imports, imports_end = extract_imports_section(lines)
print(f"Extracted {len(imports)} import lines, ending at line {imports_end}")

# Find logger definition
logger_line = None
for i in range(imports_end, min(imports_end + 20, len(lines))):
    if "logger = logging.getLogger" in lines[i]:
        logger_line = lines[i]
        break

# Find FactDataGenerator class
fact_gen_start, fact_gen_end = find_class_or_function(lines, "FactDataGenerator", 0)
print(f"FactDataGenerator class: lines {fact_gen_start + 1} to {fact_gen_end}")

# Find HourlyProgressTracker class
hourly_start, hourly_end = find_class_or_function(lines, "HourlyProgressTracker", 0)
print(f"HourlyProgressTracker class: lines {hourly_start + 1} to {hourly_end}")

# Find utility classes
summary_start, summary_end = find_class_or_function(lines, "FactGenerationSummary", 0)
master_start, master_end = find_class_or_function(lines, "MasterTableSpec", 0)
print(f"FactGenerationSummary class: lines {summary_start + 1} to {summary_end}")
print(f"MasterTableSpec class: lines {master_start + 1} to {master_end}")

# Find module-level convenience function
convenience_start, convenience_end = find_class_or_function(
    lines, "generate_historical_facts", fact_gen_end
)
if convenience_start:
    print(
        f"generate_historical_facts function: lines {convenience_start + 1} to {convenience_end}"
    )

print("\n" + "=" * 60)
print("Extraction complete. Run phase 2 to create module files.")
