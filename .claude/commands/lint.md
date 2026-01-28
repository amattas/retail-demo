---
description: Run ruff and mypy on the codebase
---
```bash
cd /home/user/retail-demo/datagen && echo "=== Ruff Check ===" && ruff check src/ tests/ 2>&1 | tail -50 && echo -e "\n=== Mypy Check ===" && mypy src/ 2>&1 | tail -50
```

Analyze the output:
1. Count and categorize any issues found
2. For errors, explain the problem and suggest fixes
3. Prioritize: errors > warnings > style issues
4. If clean, confirm the codebase passes all checks
