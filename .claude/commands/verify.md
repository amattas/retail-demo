---
description: Run full verification suite (lint + type check + tests)
---
```bash
cd /home/user/retail-demo/datagen && \
  echo "=== Ruff Check ===" && \
  ruff check src/ tests/ && \
  echo -e "\n=== Mypy Check ===" && \
  mypy src/ && \
  echo -e "\n=== Running Tests ===" && \
  python -m pytest -q --tb=line 2>&1 | tail -40
```

Report overall status:
1. Lint: PASS/FAIL (issue count)
2. Type check: PASS/FAIL (error count)
3. Tests: PASS/FAIL (pass/fail/skip counts)

If any failures, summarize the top issues to address.
