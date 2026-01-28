---
description: Run tests with coverage analysis
---
```bash
cd /home/user/retail-demo/datagen && python -m pytest $ARGUMENTS -v --tb=short 2>&1 | head -150
```

Analyze the test output:
1. Summarize pass/fail counts
2. For failures, identify the root cause and suggest fixes
3. If all tests pass, confirm success

Common test commands:
- `/test` - run all tests
- `/test tests/unit/` - run only unit tests
- `/test -k "test_name"` - run specific test
- `/test --cov=src/` - run with coverage
