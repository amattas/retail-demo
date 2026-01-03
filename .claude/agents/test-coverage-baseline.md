---
name: test-coverage-baseline
description: USE to establish current test coverage before major changes.
tools: Bash, Read, Grep
model: haiku
skills: testing-guidelines
---
Run the existing test suite and coverage tools (if present) to produce `context/test-coverage-baseline.md` summarizing:
- Overall coverage
- Major untested areas
- Known flaky or slow tests (if detectable)

Do not modify code.
