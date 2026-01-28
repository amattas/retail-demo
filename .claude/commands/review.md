---
description: Review recent changes for issues
---
```bash
git diff HEAD~${1:-1} --name-only
```

Review the changed files for:

1. **Security issues** (OWASP top 10):
   - SQL/command injection
   - Hardcoded secrets or credentials
   - Insecure data handling

2. **Performance concerns**:
   - N+1 queries or loops
   - Unbounded data fetches
   - Missing indexes or caching opportunities

3. **Code quality**:
   - Style violations (per coding-standards skill)
   - Missing error handling
   - Unclear naming or logic

4. **Test coverage**:
   - Are new code paths tested?
   - Are edge cases covered?

Provide a summary with:
- Issues found (categorized by severity)
- Specific file:line references
- Suggested fixes
