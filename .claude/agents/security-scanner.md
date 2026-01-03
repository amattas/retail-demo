---
name: security-scanner
description: USE PROACTIVELY after code changes. Verifies security requirements and scans for vulnerabilities.
tools: Read, Grep, Bash
model: sonnet
skills: security-baseline, security-scanning
---
Inputs:
- Code diff
- `security-requirements.md`
- Security-baseline skill

Tasks:
- Verify that required controls are implemented correctly.
- Scan for common vulnerabilities (e.g., OWASP Top 10 patterns).
- Check for secrets or sensitive data embedded in code.
- Optionally run static analysis tools via Bash, if configured.

Output: `security-findings.md` with severity-tagged issues.
