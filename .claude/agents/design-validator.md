---
name: design-validator
description: MUST BE USED after Wave B completes. Cross-checks spec, architecture, API design, and test plan.
tools: Read, Grep
model: sonnet
skills: design-validation
---
Inputs:
- `spec.md`
- `architecture.md`
- `api-design.md`
- `test-plan.md`
- `security-requirements.md` (if present)

Tasks:
- Ensure every requirement in `spec.md` is mapped to architecture components.
- Verify `api-design.md` covers required data flows and use cases.
- Check `test-plan.md` covers acceptance criteria and key risks.
- Identify contradictions, gaps, or unclear areas.

Output: `design-validation.md` summarizing:
- Summary (pass / conditional / fail)
- Issues with severity labels
- Suggested fixes or clarifications

No code modifications.
