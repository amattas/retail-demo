---
name: coding-standards
description: Coding style and structural conventions for this codebase.
---

# Coding Standards

## Structure

- Prefer small, focused functions.
- Avoid deeply nested conditionals; use early returns.
- Extract reusable logic into shared modules.

## Comments & Documentation

- Write comments to explain *why*, not *what*.
- Keep docstrings in sync with behavior; avoid outdated comments.

## Error & Logging

- Use structured logging with consistent fields:
  - `requestId`, `userId`, `component`, `severity`
- Never log secrets, tokens, or sensitive PII.

## Misc

- Avoid long parameter lists; group related values into objects.
- Prefer pure functions where practical.
- Add TODOs with owner and context, e.g.:
  - `// TODO [owner]: [short description] ([ticket link])`
