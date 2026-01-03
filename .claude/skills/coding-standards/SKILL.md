---
name: coding-standards
description: Coding style and structural conventions for this codebase.
---

# Coding Standards

General principles that apply across all languages in this project.

## Language-Specific Guidelines

- **Python**: See [PYTHON.md](./PYTHON.md) for PEP 8 & PEP 20 conventions
- **KQL**: See [KQL.md](./KQL.md) for Kusto Query Language conventions

## Universal Principles

### Structure & Modularity
- **Single Responsibility**: Each function/module does one thing well
- **Small units**: Keep functions focused and concise
- **Avoid deep nesting**: Use early returns, guard clauses
- **Extract reusable logic**: If you copy-paste, refactor

### Comments & Documentation
- Write comments to explain *why*, not *what*
- Keep documentation in sync with behavior
- Remove outdated comments promptly

### Error Handling
- Use specific error types, not generic catches
- Fail fast with clear error messages
- Log errors with context

### Logging
- Use structured logging with consistent fields
- Never log secrets, tokens, or sensitive PII
- Use appropriate log levels

### TODOs
- Include owner and context
- Link to tickets when applicable
- Clean up when resolved
