---
name: documentation-standards
description: How user-facing and internal documentation should be written and maintained.
---

# Documentation Standards

## Types of Documentation

- **User-facing docs**
  - Explain features, configuration, and usage.
  - Assume no access to source code.

- **Internal docs**
  - Capture system behavior, architecture, and decisions for developers.
  - Include rationale and trade-offs.

- **Inline docs / comments**
  - Explain non-obvious implementation details or constraints.

## Style & Tone

- Clear, concise, and concrete.
- Prefer active voice and simple sentences.
- Avoid unexplained acronyms and internal-only jargon.

## Structure Guidelines

### Feature Docs

- Overview
- Use cases
- Configuration and defaults
- Examples
- Limitations and edge cases
- Troubleshooting

### Architecture Docs

- High-level diagram (or description)
- Components and responsibilities
- Data flows and interactions
- Trade-offs and constraints
- Links to related RFCs or ADRs

## Maintenance Rules

- Documentation must be updated as part of behavior changes.
- Prefer small, incremental doc updates rather than large rewrites.
- Link docs to relevant issues or tickets where useful.

## Formatting

- Use Markdown (`.md`) for text docs.
- Use consistent heading levels and lists.
- Include code blocks with language tags.
