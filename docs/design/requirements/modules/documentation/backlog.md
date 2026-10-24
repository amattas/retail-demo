# Documentation backlog

## Open

- Inspect the next Docs publication, `gh-pages`, `/latest/`, search index,
  and tagged versions for the corrected publication boundary (`SEC-010`).
  Local build success is not a live-site verification result.
- Update the records with final generator-review and hosted/live evidence when available.
  Packaging/build and readiness code are complete locally; all five notebooks
  were rebuilt and drift checked. Do not keep completed implementation work
  open solely because external service validation remains outstanding.
- The pinned generator's `404.html` emits `#__skip` without a matching anchor.
  Source-document and navigation links pass; retain the theme-owned defect
  separately from content-link validation.

## Settled — do not reopen

- Data Agent source `.platform` descriptions may be nonempty, agent-specific
  summaries. Other source artifact types retain exact manifest-description
  matching; staged items still receive the manifest-owned description.
- Canonical site source is `docs/`; `website/docs/` is retired.
- Zensical replaces Docusaurus.
- Generated site output is pushed to `gh-pages`.
- Versioned documentation shows `main` as **Latest** plus the highest tagged
  patch revision for each SemVer minor line.
- Backlogs contain only open work and settled decisions, never completed-work
  archives.
- Dated implementation plans are removed after durable content is promoted.
