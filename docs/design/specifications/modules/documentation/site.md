# Documentation site

## Source and output

- Canonical source: `docs/`
- Configuration: `zensical.toml`
- Pinned dependency: `requirements-docs.txt`
- Generated output: `site/`
- Publish branch: `gh-pages`

`website/` and Docusaurus are retired and must not return as a parallel source.

## Information architecture

Navigation is explicit in `zensical.toml`:

1. Home
2. Guides
3. Design Documentation
   - Requirements
   - Specifications
   - Architecture
   - Security

All nav targets must exist. Canonical Markdown uses relative links, Mermaid
diagrams, and Zensical-compatible Markdown rather than MDX/React or
Docusaurus-only metadata.

Task-focused user documentation remains under `docs/guides/`. Normative
technical material remains under `docs/design/` so implementation detail does
not dominate the primary user journey.

Top-level navigation groups remain collapsible. Do not enable
`navigation.sections`, which renders them as persistent sidebar groups on
desktop.

## Owner rules

- Requirements: outcomes, constraints, acceptance, stable IDs, backlogs.
- Specifications: exact interfaces, schemas, workflows, states, errors.
- Architecture: current boundaries, deployment, dependencies, and flow.
- Security: threats, controls, assumptions, and residual risks.
- Guides: derived user procedures.
- Root/component READMEs: concise local entry points and links.

Module backlogs contain only `Open` and `Settled - do not reopen`.

## Temporary artifacts

Dated plans/specs, raw review findings, generated context, and migration audits
must not be public systems of record. Durable content is promoted before those
artifacts are deleted or replaced by pointers.

The prior `docs/superpowers/` implementation plans were removed after durable
content was promoted. Temporary planning material must remain outside the
published source tree.

## Build

```powershell
python -m pip install -r requirements-docs.txt
python -m zensical build --clean
```

The build must succeed with no missing nav target or broken internal link.
Generated output is written to the ignored `site/` directory.

For a local preview:

```powershell
python -m zensical serve
```

## Maintainer checklist

1. Update the canonical owner and all derived links in the same change.
2. Preserve stable requirement, threat, control, improvement, and enhancement
   IDs.
3. Keep module backlogs limited to `Open` and `Settled - do not reopen`.
4. Use Mermaid for diagrams.
5. Run `python -m zensical build --clean`.
6. Check for broken links, missing navigation targets, stale `website/`
   references, and dated plans that still own durable content.

## Publishing workflow

`.github/workflows/docs.yml`:

- triggers from canonical docs/config changes;
- checks out with an immutable action SHA;
- installs Python and the pinned Zensical package;
- builds `site/`;
- pushes `site/` to an orphan `gh-pages` branch with an immutable action SHA;
- uses `contents: write`;
- does not request Pages OIDC permissions;
- does not use Pages artifact deployment actions.

## External Pages setting

GitHub repository settings must select:

- Source: **Deploy from a branch**
- Branch: `gh-pages`
- Folder: `/ (root)`

The workflow cannot prove this external setting from source. Track it as a
blocked publishing requirement until the branch is created and the site URL is
verified.
