# Documentation site

## Source and output

- Canonical source: `docs/`
- Configuration: `zensical.toml`
- Dependency input: `requirements-docs.in`
- Hash-locked dependency set: `requirements-docs.txt`
- Generated output: `site/`
- Publish branch: `gh-pages`
- Version selection: `scripts/docs_versioning.py`
- Version publisher: `scripts/publish_versioned_docs.py`

`website/` and Docusaurus are retired and must not return as a parallel current
source. The version publisher may read their documentation from immutable
historical tags when reconstructing an archived release.

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

Module backlogs contain only `Open` and `Settled — do not reopen`.

## Temporary artifacts

Dated plans/specs, raw review findings, generated context, and migration audits
must not be public systems of record. Durable content is promoted before those
artifacts are deleted or replaced by pointers.

The prior `docs/superpowers/` implementation plans were removed after durable
content was promoted. Temporary planning material must remain outside the
published source tree.

## Build

```powershell
python -m pip install --require-hashes -r requirements-docs.txt
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
- fetches `main` plus the complete tag history;
- checks out with an immutable action SHA;
- installs Python, pinned Zensical, and the commit-pinned Zensical `mike` fork;
- rebuilds the orphan `gh-pages` branch through `mike`;
- uses `contents: write`;
- does not request Pages OIDC permissions;
- does not use Pages artifact deployment actions.

## Published versions

The version selector contains:

- `main`, displayed as **Latest** and published at `/latest/`;
- one entry for each stable SemVer `major.minor` line that has tags;
- the highest numeric patch revision in each line, displayed as the normalized
  `major.minor.patch` value and published at `/major.minor/`.

Tags may use an optional `v` prefix. Pre-release tags and non-SemVer tags are
excluded. Historical pages are built from the selected tag's own documentation
source. Current pages always build with Zensical. Historical compatibility can
read the repository's previous MkDocs and Docusaurus layouts so older release
entries do not substitute current documentation.

Every publication reconstructs the generated branch from the currently tagged
version set. The root page redirects to **Latest**.

Pull-request CI builds the current site and the complete version set without
pushing, so tag selection, legacy-source compatibility, and selector metadata
fail before publication.

## External Pages setting

GitHub repository settings currently select:

- Source: **Deploy from a branch**
- Branch: `gh-pages`
- Folder: `/ (root)`

GitHub reports the site as built, the root redirects to `/latest/`, and the
published selector metadata contains **Latest** plus the selected stable minor
lines. Recheck the Pages API, the latest Docs workflow run, and the live site
after changing the workflow, branch, or repository Pages settings.
