# Documentation operations

The canonical site source is `docs/`. Docusaurus and `website/` are retired.

## Local build

```powershell
python -m pip install -r requirements-docs.txt
python -m zensical build --clean
```

Generated output is written to `site/` and is ignored by Git.

For a local preview:

```powershell
python -m zensical serve
```

## Ownership

- Design documentation is grouped under `docs/design/`.
- Requirements own outcomes, stable IDs, acceptance criteria, and backlogs.
- Specifications own exact interfaces, mappings, workflows, and errors.
- Architecture owns current components, boundaries, dependencies, and flow.
- Security owns threats, controls, assumptions, and residual risk.
- Guides derive task instructions from those owners.

Do not duplicate normative prose in a README or guide. Link to the canonical
owner.

## Publishing

`.github/workflows/docs.yml`:

1. installs the pinned Zensical version;
2. builds `docs/` into `site/`;
3. publishes `site/` to an orphan `gh-pages` branch.

The workflow intentionally does not use `actions/upload-pages-artifact` or
`actions/deploy-pages`.

One repository setting remains external to the workflow:

1. Open **Settings > Pages**.
2. Select **Deploy from a branch**.
3. Select `gh-pages` and `/ (root)`.

Until that setting is changed and the first workflow succeeds,
`REQ-PUBLISH-001` remains
[blocked](../design/requirements/traceability.md#requirements).

## Change checklist

1. Update the canonical owner and all derived links in the same change.
2. Add or preserve stable requirement, threat, control, improvement, and
   enhancement IDs.
3. Keep module backlogs limited to `Open` and `Settled - do not reopen`.
4. Use Mermaid for diagrams.
5. Run `python -m zensical build --clean`.
6. Check for broken links, missing nav targets, stale `website/` references, and
   dated plans that still own durable content.
