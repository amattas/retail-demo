# Documentation requirements

### REQ-DOCS-001 - Canonical source

`docs/` shall be the only documentation site source. Root and component READMEs
shall remain concise entry points that link to canonical owners.

### REQ-DOCS-002 - Durable ownership

Requirements, specifications, architecture, security, guides, backlogs, and
traceability shall have explicit owners. Dated plans and findings shall not
remain systems of record.

### REQ-DOCS-003 - Zensical site

The canonical Markdown shall build with the pinned Zensical version and
generator-neutral links, navigation, Mermaid diagrams, and assets.

### REQ-DOCS-004 - Reconciled claims

Commands, table counts, event counts, deployment inventory, schedules,
semantic-model mode, optional features, and support status shall agree with the
current repository.

### REQ-PUBLISH-001 - Branch publishing

The documentation workflow shall build current content from `docs/`, build
historical content from immutable SemVer tags, and push generated output to an
orphan `gh-pages` branch without the Pages artifact deployment actions.

### REQ-PUBLISH-002 - Version selector

The published site shall show `main` as **Latest** and the highest stable SemVer
patch revision for every tagged `major.minor` line. Release URLs shall remain
stable at `major.minor` while selector labels show the selected
`major.minor.patch` revision.

See [the site specification](../../../specifications/modules/documentation/site.md).
