# Setup backlog

## Open

### ENH-005 - Publish persona-led demo journeys, presets, and prompt packs {#enh-005}

- **Priority / effort:** Idea / M
- **Outcome:** `lite`, `standard`, and `full-demo` presets have known runtimes,
  expected assets, presenter controls, and persona-specific questions.
- **Acceptance:** A presenter can choose a preset and reproduce its documented
  "wow moment" without inventing setup steps.
- **Current boundary:** Persona journeys and prompt packs are documented in the
  [presenter guide](../../../guides/presenter-journeys.md). The deployment
  presets, measured runtimes, and preset-specific asset controls remain open.

## Settled - do not reopen

- `retail-setup` is the supported setup path.
- Historical range input is `--months`; explicit public
  `--start-date`/`--end-date` examples are retired.
- Setup logs use linear plain output rather than a fixed-footer TUI.
