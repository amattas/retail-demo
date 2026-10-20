# Live tests: Operations Agent updates and Fabric Apps (2026-10-07)

**Status:** Prepared offline. All steps below need an interactive sign-in to
the demo tenant (`9d379b85-7b26-46a5-bbc8-2ce467f202b8`), which was not
available on 2026-10-07. Temporary artifact; record outcomes in the ML/AI
backlog, traceability, and `STATUS.md`, then delete this file.

## Sign in

```powershell
az login --tenant 9d379b85-7b26-46a5-bbc8-2ce467f202b8 --scope https://api.fabric.microsoft.com/.default
```

## A. Operations Agent (FabCon: pre-approved actions and root-cause analysis)

Offline findings:

- The June 2026 definition schema has two breaking changes: it removed
  `configuration.goals` and replaced `recipient` with `messageDestination`.
  `deploy/scripts/_operations_definition.py` already matches the current
  [definition reference](https://learn.microsoft.com/rest/api/fabric/articles/item-management/definitions/operations-agent-definition)
  field for field. The earlier HTTP 400 `UnknownError` is therefore not
  explained by the schema.
- The schema now also accepts `Ontology` data sources and
  `identity.sponsor`.
- Pre-approved actions are not in the public definition schema yet; they
  appear to be configured in the UI.
- Investigator insights (anomaly root-cause analysis) are on by default and
  surface in Teams through **Investigate further**.

Steps:

1. Retry publication:
   `python -m deploy.scripts.deploy_operations_planning --environment retail-demo --operations-only`.
   If the HTTP 400 recurs, record the request ID and response body from
   `deploy/.generated/retail-demo/operations-planning-report.json`.
2. If it still fails, create the agent once in the Fabric UI with the same
   KQL database and notebook action. Export its definition (`getDefinition`)
   and diff it against `render_operations_definition()` to find the rejected
   field.
3. Install the **Fabric Operations Agent** Teams app. Set
   `operations_planning.recipient` to your UPN and republish.
4. In the agent settings, mark **Prepare replenishment case** as a
   pre-approved action if the preview option is available. Confirm that the
   action still creates a DRAFT row only (`ops.replenishment_cases`).
5. Start the agent, seed an availability signal, and confirm:
   - a Teams message arrives;
   - **Investigate further** shows Investigator insights;
   - the pre-approved action ran without an approval prompt, or the prompt
     appeared if pre-approval isn't offered.
6. Record whether pre-approval can be expressed in `Configurations.json`
   after you save it in the UI (`getDefinition`). If it can, add it to the
   publisher.
7. Stop the agent afterwards (`shouldRun: false`).

## B. Fabric Apps approvals spike (`app/fabric-app`)

Prerequisites: the Fabric Apps (preview) tenant setting is enabled, and the
capacity region supports Fabric Apps.

1. Deploy:
   ```powershell
   cd app/fabric-app
   npm ci
   npx rayfin login --tenant 9d379b85-7b26-46a5-bbc8-2ce467f202b8
   npx rayfin up --workspace-id <workspace-id> --dry-run --verbose
   npx rayfin up --workspace-id <workspace-id> --yes
   ```
2. Open the app URL from the item properties. Confirm Fabric SSO sign-in.
3. As user A, submit a proposal. Confirm that the UI blocks A from deciding
   it.
4. As user B, who needs **Run and interact** permission, approve it with a
   comment. Confirm that the status, reviewer and comment show for both
   users.
5. In the child SQL database, query `ReplenishmentProposal` and
   `ApprovalDecision`. Confirm that `created_by` and `reviewer_id` equal each
   caller's subject claim.
6. Negative API checks, using the app's `/api/graphql` endpoint with user B's
   session:
   - an update or delete of a decision is denied;
   - creating a decision with someone else's `reviewer_id` is denied.
7. Known gap to confirm: user A can create a decision on their own proposal
   through GraphQL, and can backdate `decided_at`. If confirmed, add a Rayfin
   Function that sets the timestamp server-side and checks the proposal's
   creator, and remove the client `create` grant on `ApprovalDecision`.
8. Optional: add a semantic-model connector (`npx rayfin connector add --type
   fabric-semanticmodel`) to show `stockout_risk` context beside each
   proposal.
