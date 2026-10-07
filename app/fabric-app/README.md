# Replenishment approvals (Fabric Apps spike)

A Fabric App, built with the Rayfin SDK, that moves the decision demo's draft
approvals into durable, identity-bound storage. It addresses the gaps tracked
by `THREAT-009` and `SEC-012`:

| Local `app/` demo today | This Fabric App |
| --- | --- |
| Local proposal records in `proposals_store.json` | SQL database in Fabric, managed by the app |
| No per-user identity; approval is a button | Fabric SSO (Microsoft Entra ID); the reviewer is the signed-in caller |
| Best-effort Eventhouse telemetry | Append-only `ApprovalDecision` rows: Data API Builder grants only `read` and `create`, with no update or delete |

Proposals are synthetic drafts. Nothing is ordered, and no ERP or Plan
writeback exists.

## Data model and authorization

`rayfin/data/` defines two entities. `src/__tests__/permissions.test.ts`
compiles them to Data API Builder permissions and checks the result:

- **`ReplenishmentProposal`:** any signed-in user can read. Create requires
  `@claims.sub eq @item.created_by`, so a user can submit only as themselves.
- **`ApprovalDecision`:** any signed-in user can read. Create requires
  `@claims.sub eq @item.reviewer_id`. There are no update or delete grants,
  so a decision cannot be edited or erased through the API.

`src/services/approvals.ts` adds the workflow rules:

- the submitter cannot decide their own proposal (separation of duties);
- a rejection needs a comment;
- the earliest decision wins.

**Limitation:** these workflow rules run in the client. Row policies compare
claims to the row being written; they cannot join to another entity, and
`decided_at` is client-supplied. A user calling the GraphQL API directly could
therefore approve their own proposal or record a second, backdated decision
that the UI would treat as the earliest. Enforcing these rules server-side
needs a Rayfin Function that sets the timestamp and checks the proposal.
Treat this as a demo spike, not a production control.

## Local development

```powershell
npm ci
npm test            # workflow rules, generated permissions, auth provider
npx tsc -b          # type-check the app and data model
npm run dev         # local backend (Docker) with mock sign-in
```

## Deploy and live test

Prerequisites:
- a tenant admin has enabled **Fabric Apps (preview)**;
- the capacity is in a [supported region](https://learn.microsoft.com/fabric/admin/region-availability);
- you can sign in to the demo tenant.

```powershell
npx rayfin login --tenant <tenant-id>
npx rayfin up --workspace-id <workspace-id> --dry-run --verbose
npx rayfin up --workspace-id <workspace-id> --yes
```

Then follow the acceptance checklist in
`context/plans/2026-10-07-fabcon-live-tests.md`.
