# Retail Operations (Fabric Apps examples)

One Rayfin SDK codebase that ships as **three standalone Fabric Apps** for
separate business functions. Each build contains one app, selected by
`VITE_APP_VARIANT`:

| Fabric App item | Variant and route | What it shows |
| --- | --- | --- |
| `retail-merchandising` | `merchandising`, `/merchandising` | Store, category, supplier, search and period filters; sales, gross profit, in-stock rate and attention KPIs; a this-period versus previous-period trend (in-stock %, sales or gross profit; daily, weekly or 4-weekly); a sortable item table with days of cover, stock status and CSV export. Filter combinations can be saved as private views. |
| `retail-promotions` | `promotions`, `/promotions` | Choose a product, store, dates, discount and assumed unit uplift to see promoted price, revenue, gross profit, discount investment and the uplift needed to break even. Scenarios are saved privately and compared side by side. |
| `retail-approvals` | `approvals`, `/approvals` | Submit draft replenishment proposals and approve or reject other planners' proposals, with pending/decided queues and append-only decisions. |

Without `VITE_APP_VARIANT` (local development and the design preview), one
build contains all three apps side by side. Each deployed app stands alone,
with its own sign-in, item, and app-managed SQL database; the apps do not link
to or depend on each other. The deployed bundle reads
`rayfin.config.json`, which `rayfin up` writes for each item, so the same
source serves every app.

All three apps deploy the same four-entity data model for simplicity; each app
uses only its own tables.

The merchandising and promotion apps use a deliberately **synthetic
sample catalog** (`src/data/merchandising.ts`): 16 products in three stores,
with deterministic daily observations. They do not read the Lakehouse, change
prices, or place orders.

The approvals app moves the decision demo's draft approvals into durable,
identity-bound storage. It addresses the gaps tracked by `THREAT-009` and
`SEC-012`:

| Local `app/` demo today | This Fabric App |
| --- | --- |
| Local proposal records in `proposals_store.json` | SQL database in Fabric, managed by the app |
| No per-user identity; approval is a button | Fabric SSO (Microsoft Entra ID); the reviewer is the signed-in caller |
| Best-effort Eventhouse telemetry | Append-only `ApprovalDecision` rows: Data API Builder grants only `read` and `create`, with no update or delete |

Proposals are synthetic drafts. Nothing is ordered, and no ERP or Plan
writeback exists.

## Data model and authorization

`rayfin/data/` defines four entities. `src/__tests__/permissions.test.ts`
compiles them to Data API Builder permissions and checks the result:

- **`ReplenishmentProposal`:** any signed-in user can read. Create requires
  `@claims.sub eq @item.created_by`, so a user can submit only as themselves.
- **`ApprovalDecision`:** any signed-in user can read. Create requires
  `@claims.sub eq @item.reviewer_id`. There are no update or delete grants,
  so a decision cannot be edited or erased through the API.
- **`MerchandisingView`** and **`PromotionScenario`:** private workspace
  records. Read, create and delete all require
  `@claims.sub eq @item.owner_id`, so users see and remove only their own
  saved views and scenarios. There is no update grant; load a record, change
  it, and save a new one.

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
npm run dev:preview # UI-only design preview: seeded sample data, no backend or sign-in
```

## Deploy and live test

`retail-setup deploy` runs this deployment automatically for Reporting
profiles, through the non-required `deploy-fabric-app` step
(`deploy/scripts/deploy_fabric_app.py`). That step needs Node.js 20+. It stages
each app under `deploy/.generated/<env>/fabric-apps/<variant>/` with its own
`rayfin.yml` identity and deployment registry, builds it with its
`VITE_APP_VARIANT`, and deploys a new app a second time in the same run once
its own sign-in origin is known. The approvals app adopts the
item first deployed from this folder, so its data is kept.

Prerequisites:
- a tenant admin has enabled **Fabric Apps (preview)**;
- the capacity is in a [supported region](https://learn.microsoft.com/fabric/admin/region-availability);
- you can sign in to the demo tenant.

To deploy only the apps:

```powershell
npx rayfin login --tenant <tenant-id>
python -m deploy.scripts.deploy_fabric_app --environment <env> --dry-run
python -m deploy.scripts.deploy_fabric_app --environment <env>
```

Then follow the acceptance checklist in
`context/plans/2026-10-07-fabcon-live-tests.md`.
