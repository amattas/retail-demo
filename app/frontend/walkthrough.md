# Protect the Winner - 15-minute flagship walkthrough

> **Data:** independently generated and wholly fictional
> **Protagonist:** Dana Reyes, Regional Merchandising Lead
> **Primary page:** **Protect the Winner**

## Fixed scope

Keep this scope visible throughout the story:

| Scope | Value |
|---|---|
| Territory | Central Region |
| Sales period | Retail Fiscal Month 11, Fall 2026 |
| Comparison | Retail Fiscal Month 11, Fall 2025 |
| Inventory snapshot | 2026-08-28 |
| Mode | Certified synthetic replay |

The story never changes period, territory, inventory date, or protagonist.

## Opening

Say:

> Dana starts her morning with one decision-worthy signal. Fabric does not ask
> her to inspect every dashboard or trust one autonomous agent. It connects
> governed evidence, business relationships, decision policy, and human context
> to help her produce a reviewable action package.

## Beat 1 - Morning signal

**Click:** **Open the signal**

Point to the performance waterfall:

- Central Region Footwear is down **6.2%** year over year.
- Outlet stores are down **11%**.
- Full-price stores are up **2%**.
- Momentum Runner is up **38%**.
- Everything else is approximately flat.

Say:

> The category decline is real, but it hides a winner. Dana needs to know
> whether this is a small anomaly or a regional opportunity.

**Click:** **Continue to concentration**

## Beat 2 - Test concentration

Point to the ten-store velocity and coverage matrix:

- **7 of 10** stores are growing.
- **3 stores** are growing above 50%.
- Stores A-C have strong velocity and low coverage.
- Stores D-G have strong velocity and adequate coverage.
- Stores H-J have weak velocity and adequate coverage.

**Click:** **Test territory-wide activation**

The system should contradict the blanket promotion:

> Do not activate the whole territory. Stores A-C have less than 1.5 weeks of
> cover, so promotion would accelerate avoidable stockouts.

Point to the guardrail:

> Reserved inventory is validated at location × style × size × snapshot date.
> It cannot be treated as a generic regional pool.

**Click:** **Review recommendation**

## Beat 3 - Shape the decision

Read the selective recommendation:

- **Hold + replenish:** Stores A-C.
- **Activate:** Stores D-G.
- **Transfer:** compatible style-size inventory from Stores H-J into A-C.

Say:

> The recommendation is not “promote the winner.” It is a coordinated inventory
> and activation decision that respects where demand and coverage intersect.

**Click:** **Apply Dana's override**

Use the prefilled reason:

> Local community event requires protected inventory.

Apply the override. Store J is excluded and the transfer plan recalculates from
Stores H and I.

Say:

> Dana knows something the inventory snapshot does not. Her intervention is not
> hidden; the reason and recalculation remain part of the decision record.

**Click:** **Continue to package**

## Beat 4 - Build the package

**Click:** **Build the package**

Three artifacts appear:

1. **Transfer review list** — sources, destinations, compatible styles and
   sizes, quantity, grain, and snapshot date.
2. **Activation brief** — activates Stores D-G and explicitly excludes A-C and
   J with reasons.
3. **Decision log** — evidence sources, periods, grains, recommendation,
   override, owners, and review date.

Say:

> The output is not another chat transcript. It is an operational package that
> can be reviewed, audited, and connected to downstream workflows.

**Click:** **Continue to review**

## Beat 5 - Send for review

**Click:** **Send for review**

The Decision Canvas should show:

- selective activation and inventory protection;
- Dana's Store J override;
- transfer recalculation from Stores H and I;
- the three package artifacts;
- the morning signal resolved with monitoring continuing.

Close with:

> This customer-safe demonstration uses no customer records, names, branding,
> tenant identifiers, or published customer agent. Fabric supplies governed
> data and business context; specialized agents prepare evidence-bound drafts;
> Dana remains accountable for the final decision.

## Optional technical reveal

Open **Evidence and agent details** only after the business story is clear:

- **Fabric IQ** grounds performance and inventory evidence.
- **Foundry IQ** orchestrates recommendation and policy checks.
- **Work IQ** supplies fictional ownership and review context.
- **Web IQ** can add mocked public-market context but is not required.

Open **Architecture** for the Fabric implementation view. Do not end the
flagship story on the architecture screen; end on the completed Decision Canvas.

## Presenter recovery

- Refreshing the page preserves package/review state until the app restarts.
- If the override was already applied, continue directly to the package.
- Restart the app to reset all in-memory flagship state.
