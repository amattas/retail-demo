# Protect the Winner - Presenter Walkthrough

> **Run time:** 12-15 minutes
> **Data:** wholly synthetic
> **Persona:** Morgan Lee, Regional Merchandising Lead

## Opening

Say:

> Aster & Pine is a fictional omnichannel retailer. This experience shows how
> Microsoft Fabric can connect governed analytics, a business ontology, AI
> agents, and human decisions without using customer data.

The story is:

**See the signal → understand the connected business context → make a
supply-aware recommendation → apply a human override → audit the result.**

## 1. See the signal

Open **Dashboard**.

Point out:

- Performance Footwear is down 4.8%;
- Momentum Runner is up 18.6%;
- three stores have less than 1.5 weeks of cover;
- the modeled opportunity is $286K.

Ask:

> Which product families are growing inside declining categories?

Expand **how I reached this**.

Explain that the semantic-model Data Agent is used because the question requires
aggregation, comparison, and contribution analysis.

## 2. Test whether growth is broad

Ask:

> Is Momentum Runner growth broad or concentrated?

Then ask:

> Which stores have traffic growth but declining conversion?

Explain that the semantic model is still the correct source because these are
ranked, period-aware measures across many rows.

## 3. Use the business graph

Ask:

> Which stores carry Momentum Runner and how are they supplied?

Expand the trace and point to:

```text
ProductFamily → Product → SKU → InventorySnapshot → Store → FulfillmentNode
```

Say:

> The semantic model found the opportunity. The ontology now explains the
> connected products, compatible SKUs, stores, inventory context, and serving
> fulfillment node.

Open **Ontology Explorer** and click `Store`, `InventorySnapshot`, and
`Recommendation`.

## 4. Ask for a decision

Return to **Dashboard** and ask:

> What should we do to protect Momentum Runner growth without creating
> stockouts?

The recommendation should:

- replenish `STORE-014`;
- transfer 180 units from `STORE-031` to `STORE-022`;
- activate `STORE-009`;
- preserve the fulfillment-node grain limitation.

Approve one action and dismiss another to show that the agent proposes while the
operator decides.

## 5. Apply a human override

Open **Decision Canvas**.

Read the signal, diagnosis, constraint, and recommendation from left to right.

Enter this override reason:

> Local event requires safety stock at the proposed source store.

Apply the override.

The source candidate changes from `STORE-031` to `STORE-027`, while the original
recommendation and reason remain visible.

Say:

> The human knows something the data does not. The system does not hide that
> intervention; it records it and recalculates transparently.

## 6. Close the loop

Open **Agent Operations**.

Show:

- total proposed actions;
- pending, approved, and dismissed states;
- approved replenishment units;
- the recent action feed.

In replay mode these are synthetic local events. In live mode the identical
contract is backed by the Fabric Eventhouse `agent_actions` table.

## Closing message

Say:

> This is not one published agent being reused against another customer's data.
> It is a customer-safe pattern: independently generated data, a purpose-built
> semantic model and ontology, specialized instructions, evidence-bound action
> tools, and a replayable application. Fabric provides the governed data and
> business context; the human remains accountable for the decision.

## Presenter recovery

- If live configuration is unavailable, use replay mode.
- If an answer is delayed, click the matching guided prompt chip.
- If a draft was already created, the replay service returns the existing
  proposal instead of duplicating it.
- Restart the app to clear the in-memory override.
