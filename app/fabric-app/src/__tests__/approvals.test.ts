import { beforeEach, describe, expect, it, vi } from 'vitest';

vi.mock('@/services/rayfinClient', () => ({
  isLocalBackend: () => true,
  getRayfinClient: vi.fn(),
}));

import {
  decide,
  decisionError,
  draftProposal,
  MAX_QUANTITY,
  listProposals,
  resetLocalStore,
  submitProposal,
  validateProposal,
  withStatus,
  type DecisionRecord,
  type Proposal,
} from '@/services/approvals';

const planner = { id: 'planner-sub', email: 'planner@contoso.com' };
const reviewer = { id: 'reviewer-sub', email: 'reviewer@contoso.com' };
const input = { store_id: 3, product_id: 17, quantity: 24, rationale: 'High stockout risk.' };

describe('proposal validation', () => {
  it('accepts a bounded draft and rejects invalid fields', () => {
    expect(validateProposal(input)).toEqual([]);
    expect(validateProposal({ ...input, store_id: 0, quantity: 10001, rationale: ' ' })).toHaveLength(3);
    expect(validateProposal({ ...input, product_id: 1.5 })).toHaveLength(1);
  });
});

describe('decision rules', () => {
  const proposal = {
    ...input,
    id: 'p1',
    evidence_mode: 'synthetic-draft',
    created_by: planner.id,
    created_at: new Date('2026-10-07T10:00:00Z'),
  } satisfies Proposal;
  const pending = withStatus([proposal], [])[0]!;

  it('enforces separation of duties and requires a rejection reason', () => {
    expect(decisionError(pending, planner, 'APPROVED', '')).toMatch(/different reviewer/);
    expect(decisionError(pending, reviewer, 'REJECTED', ' ')).toMatch(/Explain why/);
    expect(decisionError(pending, reviewer, 'APPROVED', '')).toBeNull();
  });

  it('keeps the earliest decision so a concurrent duplicate cannot overwrite it', () => {
    const at = (iso: string, decision: 'APPROVED' | 'REJECTED'): DecisionRecord => ({
      id: iso, proposal_id: 'p1', decision, reviewer_id: reviewer.id, decided_at: new Date(iso),
    });
    const [view] = withStatus(
      [proposal],
      [at('2026-10-07T12:00:00Z', 'REJECTED'), at('2026-10-07T11:00:00Z', 'APPROVED')]
    );
    expect(view!.status).toBe('APPROVED');
    expect(decisionError(view!, reviewer, 'REJECTED', 'late')).toMatch(/already/);
  });
});

describe('approval workflow (in-memory mode)', () => {
  beforeEach(() => resetLocalStore());

  it('records a proposal and one durable reviewer decision', async () => {
    const created = await submitProposal(input, planner);
    expect(created.created_by).toBe(planner.id);
    await expect(decide(created.id, 'APPROVED', '', planner)).rejects.toThrow(/different reviewer/);

    const record = await decide(created.id, 'APPROVED', 'Covers weekend demand.', reviewer);
    expect(record.reviewer_id).toBe(reviewer.id);

    const [view] = await listProposals();
    expect(view!.status).toBe('APPROVED');
    expect(view!.decision?.comment).toBe('Covers weekend demand.');
    await expect(decide(created.id, 'REJECTED', 'no', reviewer)).rejects.toThrow(/already/);
  });
});

describe('drafting from stock evidence', () => {
  const evidence = {
    store_id: 2, store_name: 'Riverside Market', product_id: 112, product_name: 'Free-range eggs',
    sku: 'SKU-112', units_sold: 280, period_days: 28, on_hand: 20, days_cover: 2, in_stock_rate: 91.25,
  };

  it('proposes enough units for 14 days of average sales and records the evidence', () => {
    const draft = draftProposal(evidence);
    expect(draft).toMatchObject({ store_id: 2, product_id: 112, quantity: 120 });
    expect(draft.rationale).toContain('Free-range eggs (SKU-112) at Riverside Market');
    expect(draft.rationale).toContain('2.0 days of cover, 91.3% in stock over the last 28 days');
    expect(validateProposal(draft)).toEqual([]);
  });

  it('keeps the quantity within the proposal bounds', () => {
    expect(draftProposal({ ...evidence, on_hand: 5000 }).quantity).toBe(1);
    expect(draftProposal({ ...evidence, units_sold: 10_000_000 }).quantity).toBe(MAX_QUANTITY);
    expect(draftProposal({ ...evidence, units_sold: 0, days_cover: null }).rationale).toContain('no sales');
  });
});
