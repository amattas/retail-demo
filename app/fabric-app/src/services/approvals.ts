import { getRayfinClient, isLocalBackend } from './rayfinClient';
import { isDesignPreview } from './runtimeMode';

export type Decision = 'APPROVED' | 'REJECTED';
export type ProposalStatus = 'PENDING' | Decision;

export interface Reviewer {
  id: string;
  email?: string;
}

export interface ProposalInput {
  store_id: number;
  product_id: number;
  quantity: number;
  rationale: string;
}

export interface Proposal extends ProposalInput {
  id: string;
  evidence_mode: string;
  created_by: string;
  created_by_email?: string;
  created_at: Date;
}

export interface DecisionRecord {
  id: string;
  proposal_id: string;
  decision: Decision;
  comment?: string;
  reviewer_id: string;
  reviewer_email?: string;
  decided_at: Date;
}

export interface ProposalView extends Proposal {
  status: ProposalStatus;
  decision?: DecisionRecord;
}

export const MAX_QUANTITY = 10000;
const EVIDENCE_MODE = 'synthetic-draft';

export function validateProposal(input: ProposalInput): string[] {
  const errors: string[] = [];
  for (const key of ['store_id', 'product_id'] as const) {
    if (!Number.isInteger(input[key]) || input[key] < 1) {
      errors.push(`${key} must be a positive whole number.`);
    }
  }
  if (!Number.isInteger(input.quantity) || input.quantity < 1 || input.quantity > MAX_QUANTITY) {
    errors.push(`quantity must be a whole number from 1 to ${MAX_QUANTITY}.`);
  }
  const rationale = input.rationale.trim();
  if (!rationale || rationale.length > 1000) {
    errors.push('rationale is required and must be at most 1,000 characters.');
  }
  return errors;
}

/** Earliest decision wins; later rows indicate a concurrent duplicate. */
export function withStatus(proposals: Proposal[], decisions: DecisionRecord[]): ProposalView[] {
  const first = new Map<string, DecisionRecord>();
  for (const decision of [...decisions].sort(
    (a, b) => a.decided_at.getTime() - b.decided_at.getTime()
  )) {
    if (!first.has(decision.proposal_id)) first.set(decision.proposal_id, decision);
  }
  return [...proposals]
    .sort((a, b) => b.created_at.getTime() - a.created_at.getTime())
    .map((proposal) => {
      const decision = first.get(proposal.id);
      return { ...proposal, status: decision?.decision ?? 'PENDING', decision };
    });
}

/** Separation of duties: the submitter cannot decide their own proposal. */
export function decisionError(
  proposal: ProposalView,
  reviewer: Reviewer,
  decision: Decision,
  comment: string
): string | null {
  if (proposal.status !== 'PENDING') return 'This proposal already has a decision.';
  if (proposal.created_by === reviewer.id) {
    return 'A different reviewer must decide a proposal you submitted.';
  }
  if (decision === 'REJECTED' && !comment.trim()) return 'Explain why the proposal is rejected.';
  if (comment.length > 1000) return 'Comments must be at most 1,000 characters.';
  return null;
}

let memoryProposals: Proposal[] = [];
let memoryDecisions: DecisionRecord[] = [];

export function resetLocalStore(): void {
  memoryProposals = [];
  memoryDecisions = [];
}

export function seedPreviewProposals(): void {
  if (!isDesignPreview()) throw new Error('Sample queues are available only in the development preview.');
  if (memoryProposals.length) return;
  const now = Date.now();
  memoryProposals = [
    { store_id: 1, product_id: 103, quantity: 120, rationale: 'Lemon sparkling water has less than three days of sample cover at Downtown Market. Review a 14-day replenishment draft before the weekend.', minutes: 18 },
    { store_id: 2, product_id: 112, quantity: 84, rationale: 'Free-range eggs are below the sample seven-day cover threshold at Riverside Market. Confirm delivery timing before approval.', minutes: 54 },
    { store_id: 3, product_id: 107, quantity: 60, rationale: 'Dark chocolate bites have strong sample velocity at Northside Market. Review shelf space and the proposed quantity.', minutes: 125 },
  ].map(({ minutes, ...input }) => ({
    ...input, id: crypto.randomUUID(), evidence_mode: EVIDENCE_MODE,
    created_by: 'preview-planner', created_by_email: 'planner@example.test',
    created_at: new Date(now - minutes * 60_000),
  }));
}

async function loadAll(): Promise<{ proposals: Proposal[]; decisions: DecisionRecord[] }> {
  if (isLocalBackend()) {
    return { proposals: [...memoryProposals], decisions: [...memoryDecisions] };
  }
  const client = getRayfinClient();
  const proposals = await client.data.ReplenishmentProposal.select([
    'id', 'store_id', 'product_id', 'quantity', 'rationale', 'evidence_mode',
    'created_by', 'created_by_email', 'created_at',
  ]).execute();
  const decisions = await client.data.ApprovalDecision.select([
    'id', 'proposal_id', 'decision', 'comment', 'reviewer_id', 'reviewer_email', 'decided_at',
  ]).execute();
  const asDate = (value: Date | string) => (value instanceof Date ? value : new Date(value));
  return {
    proposals: (proposals as Proposal[]).map((p) => ({ ...p, created_at: asDate(p.created_at) })),
    decisions: (decisions as DecisionRecord[]).map((d) => ({ ...d, decided_at: asDate(d.decided_at) })),
  };
}

export async function listProposals(): Promise<ProposalView[]> {
  const { proposals, decisions } = await loadAll();
  return withStatus(proposals, decisions);
}

export async function submitProposal(input: ProposalInput, author: Reviewer): Promise<Proposal> {
  const errors = validateProposal(input);
  if (errors.length) throw new Error(errors.join(' '));
  const row = {
    ...input,
    rationale: input.rationale.trim(),
    evidence_mode: EVIDENCE_MODE,
    created_by: author.id,
    created_by_email: author.email,
    created_at: new Date(),
  };
  if (isLocalBackend()) {
    const proposal = { id: crypto.randomUUID(), ...row };
    memoryProposals.push(proposal);
    return proposal;
  }
  return (await getRayfinClient().data.ReplenishmentProposal.create(row)) as Proposal;
}

export async function decide(
  proposalId: string,
  decision: Decision,
  comment: string,
  reviewer: Reviewer
): Promise<DecisionRecord> {
  const proposal = (await listProposals()).find((item) => item.id === proposalId);
  if (!proposal) throw new Error('Proposal not found.');
  const error = decisionError(proposal, reviewer, decision, comment);
  if (error) throw new Error(error);
  const row = {
    proposal_id: proposalId,
    decision,
    comment: comment.trim() || undefined,
    reviewer_id: reviewer.id,
    reviewer_email: reviewer.email,
    decided_at: new Date(),
  };
  if (isLocalBackend()) {
    const record = { id: crypto.randomUUID(), ...row };
    memoryDecisions.push(record);
    return record;
  }
  return (await getRayfinClient().data.ApprovalDecision.create(row)) as DecisionRecord;
}
