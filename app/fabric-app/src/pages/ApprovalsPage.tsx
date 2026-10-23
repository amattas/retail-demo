import { useCallback, useMemo, useState } from 'react';

import { Icon } from '@/components/Icon';
import { EmptyState, MetricCard, Notice, PageHeading } from '@/components/WorkspaceUI';
import { PRODUCTS, STORES, number } from '@/data/merchandising';
import { useAuth } from '@/hooks/AuthContext';
import { useCollection } from '@/hooks/useCollection';
import {
  decide, listProposals, MAX_QUANTITY, submitProposal,
  type Decision, type ProposalView,
} from '@/services/approvals';

type Tab = 'pending' | 'decided' | 'all';

const productName = (id: number) => PRODUCTS.find((item) => item.id === id)?.name ?? `Product ${id}`;
const productSku = (id: number) => PRODUCTS.find((item) => item.id === id)?.sku ?? `#${id}`;
const storeName = (id: number) => STORES.find((item) => item.id === id)?.name ?? `Store ${id}`;
const STATUS_TONE = { PENDING: 'info', APPROVED: 'good', REJECTED: 'bad' } as const;

function ago(date: Date): string {
  const minutes = Math.max(0, Math.round((Date.now() - date.getTime()) / 60_000));
  if (minutes < 1) return 'just now';
  if (minutes < 60) return `${minutes} min ago`;
  const hours = Math.round(minutes / 60);
  return hours < 48 ? `${hours} h ago` : date.toLocaleDateString();
}

const EMPTY = { store_id: STORES[0].id, product_id: PRODUCTS[0].id, quantity: '', rationale: '' };

export function ApprovalsPage() {
  const { user } = useAuth();
  const proposals = useCollection<ProposalView>(listProposals);
  const [tab, setTab] = useState<Tab>('pending');
  const [form, setForm] = useState(EMPTY);
  const [showForm, setShowForm] = useState(false);
  const [message, setMessage] = useState<{ text: string; danger?: boolean } | null>(null);
  const reviewer = useMemo(() => ({ id: user?.id ?? '', email: user?.email }), [user?.id, user?.email]);


  const counts = useMemo(() => ({
    pending: proposals.items.filter((p) => p.status === 'PENDING').length,
    approved: proposals.items.filter((p) => p.status === 'APPROVED').length,
    rejected: proposals.items.filter((p) => p.status === 'REJECTED').length,
    forMe: proposals.items.filter((p) => p.status === 'PENDING' && p.created_by !== user?.id).length,
  }), [proposals.items, user?.id]);
  const visible = proposals.items.filter((p) =>
    tab === 'all' || (tab === 'pending' ? p.status === 'PENDING' : p.status !== 'PENDING'));

  const run = useCallback(async (action: () => Promise<unknown>, success: string) => {
    setMessage(null);
    try {
      await action();
      setMessage({ text: success });
      await proposals.reload();
      return true;
    } catch (error) {
      setMessage({ text: error instanceof Error ? error.message : String(error), danger: true });
      return false;
    }
  }, [proposals]);

  const onSubmit = async (event: React.FormEvent) => {
    event.preventDefault();
    const ok = await run(() => submitProposal({
      store_id: Number(form.store_id), product_id: Number(form.product_id),
      quantity: Number(form.quantity), rationale: form.rationale,
    }, reviewer), 'Proposal submitted. Another reviewer must decide it.');
    if (ok) { setForm(EMPTY); setShowForm(false); setTab('pending'); }
  };

  const onDecide = (id: string, decision: Decision, comment: string) =>
    run(() => decide(id, decision, comment, reviewer), decision === 'APPROVED' ? 'Proposal approved.' : 'Proposal rejected.');

  return <>
    <PageHeading eyebrow="Replenishment approvals" title="Review replenishment proposals"
      description="Planners submit draft replenishment proposals; a different reviewer approves or rejects them. Decisions are append-only audit records bound to your Fabric identity. Nothing is ordered."
      actions={<button type="button" className="btn btn-primary" onClick={() => setShowForm((value) => !value)}>
        <Icon name={showForm ? 'close' : 'plus'} width="15" height="15" />{showForm ? 'Close form' : 'New proposal'}</button>} />

    {message && <Notice danger={message.danger}>{message.text}</Notice>}

    <div className="metrics">
      <MetricCard label="Awaiting your review" icon="inbox" value={number(counts.forMe)} detail="Pending proposals from other planners" tone={counts.forMe ? 'warning' : 'good'} />
      <MetricCard label="Pending" icon="clock" value={number(counts.pending)} detail="Including your own submissions" />
      <MetricCard label="Approved" icon="check" value={number(counts.approved)} detail="Earliest decision wins" tone="good" />
      <MetricCard label="Rejected" icon="close" value={number(counts.rejected)} detail="Always with a reason" />
    </div>

    {showForm && <form className="card stack" onSubmit={onSubmit} aria-label="New proposal">
      <div className="section-title"><div><h2>New proposal</h2><p>Draft only. Up to {number(MAX_QUANTITY)} units.</p></div></div>
      <div className="form-grid">
        <label className="field">Store
          <select className="select" value={form.store_id} onChange={(e) => setForm({ ...form, store_id: Number(e.target.value) })}>
            {STORES.map((item) => <option key={item.id} value={item.id}>{item.name}</option>)}
          </select></label>
        <label className="field">Product
          <select className="select" value={form.product_id} onChange={(e) => setForm({ ...form, product_id: Number(e.target.value) })}>
            {PRODUCTS.map((item) => <option key={item.id} value={item.id}>{item.name} ({item.sku})</option>)}
          </select></label>
        <label className="field">Quantity
          <input className="input" type="number" min={1} max={MAX_QUANTITY} required value={form.quantity}
            onChange={(e) => setForm({ ...form, quantity: e.target.value })} /></label>
        <div />
        <label className="field span-2">Rationale
          <textarea className="textarea" required maxLength={1000} value={form.rationale}
            placeholder="What evidence supports this quantity?" onChange={(e) => setForm({ ...form, rationale: e.target.value })} /></label>
      </div>
      <div><button type="submit" className="btn btn-primary"><Icon name="arrow" width="15" height="15" />Submit for review</button></div>
    </form>}

    <section className="stack" aria-label="Proposals">
      <div className="section-title">
        <div className="segmented" role="group" aria-label="Proposal status">
          {([['pending', `Pending (${counts.pending})`], ['decided', `Decided (${counts.approved + counts.rejected})`], ['all', 'All']] as const)
            .map(([id, label]) => <button key={id} type="button" aria-pressed={tab === id} onClick={() => setTab(id)}>{label}</button>)}
        </div>
        <button type="button" className="btn btn-sm" onClick={() => void proposals.reload()}><Icon name="refresh" width="14" height="14" />Refresh</button>
      </div>
      {proposals.error && <Notice danger>{proposals.error}</Notice>}
      {proposals.loading && !proposals.items.length ? <p className="muted">Loading proposals…</p>
        : !visible.length ? <div className="card"><EmptyState title={tab === 'pending' ? 'Queue is clear' : 'Nothing here yet'} icon="inbox">
          {tab === 'pending' ? 'No proposals are waiting for a decision.' : 'Decided proposals will appear here.'}</EmptyState></div>
          : visible.map((proposal) => <ProposalCard key={proposal.id} proposal={proposal}
            isOwn={proposal.created_by === user?.id} onDecide={onDecide} />)}
    </section>
  </>;
}

function ProposalCard({ proposal, isOwn, onDecide }: {
  proposal: ProposalView; isOwn: boolean;
  onDecide: (id: string, decision: Decision, comment: string) => Promise<boolean>;
}) {
  const [comment, setComment] = useState('');
  const [busy, setBusy] = useState(false);
  const act = async (decision: Decision) => {
    setBusy(true);
    try { await onDecide(proposal.id, decision, comment); } finally { setBusy(false); }
  };
  return <article className="card proposal">
    <div className="proposal-head">
      <div><h3>{productName(proposal.product_id)} · {number(proposal.quantity)} units</h3>
        <p>{storeName(proposal.store_id)} · {productSku(proposal.product_id)}</p></div>
      <span className={`pill ${STATUS_TONE[proposal.status]}`}>{proposal.status.toLowerCase()}</span>
    </div>
    <p className="proposal-body">{proposal.rationale}</p>
    <div className="proposal-meta">
      <span>Submitted by {proposal.created_by_email ?? proposal.created_by}</span>
      <span title={proposal.created_at.toLocaleString()}>{ago(proposal.created_at)}</span>
      <span>Evidence: {proposal.evidence_mode}</span>
    </div>
    {proposal.decision ? (
      <div className="decision-note">
        <strong>{proposal.decision.decision === 'APPROVED' ? 'Approved' : 'Rejected'}</strong> by{' '}
        {proposal.decision.reviewer_email ?? proposal.decision.reviewer_id} · {proposal.decision.decided_at.toLocaleString()}
        {proposal.decision.comment && <div className="muted" style={{ marginTop: 4 }}>“{proposal.decision.comment}”</div>}
      </div>
    ) : isOwn ? (
      <div className="decision-note muted">Awaiting another reviewer. You cannot decide your own proposal.</div>
    ) : (
      <div className="decision-row">
        <input className="input" aria-label="Decision comment" value={comment} maxLength={1000}
          placeholder="Comment (required to reject)" onChange={(e) => setComment(e.target.value)} />
        <button type="button" className="btn btn-good" disabled={busy} onClick={() => void act('APPROVED')}><Icon name="check" width="15" height="15" />Approve</button>
        <button type="button" className="btn btn-danger" disabled={busy} onClick={() => void act('REJECTED')}><Icon name="close" width="15" height="15" />Reject</button>
      </div>
    )}
  </article>;
}
