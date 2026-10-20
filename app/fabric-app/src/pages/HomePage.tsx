import { useCallback, useEffect, useState } from 'react';

import { useAuth } from '@/hooks/AuthContext';
import {
  decide,
  listProposals,
  MAX_QUANTITY,
  submitProposal,
  type Decision,
  type ProposalView,
} from '@/services/approvals';

const EMPTY_FORM = { store_id: '', product_id: '', quantity: '', rationale: '' };

export function HomePage() {
  const { signOut, user } = useAuth();
  const [proposals, setProposals] = useState<ProposalView[]>([]);
  const [form, setForm] = useState(EMPTY_FORM);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);

  const refresh = useCallback(async () => {
    try {
      setProposals(await listProposals());
    } catch (exc) {
      setError(exc instanceof Error ? exc.message : String(exc));
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    void refresh();
  }, [refresh]);

  const run = async (action: () => Promise<unknown>) => {
    setError(null);
    try {
      await action();
      await refresh();
    } catch (exc) {
      setError(exc instanceof Error ? exc.message : String(exc));
    }
  };

  const onSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    if (!user) return;
    void run(async () => {
      await submitProposal(
        {
          store_id: Number(form.store_id),
          product_id: Number(form.product_id),
          quantity: Number(form.quantity),
          rationale: form.rationale,
        },
        { id: user.id, email: user.email }
      );
      setForm(EMPTY_FORM);
    });
  };

  const onDecide = (id: string, decision: Decision, comment: string) => {
    if (!user) return;
    void run(() => decide(id, decision, comment, { id: user.id, email: user.email }));
  };

  return (
    <div className="bg-gray-50 min-h-screen">
      <header className="flex items-center justify-between px-8 py-5 bg-white border-b border-gray-200">
        <div>
          <h1 className="text-xl font-bold text-gray-900">Replenishment Approvals</h1>
          <p className="text-xs text-gray-500">
            Synthetic draft proposals only. Decisions are recorded; nothing is ordered.
          </p>
        </div>
        <div className="flex items-center gap-4">
          {user?.email && <span className="text-sm text-gray-600">{user.email}</span>}
          <button
            onClick={() => void signOut()}
            className="text-gray-400 hover:text-gray-600 text-sm"
          >
            Sign out
          </button>
        </div>
      </header>

      <main className="max-w-3xl mx-auto px-4 py-8 space-y-8">
        {error && (
          <p role="alert" className="rounded-lg bg-red-50 px-4 py-3 text-sm text-red-700">
            {error}
          </p>
        )}

        <form
          onSubmit={onSubmit}
          aria-label="New proposal"
          className="grid grid-cols-3 gap-3 rounded-xl bg-white p-4 shadow-sm"
        >
          {(['store_id', 'product_id', 'quantity'] as const).map((key) => (
            <label key={key} className="text-xs text-gray-600">
              {key.replace('_', ' ')}
              <input
                type="number"
                min={1}
                max={key === 'quantity' ? MAX_QUANTITY : undefined}
                required
                value={form[key]}
                onChange={(e) => setForm({ ...form, [key]: e.target.value })}
                className="mt-1 w-full rounded-lg border border-gray-300 px-3 py-2 text-sm"
              />
            </label>
          ))}
          <label className="col-span-3 text-xs text-gray-600">
            rationale
            <textarea
              required
              maxLength={1000}
              value={form.rationale}
              onChange={(e) => setForm({ ...form, rationale: e.target.value })}
              className="mt-1 w-full rounded-lg border border-gray-300 px-3 py-2 text-sm"
            />
          </label>
          <button
            type="submit"
            className="col-span-3 rounded-lg bg-blue-600 px-4 py-2 text-sm font-medium text-white hover:bg-blue-700"
          >
            Submit proposal
          </button>
        </form>

        {loading ? (
          <p className="text-center text-sm text-gray-400">Loading...</p>
        ) : proposals.length === 0 ? (
          <p className="text-center text-sm text-gray-400">No proposals yet.</p>
        ) : (
          <ul className="space-y-3">
            {proposals.map((proposal) => (
              <ProposalRow
                key={proposal.id}
                proposal={proposal}
                isOwn={proposal.created_by === user?.id}
                onDecide={onDecide}
              />
            ))}
          </ul>
        )}
      </main>
    </div>
  );
}

function ProposalRow({
  proposal,
  isOwn,
  onDecide,
}: {
  proposal: ProposalView;
  isOwn: boolean;
  onDecide: (id: string, decision: Decision, comment: string) => void;
}) {
  const [comment, setComment] = useState('');
  return (
    <li className="rounded-xl bg-white p-4 shadow-sm">
      <div className="flex items-center justify-between">
        <p className="text-sm font-medium text-gray-900">
          Store {proposal.store_id} · Product {proposal.product_id} · {proposal.quantity} units
        </p>
        <span className="text-xs font-semibold text-gray-600">{proposal.status}</span>
      </div>
      <p className="mt-1 text-sm text-gray-700">{proposal.rationale}</p>
      <p className="mt-1 text-xs text-gray-400">
        Submitted by {proposal.created_by_email ?? proposal.created_by} on{' '}
        {proposal.created_at.toLocaleString()}
      </p>
      {proposal.decision ? (
        <p className="mt-2 text-xs text-gray-500">
          {proposal.decision.decision} by{' '}
          {proposal.decision.reviewer_email ?? proposal.decision.reviewer_id}
          {proposal.decision.comment ? `: ${proposal.decision.comment}` : ''}
        </p>
      ) : isOwn ? (
        <p className="mt-2 text-xs text-gray-500">Awaiting another reviewer.</p>
      ) : (
        <div className="mt-3 flex gap-2">
          <input
            aria-label="Decision comment"
            value={comment}
            maxLength={1000}
            onChange={(e) => setComment(e.target.value)}
            placeholder="Comment (required to reject)"
            className="flex-1 rounded-lg border border-gray-300 px-3 py-1 text-sm"
          />
          <button
            onClick={() => onDecide(proposal.id, 'APPROVED', comment)}
            className="rounded-lg bg-green-600 px-3 py-1 text-sm text-white"
          >
            Approve
          </button>
          <button
            onClick={() => onDecide(proposal.id, 'REJECTED', comment)}
            className="rounded-lg bg-red-600 px-3 py-1 text-sm text-white"
          >
            Reject
          </button>
        </div>
      )}
    </li>
  );
}