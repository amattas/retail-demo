import { useState } from 'react';

import { Icon } from '@/components/Icon';
import { PRODUCTS, STORES, number } from '@/data/merchandising';
import { MAX_QUANTITY, type ProposalInput } from '@/services/approvals';

interface FormState {
  store_id: number;
  product_id: number;
  quantity: string;
  rationale: string;
}

const toForm = (input: ProposalInput): FormState => ({
  ...input, quantity: input.quantity ? String(input.quantity) : '',
});

/**
 * The replenishment proposal form, shared by the approvals queue (blank) and
 * the merchandising item table (pre-filled from the item's stock evidence).
 * ``redraft`` re-derives quantity and rationale when the planner picks another
 * store; edits to those fields are otherwise kept.
 */
export function ProposalForm({
  initial, title, description, lockProduct = false, redraft, onSubmit, onCancel,
}: {
  initial: ProposalInput;
  title: string;
  description: string;
  lockProduct?: boolean;
  redraft?: (storeId: number) => ProposalInput;
  onSubmit: (input: ProposalInput) => Promise<boolean>;
  onCancel?: () => void;
}) {
  const [form, setForm] = useState<FormState>(() => toForm(initial));
  const [busy, setBusy] = useState(false);

  const chooseStore = (storeId: number) =>
    setForm(redraft ? toForm(redraft(storeId)) : { ...form, store_id: storeId });

  const submit = async (event: React.FormEvent) => {
    event.preventDefault();
    setBusy(true);
    try {
      const ok = await onSubmit({
        store_id: Number(form.store_id), product_id: Number(form.product_id),
        quantity: Number(form.quantity), rationale: form.rationale,
      });
      if (ok) setForm(toForm(initial));
    } finally {
      setBusy(false);
    }
  };

  return <form className="card stack" onSubmit={submit} aria-label={title}>
    <div className="section-title"><div><h2>{title}</h2><p>{description} Draft only. Up to {number(MAX_QUANTITY)} units.</p></div>
      {onCancel && <button type="button" className="btn btn-sm btn-ghost icon-btn" aria-label="Close proposal" onClick={onCancel}>
        <Icon name="close" width="15" height="15" /></button>}</div>
    <div className="form-grid">
      <label className="field">Store
        <select className="select" value={form.store_id} onChange={(e) => chooseStore(Number(e.target.value))}>
          {STORES.map((item) => <option key={item.id} value={item.id}>{item.name}</option>)}
        </select></label>
      <label className="field">Product
        <select className="select" value={form.product_id} disabled={lockProduct}
          onChange={(e) => setForm({ ...form, product_id: Number(e.target.value) })}>
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
    <div><button type="submit" className="btn btn-primary" disabled={busy}><Icon name="arrow" width="15" height="15" />Submit for review</button></div>
  </form>;
}
