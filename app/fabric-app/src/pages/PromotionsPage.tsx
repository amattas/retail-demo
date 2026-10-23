import { useCallback, useMemo, useState } from 'react';

import { Icon } from '@/components/Icon';
import { EmptyState, MetricCard, Notice, PageHeading, SourceNote } from '@/components/WorkspaceUI';
import { PRODUCTS, STORES, money, number, price } from '@/data/merchandising';
import { useAuth } from '@/hooks/AuthContext';
import { useCollection } from '@/hooks/useCollection';
import { campaignDays, scenarioImpact } from '@/services/planning';
import { deleteScenario, listScenarios, saveScenario, type PromotionScenario } from '@/services/workspaces';

const DAY = 86_400_000;
const isoDay = (date: Date) => date.toISOString().slice(0, 10);
const utcDate = (value: string) => new Date(`${value}T00:00:00Z`);

function defaultDates() {
  const start = new Date();
  start.setUTCHours(0, 0, 0, 0);
  start.setUTCDate(start.getUTCDate() + 7);
  return { start: isoDay(start), end: isoDay(new Date(start.getTime() + 13 * DAY)) };
}

function safeDays(start: string, end: string): number | null {
  try { return campaignDays(utcDate(start), utcDate(end)); } catch { return null; }
}

export function PromotionsPage() {
  const { user } = useAuth();
  const actor = useMemo(() => ({ id: user?.id ?? '', email: user?.email }), [user?.id, user?.email]);
  const dates = useMemo(defaultDates, []);
  const [form, setForm] = useState({
    name: '', productId: PRODUCTS[0].id, storeId: STORES[0].id, start: dates.start, end: dates.end,
    discount: 15, uplift: 25, baseline: PRODUCTS[0].dailyUnits * 14,
  });
  const [message, setMessage] = useState<{ text: string; danger?: boolean } | null>(null);
  const loader = useCallback(() => listScenarios(actor), [actor]);
  const saved = useCollection<PromotionScenario>(loader);

  const product = PRODUCTS.find((item) => item.id === form.productId) ?? PRODUCTS[0];
  const store = STORES.find((item) => item.id === form.storeId) ?? STORES[0];
  const days = safeDays(form.start, form.end);
  const numbers = {
    baseline_units: form.baseline, unit_price_cents: product.priceCents, unit_cost_cents: product.costCents,
    discount_percent: form.discount, uplift_percent: form.uplift,
  };
  const impact = (() => {
    try { return scenarioImpact(numbers); } catch { return null; }
  })();

  const resetBaseline = (productId: number, start = form.start, end = form.end) => {
    const item = PRODUCTS.find((entry) => entry.id === productId) ?? PRODUCTS[0];
    return item.dailyUnits * (safeDays(start, end) ?? 14);
  };
  const set = (patch: Partial<typeof form>) => setForm((current) => ({ ...current, ...patch }));

  const onSave = async (event: React.FormEvent) => {
    event.preventDefault();
    try {
      await saveScenario({
        name: form.name, product_id: product.id, product_name: product.name, store_id: store.id,
        store_name: store.name, start_date: utcDate(form.start), end_date: utcDate(form.end), ...numbers,
      }, actor);
      set({ name: '' });
      setMessage({ text: 'Scenario saved. Compare it with your other scenarios below.' });
      await saved.reload();
    } catch (error) {
      setMessage({ text: error instanceof Error ? error.message : String(error), danger: true });
    }
  };

  const load = (row: PromotionScenario) => {
    setForm({
      name: '', productId: row.product_id, storeId: row.store_id, start: isoDay(row.start_date), end: isoDay(row.end_date),
      discount: row.discount_percent, uplift: row.uplift_percent, baseline: row.baseline_units,
    });
    setMessage({ text: `Loaded “${row.name}”. Adjust it and save as a new scenario.` });
  };

  const remove = async (id: string) => {
    try { await deleteScenario(id, actor); await saved.reload(); } catch (error) {
      setMessage({ text: error instanceof Error ? error.message : String(error), danger: true });
    }
  };

  const max = impact ? Math.max(impact.baselineRevenueCents, impact.revenueCents, 1) : 1;
  const accretive = impact ? impact.profitChangeCents >= 0 : false;

  return <>
    <PageHeading eyebrow="Promotion studio" title="Model a promotion before you run it"
      description="Combine a discount with an assumed unit uplift to see revenue, gross profit and the uplift needed to break even. Scenarios are private what-ifs; prices are never changed." />
    {message && <Notice danger={message.danger}>{message.text}</Notice>}

    <div className="grid-2">
      <form className="card stack" onSubmit={onSave} aria-label="Promotion scenario">
        <div className="section-title"><div><h2>Scenario inputs</h2><p>{product.sku} · list {price(product.priceCents)} · cost {price(product.costCents)}</p></div></div>
        <div className="form-grid">
          <label className="field">Product
            <select className="select" value={form.productId} onChange={(e) => {
              const productId = Number(e.target.value);
              set({ productId, baseline: resetBaseline(productId) });
            }}>
              {PRODUCTS.map((item) => <option key={item.id} value={item.id}>{item.name}</option>)}
            </select></label>
          <label className="field">Store
            <select className="select" value={form.storeId} onChange={(e) => set({ storeId: Number(e.target.value) })}>
              {STORES.map((item) => <option key={item.id} value={item.id}>{item.name}</option>)}
            </select></label>
          <label className="field">Start date
            <input className="input" type="date" value={form.start} required onChange={(e) => set({ start: e.target.value })} /></label>
          <label className="field">End date
            <input className="input" type="date" value={form.end} min={form.start} required onChange={(e) => set({ end: e.target.value })} /></label>
          <label className="field span-2">Baseline units for the campaign
            <span style={{ display: 'flex', gap: 8 }}>
              <input className="input" type="number" min={1} max={1_000_000} required value={form.baseline}
                onChange={(e) => set({ baseline: Number(e.target.value) })} />
              <button type="button" className="btn" onClick={() => set({ baseline: resetBaseline(form.productId) })}>
                <Icon name="refresh" width="15" height="15" />Sample baseline</button>
            </span></label>
          <label className="field span-2">Discount: {form.discount}% → {price(Math.round(product.priceCents * (1 - form.discount / 100)))}
            <input className="range" type="range" min={0} max={70} value={form.discount} onChange={(e) => set({ discount: Number(e.target.value) })} /></label>
          <label className="field span-2">Assumed unit uplift: {form.uplift >= 0 ? '+' : ''}{form.uplift}%
            <input className="range" type="range" min={-30} max={100} value={form.uplift} onChange={(e) => set({ uplift: Number(e.target.value) })} /></label>
        </div>
        {days === null && <Notice danger>Choose a campaign lasting 1 to 84 days, with the end on or after the start.</Notice>}
        <div style={{ display: 'flex', gap: 8 }}>
          <input className="input" aria-label="Scenario name" placeholder="Scenario name" maxLength={80} required value={form.name}
            onChange={(e) => set({ name: e.target.value })} />
          <button type="submit" className="btn btn-primary" disabled={days === null || !impact}><Icon name="plus" width="15" height="15" />Save scenario</button>
        </div>
      </form>

      <section className="card stack" aria-label="Projected impact">
        <div className="section-title"><div><h2>Projected impact</h2><p>{store.name} · {days ?? '—'} days</p></div></div>
        {impact ? <>
          <div className={`verdict ${accretive ? 'good' : 'warning'}`}>
            <Icon name={accretive ? 'check' : 'alert'} />
            <div>{accretive
              ? `Profit-accretive: gross profit rises ${money(impact.profitChangeCents)} versus no promotion.`
              : impact.breakEvenUplift === null
                ? 'The promoted price is at or below cost, so no volume uplift can recover gross profit.'
                : `Dilutive: needs at least +${impact.breakEvenUplift}% unit uplift to break even (assumed +${form.uplift}%).`}</div>
          </div>
          <div className="impact-grid">
            <div className="impact"><span>Estimated units</span><strong>{number(impact.estimatedUnits)}</strong><small className="muted">baseline {number(form.baseline)}</small></div>
            <div className="impact"><span>Promoted price</span><strong>{price(impact.promoPriceCents)}</strong><small className="muted">list {price(product.priceCents)}</small></div>
            <div className="impact"><span>Discount investment</span><strong>{money(impact.discountInvestmentCents)}</strong><small className="muted">on baseline units</small></div>
            <div className="impact"><span>Incremental volume profit</span><strong>{money(impact.volumeContributionCents)}</strong><small className="muted">from uplifted units</small></div>
          </div>
          <div className="bars" aria-label="Baseline versus promotion">
            {[
              ['Baseline revenue', impact.baselineRevenueCents, false], ['Promo revenue', impact.revenueCents, true],
              ['Baseline profit', impact.baselineProfitCents, false], ['Promo profit', impact.profitCents, true],
            ].map(([label, value, accent]) => (
              <div className="bar-row" key={String(label)}><span>{label}</span>
                <div className="bar"><div className={accent ? 'accent' : ''} style={{ width: `${Math.max(0, Number(value)) / max * 100}%` }} /></div>
                <b>{money(Number(value))}</b></div>
            ))}
          </div>
        </> : <EmptyState title="Check the inputs" icon="alert">Baseline units must be a whole number from 1 to 1,000,000.</EmptyState>}
      </section>
    </div>

    <div className="metrics">
      <MetricCard label="Saved scenarios" icon="bookmark" value={number(saved.items.length)} detail="Private to you" />
      <MetricCard label="Profit-accretive" icon="check" tone="good"
        value={number(saved.items.filter((row) => scenarioImpact(row).profitChangeCents >= 0).length)} detail="Scenarios that grow gross profit" />
    </div>

    <section className="card flush">
      <div className="section-title" style={{ padding: '16px 18px 0' }}><div><h2>Compare scenarios</h2><p>Gross profit change versus running no promotion.</p></div></div>
      {saved.error && <div style={{ padding: 16 }}><Notice danger>{saved.error}</Notice></div>}
      {saved.items.length ? <div className="table-wrap"><table className="table">
        <thead><tr><th>Scenario</th><th>Product · store</th><th>Dates</th><th className="num">Discount</th><th className="num">Uplift</th>
          <th className="num">Break-even</th><th className="num">Profit change</th><th><span className="sr-only">Actions</span></th></tr></thead>
        <tbody>{saved.items.map((row) => {
          const result = scenarioImpact(row);
          return <tr key={row.id}>
            <td className="product-cell"><strong>{row.name}</strong><span>Saved {row.created_at.toLocaleDateString()}</span></td>
            <td className="product-cell"><strong>{row.product_name}</strong><span>{row.store_name}</span></td>
            <td><span className="muted">{isoDay(row.start_date)} → {isoDay(row.end_date)}</span></td>
            <td className="num">{row.discount_percent}%</td>
            <td className="num">{row.uplift_percent >= 0 ? '+' : ''}{row.uplift_percent}%</td>
            <td className="num">{result.breakEvenUplift === null ? 'n/a' : `+${result.breakEvenUplift}%`}</td>
            <td className="num"><span className={result.profitChangeCents >= 0 ? 'delta-up' : 'delta-down'}>{result.profitChangeCents >= 0 ? '+' : ''}{money(result.profitChangeCents)}</span></td>
            <td style={{ whiteSpace: 'nowrap' }}>
              <button type="button" className="btn btn-sm" onClick={() => load(row)}>Load</button>
              <button type="button" className="btn btn-sm btn-ghost icon-btn" aria-label={`Delete ${row.name}`} onClick={() => void remove(row.id)}><Icon name="trash" width="15" height="15" /></button>
            </td>
          </tr>;
        })}</tbody>
      </table></div> : !saved.loading && <EmptyState title="No scenarios yet" icon="tag">Save a scenario to compare discount depth and uplift assumptions.</EmptyState>}
      <div className="table-footer"><SourceNote /></div>
    </section>
  </>;
}

