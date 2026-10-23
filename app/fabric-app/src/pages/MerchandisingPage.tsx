import { useCallback, useMemo, useState } from 'react';

import { Icon } from '@/components/Icon';
import { TrendChart } from '@/components/TrendChart';
import { EmptyState, MetricCard, Notice, PageHeading, SourceNote } from '@/components/WorkspaceUI';
import {
  CATEGORIES, DEFAULT_FILTERS, PERIODS, STORES, SUPPLIERS,
  changeLabel, exportMerchandisingCsv, groupTrend, merchandisingView, money, number, shortDate, trendValue,
  type Granularity, type MerchFilters, type MerchRow, type TrendMetric,
} from '@/data/merchandising';
import { useAuth } from '@/hooks/AuthContext';
import { useCollection } from '@/hooks/useCollection';
import { deleteView, listSavedViews, saveView, type MerchandisingView } from '@/services/workspaces';

type SortKey = 'name' | 'salesCents' | 'profitCents' | 'inStockRate' | 'onHand' | 'daysCover';
const METRICS: { id: TrendMetric; label: string }[] = [
  { id: 'availability', label: 'In-stock %' }, { id: 'sales', label: 'Sales' }, { id: 'margin', label: 'Gross profit' },
];
const GROUPINGS: { id: Granularity; label: string }[] = [
  { id: 'day', label: 'Daily' }, { id: 'week', label: 'Weekly' }, { id: 'fourWeeks', label: '4-weekly' },
];
const STATUS = {
  healthy: { label: 'Healthy', tone: 'good' }, low: { label: 'Low cover', tone: 'warning' }, out: { label: 'Out of stock', tone: 'bad' },
} as const;

function downloadCsv(content: string, filename: string) {
  const url = URL.createObjectURL(new Blob([content], { type: 'text/csv;charset=utf-8' }));
  const link = Object.assign(document.createElement('a'), { href: url, download: filename });
  link.click();
  URL.revokeObjectURL(url);
}

export function MerchandisingPage() {
  const { user } = useAuth();
  const actor = useMemo(() => ({ id: user?.id ?? '', email: user?.email }), [user?.id, user?.email]);
  const [filters, setFilters] = useState<MerchFilters>(DEFAULT_FILTERS);
  const [metric, setMetric] = useState<TrendMetric>('availability');
  const [granularity, setGranularity] = useState<Granularity>('day');
  const [sort, setSort] = useState<{ key: SortKey; desc: boolean }>({ key: 'salesCents', desc: true });
  const [attentionOnly, setAttentionOnly] = useState(false);
  const [viewName, setViewName] = useState('');
  const [message, setMessage] = useState<{ text: string; danger?: boolean } | null>(null);

  const loader = useCallback(() => listSavedViews(actor), [actor]);
  const saved = useCollection<MerchandisingView>(loader);

  const data = useMemo(() => merchandisingView(filters), [filters]);
  const grouping: Granularity = granularity === 'fourWeeks' && filters.days < 56 ? 'week' : granularity;
  const trend = useMemo(() => groupTrend(data.points, grouping).map((point) => ({
    label: shortDate(point.date), previousLabel: shortDate(point.previousDate),
    current: trendValue(point, metric), previous: trendValue(point, metric, true),
  })), [data.points, grouping, metric]);
  const formatTrend = metric === 'availability' ? (v: number) => `${v.toFixed(1)}%` : (v: number) => money(v * 100, true);

  const rows = useMemo(() => {
    const visible = attentionOnly ? data.rows.filter((row) => row.status !== 'healthy') : data.rows;
    const value = (row: MerchRow) => sort.key === 'name' ? row.name : row[sort.key] ?? -1;
    return [...visible].sort((a, b) => {
      const [left, right] = [value(a), value(b)];
      const order = typeof left === 'string' ? left.localeCompare(String(right)) : left - Number(right);
      return sort.desc ? -order : order;
    });
  }, [data.rows, sort, attentionOnly]);

  const { summary } = data;
  const update = (patch: Partial<MerchFilters>) => setFilters((current) => ({ ...current, ...patch }));
  const sortBy = (key: SortKey) => setSort((current) => ({ key, desc: current.key === key ? !current.desc : key !== 'name' }));
  const sortIndicator = (key: SortKey) => sort.key === key ? (sort.desc ? '↓' : '↑') : '';
  const ariaSort = (key: SortKey) => sort.key === key ? (sort.desc ? 'descending' : 'ascending') : 'none';

  const onSave = async (event: React.FormEvent) => {
    event.preventDefault();
    try {
      await saveView({
        name: viewName, store_filter: filters.store, category_filter: filters.category,
        supplier_filter: filters.supplier, search_filter: filters.search, period_days: filters.days,
        trend_metric: metric, granularity: grouping,
      }, actor);
      setViewName('');
      setMessage({ text: 'View saved to your workspace.' });
      await saved.reload();
    } catch (error) {
      setMessage({ text: error instanceof Error ? error.message : String(error), danger: true });
    }
  };

  const applyView = (view: MerchandisingView) => {
    setFilters({
      store: view.store_filter, category: view.category_filter, supplier: view.supplier_filter,
      search: view.search_filter, days: view.period_days,
    });
    setMetric(view.trend_metric);
    setGranularity(view.granularity);
    setMessage({ text: `Applied “${view.name}”.` });
  };

  const removeView = async (id: string) => {
    try {
      await deleteView(id, actor);
      await saved.reload();
    } catch (error) {
      setMessage({ text: error instanceof Error ? error.message : String(error), danger: true });
    }
  };


  return <>
    <PageHeading
      eyebrow="Merchandising portal"
      title="Item performance and availability"
      description={`Track in-stock rate, sales and margin against the previous period, then act on the items that need attention. Sample data from ${shortDate(data.start)} to ${shortDate(data.asOf)}.`}
      actions={<button type="button" className="btn" onClick={() => downloadCsv(exportMerchandisingCsv(rows), `merchandising-${data.asOf}.csv`)} disabled={!rows.length}>
        <Icon name="download" width="16" height="16" />Export CSV
      </button>}
    />

    {message && <Notice danger={message.danger}>{message.text}</Notice>}

    <section className="card" aria-label="Filters">
      <div className="filter-bar">
        <label className="field">Search items
          <span className="search"><Icon name="search" width="16" height="16" />
            <input className="input" value={filters.search} maxLength={120} placeholder="Name, SKU or brand"
              onChange={(e) => update({ search: e.target.value })} /></span>
        </label>
        <label className="field">Store
          <select className="select" value={filters.store} onChange={(e) => update({ store: e.target.value })}>
            <option value="all">All stores</option>
            {STORES.map((store) => <option key={store.id} value={String(store.id)}>{store.name}</option>)}
          </select>
        </label>
        <label className="field">Category
          <select className="select" value={filters.category} onChange={(e) => update({ category: e.target.value })}>
            <option value="all">All categories</option>
            {CATEGORIES.map((item) => <option key={item}>{item}</option>)}
          </select>
        </label>
        <label className="field">Supplier
          <select className="select" value={filters.supplier} onChange={(e) => update({ supplier: e.target.value })}>
            <option value="all">All suppliers</option>
            {SUPPLIERS.map((item) => <option key={item}>{item}</option>)}
          </select>
        </label>
        <label className="field">Period
          <select className="select" value={filters.days} onChange={(e) => update({ days: Number(e.target.value) })}>
            {PERIODS.map((days) => <option key={days} value={days}>Last {days} days</option>)}
          </select>
        </label>
      </div>
    </section>

    <div className="metrics">
      <MetricCard label="Sales" icon="chart" value={money(summary.salesCents, true)}
        detail={changeLabel(summary.salesCents, summary.previousSalesCents)}
        tone={summary.salesCents >= summary.previousSalesCents ? 'good' : 'warning'} />
      <MetricCard label="Gross profit" icon="tag" value={money(summary.profitCents, true)}
        detail={summary.salesCents ? `${(summary.profitCents / summary.salesCents * 100).toFixed(1)}% margin` : 'No sales in period'} />
      <MetricCard label="In-stock rate" icon="box" value={summary.inStockRate === null ? '—' : `${summary.inStockRate.toFixed(1)}%`}
        detail={summary.inStockRate !== null && summary.previousInStockRate !== null
          ? `${(summary.inStockRate - summary.previousInStockRate >= 0 ? '+' : '')}${(summary.inStockRate - summary.previousInStockRate).toFixed(1)} pts vs previous period`
          : 'No observations'}
        tone={summary.inStockRate !== null && summary.inStockRate >= 95 ? 'good' : 'warning'} />
      <MetricCard label="Items needing attention" icon="alert" value={number(summary.alerts)}
        detail={`${number(summary.onHand)} units on hand`} tone={summary.alerts ? 'warning' : 'good'} />
    </div>

    <div className="grid-main">
      <section className="card">
        <div className="section-title">
          <div><h2>{METRICS.find((item) => item.id === metric)?.label} trend</h2>
            <div className="chart-legend"><span><i />This period</span><span><i className="prev" />Previous period</span></div></div>
          <div className="page-actions">
            <div className="segmented" role="group" aria-label="Trend metric">
              {METRICS.map((item) => <button key={item.id} type="button" aria-pressed={metric === item.id} onClick={() => setMetric(item.id)}>{item.label}</button>)}
            </div>
            <div className="segmented" role="group" aria-label="Trend grouping">
              {GROUPINGS.map((item) => <button key={item.id} type="button" aria-pressed={grouping === item.id}
                disabled={item.id === 'fourWeeks' && filters.days < 56} onClick={() => setGranularity(item.id)}>{item.label}</button>)}
            </div>
          </div>
        </div>
        {trend.length ? <TrendChart points={trend} format={formatTrend} title={`${metric} trend`} />
          : <EmptyState title="No matching items" icon="search">Adjust the filters to see a trend.</EmptyState>}
      </section>

      <aside className="card">
        <div className="section-title"><div><h2>Saved views</h2><p>Private to you, stored in Rayfin.</p></div></div>
        <form className="stack" onSubmit={onSave}>
          <label className="field">View name
            <input className="input" value={viewName} maxLength={80} required placeholder="e.g. Beverage availability"
              onChange={(e) => setViewName(e.target.value)} /></label>
          <button type="submit" className="btn btn-primary"><Icon name="bookmark" width="15" height="15" />Save current view</button>
        </form>
        <div className="list" style={{ marginTop: 12 }}>
          {saved.error && <Notice danger>{saved.error}</Notice>}
          {!saved.loading && !saved.items.length && !saved.error && <p className="muted" style={{ fontSize: 13 }}>No saved views yet.</p>}
          {saved.items.map((view) => (
            <div className="list-item" key={view.id}>
              <div className="grow"><strong>{view.name}</strong>
                <span>{view.category_filter === 'all' ? 'All categories' : view.category_filter} · {view.period_days} days</span></div>
              <button type="button" className="btn btn-sm" onClick={() => applyView(view)}>Apply</button>
              <button type="button" className="btn btn-sm btn-ghost icon-btn" aria-label={`Delete ${view.name}`} onClick={() => void removeView(view.id)}>
                <Icon name="trash" width="15" height="15" /></button>
            </div>
          ))}
        </div>
      </aside>
    </div>

    <section className="card flush">
      <div className="section-title" style={{ padding: '16px 18px 0' }}>
        <div><h2>Item details</h2><p>{rows.length} of {data.rows.length} items · days of cover uses average daily sales for the period</p></div>
        <div className="segmented" role="group" aria-label="Item filter">
          <button type="button" aria-pressed={!attentionOnly} onClick={() => setAttentionOnly(false)}>All items</button>
          <button type="button" aria-pressed={attentionOnly} onClick={() => setAttentionOnly(true)}>Needs attention ({summary.alerts})</button>
        </div>
      </div>
      {rows.length ? <div className="table-wrap"><table className="table">
        <thead><tr>
          <th aria-sort={ariaSort('name')}><button type="button" onClick={() => sortBy('name')}>Item {sortIndicator('name')}</button></th>
          <th>Category</th>
          <th className="num" aria-sort={ariaSort('salesCents')}><button type="button" onClick={() => sortBy('salesCents')}>Sales {sortIndicator('salesCents')}</button></th>
          <th className="num" aria-sort={ariaSort('profitCents')}><button type="button" onClick={() => sortBy('profitCents')}>Gross profit {sortIndicator('profitCents')}</button></th>
          <th className="num" aria-sort={ariaSort('inStockRate')}><button type="button" onClick={() => sortBy('inStockRate')}>In-stock {sortIndicator('inStockRate')}</button></th>
          <th className="num" aria-sort={ariaSort('onHand')}><button type="button" onClick={() => sortBy('onHand')}>On hand {sortIndicator('onHand')}</button></th>
          <th className="num" aria-sort={ariaSort('daysCover')}><button type="button" onClick={() => sortBy('daysCover')}>Days cover {sortIndicator('daysCover')}</button></th>
          <th>Status</th>
        </tr></thead>
        <tbody>{rows.map((row) => {
          const tone = row.inStockRate >= 95 ? '' : row.inStockRate >= 90 ? 'warning' : 'bad';
          return <tr key={row.id}>
            <td className="product-cell"><strong>{row.name}</strong><span>{row.sku} · {row.brand}</span></td>
            <td><span className="muted">{row.category}</span></td>
            <td className="num">{money(row.salesCents)}</td>
            <td className="num">{money(row.profitCents)}</td>
            <td className="num"><span className="meter"><span className="meter-track"><span className={`meter-fill ${tone}`} style={{ width: `${Math.max(4, (row.inStockRate - 80) * 5)}%`, display: 'block' }} /></span>{row.inStockRate.toFixed(1)}%</span></td>
            <td className="num">{number(row.onHand)}</td>
            <td className="num">{row.daysCover === null ? '—' : row.daysCover.toFixed(1)}</td>
            <td><span className={`pill ${STATUS[row.status].tone}`}>{STATUS[row.status].label}</span></td>
          </tr>;
        })}</tbody>
        <tfoot><tr>
          <td colSpan={2}>Filtered total</td>
          <td className="num">{money(rows.reduce((sum, row) => sum + row.salesCents, 0))}</td>
          <td className="num">{money(rows.reduce((sum, row) => sum + row.profitCents, 0))}</td>
          <td className="num">{rows.length ? `${(rows.reduce((sum, row) => sum + row.inStockRate, 0) / rows.length).toFixed(1)}%` : '—'}</td>
          <td className="num">{number(rows.reduce((sum, row) => sum + row.onHand, 0))}</td>
          <td colSpan={2} />
        </tr></tfoot>
      </table></div> : <EmptyState title="No items match" icon="search">Clear the search or change the filters.</EmptyState>}
      <div className="table-footer"><SourceNote /></div>
    </section>
  </>;
}

