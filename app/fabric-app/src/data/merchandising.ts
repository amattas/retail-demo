export interface Product {
  id: number;
  sku: string;
  name: string;
  brand: string;
  category: string;
  supplier: string;
  priceCents: number;
  costCents: number;
  dailyUnits: number;
  availability: number;
}

export const STORES = [
  { id: 1, name: 'Downtown Market', code: 'DT-01' },
  { id: 2, name: 'Riverside Market', code: 'RV-02' },
  { id: 3, name: 'Northside Market', code: 'NS-03' },
];

function product(
  id: number, name: string, brand: string, category: string, supplier: string,
  priceCents: number, costCents: number, dailyUnits: number, availability: number
): Product {
  return { id, sku: `SKU-${id}`, name, brand, category, supplier, priceCents, costCents, dailyUnits, availability };
}

// A deliberately synthetic catalog, not a live Lakehouse or supplier feed.
export const PRODUCTS: Product[] = [
  product(101, 'Barista oat drink', 'Meadow Farms', 'Beverages', 'Meadow Foods', 429, 238, 18, 97),
  product(102, 'Cold brew coffee', 'Northline', 'Beverages', 'Northline Supply', 649, 344, 14, 96),
  product(103, 'Lemon sparkling water', 'Pure Select', 'Beverages', 'Meadow Foods', 599, 328, 22, 88),
  product(104, 'Pressed orange juice', 'Harvest Crown', 'Beverages', 'Harvest Collective', 749, 461, 11, 98),
  product(105, 'Honey granola', 'Bluebird Farms', 'Pantry', 'Meadow Foods', 679, 368, 13, 96),
  product(106, 'Organic rolled oats', 'Bluebird Farms', 'Pantry', 'Meadow Foods', 449, 217, 16, 99),
  product(107, 'Dark chocolate bites', 'Cocoa House', 'Pantry', 'Northline Supply', 549, 292, 19, 86),
  product(108, 'Almond butter', 'Harvest Crown', 'Pantry', 'Harvest Collective', 899, 534, 10, 97),
  product(109, 'Organic bananas', 'Harvest Crown', 'Fresh', 'Harvest Collective', 249, 141, 31, 94),
  product(110, 'Garden salad mix', 'Green Table', 'Fresh', 'Harvest Collective', 479, 287, 17, 95),
  product(111, 'Greek yogurt', 'Meadow Farms', 'Fresh', 'Meadow Foods', 399, 214, 24, 98),
  product(112, 'Free-range eggs', 'Meadow Farms', 'Fresh', 'Meadow Foods', 699, 418, 21, 89),
  product(113, 'Everyday hand soap', 'Homekind', 'Home care', 'Northline Supply', 449, 198, 9, 99),
  product(114, 'Plant-based cleaner', 'Homekind', 'Home care', 'Northline Supply', 729, 349, 8, 87),
  product(115, 'Recycled kitchen towels', 'Homekind', 'Home care', 'Northline Supply', 899, 486, 12, 98),
  product(116, 'Cotton dish cloths', 'Pure Select', 'Home care', 'Harvest Collective', 599, 263, 7, 97),
];

export const CATEGORIES = [...new Set(PRODUCTS.map((item) => item.category))].sort();
export const SUPPLIERS = [...new Set(PRODUCTS.map((item) => item.supplier))].sort();
export const PERIODS = [7, 28, 56, 84];
export type TrendMetric = 'availability' | 'sales' | 'margin';
export type Granularity = 'day' | 'week' | 'fourWeeks';
export interface MerchFilters {
  store: string;
  category: string;
  supplier: string;
  search: string;
  days: number;
}
export const DEFAULT_FILTERS: MerchFilters = {
  store: 'all', category: 'all', supplier: 'all', search: '', days: 28,
};

const DAY = 86_400_000;
export const SAMPLE_AS_OF = new Date(Date.now() - DAY).toISOString().slice(0, 10);

export function validateFilters(filters: MerchFilters): string[] {
  const errors: string[] = [];
  if (filters.store !== 'all' && !STORES.some((store) => String(store.id) === filters.store)) {
    errors.push('Choose a store in the sample catalog.');
  }
  if (filters.category !== 'all' && !CATEGORIES.includes(filters.category)) errors.push('Unknown category.');
  if (filters.supplier !== 'all' && !SUPPLIERS.includes(filters.supplier)) errors.push('Unknown supplier.');
  if (!PERIODS.includes(filters.days)) errors.push('Choose a supported reporting period.');
  if (filters.search.length > 120) errors.push('Search must be at most 120 characters.');
  return errors;
}

interface Totals {
  units: number;
  salesCents: number;
  profitCents: number;
  available: number;
  observations: number;
}
const emptyTotals = (): Totals => ({ units: 0, salesCents: 0, profitCents: 0, available: 0, observations: 0 });
function add(target: Totals, value: Totals) {
  for (const key of ['units', 'salesCents', 'profitCents', 'available', 'observations'] as const) {
    target[key] += value[key];
  }
}
const rate = (value: Totals) => value.observations ? value.available / value.observations * 100 : null;

function observation(item: Product, storeId: number, ordinal: number): Totals {
  const seed = (item.id * 37 + storeId * 19 + ordinal * 13) % 100;
  const available = seed < item.availability;
  const units = available
    ? Math.max(1, Math.round(item.dailyUnits * (0.82 + storeId * 0.12) * (0.8 + seed / 200)))
    : 0;
  return {
    units,
    salesCents: units * item.priceCents,
    profitCents: units * (item.priceCents - item.costCents),
    available: Number(available),
    observations: 1,
  };
}

export interface MerchRow extends Product {
  units: number;
  salesCents: number;
  profitCents: number;
  inStockRate: number;
  onHand: number;
  daysCover: number | null;
  status: 'healthy' | 'low' | 'out';
}
export interface TrendPoint {
  date: string;
  previousDate: string;
  current: Totals;
  previous: Totals;
}

export function merchandisingView(filters: MerchFilters, asOf = SAMPLE_AS_OF) {
  const errors = validateFilters(filters);
  if (errors.length) throw new Error(errors.join(' '));
  const end = new Date(`${asOf}T00:00:00Z`).getTime();
  if (!Number.isFinite(end) || new Date(end).toISOString().slice(0, 10) !== asOf) {
    throw new Error('The sample snapshot date is invalid.');
  }
  const query = filters.search.trim().toLowerCase();
  const products = PRODUCTS.filter((item) =>
    (filters.category === 'all' || item.category === filters.category)
    && (filters.supplier === 'all' || item.supplier === filters.supplier)
    && `${item.name} ${item.sku} ${item.brand}`.toLowerCase().includes(query)
  );
  const stores = STORES.filter((store) => filters.store === 'all' || String(store.id) === filters.store);
  const points: TrendPoint[] = Array.from({ length: filters.days }, (_, index) => {
    const date = end - (filters.days - index - 1) * DAY;
    return {
      date: new Date(date).toISOString().slice(0, 10),
      previousDate: new Date(date - filters.days * DAY).toISOString().slice(0, 10),
      current: emptyTotals(), previous: emptyTotals(),
    };
  });
  const rows: MerchRow[] = products.map((item) => {
    const totals = emptyTotals();
    for (const point of points) {
      const ordinal = Math.floor(new Date(`${point.date}T00:00:00Z`).getTime() / DAY);
      for (const store of stores) {
        const current = observation(item, store.id, ordinal);
        const previous = observation(item, store.id, ordinal - filters.days);
        add(totals, current);
        add(point.current, current);
        add(point.previous, previous);
      }
    }
    const ordinal = Math.floor(end / DAY);
    const onHand = stores.reduce((sum, store) => {
      const current = observation(item, store.id, ordinal);
      if (!current.available) return sum;
      const cover = [103, 107, 112, 114].includes(item.id) ? 2 : 9 + (item.id + store.id) % 9;
      return sum + Math.round(current.units * cover);
    }, 0);
    const daysCover = totals.units ? onHand / (totals.units / filters.days) : null;
    return {
      ...item, units: totals.units, salesCents: totals.salesCents, profitCents: totals.profitCents,
      inStockRate: rate(totals) ?? 0, onHand, daysCover,
      status: onHand === 0 ? 'out' : daysCover !== null && daysCover < 7 ? 'low' : 'healthy',
    };
  });
  const current = emptyTotals();
  const previous = emptyTotals();
  for (const point of points) {
    add(current, point.current);
    add(previous, point.previous);
  }
  return {
    rows,
    points: products.length ? points : [],
    asOf,
    start: points[0].date,
    summary: {
      ...current, previousSalesCents: previous.salesCents, previousProfitCents: previous.profitCents,
      inStockRate: rate(current), previousInStockRate: rate(previous),
      alerts: rows.filter((row) => row.status !== 'healthy').length,
      onHand: rows.reduce((sum, row) => sum + row.onHand, 0),
    },
  };
}

export function groupTrend(points: TrendPoint[], granularity: Granularity): TrendPoint[] {
  const size = granularity === 'day' ? 1 : granularity === 'week' ? 7 : 28;
  const result: TrendPoint[] = [];
  for (let index = 0; index < points.length; index += size) {
    const point = points[index];
    const grouped = { ...point, current: emptyTotals(), previous: emptyTotals() };
    for (const value of points.slice(index, index + size)) {
      add(grouped.current, value.current);
      add(grouped.previous, value.previous);
    }
    result.push(grouped);
  }
  return result;
}

export function trendValue(point: TrendPoint, metric: TrendMetric, previous = false): number {
  const value = previous ? point.previous : point.current;
  return metric === 'availability' ? (rate(value) ?? 0) : metric === 'sales' ? value.salesCents / 100 : value.profitCents / 100;
}

export const money = (cents: number, compact = false) => new Intl.NumberFormat('en-US', {
  style: 'currency', currency: 'USD', maximumFractionDigits: compact ? 1 : 0,
  ...(compact ? { notation: 'compact' as const } : {}),
}).format(cents / 100);
export const number = (value: number) => new Intl.NumberFormat('en-US', { maximumFractionDigits: 0 }).format(value);
/** Unit prices keep cents; `money` rounds to whole dollars for totals. */
export const price = (cents: number) => new Intl.NumberFormat('en-US', { style: 'currency', currency: 'USD' }).format(cents / 100);
export const shortDate = (value: string) => new Intl.DateTimeFormat('en-US', {
  month: 'short', day: 'numeric', timeZone: 'UTC',
}).format(new Date(`${value.slice(0, 10)}T00:00:00Z`));

export function changeLabel(current: number, previous: number): string {
  if (!previous) return current ? 'No prior-period baseline' : 'No change';
  const change = (current - previous) / previous * 100;
  return `${change >= 0 ? '+' : ''}${change.toFixed(1)}% vs previous period`;
}

export function exportMerchandisingCsv(rows: MerchRow[]): string {
  const escape = (value: string | number) => {
    const text = String(value);
    const safe = /^[=+\-@\t\r]/.test(text) ? `'${text}` : text;
    return `"${safe.replaceAll('"', '""')}"`;
  };
  const data = rows.map((row) => [
    row.sku, row.name, row.category, row.supplier, (row.salesCents / 100).toFixed(2),
    (row.profitCents / 100).toFixed(2), row.inStockRate.toFixed(1), row.onHand,
    row.daysCover?.toFixed(1) ?? '',
  ]);
  return [
    ['Sample SKU', 'Product', 'Category', 'Supplier', 'Sales USD', 'Gross profit USD', 'In-stock %', 'On hand', 'Days cover'],
    ...data,
  ].map((row) => row.map(escape).join(',')).join('\r\n');
}
