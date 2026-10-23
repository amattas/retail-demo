import { describe, expect, it } from 'vitest';

import {
  DEFAULT_FILTERS, exportMerchandisingCsv, groupTrend, merchandisingView,
  validateFilters, type MerchRow,
} from '@/data/merchandising';
import { campaignDays, scenarioImpact } from '@/services/planning';

const AS_OF = '2026-10-04';

describe('merchandising view', () => {
  it('rejects filters outside the sample catalog', () => {
    expect(validateFilters({ ...DEFAULT_FILTERS, store: '99', category: 'Toys', days: 30 })).toHaveLength(3);
    expect(() => merchandisingView({ ...DEFAULT_FILTERS, supplier: 'Nobody' }, AS_OF)).toThrow(/Unknown supplier/);
  });

  it('is deterministic, filters items and keeps the summary consistent with rows and trend', () => {
    const filters = { ...DEFAULT_FILTERS, category: 'Fresh', store: '2' };
    const view = merchandisingView(filters, AS_OF);
    expect(view).toEqual(merchandisingView(filters, AS_OF));
    expect(view.rows.map((row) => row.category)).toEqual(Array(4).fill('Fresh'));
    expect(view.points).toHaveLength(28);
    expect(view.points.at(-1)!.date).toBe(AS_OF);
    expect(view.summary.salesCents).toBe(view.rows.reduce((sum, row) => sum + row.salesCents, 0));
    expect(view.summary.alerts).toBe(view.rows.filter((row) => row.status !== 'healthy').length);
  });

  it('groups the trend without losing totals', () => {
    const { points } = merchandisingView({ ...DEFAULT_FILTERS, days: 56 }, AS_OF);
    const weeks = groupTrend(points, 'week');
    expect(weeks).toHaveLength(8);
    const sum = (list: typeof points) => list.reduce((total, p) => total + p.current.salesCents, 0);
    expect(sum(weeks)).toBe(sum(points));
  });

  it('neutralises spreadsheet formulas in exported text', () => {
    const row = { ...merchandisingView(DEFAULT_FILTERS, AS_OF).rows[0], name: '=HYPERLINK("x")' } as MerchRow;
    const [, line] = exportMerchandisingCsv([row]).split('\r\n');
    expect(line).toContain(`"'=HYPERLINK(""x"")"`);
  });

});

describe('promotion planning', () => {
  const input = { baseline_units: 100, unit_price_cents: 1000, unit_cost_cents: 600, discount_percent: 20, uplift_percent: 50 };

  it('computes promoted economics and the break-even uplift', () => {
    const impact = scenarioImpact(input);
    expect(impact).toMatchObject({
      promoPriceCents: 800, estimatedUnits: 150, revenueCents: 120_000, profitCents: 30_000,
      baselineProfitCents: 40_000, profitChangeCents: -10_000, breakEvenUplift: 100,
    });
    expect(scenarioImpact({ ...input, uplift_percent: 100 }).profitChangeCents).toBe(0);
  });

  it('reports no break-even when the promoted price is at or below cost', () => {
    expect(scenarioImpact({ ...input, discount_percent: 40 }).breakEvenUplift).toBeNull();
    expect(() => scenarioImpact({ ...input, discount_percent: 71 })).toThrow(/Discount/);
  });

  it('bounds campaign length', () => {
    const day = (value: string) => new Date(`${value}T00:00:00Z`);
    expect(campaignDays(day('2026-10-01'), day('2026-10-14'))).toBe(14);
    expect(() => campaignDays(day('2026-10-14'), day('2026-10-01'))).toThrow();
    expect(() => campaignDays(day('2026-10-01'), day('2026-12-24'))).toThrow();
  });
});
