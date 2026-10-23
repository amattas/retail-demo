import { beforeEach, describe, expect, it, vi } from 'vitest';

vi.mock('@/services/rayfinClient', () => ({
  isLocalBackend: () => true,
  getRayfinClient: vi.fn(),
}));

import { PRODUCTS, STORES } from '@/data/merchandising';
import {
  deleteScenario, deleteView, listSavedViews, listScenarios, resetWorkspaceStore, saveScenario, saveView,
} from '@/services/workspaces';

const alice = { id: 'alice-sub' };
const bob = { id: 'bob-sub' };
const view = {
  name: ' Beverages ', store_filter: 'all', category_filter: 'Beverages', supplier_filter: 'all',
  search_filter: '', period_days: 28, trend_metric: 'availability' as const, granularity: 'week' as const,
};

describe('saved merchandising workspaces (in-memory mode)', () => {
  beforeEach(() => resetWorkspaceStore());

  it('keeps saved views private to their owner', async () => {
    const saved = await saveView(view, alice);
    expect(saved).toMatchObject({ name: 'Beverages', owner_id: alice.id, source_mode: 'sample-catalog' });
    expect(await listSavedViews(bob)).toEqual([]);
    await expect(deleteView(saved.id, bob)).rejects.toThrow(/not found/);
    await deleteView(saved.id, alice);
    expect(await listSavedViews(alice)).toEqual([]);
  });

  it('rejects invalid views and anonymous callers', async () => {
    await expect(saveView({ ...view, category_filter: 'Toys', period_days: 30 }, alice)).rejects.toThrow(/Unknown category/);
    await expect(listSavedViews({ id: ' ' })).rejects.toThrow(/Sign in/);
  });

  it('saves only catalog-consistent promotion scenarios', async () => {
    const scenario = {
      name: 'Oat drink event', product_id: PRODUCTS[0].id, product_name: PRODUCTS[0].name,
      store_id: STORES[0].id, store_name: STORES[0].name, baseline_units: 240,
      unit_price_cents: PRODUCTS[0].priceCents, unit_cost_cents: PRODUCTS[0].costCents,
      discount_percent: 15, uplift_percent: 20,
      start_date: new Date('2026-11-01T00:00:00Z'), end_date: new Date('2026-11-14T00:00:00Z'),
    };
    await expect(saveScenario({ ...scenario, product_name: 'Renamed' }, alice)).rejects.toThrow(/sample catalog/);
    const saved = await saveScenario(scenario, alice);
    expect(await listScenarios(bob)).toEqual([]);
    expect((await listScenarios(alice)).map((row) => row.id)).toEqual([saved.id]);
    await deleteScenario(saved.id, alice);
    expect(await listScenarios(alice)).toEqual([]);
  });
});
