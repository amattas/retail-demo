import type { MerchandisingView } from '../../rayfin/data/MerchandisingView';
import type { PromotionScenario } from '../../rayfin/data/PromotionScenario';
import { DEFAULT_FILTERS, PRODUCTS, STORES, validateFilters } from '@/data/merchandising';
import type { Reviewer } from './approvals';
import { campaignDays, validateScenarioNumbers } from './planning';
import { getRayfinClient, isLocalBackend } from './rayfinClient';
import { isDesignPreview } from './runtimeMode';

export type SavedViewInput = Omit<MerchandisingView, 'id' | 'owner_id' | 'created_at' | 'source_mode'>;
export type PromotionInput = Omit<PromotionScenario, 'id' | 'owner_id' | 'created_at' | 'source_mode'>;
export type { MerchandisingView, PromotionScenario };

let views: MerchandisingView[] = [];
let scenarios: PromotionScenario[] = [];

function requireActor(actor: Reviewer) {
  if (!actor.id.trim()) throw new Error('Sign in before opening your saved workspace.');
}

function asDate(value: Date | string): Date {
  if (!(value instanceof Date) && typeof value !== 'string') throw new Error('Saved record has an invalid date.');
  const date = value instanceof Date ? value : new Date(value);
  if (!Number.isFinite(date.getTime())) throw new Error('Saved record has an invalid date.');
  return date;
}

function ownRecord<T extends { owner_id: string; created_at: Date; source_mode: string }>(row: T, actor: Reviewer): T {
  if (row.owner_id !== actor.id || row.source_mode !== 'sample-catalog') {
    throw new Error('The saved record does not belong to this sample workspace.');
  }
  return { ...row, created_at: asDate(row.created_at) };
}

export function validateSavedView(input: SavedViewInput): string[] {
  const errors = validateFilters({
    store: input.store_filter, category: input.category_filter, supplier: input.supplier_filter,
    search: input.search_filter, days: input.period_days,
  });
  if (!input.name.trim() || input.name.trim().length > 80) errors.push('Name the view using 1 to 80 characters.');
  if (!['availability', 'sales', 'margin'].includes(input.trend_metric)) errors.push('Unknown trend metric.');
  if (!['day', 'week', 'fourWeeks'].includes(input.granularity)) errors.push('Unknown trend grouping.');
  return errors;
}

export async function listSavedViews(actor: Reviewer): Promise<MerchandisingView[]> {
  requireActor(actor);
  const rows = isLocalBackend()
    ? views.filter((row) => row.owner_id === actor.id)
    : await getRayfinClient().data.MerchandisingView
      .where({ owner_id: { eq: actor.id } }).orderBy({ created_at: 'desc' }).first(50).execute();
  return rows.map((row) => ownRecord(row, actor))
    .sort((a, b) => b.created_at.getTime() - a.created_at.getTime()).slice(0, 50);
}

export async function saveView(input: SavedViewInput, actor: Reviewer): Promise<MerchandisingView> {
  requireActor(actor);
  const errors = validateSavedView(input);
  if (errors.length) throw new Error(errors.join(' '));
  const row = {
    ...input, name: input.name.trim(), owner_id: actor.id,
    source_mode: 'sample-catalog' as const, created_at: new Date(),
  };
  if (isLocalBackend()) {
    const saved = { ...row, id: crypto.randomUUID() };
    views.push(saved);
    return saved;
  }
  return ownRecord(await getRayfinClient().data.MerchandisingView.create(row), actor);
}

export async function deleteView(id: string, actor: Reviewer): Promise<void> {
  requireActor(actor);
  const row = isLocalBackend()
    ? views.find((item) => item.id === id && item.owner_id === actor.id)
    : await getRayfinClient().data.MerchandisingView.findById(id);
  if (!row) throw new Error('Saved view not found.');
  ownRecord(row, actor);
  if (isLocalBackend()) views = views.filter((item) => item.id !== id);
  else await getRayfinClient().data.MerchandisingView.delete({ id });
}

export function validatePromotion(input: PromotionInput): string[] {
  const errors = validateScenarioNumbers(input);
  if (!input.name.trim() || input.name.trim().length > 80) errors.push('Name the scenario using 1 to 80 characters.');
  if (!PRODUCTS.some((item) => item.id === input.product_id && item.name === input.product_name)) {
    errors.push('Choose a product from the sample catalog.');
  }
  if (!STORES.some((store) => store.id === input.store_id && store.name === input.store_name)) {
    errors.push('Choose a store from the sample catalog.');
  }
  try {
    campaignDays(input.start_date, input.end_date);
  } catch (error) {
    errors.push(error instanceof Error ? error.message : 'Invalid campaign dates.');
  }
  return errors;
}

function normalizeScenario(row: PromotionScenario, actor: Reviewer): PromotionScenario {
  return { ...ownRecord(row, actor), start_date: asDate(row.start_date), end_date: asDate(row.end_date) };
}

export async function listScenarios(actor: Reviewer): Promise<PromotionScenario[]> {
  requireActor(actor);
  const rows = isLocalBackend()
    ? scenarios.filter((row) => row.owner_id === actor.id)
    : await getRayfinClient().data.PromotionScenario
      .where({ owner_id: { eq: actor.id } }).orderBy({ created_at: 'desc' }).first(50).execute();
  return rows.map((row) => normalizeScenario(row, actor))
    .sort((a, b) => b.created_at.getTime() - a.created_at.getTime()).slice(0, 50);
}

export async function saveScenario(input: PromotionInput, actor: Reviewer): Promise<PromotionScenario> {
  requireActor(actor);
  const errors = validatePromotion(input);
  if (errors.length) throw new Error(errors.join(' '));
  const row = {
    ...input, name: input.name.trim(), owner_id: actor.id,
    source_mode: 'sample-catalog' as const, created_at: new Date(),
  };
  if (isLocalBackend()) {
    const saved = { ...row, id: crypto.randomUUID() };
    scenarios.push(saved);
    return saved;
  }
  return normalizeScenario(await getRayfinClient().data.PromotionScenario.create(row), actor);
}

export async function deleteScenario(id: string, actor: Reviewer): Promise<void> {
  requireActor(actor);
  const row = isLocalBackend()
    ? scenarios.find((item) => item.id === id && item.owner_id === actor.id)
    : await getRayfinClient().data.PromotionScenario.findById(id);
  if (!row) throw new Error('Saved scenario not found.');
  ownRecord(row, actor);
  if (isLocalBackend()) scenarios = scenarios.filter((item) => item.id !== id);
  else await getRayfinClient().data.PromotionScenario.delete({ id });
}

export function resetWorkspaceStore(): void {
  views = [];
  scenarios = [];
}

export function seedPreviewWorkspaces(ownerId: string): void {
  if (!isDesignPreview()) throw new Error('Sample workspaces are available only in the development preview.');
  if (views.length || scenarios.length) return;
  const created_at = new Date();
  views = [{
    id: crypto.randomUUID(), name: 'Beverage availability', owner_id: ownerId,
    store_filter: DEFAULT_FILTERS.store, category_filter: 'Beverages', supplier_filter: 'all',
    search_filter: '', period_days: 28, trend_metric: 'availability', granularity: 'week',
    source_mode: 'sample-catalog', created_at,
  }];
  const start_date = new Date();
  start_date.setUTCHours(0, 0, 0, 0);
  start_date.setUTCDate(start_date.getUTCDate() + 1);
  const end_date = new Date(start_date.getTime() + 13 * 86_400_000);
  scenarios = [
    { name: 'Weekend oat drink event', discount_percent: 10, uplift_percent: 25 },
    { name: 'Deeper discount comparison', discount_percent: 20, uplift_percent: 35 },
  ].map((item) => ({
    ...item, id: crypto.randomUUID(), owner_id: ownerId, product_id: PRODUCTS[0].id,
    product_name: PRODUCTS[0].name, store_id: STORES[0].id, store_name: STORES[0].name,
    baseline_units: 240, unit_price_cents: PRODUCTS[0].priceCents, unit_cost_cents: PRODUCTS[0].costCents,
    start_date, end_date, created_at, source_mode: 'sample-catalog',
  }));
}
