import { authenticated, date, entity, int, set, text, uuid } from '@microsoft/rayfin-core';

@entity()
@authenticated('read', { policy: (claims, item) => claims.sub.eq(item.owner_id) })
@authenticated('create', { policy: (claims, item) => claims.sub.eq(item.owner_id) })
@authenticated('delete', { policy: (claims, item) => claims.sub.eq(item.owner_id) })
export class MerchandisingView {
  @uuid() id!: string;
  @text({ min: 1, max: 80 }) name!: string;
  @text({ max: 40 }) store_filter!: string;
  @text({ max: 80 }) category_filter!: string;
  @text({ max: 80 }) supplier_filter!: string;
  @text({ max: 120 }) search_filter!: string;
  @int() period_days!: number;
  @set('availability', 'sales', 'margin') trend_metric!: 'availability' | 'sales' | 'margin';
  @set('day', 'week', 'fourWeeks') granularity!: 'day' | 'week' | 'fourWeeks';
  @set('sample-catalog') source_mode!: 'sample-catalog';
  @text({ min: 1, max: 256 }) owner_id!: string;
  @date() created_at!: Date;
}
