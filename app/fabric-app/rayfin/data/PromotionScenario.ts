import { authenticated, date, entity, int, set, text, uuid } from '@microsoft/rayfin-core';

@entity()
@authenticated('read', { policy: (claims, item) => claims.sub.eq(item.owner_id) })
@authenticated('create', { policy: (claims, item) => claims.sub.eq(item.owner_id) })
@authenticated('delete', { policy: (claims, item) => claims.sub.eq(item.owner_id) })
export class PromotionScenario {
  @uuid() id!: string;
  @text({ min: 1, max: 80 }) name!: string;
  @int() product_id!: number;
  @text({ min: 1, max: 120 }) product_name!: string;
  @int() store_id!: number;
  @text({ min: 1, max: 100 }) store_name!: string;
  @int() baseline_units!: number;
  @int() unit_price_cents!: number;
  @int() unit_cost_cents!: number;
  @int() discount_percent!: number;
  @int() uplift_percent!: number;
  @date() start_date!: Date;
  @date() end_date!: Date;
  @set('sample-catalog') source_mode!: 'sample-catalog';
  @text({ min: 1, max: 256 }) owner_id!: string;
  @date() created_at!: Date;
}
