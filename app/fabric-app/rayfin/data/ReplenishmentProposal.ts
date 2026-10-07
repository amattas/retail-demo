import {
  authenticated,
  date,
  entity,
  int,
  text,
  uuid,
} from '@microsoft/rayfin-core';

/**
 * A draft replenishment proposal. Proposals are never executed; they record
 * the synthetic evidence a planner reviewed. There is no update or delete
 * grant, so the submitted evidence cannot be edited after the fact.
 */
@entity()
@authenticated('read')
@authenticated('create', {
  policy: (claims, item) => claims.sub.eq(item.created_by),
})
export class ReplenishmentProposal {
  @uuid() id!: string;
  @int() store_id!: number;
  @int() product_id!: number;
  @int() quantity!: number;
  @text({ min: 1, max: 1000 }) rationale!: string;
  @text({ max: 40 }) evidence_mode!: string;
  @text({ max: 256 }) created_by!: string;
  @text({ max: 256, optional: true }) created_by_email?: string;
  @date() created_at!: Date;
}
