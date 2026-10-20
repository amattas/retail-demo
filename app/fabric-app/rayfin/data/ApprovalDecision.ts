import {
  authenticated,
  date,
  entity,
  set,
  text,
  uuid,
} from '@microsoft/rayfin-core';

/**
 * An append-only reviewer decision on a proposal. The row-level policy binds
 * the reviewer to the signed-in identity, and no update or delete grant exists,
 * so the database row is the durable audit record.
 */
@entity()
@authenticated('read')
@authenticated('create', {
  policy: (claims, item) => claims.sub.eq(item.reviewer_id),
})
export class ApprovalDecision {
  @uuid() id!: string;
  @uuid() proposal_id!: string;
  @set('APPROVED', 'REJECTED') decision!: 'APPROVED' | 'REJECTED';
  @text({ max: 1000, optional: true }) comment?: string;
  @text({ max: 256 }) reviewer_id!: string;
  @text({ max: 256, optional: true }) reviewer_email?: string;
  @date() decided_at!: Date;
}
