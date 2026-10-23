import { ConfigGenerator, SchemaAnalyzer } from '@microsoft/rayfin-core/analysis';
import { describe, expect, it } from 'vitest';

import { schema } from '../../rayfin/data/schema';

describe('generated Data API Builder permissions', () => {
  const config = new ConfigGenerator('mssql').generateConfig(
    new SchemaAnalyzer(schema as never, 'mssql').analyzeEntities()
  );

  it.each([
    ['ReplenishmentProposal', 'created_by'],
    ['ApprovalDecision', 'reviewer_id'],
  ])('%s is append-only and binds creates to the signed-in caller', (name, ownerField) => {
    expect(config.entities[name]?.permissions).toEqual([
      {
        role: 'authenticated',
        actions: [
          { action: 'create', policy: { database: `@claims.sub eq @item.${ownerField}` } },
          'read',
        ],
      },
    ]);
  });

  it.each(['MerchandisingView', 'PromotionScenario'])(
    '%s is private: every grant is bound to the owner',
    (name) => {
      const owner = { database: '@claims.sub eq @item.owner_id' };
      const [grant] = config.entities[name]?.permissions ?? [];
      expect(grant?.role).toBe('authenticated');
      expect(grant?.actions).toEqual(expect.arrayContaining(
        ['create', 'read', 'delete'].map((action) => ({ action, policy: owner }))
      ));
      expect(grant?.actions).toHaveLength(3);
    }
  );
});
