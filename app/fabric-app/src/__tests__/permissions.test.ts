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
});
