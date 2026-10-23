import { describe, expect, it } from 'vitest';

import { appTitle, appVariant, includedApps } from '@/config/apps';

describe('app variants', () => {
  it('builds one standalone app per variant and falls back to the full suite', () => {
    expect(appVariant('promotions')).toBe('promotions');
    expect(appVariant('constructor')).toBe('suite');
    expect(appVariant(undefined)).toBe('suite');
    expect(includedApps('merchandising').map((app) => app.id)).toEqual(['merchandising']);
    expect(includedApps('suite')).toHaveLength(3);
    expect(appTitle('approvals')).toBe('Replenishment Approvals');
    expect(appTitle('suite')).toBe('Retail Operations');
  });
});
