import { describe, expect, it } from 'vitest';

import { appTitle, appVariant, brandLogoUrl, includedApps } from '@/config/apps';

describe('app variants', () => {
  it('builds one standalone app per variant and falls back to the full suite', () => {
    expect(appVariant('promotions')).toBe('promotions');
    expect(appVariant('approvals')).toBe('suite');
    expect(appVariant('constructor')).toBe('suite');
    expect(appVariant(undefined)).toBe('suite');
    expect(includedApps('merchandising').map((app) => app.id)).toEqual(['merchandising', 'approvals']);
    expect(includedApps('promotions').map((app) => app.id)).toEqual(['promotions']);
    expect(includedApps('suite')).toHaveLength(3);
    expect(appTitle('merchandising', undefined)).toBe('Merchandising & Replenishment');
    expect(appTitle('suite', undefined)).toBe('Retail Operations');
  });

  it('prefixes titles with a custom business brand name', () => {
    expect(appTitle('merchandising', 'Kroger')).toBe('Kroger Merchandising & Replenishment');
    expect(appTitle('suite', '  ')).toBe('Retail Operations');
  });

  it('shows a staged brand logo only when deployment added one', () => {
    expect(brandLogoUrl()).toBeUndefined();
    expect(brandLogoUrl({ '../assets/brand-logo.svg': '/assets/brand-logo-abc.svg' })).toBe(
      '/assets/brand-logo-abc.svg',
    );
  });
});
