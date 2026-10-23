import type { IconName } from '@/components/Icon';

/**
 * One codebase ships as three Fabric Apps. `VITE_APP_VARIANT` selects which
 * standalone app a build contains; without it (local dev and the design
 * preview) every app is included so they can be developed side by side.
 */
export type AppId = 'merchandising' | 'promotions' | 'approvals';
export type AppVariant = AppId | 'suite';

export interface AppInfo {
  id: AppId;
  route: string;
  label: string;
  title: string;
  icon: IconName;
}

export const APPS: Record<AppId, AppInfo> = {
  merchandising: {
    id: 'merchandising', route: '/merchandising', label: 'Merchandising', title: 'Merchandising Portal',
    icon: 'chart',
  },
  promotions: {
    id: 'promotions', route: '/promotions', label: 'Promotion studio', title: 'Promotion Studio',
    icon: 'tag',
  },
  approvals: {
    id: 'approvals', route: '/approvals', label: 'Replenishment approvals', title: 'Replenishment Approvals',
    icon: 'inbox',
  },
};

export function appVariant(value: string | undefined = import.meta.env.VITE_APP_VARIANT): AppVariant {
  return value && Object.hasOwn(APPS, value) ? (value as AppId) : 'suite';
}

export function includedApps(variant: AppVariant = appVariant()): AppInfo[] {
  return variant === 'suite' ? Object.values(APPS) : [APPS[variant]];
}

export function appTitle(variant: AppVariant = appVariant()): string {
  return variant === 'suite' ? 'Retail Operations' : APPS[variant].title;
}
