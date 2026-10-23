import type { ReactNode, SVGProps } from 'react';

const paths = {
  grid: <><rect x="3" y="3" width="7" height="7" rx="1.5" /><rect x="14" y="3" width="7" height="7" rx="1.5" /><rect x="3" y="14" width="7" height="7" rx="1.5" /><rect x="14" y="14" width="7" height="7" rx="1.5" /></>,
  box: <><path d="m3 7 9-4 9 4v10l-9 4-9-4Z" /><path d="m3 7 9 4 9-4M12 11v10M7 5l10 5" /></>,
  check: <path d="m5 12 4 4L19 6" />,
  arrow: <path d="M4 12h15m-6-6 6 6-6 6" />,
  chart: <><path d="M4 3v17h17M8 15l4-5 4 2 5-7" /></>,
  sun: <><circle cx="12" cy="12" r="4" /><path d="M12 1v2m0 18v2M1 12h2m18 0h2M4 4l2 2m12 12 2 2M4 20l2-2M18 6l2-2" /></>,
  moon: <path d="M20 15A9 9 0 0 1 9 4a9 9 0 1 0 11 11Z" />,
  download: <><path d="M12 3v12m-5-5 5 5 5-5M4 16v5h16v-5" /></>,
  search: <><circle cx="10.5" cy="10.5" r="6.5" /><path d="m16 16 5 5" /></>,
  close: <path d="m6 6 12 12M6 18 18 6" />,
  plus: <path d="M12 4v16M4 12h16" />,
  refresh: <><path d="M20 11a8 8 0 0 0-14-5L3 9m0-6v6h6M4 13a8 8 0 0 0 14 5l3-3m0 6v-6h-6" /></>,
  shield: <><path d="m12 2 8 3v7c0 5-8 10-8 10S4 17 4 12V5Z" /><path d="m8 12 3 3 5-6" /></>,
  tag: <><path d="M3 3h8l10 10-8 8L3 11Z" /><circle cx="7.5" cy="7.5" r="1" /></>,
  trash: <><path d="M3 6h18M9 6V3h6v3M5 6l1 15h12l1-15M10 10v7m4-7v7" /></>,
  clock: <><circle cx="12" cy="12" r="9" /><path d="M12 6v6l4 2" /></>,
  store: <><path d="M3 10h18l-2-7H5Zm2 0v11h14V10M9 21v-7h6v7" /></>,
  filter: <><path d="M3 5h18M6 12h12M10 19h4" /><circle cx="8" cy="5" r="2" /><circle cx="16" cy="12" r="2" /></>,
  inbox: <><path d="M3 13h5l2 3h4l2-3h5" /><path d="M5 5h14l2 8v6H3v-6Z" /></>,
  logout: <><path d="M9 21H5V3h4M16 17l5-5-5-5M21 12H9" /></>,
  bookmark: <path d="M6 3h12v18l-6-4-6 4Z" />,
  alert: <><path d="M12 3 2 20h20Z" /><path d="M12 10v4m0 3v.01" /></>,
} satisfies Record<string, ReactNode>;

export type IconName = keyof typeof paths;

export function Icon({ name, ...props }: { name: IconName } & SVGProps<SVGSVGElement>) {
  return <svg width="20" height="20" viewBox="0 0 24 24" fill="none"
    stroke="var(--cp-icon, var(--cp-text))" strokeWidth="1.7" strokeLinecap="round"
    strokeLinejoin="round" aria-hidden="true" {...props}>{paths[name]}</svg>;
}
