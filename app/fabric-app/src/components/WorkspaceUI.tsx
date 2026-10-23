import type { ReactNode } from 'react';
import { Icon, type IconName } from './Icon';

export function PageHeading({ eyebrow, title, description, actions }: {
  eyebrow: string; title: string; description: string; actions?: ReactNode;
}) {
  return <header className="page-heading">
    <div><p className="eyebrow">{eyebrow}</p><h1>{title}</h1><p className="page-description">{description}</p></div>
    {actions && <div className="page-actions">{actions}</div>}
  </header>;
}

export function MetricCard({ label, value, detail, icon, tone = 'neutral' }: {
  label: string; value: string; detail: string; icon: IconName; tone?: 'neutral' | 'good' | 'warning';
}) {
  return <section className={`metric-card ${tone}`}>
    <div className="metric-top"><span>{label}</span><span className="metric-icon"><Icon name={icon} /></span></div>
    <strong className="metric-value">{value}</strong><span className="metric-detail">{detail}</span>
  </section>;
}

export function Notice({ children, danger = false }: { children: ReactNode; danger?: boolean }) {
  return <div className={`notice ${danger ? 'notice-danger' : ''}`} role={danger ? 'alert' : 'status'}>
    <Icon name={danger ? 'shield' : 'check'} /><div>{children}</div>
  </div>;
}

export function EmptyState({ title, children, icon = 'box' }: {
  title: string; children: ReactNode; icon?: IconName;
}) {
  return <div className="empty-state"><span className="empty-icon"><Icon name={icon} width="28" height="28" /></span>
    <h3>{title}</h3><div>{children}</div></div>;
}

export function SourceNote() {
  return <p className="source-note"><Icon name="shield" width="15" height="15" />
    Synthetic sample catalog, not live inventory. Saved views and scenarios use your Rayfin workspace.
    No prices, orders, or operational systems are changed.
  </p>;
}
