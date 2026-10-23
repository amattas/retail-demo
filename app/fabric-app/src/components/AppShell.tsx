import { useEffect, useState } from 'react';
import { NavLink, Outlet } from 'react-router-dom';

import { appTitle, includedApps } from '@/config/apps';
import { useAuth } from '@/hooks/AuthContext';
import { Icon } from './Icon';

const THEME_KEY = 'retail-ops-theme';

function initialTheme(): 'light' | 'dark' {
  const saved = localStorage.getItem(THEME_KEY);
  if (saved === 'light' || saved === 'dark') return saved;
  return window.matchMedia?.('(prefers-color-scheme: dark)').matches ? 'dark' : 'light';
}

export function AppShell() {
  const { user, signOut } = useAuth();
  const [theme, setTheme] = useState(initialTheme);

  useEffect(() => {
    document.documentElement.classList.toggle('dark', theme === 'dark');
    localStorage.setItem(THEME_KEY, theme);
  }, [theme]);

  const displayName = user?.name || user?.email || 'Signed-in user';
  return (
    <div className="shell">
      <aside className="sidebar">
        <div className="brand">
          <span className="brand-mark"><Icon name="store" /></span>
          <div><strong>{appTitle()}</strong><span>Fabric App · sample data</span></div>
        </div>
        <nav className="nav" aria-label="Workspaces">
          <p className="nav-label">Workspace</p>
          {includedApps().map((item) => (
            <NavLink key={item.id} to={item.route}>
              <Icon name={item.icon} />{item.label}
            </NavLink>
          ))}
        </nav>
        <div className="sidebar-footer">
          <div className="user-chip">
            <span className="avatar" aria-hidden="true">{displayName.slice(0, 1).toUpperCase()}</span>
            <div><strong>{displayName}</strong><span>{user?.email}</span></div>
          </div>
          <div className="sidebar-actions">
            <button
              type="button" className="btn btn-sm"
              onClick={() => setTheme(theme === 'dark' ? 'light' : 'dark')}
              aria-label={`Switch to ${theme === 'dark' ? 'light' : 'dark'} theme`}
            >
              <Icon name={theme === 'dark' ? 'sun' : 'moon'} width="15" height="15" />
              {theme === 'dark' ? 'Light' : 'Dark'}
            </button>
            <button type="button" className="btn btn-sm btn-ghost" onClick={() => void signOut()}>
              <Icon name="logout" width="15" height="15" />Sign out
            </button>
          </div>
        </div>
      </aside>
      <main className="content"><div className="content-inner"><Outlet /></div></main>
    </div>
  );
}
