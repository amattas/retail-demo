import { createRoot } from 'react-dom/client';

import App from '@/App';
import { appTitle } from '@/config/apps';
import { AuthProvider } from '@/hooks/AuthContext';
import { bootstrapAuth } from '@/services/bootstrap';

import './main.css';

document.title = appTitle();
const authService = await bootstrapAuth();

createRoot(document.getElementById('root')!).render(
  <AuthProvider authService={authService}>
    <App />
  </AuthProvider>
);
