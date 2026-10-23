import { render, screen, within } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { MemoryRouter } from 'react-router-dom';
import { beforeEach, describe, expect, it, vi } from 'vitest';

vi.mock('@/services/rayfinClient', () => ({
  isLocalBackend: () => true,
  getRayfinClient: vi.fn(),
}));

import { AuthProvider } from '@/hooks/AuthContext';
import { ApprovalsPage } from '@/pages/ApprovalsPage';
import { MerchandisingPage } from '@/pages/MerchandisingPage';
import { listProposals, resetLocalStore } from '@/services/approvals';
import type { IAuthService } from '@/services/IAuthService';
import { resetWorkspaceStore } from '@/services/workspaces';

const planner = { id: 'planner-sub', email: 'planner@contoso.com', name: 'Planner' };
const auth: IAuthService = {
  fabricAuthEnabled: false,
  signIn: async () => planner,
  signOut: async () => {},
  getCurrentUser: async () => planner,
  initEmbeddedAuth: async () => null,
};

function renderPage(page: React.ReactNode) {
  return render(<AuthProvider authService={auth}><MemoryRouter>{page}</MemoryRouter></AuthProvider>);
}

describe('standalone pages', () => {
  beforeEach(() => { resetLocalStore(); resetWorkspaceStore(); });

  it('filters merchandising items to those needing attention', async () => {
    const user = userEvent.setup();
    renderPage(<MerchandisingPage />);

    const toggle = await screen.findByRole('button', { name: /Needs attention \((\d+)\)/ });
    const expected = Number(toggle.textContent!.match(/\((\d+)\)/)![1]);
    await user.click(toggle);
    const body = screen.getAllByRole('rowgroup')[1];
    expect(within(body).getAllByRole('row')).toHaveLength(expected);
    expect(within(body).queryByText('Healthy')).toBeNull();
  });

  it('submits a proposal that only another reviewer can decide', async () => {
    const user = userEvent.setup();
    renderPage(<ApprovalsPage />);

    await user.click(await screen.findByRole('button', { name: 'New proposal' }));
    const form = screen.getByRole('form', { name: 'New proposal' });
    await user.type(within(form).getByRole('spinbutton', { name: 'Quantity' }), '48');
    await user.type(within(form).getByRole('textbox', { name: 'Rationale' }), 'Weekend cover is below target.');
    await user.click(within(form).getByRole('button', { name: 'Submit for review' }));

    expect(await screen.findByText(/Awaiting another reviewer/)).toBeInTheDocument();
    const [proposal] = await listProposals();
    expect(proposal).toMatchObject({ created_by: planner.id, quantity: 48, status: 'PENDING' });
  });
});
