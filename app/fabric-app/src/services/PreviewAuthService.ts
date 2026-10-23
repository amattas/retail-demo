import type { AuthUser, IAuthService } from './IAuthService';

export const PREVIEW_USER: AuthUser = {
  id: 'preview-reviewer', name: 'Demo reviewer', email: 'reviewer@example.test',
};

/** Only selected by the explicitly requested Vite development preview mode. */
export class PreviewAuthService implements IAuthService {
  readonly fabricAuthEnabled = false;
  private user: AuthUser | null = PREVIEW_USER;

  async signIn() { this.user = PREVIEW_USER; return PREVIEW_USER; }
  async signOut() { this.user = null; }
  async getCurrentUser() { return this.user; }
  async initEmbeddedAuth() { return null; }
}
