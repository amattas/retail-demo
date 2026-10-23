import { resolveRayfinFunctionsBaseUrl } from '@microsoft/rayfin-local-dev';

import type { IAuthService } from './IAuthService';
import { MockAuthService } from './MockAuthService';
import { RayfinAuthService } from './RayfinAuthService';
import { PREVIEW_USER, PreviewAuthService } from './PreviewAuthService';
import { seedPreviewProposals } from './approvals';
import { initRayfinClient, isLocalBackend, LOCAL_DEV_KEY } from './rayfinClient';
import { isDesignPreview } from './runtimeMode';
import { seedPreviewWorkspaces } from './workspaces';

/**
 * Initialize the Rayfin client from build-time VITE_* values overlaid by the
 * deployed runtime config, and return the right auth service for the backend.
 *
 * - Localhost API URL → {@link MockAuthService}
 * - Anything else     → {@link RayfinAuthService} (requires Fabric coordinates)
 */
export async function bootstrapAuth(): Promise<IAuthService> {
  if (isDesignPreview()) {
    seedPreviewProposals();
    seedPreviewWorkspaces(PREVIEW_USER.id);
    return new PreviewAuthService();
  }
  const apiUrl = import.meta.env.VITE_RAYFIN_API_URL || 'http://localhost:5168';

  const client = await initRayfinClient({
    baseUrl: apiUrl.endsWith('/') ? apiUrl : `${apiUrl}/`,
    publishableKey: import.meta.env.VITE_RAYFIN_PUBLISHABLE_KEY || LOCAL_DEV_KEY,
    functionsBaseUrl: resolveRayfinFunctionsBaseUrl(),
    runtimeConfig: {
      workspaceId: import.meta.env.VITE_FABRIC_WORKSPACE_ID,
      itemId: import.meta.env.VITE_FABRIC_ITEM_ID,
      portalUrl: import.meta.env.VITE_FABRIC_PORTAL_URL,
    },
  });

  if (isLocalBackend()) {
    return new MockAuthService(client);
  }

  const {
    workspaceId,
    itemId: projectId,
    portalUrl: fabricPortalUrl,
  } = client.runtimeConfig ?? {};

  if (!workspaceId || !projectId || !fabricPortalUrl) {
    throw new Error(
      'Missing required Fabric config. Set VITE_FABRIC_WORKSPACE_ID, VITE_FABRIC_ITEM_ID, and VITE_FABRIC_PORTAL_URL.'
    );
  }

  return new RayfinAuthService(client, {
    workspaceId,
    projectId,
    fabricPortalUrl,
    returnOrigin: window.location.origin,
  });
}
