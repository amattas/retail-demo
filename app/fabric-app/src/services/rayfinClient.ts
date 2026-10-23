import {
  RayfinClient,
  resolveRayfinConfig,
  type RayfinRuntimeConfig,
} from '@microsoft/rayfin-client';

import type { ApprovalsAppSchema } from '../../rayfin/data/schema';
import { isDesignPreview } from './runtimeMode';

export interface RayfinClientConfig {
  baseUrl: string;
  publishableKey: string;
  /** Absolute same-origin URL for local Functions calls when the Vite adapter is active. */
  functionsBaseUrl?: string;
  /** Fabric coordinates fallback for local dev, where no runtime config is emitted. */
  runtimeConfig?: RayfinRuntimeConfig;
}

let client: RayfinClient<ApprovalsAppSchema> | null = null;
let localDev = false;

/** Placeholder key for the bundled local backend, which does not check keys. */
export const LOCAL_DEV_KEY = 'local-dev-key';

export function isLocalBackendUrl(url: string): boolean {
  try {
    const { hostname } = new URL(url);
    return hostname === 'localhost' || hostname === '127.0.0.1';
  } catch {
    return false;
  }
}

/**
 * Initialize the client. `rayfin up` emits `rayfin.config.json` into each
 * deployed build, and that runtime config overrides the build-time values, so
 * one bundle serves whichever Fabric App item it is deployed to. Local mode is
 * decided from the resolved URL, not the build-time default.
 */
export async function initRayfinClient(
  config: RayfinClientConfig
): Promise<RayfinClient<ApprovalsAppSchema>> {
  if (client) {
    throw new Error('Rayfin client is already initialized.');
  }
  const resolved = await resolveRayfinConfig({
    apiUrl: config.baseUrl,
    publishableKey: config.publishableKey,
    ...config.runtimeConfig,
  });
  localDev = isLocalBackendUrl(resolved.baseUrl!);
  if (!localDev && (!resolved.publishableKey || resolved.publishableKey === LOCAL_DEV_KEY)) {
    throw new Error('A Rayfin publishable key is required for a deployed backend.');
  }
  client = new RayfinClient<ApprovalsAppSchema>({
    // resolved.baseUrl/publishableKey are always set: config.baseUrl/publishableKey
    // are non-optional defaults, so the resolve step can only overlay on top of them.
    baseUrl: resolved.baseUrl!,
    publishableKey: resolved.publishableKey!,
    authStorage: true,
    functionsBaseUrl: config.functionsBaseUrl,
    runtimeConfig: resolved.runtimeConfig,
  });
  return client;
}


export function getRayfinClient(): RayfinClient<ApprovalsAppSchema> {
  if (!client) {
    throw new Error(
      'Rayfin client not initialized. Call bootstrapAuth() first.'
    );
  }
  return client;
}

/** True when the app was bootstrapped against a localhost backend. */
export function isLocalBackend(): boolean {
  return localDev || isDesignPreview();
}
