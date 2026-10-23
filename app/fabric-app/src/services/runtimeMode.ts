export function isDesignPreview(): boolean {
  return import.meta.env.DEV && import.meta.env.MODE === 'preview';
}
