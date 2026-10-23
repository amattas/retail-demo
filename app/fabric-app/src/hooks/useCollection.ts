import { useCallback, useEffect, useRef, useState } from 'react';

export function useCollection<T>(loader: () => Promise<T[]>) {
  const [items, setItems] = useState<T[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const request = useRef(0);
  const reload = useCallback(async () => {
    const current = ++request.current;
    setLoading(true);
    setError(null);
    try {
      const rows = await loader();
      if (current === request.current) setItems(rows);
      return true;
    } catch (exc) {
      if (current === request.current) setError(exc instanceof Error ? exc.message : String(exc));
      return false;
    } finally {
      if (current === request.current) setLoading(false);
    }
  }, [loader]);
  useEffect(() => {
    void reload();
    return () => { request.current++; };
  }, [reload]);
  return { items, loading, error, reload };
}
