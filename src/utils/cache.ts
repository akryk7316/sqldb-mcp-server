const DEFAULT_TTL_MS = 60_000;

interface CacheEntry<T> {
  value: T;
  expiresAt: number;
}

/**
 * Simple in-memory TTL cache.
 *
 * Keys are arbitrary strings. TTL is specified per-entry in milliseconds.
 * Expired entries are lazily evicted on the next `get` call.
 */
export class TTLCache<T> {
  private readonly store = new Map<string, CacheEntry<T>>();
  private readonly ttlMs: number;

  constructor(ttlMs = DEFAULT_TTL_MS) {
    this.ttlMs = ttlMs;
  }

  get(key: string): T | undefined {
    const entry = this.store.get(key);
    if (!entry) return undefined;
    if (Date.now() > entry.expiresAt) {
      this.store.delete(key);
      return undefined;
    }
    return entry.value;
  }

  set(key: string, value: T, ttlMs?: number): void {
    const ttl = ttlMs ?? this.ttlMs;
    this.store.set(key, { value, expiresAt: Date.now() + ttl });
  }

  delete(key: string): void {
    this.store.delete(key);
  }

  clear(): void {
    this.store.clear();
  }

  get size(): number {
    return this.store.size;
  }
}

/**
 * Returns the cache TTL in milliseconds, reading from the CACHE_TTL env var
 * (value in seconds).  Defaults to 60 seconds.
 */
export function getCacheTTL(): number {
  const seconds = Number(process.env.CACHE_TTL);
  return Number.isFinite(seconds) && seconds > 0 ? seconds * 1_000 : DEFAULT_TTL_MS;
}
