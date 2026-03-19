import { TTLCache, getCacheTTL } from "../utils/cache";

describe("TTLCache", () => {
  beforeEach(() => {
    jest.useFakeTimers();
  });

  afterEach(() => {
    jest.useRealTimers();
  });

  it("stores and retrieves values", () => {
    const cache = new TTLCache<string>(1000);
    cache.set("key", "value");
    expect(cache.get("key")).toBe("value");
  });

  it("returns undefined for missing keys", () => {
    const cache = new TTLCache<string>(1000);
    expect(cache.get("missing")).toBeUndefined();
  });

  it("expires entries after TTL", () => {
    const cache = new TTLCache<string>(1000);
    cache.set("key", "value");
    jest.advanceTimersByTime(1001);
    expect(cache.get("key")).toBeUndefined();
  });

  it("does not expire entries before TTL", () => {
    const cache = new TTLCache<string>(1000);
    cache.set("key", "value");
    jest.advanceTimersByTime(999);
    expect(cache.get("key")).toBe("value");
  });

  it("allows per-entry TTL override", () => {
    const cache = new TTLCache<string>(5000);
    cache.set("shortKey", "short", 100);
    jest.advanceTimersByTime(200);
    expect(cache.get("shortKey")).toBeUndefined();
  });

  it("deletes entries", () => {
    const cache = new TTLCache<string>(5000);
    cache.set("key", "value");
    cache.delete("key");
    expect(cache.get("key")).toBeUndefined();
  });

  it("clears all entries", () => {
    const cache = new TTLCache<string>(5000);
    cache.set("a", "1");
    cache.set("b", "2");
    cache.clear();
    expect(cache.size).toBe(0);
  });
});

describe("getCacheTTL", () => {
  const originalEnv = process.env;

  beforeEach(() => {
    process.env = { ...originalEnv };
  });

  afterEach(() => {
    process.env = originalEnv;
  });

  it("returns 60000ms as default when CACHE_TTL is not set", () => {
    delete process.env.CACHE_TTL;
    expect(getCacheTTL()).toBe(60_000);
  });

  it("converts CACHE_TTL seconds to milliseconds", () => {
    process.env.CACHE_TTL = "30";
    expect(getCacheTTL()).toBe(30_000);
  });

  it("returns default for invalid CACHE_TTL", () => {
    process.env.CACHE_TTL = "abc";
    expect(getCacheTTL()).toBe(60_000);
  });

  it("returns default for zero CACHE_TTL", () => {
    process.env.CACHE_TTL = "0";
    expect(getCacheTTL()).toBe(60_000);
  });
});
