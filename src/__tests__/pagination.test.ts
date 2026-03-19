import { normalizePagination, DEFAULT_SKIP, DEFAULT_TAKE, MAX_TAKE } from "../utils/pagination";

describe("normalizePagination", () => {
  it("uses defaults when no arguments are provided", () => {
    const result = normalizePagination(undefined, undefined);
    expect(result.skip).toBe(DEFAULT_SKIP);
    expect(result.take).toBe(DEFAULT_TAKE);
  });

  it("uses provided values", () => {
    expect(normalizePagination(10, 25)).toEqual({ skip: 10, take: 25 });
  });

  it("clamps take to MAX_TAKE", () => {
    expect(normalizePagination(0, 999).take).toBe(MAX_TAKE);
  });

  it("ensures take is at least 1", () => {
    expect(normalizePagination(0, 0).take).toBe(1);
    expect(normalizePagination(0, -5).take).toBe(1);
  });

  it("ensures skip is non-negative", () => {
    expect(normalizePagination(-10, 10).skip).toBe(0);
  });

  it("floors fractional values", () => {
    expect(normalizePagination(1.9, 9.9)).toEqual({ skip: 1, take: 9 });
  });
});
