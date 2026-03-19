export const DEFAULT_SKIP = 0;
export const DEFAULT_TAKE = 50;
export const MAX_TAKE = 100;

/**
 * Normalizes pagination parameters, clamping `take` to [1, MAX_TAKE].
 */
export function normalizePagination(
  skip: number | undefined,
  take: number | undefined
): { skip: number; take: number } {
  const normalizedSkip = Math.max(0, Math.floor(skip ?? DEFAULT_SKIP));
  const normalizedTake = Math.min(
    MAX_TAKE,
    Math.max(1, Math.floor(take ?? DEFAULT_TAKE))
  );
  return { skip: normalizedSkip, take: normalizedTake };
}
