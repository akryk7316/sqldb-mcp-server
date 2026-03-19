import { QueryResult } from "../db/types";

export const MAX_ROWS = 100;

export interface RowResult {
  meta: {
    totalCount: number;
    returnedCount: number;
    skip: number;
    take: number;
  };
  columns: string[];
  rows: unknown[][];
}

/**
 * Converts a QueryResult into a compact, token-efficient format for LLMs.
 *
 * Instead of returning an array of objects (key repeated per row), the result
 * contains a `columns` array and `rows` as arrays of values – significantly
 * reducing token usage for wide/long result sets.
 *
 * Rows are capped at MAX_ROWS (100) regardless of the requested `take`, and the
 * original total row count is preserved in `meta.totalCount`.
 */
export function toRowResult(result: QueryResult, skip: number, take: number): RowResult {
  const rows = result.rows.slice(0, MAX_ROWS);
  const columns = rows.length > 0 ? Object.keys(rows[0]) : [];

  return {
    meta: {
      totalCount: result.totalCount,
      returnedCount: rows.length,
      skip,
      take,
    },
    columns,
    rows: rows.map((row) => columns.map((col) => row[col])),
  };
}
