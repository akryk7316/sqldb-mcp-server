import { z } from "zod";
import { DBAdapter } from "../../db/types";
import { validateSQL } from "../../utils/sanitize";
import { toRowResult } from "../../utils/row-result";
import { normalizePagination } from "../../utils/pagination";
import { TTLCache, getCacheTTL } from "../../utils/cache";
import { RowResult } from "../../utils/row-result";

const queryCache = new TTLCache<RowResult>(getCacheTTL());

export const queryInputSchema = {
  sql: z.string().min(1).describe("SELECT SQL statement to execute"),
  skip: z.number().int().min(0).optional().describe("Number of rows to skip (offset)"),
  take: z.number().int().min(1).max(100).optional().describe("Maximum rows to return (max 100)"),
};

export function registerQueryTool(server: {
  registerTool: (
    name: string,
    config: {
      description?: string;
      inputSchema: typeof queryInputSchema;
    },
    handler: (args: { sql: string; skip?: number; take?: number }) => Promise<{
      content: Array<{ type: "text"; text: string }>;
    }>
  ) => void;
}, db: DBAdapter): void {
  server.registerTool(
    "query",
    {
      description:
        "Execute a read-only SELECT SQL query. Returns results in a compact column/row format to reduce token usage. " +
        "Results are capped at 100 rows; use skip/take for pagination. " +
        "The meta.totalCount field shows the total number of matching rows.",
      inputSchema: queryInputSchema,
    },
    async ({ sql, skip, take }) => {
      validateSQL(sql);

      const { skip: normalizedSkip, take: normalizedTake } = normalizePagination(skip, take);

      const cacheKey = `query:${sql}:${normalizedSkip}:${normalizedTake}`;
      const cached = queryCache.get(cacheKey);
      if (cached) {
        return {
          content: [{ type: "text", text: JSON.stringify(cached) }],
        };
      }

      const result = await db.query(sql, normalizedSkip, normalizedTake);
      const rowResult = toRowResult(result, normalizedSkip, normalizedTake);

      queryCache.set(cacheKey, rowResult);

      return {
        content: [{ type: "text", text: JSON.stringify(rowResult) }],
      };
    }
  );
}
