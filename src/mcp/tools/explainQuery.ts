import { z } from "zod";
import { DBAdapter } from "../../db/types";
import { validateSQL } from "../../utils/sanitize";
import { TTLCache, getCacheTTL } from "../../utils/cache";

// eslint-disable-next-line @typescript-eslint/no-explicit-any
const explainCache = new TTLCache<any>(getCacheTTL());

export const explainQueryInputSchema = {
  sql: z.string().min(1).describe("SELECT SQL statement whose execution plan should be retrieved"),
};

export function registerExplainQueryTool(server: {
  registerTool: (
    name: string,
    config: {
      description?: string;
      inputSchema: typeof explainQueryInputSchema;
    },
    handler: (args: { sql: string }) => Promise<{
      content: Array<{ type: "text"; text: string }>;
    }>
  ) => void;
}, db: DBAdapter): void {
  server.registerTool(
    "explainQuery",
    {
      description:
        "Return the estimated execution plan for a SELECT SQL query without actually executing it. " +
        "The response format depends on the database engine (e.g. MSSQL, PostgreSQL, MySQL) " +
        "and is returned as-is from the database driver.",
      inputSchema: explainQueryInputSchema,
    },
    async ({ sql }) => {
      validateSQL(sql);

      const cacheKey = `explain:${sql}`;
      const cached = explainCache.get(cacheKey);
      if (cached) {
        return { content: [{ type: "text", text: JSON.stringify(cached) }] };
      }

      const result = await db.explainQuery(sql);
      explainCache.set(cacheKey, result);

      return {
        content: [{ type: "text", text: JSON.stringify(result) }],
      };
    }
  );
}
