import { z } from "zod";
import { DBAdapter, ExplainResult } from "../../db/types";
import { validateSQL } from "../../utils/sanitize";
import { TTLCache, getCacheTTL } from "../../utils/cache";

const explainCache = new TTLCache<ExplainResult>(getCacheTTL());

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
        "Each row in the `plan` array represents one operator node in the query plan tree. " +
        "Key fields: physicalOp/logicalOp (operator type), estimateRows (expected rows), " +
        "totalSubtreeCost (cumulative cost), argument (operator details), warnings (any plan warnings). " +
        "The parent field links child nodes to their parent nodeId, forming the plan tree.",
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
