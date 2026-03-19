import { DBAdapter, TableInfo } from "../../db/types";
import { TTLCache, getCacheTTL } from "../../utils/cache";

const listTablesCache = new TTLCache<TableInfo[]>(getCacheTTL());
const CACHE_KEY = "listTables";

export function registerListTablesTool(server: {
  registerTool: (
    name: string,
    config: { description?: string; inputSchema: Record<string, never> },
    handler: (args: Record<string, never>) => Promise<{
      content: Array<{ type: "text"; text: string }>;
    }>
  ) => void;
}, db: DBAdapter): void {
  server.registerTool(
    "listTables",
    {
      description: "List all base tables in the database, returning their schema and name.",
      inputSchema: {},
    },
    async (_args) => {
      const cached = listTablesCache.get(CACHE_KEY);
      if (cached) {
        return { content: [{ type: "text", text: JSON.stringify(cached) }] };
      }

      const tables = await db.listTables();
      listTablesCache.set(CACHE_KEY, tables);

      return {
        content: [{ type: "text", text: JSON.stringify(tables) }],
      };
    }
  );
}
