import "dotenv/config";
import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js";

import { createDBAdapter } from "../db";
import { registerQueryTool } from "./tools/query";
import { registerListTablesTool } from "./tools/listTables";
import { registerDescribeTableTool } from "./tools/describeTable";
import { registerExplainQueryTool } from "./tools/explainQuery";

async function main(): Promise<void> {
  const db = createDBAdapter();

  const server = new McpServer({
    name: "sqldb-mcp-server",
    version: "1.0.0",
  });

  registerQueryTool(server, db);
  registerListTablesTool(server, db);
  registerDescribeTableTool(server, db);
  registerExplainQueryTool(server, db);

  const transport = new StdioServerTransport();

  await server.connect(transport);

  // Graceful shutdown
  const shutdown = async (signal: string) => {
    process.stderr.write(`Received ${signal}, shutting down...\n`);
    await server.close();
    await db.close();
    process.exit(0);
  };

  process.on("SIGINT", () => void shutdown("SIGINT"));
  process.on("SIGTERM", () => void shutdown("SIGTERM"));
}

main().catch((err: unknown) => {
  process.stderr.write(`Fatal error: ${err instanceof Error ? err.message : String(err)}\n`);
  process.exit(1);
});
