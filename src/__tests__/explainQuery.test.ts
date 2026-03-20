import { DBAdapter, QueryResult, TableDescription, TableInfo } from "../db/types";
import { registerExplainQueryTool } from "../mcp/tools/explainQuery";

// Minimal DBAdapter stub
function makeAdapter(
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  explainFn: (sql: string) => Promise<any>
): DBAdapter {
  // eslint-disable-next-line require-yield
  async function* noopStream(): AsyncGenerator<Record<string, unknown>> { return; }
  return {
    query: async (): Promise<QueryResult> => ({ rows: [], totalCount: 0 }),
    queryStream: noopStream,
    listTables: async (): Promise<TableInfo[]> => [],
    describeTable: async (): Promise<TableDescription> => ({
      table: { name: "t", schema: "dbo", rowCount: 0, dataSizeBytes: 0, indexSizeBytes: 0, totalSizeBytes: 0 },
      columns: [],
      indexes: [],
      constraints: [],
      foreignKeys: [],
    }),
    explainQuery: explainFn,
    close: async () => undefined,
  };
}

type Handler = (args: { sql: string }) => Promise<{ content: Array<{ type: "text"; text: string }> }>;

function captureHandler(db: DBAdapter): Handler {
  let captured: Handler | undefined;
  const fakeServer = {
    registerTool: (
      _name: string,
      _config: unknown,
      handler: Handler
    ) => {
      captured = handler;
    },
  };
  registerExplainQueryTool(fakeServer, db);
  if (!captured) throw new Error("handler not registered");
  return captured;
}

describe("explainQuery tool", () => {
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const samplePlan: any = {
    sql: "SELECT * FROM users",
    plan: [
      {
        StmtText: "SELECT * FROM users",
        StmtId: 1,
        NodeId: 1,
        Parent: 0,
        PhysicalOp: "Clustered Index Scan",
        LogicalOp: "Clustered Index Scan",
        EstimateRows: 100,
        TotalSubtreeCost: 0.02,
      },
    ],
  };

  it("returns the execution plan as JSON text", async () => {
    const db = makeAdapter(async () => samplePlan);
    const handler = captureHandler(db);

    const result = await handler({ sql: "SELECT * FROM users" });

    expect(result.content).toHaveLength(1);
    expect(result.content[0].type).toBe("text");
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const parsed = JSON.parse(result.content[0].text) as any;
    expect(parsed.plan).toHaveLength(1);
    expect(parsed.plan[0].PhysicalOp).toBe("Clustered Index Scan");
    expect(parsed.plan[0].StmtText).toBe("SELECT * FROM users");
    expect(parsed.plan[0].EstimateRows).toBe(100);
    expect(parsed.plan[0].TotalSubtreeCost).toBe(0.02);
  });

  it("caches repeated calls for the same SQL", async () => {
    let callCount = 0;
    const db = makeAdapter(async () => {
      callCount++;
      return samplePlan;
    });

    const handler = captureHandler(db);
    // Use a unique SQL not used by other tests to avoid cross-test cache hits
    await handler({ sql: "SELECT id FROM caching_test_table" });
    await handler({ sql: "SELECT id FROM caching_test_table" });

    expect(callCount).toBe(1);
  });

  it("rejects non-SELECT SQL", async () => {
    const db = makeAdapter(async () => samplePlan);
    const handler = captureHandler(db);

    await expect(handler({ sql: "DELETE FROM users" })).rejects.toThrow();
  });

  it("passes the exact SQL to the DB adapter", async () => {
    let receivedSql = "";
    const db = makeAdapter(async (s) => {
      receivedSql = s;
      return { sql: s, plan: [] };
    });
    const handler = captureHandler(db);

    await handler({ sql: "SELECT id FROM orders" });

    expect(receivedSql).toBe("SELECT id FROM orders");
  });
});
