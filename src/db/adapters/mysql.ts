import mysql2 from "mysql2";
import { createPool as createPromisePool } from "mysql2/promise";
import type { Pool as PromisePool, PoolConnection } from "mysql2/promise";
import {
  DBAdapter,
  QueryResult,
  TableInfo,
  TableDescription,
  ColumnDetail,
  IndexInfo,
  ForeignKeyInfo,
  ConstraintInfo,
  ExplainResult,
} from "../types";

const DEFAULT_QUERY_TIMEOUT = 30_000;
const DEFAULT_EXPORT_QUERY_TIMEOUT = 300_000;

export class MySQLAdapter implements DBAdapter {
  private pool: PromisePool;
  private exportPool: PromisePool | null = null;
  // Raw (callback-based) pool used for streaming queries
  private rawExportPool: mysql2.Pool | null = null;
  private readonly queryTimeout: number;
  private readonly exportQueryTimeout: number;

  constructor() {
    this.queryTimeout = Number(process.env.DB_QUERY_TIMEOUT) || DEFAULT_QUERY_TIMEOUT;
    this.exportQueryTimeout = Number(process.env.EXPORT_QUERY_TIMEOUT) || DEFAULT_EXPORT_QUERY_TIMEOUT;
    this.pool = this.createPromisePool(this.queryTimeout);
  }

  private poolConfig(timeoutMs: number) {
    return {
      host: process.env.DB_HOST,
      port: Number(process.env.DB_PORT) || 3306,
      user: process.env.DB_USER,
      password: process.env.DB_PASSWORD,
      database: process.env.DB_NAME,
      connectTimeout: timeoutMs,
      connectionLimit: 10,
      idleTimeout: 30_000,
    };
  }

  private createPromisePool(timeoutMs: number): PromisePool {
    return createPromisePool(this.poolConfig(timeoutMs));
  }

  private getRawExportPool(): mysql2.Pool {
    if (!this.rawExportPool) {
      this.rawExportPool = mysql2.createPool(this.poolConfig(this.exportQueryTimeout));
    }
    return this.rawExportPool;
  }

  async close(): Promise<void> {
    await this.pool.end();
    if (this.exportPool) {
      await this.exportPool.end();
    }
    await new Promise<void>((resolve, reject) => {
      if (!this.rawExportPool) {
        resolve();
        return;
      }
      this.rawExportPool.end((err) => (err ? reject(err) : resolve()));
    });
  }

  async query(sqlText: string, skip: number, take: number): Promise<QueryResult> {
    // Security note: sqlText has been validated by validateSQL() before reaching this method.
    // skip and take are integers normalised by normalizePagination(); embedded directly because
    // MySQL does not accept LIMIT/OFFSET values as bound parameters.
    const wrapped = `
      SELECT *, COUNT(*) OVER() AS __total_count
      FROM (${sqlText}) AS __inner_query
      LIMIT ${take} OFFSET ${skip}
    `;

    const [rows] = await this.pool.query(wrapped);
    const rowArray = rows as Record<string, unknown>[];
    const totalCount = rowArray.length > 0 ? Number(rowArray[0]["__total_count"] ?? 0) : 0;

    const cleanRows = rowArray.map((row) => {
      const { __total_count: _, ...rest } = row;
      return rest;
    });

    return { rows: cleanRows, totalCount };
  }

  async *queryStream(sqlText: string, signal?: AbortSignal): AsyncGenerator<Record<string, unknown>> {
    if (signal?.aborted) {
      throw new Error("Export was cancelled before starting");
    }

    // Bridge the event-based mysql2 stream to an AsyncGenerator via a shared queue.
    type QueueItem =
      | { kind: "row"; row: Record<string, unknown> }
      | { kind: "done" }
      | { kind: "error"; err: Error };

    const queue: QueueItem[] = [];
    let notify: (() => void) | null = null;

    const push = (item: QueueItem): void => {
      queue.push(item);
      const fn = notify;
      notify = null;
      fn?.();
    };

    const stream = this.getRawExportPool().query(sqlText).stream();

    if (signal) {
      signal.addEventListener("abort", () => stream.destroy(new Error("Export was cancelled")), { once: true });
    }

    stream.on("data", (row: Record<string, unknown>) => push({ kind: "row", row }));
    stream.on("error", (err: Error) => push({ kind: "error", err }));
    stream.on("end", () => push({ kind: "done" }));

    while (true) {
      if (queue.length === 0) {
        await new Promise<void>((res) => {
          notify = res;
        });
      }

      const item = queue.shift()!;
      if (item.kind === "done") return;
      if (item.kind === "error") throw item.err;
      yield item.row;
    }
  }

  async listTables(): Promise<TableInfo[]> {
    const dbName = process.env.DB_NAME ?? "";
    const [rows] = await this.pool.query(
      `SELECT TABLE_SCHEMA AS \`schema\`, TABLE_NAME AS \`name\`
       FROM information_schema.TABLES
       WHERE TABLE_TYPE = 'BASE TABLE'
         AND TABLE_SCHEMA = ?
       ORDER BY TABLE_NAME`,
      [dbName]
    );

    return (rows as Record<string, unknown>[]).map((row) => ({
      schema: String(row["schema"]),
      name: String(row["name"]),
    }));
  }

  async describeTable(table: string, schema?: string): Promise<TableDescription> {
    const dbName = schema ?? process.env.DB_NAME ?? "";

    const dotIdx = table.indexOf(".");
    if (dotIdx !== -1) {
      const parsedSchema = table.substring(0, dotIdx);
      table = table.substring(dotIdx + 1);
      schema = parsedSchema;
    }
    const resolvedSchema = schema ?? dbName;

    // Columns + primary key info
    const [columnRows] = await this.pool.query(
      `SELECT
        c.COLUMN_NAME AS name,
        c.DATA_TYPE AS type,
        c.IS_NULLABLE AS is_nullable,
        c.CHARACTER_MAXIMUM_LENGTH AS char_max_length,
        c.NUMERIC_PRECISION AS numeric_precision,
        c.NUMERIC_SCALE AS numeric_scale,
        c.COLUMN_DEFAULT AS column_default,
        c.EXTRA AS extra,
        CASE WHEN kcu.COLUMN_NAME IS NOT NULL THEN 1 ELSE 0 END AS is_primary_key
      FROM information_schema.COLUMNS c
      LEFT JOIN information_schema.KEY_COLUMN_USAGE kcu
        ON kcu.TABLE_SCHEMA = c.TABLE_SCHEMA
        AND kcu.TABLE_NAME = c.TABLE_NAME
        AND kcu.COLUMN_NAME = c.COLUMN_NAME
        AND kcu.CONSTRAINT_NAME = 'PRIMARY'
      WHERE c.TABLE_SCHEMA = ?
        AND c.TABLE_NAME = ?
      ORDER BY c.ORDINAL_POSITION`,
      [resolvedSchema, table]
    );

    const columns: ColumnDetail[] = (columnRows as Record<string, unknown>[]).map((c) => ({
      name: String(c["name"]),
      type: String(c["type"]),
      nullable: c["is_nullable"] === "YES",
      length: c["char_max_length"] != null ? Number(c["char_max_length"]) : undefined,
      precision: c["numeric_precision"] != null ? Number(c["numeric_precision"]) : undefined,
      scale: c["numeric_scale"] != null ? Number(c["numeric_scale"]) : undefined,
      default: c["column_default"] != null ? String(c["column_default"]) : undefined,
      isPrimaryKey: Number(c["is_primary_key"]) === 1,
      isIdentity: String(c["extra"]).includes("auto_increment"),
    }));

    // Indexes via information_schema
    const [indexRows] = await this.pool.query(
      `SELECT
        INDEX_NAME AS index_name,
        GROUP_CONCAT(COLUMN_NAME ORDER BY SEQ_IN_INDEX) AS columns,
        MAX(CASE WHEN NON_UNIQUE = 0 THEN 1 ELSE 0 END) AS is_unique,
        MAX(CASE WHEN INDEX_NAME = 'PRIMARY' THEN 1 ELSE 0 END) AS is_primary
      FROM information_schema.STATISTICS
      WHERE TABLE_SCHEMA = ?
        AND TABLE_NAME = ?
      GROUP BY INDEX_NAME
      ORDER BY INDEX_NAME`,
      [resolvedSchema, table]
    );

    const indexes: IndexInfo[] = (indexRows as Record<string, unknown>[]).map((i) => ({
      name: String(i["index_name"]),
      columns: String(i["columns"] ?? "").split(",").filter(Boolean),
      isUnique: Number(i["is_unique"]) === 1,
      isPrimary: Number(i["is_primary"]) === 1,
    }));

    // Foreign keys
    const [fkRows] = await this.pool.query(
      `SELECT
        kcu.CONSTRAINT_NAME AS fk_name,
        kcu.COLUMN_NAME AS column_name,
        kcu.REFERENCED_TABLE_NAME AS ref_table,
        kcu.REFERENCED_COLUMN_NAME AS ref_column
      FROM information_schema.KEY_COLUMN_USAGE kcu
      INNER JOIN information_schema.TABLE_CONSTRAINTS tc
        ON tc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME
        AND tc.TABLE_SCHEMA = kcu.TABLE_SCHEMA
        AND tc.TABLE_NAME = kcu.TABLE_NAME
      WHERE tc.CONSTRAINT_TYPE = 'FOREIGN KEY'
        AND kcu.TABLE_SCHEMA = ?
        AND kcu.TABLE_NAME = ?
      ORDER BY kcu.CONSTRAINT_NAME`,
      [resolvedSchema, table]
    );

    const foreignKeys: ForeignKeyInfo[] = (fkRows as Record<string, unknown>[]).map((fk) => ({
      name: String(fk["fk_name"]),
      column: String(fk["column_name"]),
      referencedTable: String(fk["ref_table"]),
      referencedColumn: String(fk["ref_column"]),
    }));

    // Check constraints (MySQL 8.0.16+)
    const [checkRows] = await this.pool.query(
      `SELECT
        CONSTRAINT_NAME AS constraint_name,
        CHECK_CLAUSE AS check_clause
      FROM information_schema.CHECK_CONSTRAINTS
      WHERE CONSTRAINT_SCHEMA = ?
        AND CONSTRAINT_NAME IN (
          SELECT CONSTRAINT_NAME FROM information_schema.TABLE_CONSTRAINTS
          WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND CONSTRAINT_TYPE = 'CHECK'
        )
      ORDER BY CONSTRAINT_NAME`,
      [resolvedSchema, resolvedSchema, table]
    );

    const constraints: ConstraintInfo[] = (checkRows as Record<string, unknown>[]).map((c) => ({
      name: String(c["constraint_name"]),
      type: "CHECK",
      definition: c["check_clause"] != null ? String(c["check_clause"]) : undefined,
    }));

    // Row count and data size from information_schema
    const [sizeRows] = await this.pool.query(
      `SELECT
        TABLE_ROWS AS row_count,
        DATA_LENGTH AS data_size_bytes,
        INDEX_LENGTH AS index_size_bytes,
        (DATA_LENGTH + INDEX_LENGTH) AS total_size_bytes
      FROM information_schema.TABLES
      WHERE TABLE_SCHEMA = ?
        AND TABLE_NAME = ?`,
      [resolvedSchema, table]
    );

    const sizeRow = (sizeRows as Record<string, unknown>[])[0];

    return {
      table: {
        name: table,
        schema: resolvedSchema,
        rowCount: Number(sizeRow?.["row_count"] ?? 0),
        dataSizeBytes: Number(sizeRow?.["data_size_bytes"] ?? 0),
        indexSizeBytes: Number(sizeRow?.["index_size_bytes"] ?? 0),
        totalSizeBytes: Number(sizeRow?.["total_size_bytes"] ?? 0),
      },
      columns,
      indexes,
      constraints,
      foreignKeys,
    };
  }

  async explainQuery(sqlText: string): Promise<ExplainResult> {
    const conn: PoolConnection = await this.pool.getConnection();
    try {
      const [rows] = await conn.query(`EXPLAIN FORMAT=JSON ${sqlText}`);
      const planRow = (rows as Record<string, unknown>[])[0];
      return {
        sql: sqlText,
        plan: [{ stmtText: String(planRow?.["EXPLAIN"] ?? JSON.stringify(planRow)) }],
      };
    } finally {
      conn.release();
    }
  }
}
