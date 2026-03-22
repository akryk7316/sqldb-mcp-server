import { Pool, PoolClient } from "pg";
import Cursor from "pg-cursor";
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
import { shouldFallbackPostgreSQLSSL, shouldUseEncryptedConnection } from "./security";

const DEFAULT_QUERY_TIMEOUT = 30_000;
const DEFAULT_EXPORT_QUERY_TIMEOUT = 300_000;

export class PostgreSQLAdapter implements DBAdapter {
  private pool: Pool;
  private exportPool: Pool | null = null;
  private readonly queryTimeout: number;
  private readonly exportQueryTimeout: number;
  private readonly preferSSL: boolean;

  constructor() {
    this.queryTimeout = Number(process.env.DB_QUERY_TIMEOUT) || DEFAULT_QUERY_TIMEOUT;
    this.exportQueryTimeout = Number(process.env.EXPORT_QUERY_TIMEOUT) || DEFAULT_EXPORT_QUERY_TIMEOUT;
    this.preferSSL = shouldUseEncryptedConnection(true);
    this.pool = this.createPool(this.queryTimeout);
  }

  private createPool(timeoutMs: number, useSSL = this.preferSSL): Pool {
    return new Pool({
      host: process.env.DB_HOST,
      port: Number(process.env.DB_PORT) || 5432,
      user: process.env.DB_USER,
      password: process.env.DB_PASSWORD,
      database: process.env.DB_NAME,
      connectionTimeoutMillis: timeoutMs,
      query_timeout: timeoutMs,
      max: 10,
      idleTimeoutMillis: 30_000,
      ssl: useSSL ? { rejectUnauthorized: false } : undefined,
    });
  }

  private getExportPool(): Pool {
    if (!this.exportPool) {
      this.exportPool = this.createPool(this.exportQueryTimeout);
    }
    return this.exportPool;
  }

  private async withPoolRetry<T>(pool: Pool, timeoutMs: number, run: (targetPool: Pool) => Promise<T>): Promise<T> {
    try {
      return await run(pool);
    } catch (err) {
      if (!this.preferSSL || !shouldFallbackPostgreSQLSSL(err)) {
        throw err;
      }

      await pool.end();
      const fallbackPool = this.createPool(timeoutMs, false);

      if (pool === this.pool) {
        this.pool = fallbackPool;
      }
      if (pool === this.exportPool) {
        this.exportPool = fallbackPool;
      }

      return run(fallbackPool);
    }
  }

  async close(): Promise<void> {
    await this.pool.end();
    if (this.exportPool) {
      await this.exportPool.end();
    }
  }

  async query(sqlText: string, skip: number, take: number): Promise<QueryResult> {
    // Security note: sqlText has been validated by validateSQL() before reaching this method.
    // skip and take are integers normalised by normalizePagination() and are embedded directly
    // because pg parameterised queries do not accept LIMIT/OFFSET as bound parameters.
    const wrapped = `
      SELECT *, COUNT(*) OVER() AS __total_count
      FROM (${sqlText}) AS __inner_query
      LIMIT ${take} OFFSET ${skip}
    `;

    const result = await this.withPoolRetry(this.pool, this.queryTimeout, (pool) => pool.query(wrapped));
    const rows = result.rows as Record<string, unknown>[];
    const totalCount = rows.length > 0 ? Number(rows[0]["__total_count"] ?? 0) : 0;

    const cleanRows = rows.map((row) => {
      const { __total_count: _, ...rest } = row;
      return rest;
    });

    return { rows: cleanRows, totalCount };
  }

  async *queryStream(sqlText: string, signal?: AbortSignal): AsyncGenerator<Record<string, unknown>> {
    if (signal?.aborted) {
      throw new Error("Export was cancelled before starting");
    }

    const exportPool = await this.withPoolRetry(this.getExportPool(), this.exportQueryTimeout, async (pool) => {
      const client = await pool.connect();
      client.release();
      return pool;
    });
    const client: PoolClient = await exportPool.connect();
    const BATCH_SIZE = 1000;

    try {
      const cursor = client.query(new Cursor(sqlText));

      if (signal) {
        signal.addEventListener(
          "abort",
          () => {
            void cursor.close();
          },
          { once: true }
        );
      }

      while (true) {
        if (signal?.aborted) {
          throw new Error("Export was cancelled");
        }

        const rows: Record<string, unknown>[] = await cursor.read(BATCH_SIZE);
        if (rows.length === 0) break;

        for (const row of rows) {
          yield row;
        }

        if (rows.length < BATCH_SIZE) break;
      }

      await cursor.close();
    } finally {
      client.release();
    }
  }

  async listTables(): Promise<TableInfo[]> {
    const result = await this.withPoolRetry(this.pool, this.queryTimeout, (pool) => pool.query(`
      SELECT table_schema AS "schema", table_name AS "name"
      FROM information_schema.tables
      WHERE table_type = 'BASE TABLE'
        AND table_schema NOT IN ('pg_catalog', 'information_schema')
      ORDER BY table_schema, table_name
    `));

    return result.rows.map((row: Record<string, unknown>) => ({
      schema: String(row["schema"]),
      name: String(row["name"]),
    }));
  }

  async describeTable(table: string, schema = "public"): Promise<TableDescription> {
    const dotIdx = table.indexOf(".");
    if (dotIdx !== -1) {
      schema = table.substring(0, dotIdx);
      table = table.substring(dotIdx + 1);
    }

    // Columns
    const columnsResult = await this.withPoolRetry(this.pool, this.queryTimeout, (pool) => pool.query(
      `SELECT
        c.column_name AS name,
        c.data_type AS type,
        c.is_nullable,
        c.character_maximum_length AS char_max_length,
        c.numeric_precision,
        c.numeric_scale,
        c.column_default,
        CASE WHEN kcu.column_name IS NOT NULL THEN true ELSE false END AS is_primary_key
      FROM information_schema.columns c
      LEFT JOIN information_schema.key_column_usage kcu
        ON kcu.table_schema = c.table_schema
        AND kcu.table_name = c.table_name
        AND kcu.column_name = c.column_name
        AND EXISTS (
          SELECT 1 FROM information_schema.table_constraints tc
          WHERE tc.constraint_name = kcu.constraint_name
            AND tc.table_schema = kcu.table_schema
            AND tc.constraint_type = 'PRIMARY KEY'
        )
      WHERE c.table_schema = $1
        AND c.table_name = $2
      ORDER BY c.ordinal_position`,
      [schema, table]
    ));

    const columns: ColumnDetail[] = columnsResult.rows.map((c: Record<string, unknown>) => ({
      name: String(c["name"]),
      type: String(c["type"]),
      nullable: c["is_nullable"] === "YES",
      length: c["char_max_length"] != null ? Number(c["char_max_length"]) : undefined,
      precision: c["numeric_precision"] != null ? Number(c["numeric_precision"]) : undefined,
      scale: c["numeric_scale"] != null ? Number(c["numeric_scale"]) : undefined,
      default: c["column_default"] != null ? String(c["column_default"]) : undefined,
      isPrimaryKey: Boolean(c["is_primary_key"]),
      isIdentity: false,
    }));

    // Indexes
    const indexResult = await this.withPoolRetry(this.pool, this.queryTimeout, (pool) => pool.query(
      `SELECT
        i.relname AS index_name,
        ix.indisunique AS is_unique,
        ix.indisprimary AS is_primary,
        string_agg(a.attname, ',' ORDER BY array_position(ix.indkey, a.attnum)) AS columns
      FROM pg_class t
      INNER JOIN pg_namespace n ON n.oid = t.relnamespace
      INNER JOIN pg_index ix ON ix.indrelid = t.oid
      INNER JOIN pg_class i ON i.oid = ix.indexrelid
      INNER JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = ANY(ix.indkey)
      WHERE n.nspname = $1 AND t.relname = $2
      GROUP BY i.relname, ix.indisunique, ix.indisprimary
      ORDER BY i.relname`,
      [schema, table]
    ));

    const indexes: IndexInfo[] = indexResult.rows.map((i: Record<string, unknown>) => ({
      name: String(i["index_name"]),
      columns: String(i["columns"] ?? "").split(",").filter(Boolean),
      isUnique: Boolean(i["is_unique"]),
      isPrimary: Boolean(i["is_primary"]),
    }));

    // Foreign keys
    const fkResult = await this.withPoolRetry(this.pool, this.queryTimeout, (pool) => pool.query(
      `SELECT
        kcu.constraint_name AS fk_name,
        kcu.column_name,
        ccu.table_name AS ref_table,
        ccu.column_name AS ref_column
      FROM information_schema.table_constraints tc
      INNER JOIN information_schema.key_column_usage kcu
        ON tc.constraint_name = kcu.constraint_name
        AND tc.table_schema = kcu.table_schema
      INNER JOIN information_schema.constraint_column_usage ccu
        ON ccu.constraint_name = tc.constraint_name
        AND ccu.constraint_schema = tc.table_schema
      WHERE tc.constraint_type = 'FOREIGN KEY'
        AND tc.table_schema = $1
        AND tc.table_name = $2
      ORDER BY kcu.constraint_name`,
      [schema, table]
    ));

    const foreignKeys: ForeignKeyInfo[] = fkResult.rows.map((fk: Record<string, unknown>) => ({
      name: String(fk["fk_name"]),
      column: String(fk["column_name"]),
      referencedTable: String(fk["ref_table"]),
      referencedColumn: String(fk["ref_column"]),
    }));

    // Check constraints
    const constraintResult = await this.withPoolRetry(this.pool, this.queryTimeout, (pool) => pool.query(
      `SELECT
        cc.constraint_name,
        cc.check_clause
      FROM information_schema.check_constraints cc
      INNER JOIN information_schema.table_constraints tc
        ON cc.constraint_name = tc.constraint_name
        AND cc.constraint_schema = tc.table_schema
      WHERE tc.table_schema = $1
        AND tc.table_name = $2
      ORDER BY cc.constraint_name`,
      [schema, table]
    ));

    const constraints: ConstraintInfo[] = constraintResult.rows.map((c: Record<string, unknown>) => ({
      name: String(c["constraint_name"]),
      type: "CHECK",
      definition: c["check_clause"] != null ? String(c["check_clause"]) : undefined,
    }));

    // Row count and size
    const sizeResult = await this.withPoolRetry(this.pool, this.queryTimeout, (pool) => pool.query(
      `SELECT
        c.reltuples::bigint AS row_count,
        pg_table_size(c.oid) AS data_size_bytes,
        pg_indexes_size(c.oid) AS index_size_bytes,
        pg_total_relation_size(c.oid) AS total_size_bytes
      FROM pg_class c
      INNER JOIN pg_namespace n ON n.oid = c.relnamespace
      WHERE n.nspname = $1 AND c.relname = $2`,
      [schema, table]
    ));

    const sizeRow = sizeResult.rows[0] as Record<string, unknown> | undefined;

    return {
      table: {
        name: table,
        schema,
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
    const result = await this.withPoolRetry(
      this.pool,
      this.queryTimeout,
      (pool) => pool.query(`EXPLAIN (FORMAT JSON, ANALYZE FALSE) ${sqlText}`)
    );
    const planJson = (result.rows[0] as Record<string, unknown>)["QUERY PLAN"];
    return {
      sql: sqlText,
      plan: [{ stmtText: JSON.stringify(planJson) }],
    };
  }
}
