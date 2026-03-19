import sql from "mssql";
import { DBAdapter, QueryResult, TableInfo, TableDescription, ColumnDetail, IndexInfo, ForeignKeyInfo, ConstraintInfo } from "../types";

const DEFAULT_QUERY_TIMEOUT = 30_000;

export class MSSQLAdapter implements DBAdapter {
  private pool: sql.ConnectionPool;
  private readonly queryTimeout: number;

  constructor() {
    this.queryTimeout = Number(process.env.DB_QUERY_TIMEOUT) || DEFAULT_QUERY_TIMEOUT;
    this.pool = new sql.ConnectionPool({
      user: process.env.DB_USER,
      password: process.env.DB_PASSWORD,
      server: process.env.DB_HOST!,
      port: Number(process.env.DB_PORT) || 1433,
      database: process.env.DB_NAME,
      connectionTimeout: this.queryTimeout,
      requestTimeout: this.queryTimeout,
      options: {
        encrypt: false,
        trustServerCertificate: true,
      },
      pool: {
        max: 10,
        min: 0,
        idleTimeoutMillis: 30_000,
      },
    });
  }

  async connect(): Promise<void> {
    if (!this.pool.connected && !this.pool.connecting) {
      await this.pool.connect();
    }
  }

  async close(): Promise<void> {
    if (this.pool.connected) {
      await this.pool.close();
    }
  }

  async query(sqlText: string, skip: number, take: number): Promise<QueryResult> {
    await this.connect();

    // Security note: sqlText has been validated by validateSQL() (AST-level check) before reaching
    // this method to ensure only SELECT statements are executed.  The text is interpolated as a
    // subquery because parameterised queries do not support dynamic SQL fragments.  The adaptor is
    // intentionally read-only (no INSERT/UPDATE/DELETE/DDL) and should only be exposed to trusted
    // query sources.
    const wrapped = `
      SELECT *, COUNT(*) OVER() AS __total_count
      FROM (${sqlText}) AS __inner_query
      ORDER BY (SELECT NULL)
      OFFSET @skip ROWS FETCH NEXT @take ROWS ONLY
    `;

    const result = await this.pool
      .request()
      .input("skip", sql.Int, skip)
      .input("take", sql.Int, take)
      .query(wrapped);

    const rows: Record<string, unknown>[] = result.recordset;
    const totalCount = rows.length > 0 ? Number(rows[0]["__total_count"] ?? 0) : 0;

    // Remove the internal __total_count column from returned rows
    const cleanRows = rows.map((row) => {
      const { __total_count: _, ...rest } = row;
      return rest;
    });

    return { rows: cleanRows, totalCount };
  }

  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  async explainQuery(sqlText: string): Promise<any> {
    await this.connect();

    // Use a transaction to hold one connection so that the session-level
    // SET SHOWPLAN_ALL ON setting applies to all three batches.
    // When SHOWPLAN_ALL is ON, queries return plan rows instead of executing.
    const transaction = new sql.Transaction(this.pool);
    await transaction.begin();

    try {
      await new sql.Request(transaction).batch("SET SHOWPLAN_ALL ON");
      const planResult = await new sql.Request(transaction).query(sqlText);
      await new sql.Request(transaction).batch("SET SHOWPLAN_ALL OFF");

      // Rollback releases the connection without any side-effects (no data
      // changes occur because SHOWPLAN_ALL prevents actual execution).
      await transaction.rollback();

      return { sql: sqlText, plan: planResult.recordset };
    } catch (err) {
      try {
        await transaction.rollback();
      } catch {
        // Ignore secondary rollback errors
      }
      throw err;
    }
  }

  async listTables(): Promise<TableInfo[]> {
    await this.connect();

    const result = await this.pool.request().query(`
      SELECT TABLE_SCHEMA AS [schema], TABLE_NAME AS [name]
      FROM INFORMATION_SCHEMA.TABLES
      WHERE TABLE_TYPE = 'BASE TABLE'
      ORDER BY TABLE_SCHEMA, TABLE_NAME
    `);

    return result.recordset.map((row: Record<string, unknown>) => ({
      schema: String(row["schema"]),
      name: String(row["name"]),
    }));
  }

  async describeTable(table: string, schema = "dbo"): Promise<TableDescription> {
    await this.connect();

    // Split schema.table if provided as one string
    const dotIdx = table.indexOf(".");
    if (dotIdx !== -1) {
      schema = table.substring(0, dotIdx);
      table = table.substring(dotIdx + 1);
    }

    const req = () => this.pool.request()
      .input("schema", sql.NVarChar, schema)
      .input("table", sql.NVarChar, table);

    // Columns
    const columnsResult = await req().query(`
      SELECT
        c.COLUMN_NAME        AS name,
        c.DATA_TYPE          AS type,
        c.IS_NULLABLE        AS is_nullable,
        c.CHARACTER_MAXIMUM_LENGTH AS char_max_length,
        c.NUMERIC_PRECISION  AS numeric_precision,
        c.NUMERIC_SCALE      AS numeric_scale,
        c.COLUMN_DEFAULT     AS column_default,
        COLUMNPROPERTY(OBJECT_ID(@schema + '.' + @table), c.COLUMN_NAME, 'IsIdentity') AS is_identity,
        CASE WHEN kcu.COLUMN_NAME IS NOT NULL THEN 1 ELSE 0 END AS is_primary_key
      FROM INFORMATION_SCHEMA.COLUMNS c
      LEFT JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu
        ON  kcu.TABLE_SCHEMA = c.TABLE_SCHEMA
        AND kcu.TABLE_NAME   = c.TABLE_NAME
        AND kcu.COLUMN_NAME  = c.COLUMN_NAME
        AND EXISTS (
          SELECT 1 FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
          WHERE tc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME
            AND tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
        )
      WHERE c.TABLE_SCHEMA = @schema
        AND c.TABLE_NAME   = @table
      ORDER BY c.ORDINAL_POSITION
    `);

    const columns: ColumnDetail[] = columnsResult.recordset.map((c: Record<string, unknown>) => ({
      name: String(c["name"]),
      type: String(c["type"]),
      nullable: c["is_nullable"] === "YES",
      length: c["char_max_length"] != null ? Number(c["char_max_length"]) : undefined,
      precision: c["numeric_precision"] != null ? Number(c["numeric_precision"]) : undefined,
      scale: c["numeric_scale"] != null ? Number(c["numeric_scale"]) : undefined,
      default: c["column_default"] != null ? String(c["column_default"]) : undefined,
      isPrimaryKey: Number(c["is_primary_key"]) === 1,
      isIdentity: Number(c["is_identity"]) === 1,
    }));

    // Indexes
    const indexResult = await req().query(`
      SELECT
        i.name                   AS index_name,
        i.is_unique              AS is_unique,
        i.is_primary_key         AS is_primary,
        STRING_AGG(c.name, ',') WITHIN GROUP (ORDER BY ic.key_ordinal) AS columns
      FROM sys.indexes i
      INNER JOIN sys.index_columns ic ON ic.object_id = i.object_id AND ic.index_id = i.index_id
      INNER JOIN sys.columns c ON c.object_id = ic.object_id AND c.column_id = ic.column_id
      WHERE i.object_id = OBJECT_ID(@schema + '.' + @table)
        AND i.name IS NOT NULL
      GROUP BY i.name, i.is_unique, i.is_primary_key
      ORDER BY i.name
    `);

    const indexes: IndexInfo[] = indexResult.recordset.map((i: Record<string, unknown>) => ({
      name: String(i["index_name"]),
      columns: String(i["columns"] ?? "").split(",").filter(Boolean),
      isUnique: Boolean(i["is_unique"]),
      isPrimary: Boolean(i["is_primary"]),
    }));

    // Foreign keys
    const fkResult = await req().query(`
      SELECT
        fk.name                AS fk_name,
        COL_NAME(fkc.parent_object_id, fkc.parent_column_id) AS column_name,
        OBJECT_NAME(fkc.referenced_object_id) AS ref_table,
        COL_NAME(fkc.referenced_object_id, fkc.referenced_column_id) AS ref_column
      FROM sys.foreign_keys fk
      INNER JOIN sys.foreign_key_columns fkc ON fkc.constraint_object_id = fk.object_id
      WHERE fk.parent_object_id = OBJECT_ID(@schema + '.' + @table)
      ORDER BY fk.name
    `);

    const foreignKeys: ForeignKeyInfo[] = fkResult.recordset.map((fk: Record<string, unknown>) => ({
      name: String(fk["fk_name"]),
      column: String(fk["column_name"]),
      referencedTable: String(fk["ref_table"]),
      referencedColumn: String(fk["ref_column"]),
    }));

    // Check constraints
    const constraintResult = await req().query(`
      SELECT
        cc.name               AS constraint_name,
        'CHECK'               AS constraint_type,
        cc.definition         AS definition
      FROM sys.check_constraints cc
      WHERE cc.parent_object_id = OBJECT_ID(@schema + '.' + @table)
      ORDER BY cc.name
    `);

    const constraints: ConstraintInfo[] = constraintResult.recordset.map((c: Record<string, unknown>) => ({
      name: String(c["constraint_name"]),
      type: String(c["constraint_type"]),
      definition: c["definition"] != null ? String(c["definition"]) : undefined,
    }));

    // Table size / row count
    const sizeResult = await req().query(`
      SELECT
        SUM(p.rows)            AS row_count,
        SUM(a.total_pages) * 8192  AS total_size_bytes,
        SUM(a.data_pages)  * 8192  AS data_size_bytes,
        (SUM(a.used_pages) - SUM(a.data_pages)) * 8192 AS index_size_bytes
      FROM sys.tables t
      INNER JOIN sys.schemas s ON s.schema_id = t.schema_id
      INNER JOIN sys.indexes i ON i.object_id = t.object_id
      INNER JOIN sys.partitions p ON p.object_id = t.object_id AND p.index_id = i.index_id
      INNER JOIN sys.allocation_units a ON a.container_id = p.partition_id
      WHERE s.name = @schema AND t.name = @table
    `);

    const sizeRow = sizeResult.recordset[0] as Record<string, unknown> | undefined;

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
}
