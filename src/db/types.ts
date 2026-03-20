export interface QueryResult {
  rows: Record<string, unknown>[];
  totalCount: number;
}

export interface TableInfo {
  name: string;
  schema: string;
}

export interface ColumnDetail {
  name: string;
  type: string;
  nullable: boolean;
  length?: number;
  precision?: number;
  scale?: number;
  default?: string;
  isPrimaryKey: boolean;
  isIdentity: boolean;
  comment?: string;
}

export interface IndexInfo {
  name: string;
  columns: string[];
  isUnique: boolean;
  isPrimary: boolean;
}

export interface ConstraintInfo {
  name: string;
  type: string;
  definition?: string;
}

export interface ForeignKeyInfo {
  name: string;
  column: string;
  referencedTable: string;
  referencedColumn: string;
}

export interface TableDescription {
  table: {
    name: string;
    schema: string;
    rowCount: number;
    dataSizeBytes: number;
    indexSizeBytes: number;
    totalSizeBytes: number;
  };
  columns: ColumnDetail[];
  indexes: IndexInfo[];
  constraints: ConstraintInfo[];
  foreignKeys: ForeignKeyInfo[];
  comment?: string;
}

export interface ExplainRow {
  stmtText: string;
  stmtId?: number;
  nodeId?: number;
  parent?: number;
  physicalOp?: string;
  logicalOp?: string;
  argument?: string;
  definedValues?: string;
  estimateRows?: number;
  estimateIO?: number;
  estimateCPU?: number;
  avgRowSize?: number;
  totalSubtreeCost?: number;
  outputList?: string;
  warnings?: string;
  type?: string;
  parallel?: boolean;
  estimateExecutions?: number;
}

export interface ExplainResult {
  sql: string;
  plan: ExplainRow[];
}

export interface DBAdapter {
  query(sql: string, skip: number, take: number): Promise<QueryResult>;
  /**
   * Stream all matching rows without a hard row-count limit.
   * Intended for large exports; uses a longer timeout pool internally.
   * Pass an AbortSignal to cancel mid-stream (e.g. on timeout).
   */
  queryStream(sql: string, signal?: AbortSignal): AsyncGenerator<Record<string, unknown>>;
  listTables(): Promise<TableInfo[]>;
  describeTable(table: string, schema?: string): Promise<TableDescription>;
  explainQuery(sql: string): Promise<ExplainResult>;
  close(): Promise<void>;
}
