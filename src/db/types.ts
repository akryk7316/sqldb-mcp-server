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

export interface DBAdapter {
  query(sql: string, skip: number, take: number): Promise<QueryResult>;
  listTables(): Promise<TableInfo[]>;
  describeTable(table: string, schema?: string): Promise<TableDescription>;
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  explainQuery(sql: string): Promise<any>;
  close(): Promise<void>;
}
