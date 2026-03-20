import { DBAdapter } from "./types";
import { MSSQLAdapter } from "./adapters/mssql";
import { PostgreSQLAdapter } from "./adapters/postgresql";
import { MySQLAdapter } from "./adapters/mysql";

export function createDBAdapter(): DBAdapter {
  const dbType = (process.env.DB_TYPE ?? "mssql").toLowerCase();

  switch (dbType) {
    case "mssql":
      return new MSSQLAdapter();
    case "postgresql":
      return new PostgreSQLAdapter();
    case "mysql":
      return new MySQLAdapter();
    default:
      throw new Error(
        `Unsupported DB_TYPE: "${dbType}". Supported types: mssql, postgresql, mysql`
      );
  }
}

export * from "./types";
