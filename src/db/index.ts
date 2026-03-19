import { DBAdapter } from "./types";
import { MSSQLAdapter } from "./adapters/mssql";

export function createDBAdapter(): DBAdapter {
  const dbType = (process.env.DB_TYPE ?? "mssql").toLowerCase();

  switch (dbType) {
    case "mssql":
      return new MSSQLAdapter();
    default:
      throw new Error(
        `Unsupported DB_TYPE: "${dbType}". Supported types: mssql`
      );
  }
}

export * from "./types";
