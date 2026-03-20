import { createDBAdapter } from "../db/index";
import { MSSQLAdapter } from "../db/adapters/mssql";
import { PostgreSQLAdapter } from "../db/adapters/postgresql";
import { MySQLAdapter } from "../db/adapters/mysql";

describe("createDBAdapter", () => {
  const originalDbType = process.env.DB_TYPE;

  afterEach(() => {
    if (originalDbType === undefined) {
      delete process.env.DB_TYPE;
    } else {
      process.env.DB_TYPE = originalDbType;
    }
  });

  it("creates MSSQLAdapter when DB_TYPE=mssql", () => {
    process.env.DB_TYPE = "mssql";
    const adapter = createDBAdapter();
    expect(adapter).toBeInstanceOf(MSSQLAdapter);
  });

  it("creates PostgreSQLAdapter when DB_TYPE=postgresql", () => {
    process.env.DB_TYPE = "postgresql";
    const adapter = createDBAdapter();
    expect(adapter).toBeInstanceOf(PostgreSQLAdapter);
  });

  it("creates MySQLAdapter when DB_TYPE=mysql", () => {
    process.env.DB_TYPE = "mysql";
    const adapter = createDBAdapter();
    expect(adapter).toBeInstanceOf(MySQLAdapter);
  });

  it("defaults to MSSQLAdapter when DB_TYPE is not set", () => {
    delete process.env.DB_TYPE;
    const adapter = createDBAdapter();
    expect(adapter).toBeInstanceOf(MSSQLAdapter);
  });

  it("throws for unsupported DB_TYPE", () => {
    process.env.DB_TYPE = "oracle";
    expect(() => createDBAdapter()).toThrow(
      'Unsupported DB_TYPE: "oracle". Supported types: mssql, postgresql, mysql'
    );
  });
});
