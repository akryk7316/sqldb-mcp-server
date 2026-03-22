import { createDBAdapter } from "../db/index";
import { MSSQLAdapter } from "../db/adapters/mssql";
import { PostgreSQLAdapter } from "../db/adapters/postgresql";
import { MySQLAdapter } from "../db/adapters/mysql";

describe("createDBAdapter", () => {
  const originalEnv = { ...process.env };

  afterEach(() => {
    process.env = { ...originalEnv };
    jest.restoreAllMocks();
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

  it("enables encrypted MSSQL connections by default", () => {
    process.env.DB_TYPE = "mssql";
    delete process.env.DB_ENCRYPT;

    const adapter = createDBAdapter() as any;

    expect(adapter.pool.config.options.encrypt).toBe(true);
    expect(adapter.pool.config.options.trustServerCertificate).toBe(true);
  });

  it("uses TLS for MySQL when DB_ENCRYPT=true", () => {
    process.env.DB_TYPE = "mysql";
    process.env.DB_ENCRYPT = "true";

    const adapter = createDBAdapter() as any;

    expect(adapter.pool.pool.config.connectionConfig.ssl).toEqual({ rejectUnauthorized: false });
  });

  it("prefers SSL for PostgreSQL by default", () => {
    process.env.DB_TYPE = "postgresql";
    delete process.env.DB_ENCRYPT;

    const adapter = createDBAdapter() as any;

    expect(adapter.pool.options.ssl).toEqual({ rejectUnauthorized: false });
  });

  it("falls back to non-SSL PostgreSQL connections when the server does not support SSL", async () => {
    process.env.DB_TYPE = "postgresql";
    delete process.env.DB_ENCRYPT;

    const adapter = createDBAdapter() as any;

    const sslPool = {
      query: jest.fn().mockRejectedValueOnce(new Error("The server does not support SSL connections")),
      end: jest.fn().mockResolvedValue(undefined),
    };
    const fallbackPool = {
      query: jest.fn().mockResolvedValue({
        rows: [{ id: 1, __total_count: 1 }],
      }),
      end: jest.fn().mockResolvedValue(undefined),
    };

    adapter.pool = sslPool;
    const createPoolSpy = jest.spyOn(adapter, "createPool").mockReturnValue(fallbackPool);

    await expect(adapter.query("SELECT 1 AS id", 0, 1)).resolves.toEqual({
      rows: [{ id: 1 }],
      totalCount: 1,
    });
    expect(createPoolSpy).toHaveBeenCalledWith(adapter.queryTimeout, false);
    expect(sslPool.end).toHaveBeenCalledTimes(1);
    expect(fallbackPool.query).toHaveBeenCalledTimes(1);
  });
});
