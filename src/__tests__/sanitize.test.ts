import { validateSQL } from "../utils/sanitize";

describe("validateSQL", () => {
  describe("valid SELECT statements", () => {
    const validStatements = [
      "SELECT 1",
      "SELECT * FROM users",
      "SELECT id, name FROM users WHERE id = 1",
      "SELECT u.id, o.total FROM users u JOIN orders o ON u.id = o.user_id",
      "SELECT COUNT(*) FROM products",
      "SELECT TOP 10 * FROM logs ORDER BY created_at DESC",
      "SELECT name, (SELECT COUNT(*) FROM orders WHERE user_id = u.id) as order_count FROM users u",
      "WITH cte AS (SELECT id FROM users) SELECT * FROM cte",
    ];

    test.each(validStatements)("allows: %s", (sql) => {
      expect(() => validateSQL(sql)).not.toThrow();
    });
  });

  describe("blocked non-SELECT statements", () => {
    const blockedStatements: Array<[string, string]> = [
      ["INSERT INTO users VALUES (1)", "insert"],
      ["UPDATE users SET name = 'x' WHERE id = 1", "update"],
      ["DELETE FROM users WHERE id = 1", "delete"],
      ["DROP TABLE users", "drop"],
      ["CREATE TABLE t (id INT)", "create"],
      ["ALTER TABLE users ADD COLUMN age INT", "alter"],
    ];

    test.each(blockedStatements)("blocks: %s", (sql) => {
      expect(() => validateSQL(sql)).toThrow();
    });
  });

  it("throws on empty SQL", () => {
    expect(() => validateSQL("")).toThrow("SQL statement must not be empty");
    expect(() => validateSQL("   ")).toThrow("SQL statement must not be empty");
  });

  describe("dialect selection via DB_TYPE", () => {
    const originalDbType = process.env.DB_TYPE;

    afterEach(() => {
      if (originalDbType === undefined) {
        delete process.env.DB_TYPE;
      } else {
        process.env.DB_TYPE = originalDbType;
      }
    });

    it("uses PostgreSQL dialect when DB_TYPE=postgresql", () => {
      process.env.DB_TYPE = "postgresql";
      // PostgreSQL-specific cast syntax; TransactSQL dialect would reject this
      expect(() => validateSQL("SELECT id::text FROM users")).not.toThrow();
    });

    it("uses MySQL dialect when DB_TYPE=mysql", () => {
      process.env.DB_TYPE = "mysql";
      expect(() => validateSQL("SELECT * FROM users")).not.toThrow();
    });

    it("uses TransactSQL dialect when DB_TYPE=mssql (default)", () => {
      process.env.DB_TYPE = "mssql";
      expect(() => validateSQL("SELECT TOP 10 * FROM logs")).not.toThrow();
    });

    it("uses TransactSQL dialect when DB_TYPE is unset", () => {
      delete process.env.DB_TYPE;
      expect(() => validateSQL("SELECT TOP 10 * FROM logs")).not.toThrow();
    });

    it("blocks non-SELECT regardless of dialect", () => {
      for (const dbType of ["mssql", "postgresql", "mysql"]) {
        process.env.DB_TYPE = dbType;
        expect(() => validateSQL("DELETE FROM users WHERE id = 1")).toThrow();
        expect(() => validateSQL("INSERT INTO users VALUES (1)")).toThrow();
      }
    });
  });
});

