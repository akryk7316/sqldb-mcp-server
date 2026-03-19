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
});
