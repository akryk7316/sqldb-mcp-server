import { Parser } from "node-sql-parser";

const parser = new Parser();

/**
 * Returns the node-sql-parser database dialect string for the current DB_TYPE.
 */
function getDialect(): string {
  const dbType = (process.env.DB_TYPE ?? "mssql").toLowerCase();
  switch (dbType) {
    case "postgresql":
      return "PostgreSQL";
    case "mysql":
      return "MySQL";
    default:
      return "TransactSQL";
  }
}

/**
 * Validates that the given SQL contains only SELECT statements.
 * Uses a proper SQL parser (node-sql-parser) to detect write operations
 * such as INSERT, UPDATE, DELETE, CREATE, DROP, ALTER, etc.
 *
 * @throws Error if the SQL contains any non-SELECT statements.
 */
export function validateSQL(sqlText: string): void {
  if (!sqlText || sqlText.trim() === "") {
    throw new Error("SQL statement must not be empty");
  }

  let ast;
  try {
    ast = parser.astify(sqlText, { database: getDialect() });
  } catch (err) {
    // Parsing failed – treat as disallowed to be safe
    throw new Error(
      `SQL parsing failed: ${err instanceof Error ? err.message : String(err)}`
    );
  }

  const statements = Array.isArray(ast) ? ast : [ast];

  for (const stmt of statements) {
    if (!stmt || stmt.type !== "select") {
      throw new Error(
        `Only SELECT statements are allowed. Detected statement type: "${stmt?.type ?? "unknown"}"`
      );
    }
  }
}
