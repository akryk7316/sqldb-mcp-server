# sqldb-mcp-server

A **read-only** [Model Context Protocol (MCP)](https://modelcontextprotocol.io/) server that exposes SQL database access to LLMs.

## Features

- **Multi-database** – supports **MSSQL**, **PostgreSQL**, and **MySQL**
- **Read-only** – only `SELECT` statements are allowed (enforced via AST-level SQL parsing with the correct dialect per DB type)
- **LLM-optimised** – results use a compact columnar format (column list + value rows) to reduce token usage
- **Pagination** – `skip` / `take` parameters with automatic cap at 100 rows
- **Total-count aware** – every query result includes `meta.totalCount` so the LLM knows how many rows exist
- **Caching** – query / schema results are cached with a configurable TTL
- **File export** – stream query results to CSV or JSON files without a row-count limit
- **Five MCP tools**: `query`, `listTables`, `describeTable`, `explainQuery`, `exportQuery`

## Installation

### From npm (recommended)

```bash
# Install globally
npm install -g @akryk7316/sqldb-mcp-server

# Or run directly with npx (no install needed)
npx @akryk7316/sqldb-mcp-server
```

### From source

```bash
git clone https://github.com/akryk7316/sqldb-mcp-server.git
cd sqldb-mcp-server
npm install
npm run build
```

## Quick Start

```bash
# 1. Install globally
npm install -g @akryk7316/sqldb-mcp-server

# 2. Configure environment variables (see below)
export DB_TYPE=postgresql
export DB_HOST=localhost
export DB_USER=myuser
export DB_PASSWORD=mypassword
export DB_NAME=mydb

# 3. Run
sqldb-mcp-server
```

Or use in your MCP client configuration (e.g. Claude Desktop `claude_desktop_config.json`):

```json
{
  "mcpServers": {
    "sqldb": {
      "command": "npx",
      "args": ["-y", "@akryk7316/sqldb-mcp-server"],
      "env": {
        "DB_TYPE": "postgresql",
        "DB_HOST": "localhost",
        "DB_PORT": "5432",
        "DB_USER": "myuser",
        "DB_PASSWORD": "mypassword",
        "DB_NAME": "mydb"
      }
    }
  }
}
```

## Environment Variables

| Variable | Default | Description |
|---|---|---|
| `DB_TYPE` | `mssql` | Database type: `mssql`, `postgresql`, or `mysql` |
| `DB_HOST` | – | Database server hostname |
| `DB_PORT` | `1433` / `5432` / `3306` | Database server port (default depends on `DB_TYPE`) |
| `DB_USER` | – | Database username |
| `DB_PASSWORD` | – | Database password |
| `DB_NAME` | – | Database name |
| `DB_QUERY_TIMEOUT` | `30000` | Query timeout in milliseconds (used by `query` / `explainQuery`) |
| `EXPORT_QUERY_TIMEOUT` | `300000` | Export query timeout in milliseconds (used by `exportQuery`; default 5 min) |
| `CACHE_TTL` | `60` | Cache TTL in seconds |

### Default ports by DB type

| `DB_TYPE` | Default `DB_PORT` |
|---|---|
| `mssql` | `1433` |
| `postgresql` | `5432` |
| `mysql` | `3306` |

## MCP Tools

### `query`

Execute a `SELECT` SQL statement.

```json
{
  "sql": "SELECT id, name FROM users WHERE active = 1",
  "skip": 0,
  "take": 10
}
```

Response format (compact / token-efficient):
```json
{
  "meta": { "totalCount": 42, "returnedCount": 10, "skip": 0, "take": 10 },
  "columns": ["id", "name"],
  "rows": [[1, "Alice"], [2, "Bob"], ...]
}
```

### `listTables`

List all base tables in the database.

```json
[{ "schema": "dbo", "name": "users" }, ...]
```

### `describeTable`

Describe a table's columns, indexes, foreign keys, check constraints, and size statistics.

```json
{ "table": "dbo.users" }
```

### `explainQuery`

Return the estimated execution plan for a SELECT query without executing it.

```json
{ "sql": "SELECT * FROM orders WHERE status = 'open'" }
```

### `exportQuery`

Stream a SELECT query result to a file.  Designed for large datasets – there is no row-count limit and results are written directly to disk using Node.js streams.

```json
{
  "sql": "SELECT * FROM large_table",
  "filepath": "/tmp/export.csv",
  "format": "csv",
  "options": { "delimiter": ",", "bom": false }
}
```

`format` defaults to `"csv"` if omitted.  `"json"` is also supported.

**CSV options** (all optional):

| Option | Default | Description |
|---|---|---|
| `delimiter` | `","` | Column separator |
| `nullValue` | `""` | String to write for `NULL` / `undefined` cells |
| `bom` | `false` | Prepend UTF-8 BOM (useful for Excel) |

**JSON options** (all optional):

| Option | Default | Description |
|---|---|---|
| `pretty` | `false` | Indent the output JSON |

Response format:
```json
{
  "filepath": "/tmp/export.csv",
  "format": "csv",
  "rowCount": 50000
}
```

The tool uses a separate, longer-lived connection pool whose `requestTimeout` is controlled by `EXPORT_QUERY_TIMEOUT` (default 300 000 ms = 5 min).  Increase this value for very large exports.

## Development

```bash
# Clone the repository
git clone https://github.com/akryk7316/sqldb-mcp-server.git
cd sqldb-mcp-server

# Install dependencies
npm install

# Configure environment
cp .env.example .env
# Edit .env with your DB credentials

# Run in dev mode (no compile step)
npm run dev

# Or build and run
npm run build
npm start

# Run unit tests
npm test
```

## Project Structure

```
src/
  mcp/
    server.ts           # MCP server entry point
    tools/
      query.ts          # query tool
      listTables.ts     # listTables tool
      describeTable.ts  # describeTable tool
      explainQuery.ts   # explainQuery tool
      exportQuery.ts    # exportQuery tool (streaming file export)
  db/
    index.ts            # DB adapter factory (selects adapter from DB_TYPE)
    types.ts            # DB interfaces (including queryStream)
    adapters/
      mssql.ts          # Microsoft SQL Server implementation
      postgresql.ts     # PostgreSQL implementation (pg + pg-cursor)
      mysql.ts          # MySQL implementation (mysql2)
  utils/
    row-result.ts       # Compact columnar result format
    sanitize.ts         # AST-based SQL read-only validation (dialect-aware)
    pagination.ts       # skip/take normalisation
    cache.ts            # TTL in-memory cache
    export-writer.ts    # Streaming CSV / JSON file writer
  __tests__/            # Unit tests
```
