import * as path from "path";
import { z } from "zod";
import { DBAdapter } from "../../db/types";
import { validateSQL } from "../../utils/sanitize";
import { writeQueryResultToFile, ExportFormat } from "../../utils/export-writer";

const DEFAULT_EXPORT_TIMEOUT_MS = 300_000; // 5 minutes

export const exportQueryInputSchema = {
  sql: z.string().min(1).describe("SELECT SQL statement whose results should be exported"),
  filepath: z.string().min(1).describe("Destination file path (absolute, or relative to the server working directory)"),
  format: z
    .enum(["csv", "json"])
    .optional()
    .describe('Output format. "csv" (default) or "json"'),
  options: z
    .record(z.string(), z.unknown())
    .optional()
    .describe(
      "Format-specific options. " +
      'CSV: delimiter (default ","), nullValue (default ""), bom (default false). ' +
      "JSON: pretty (default false). " +
      "Additional keys are accepted for forward compatibility."
    ),
};

type ExportQueryArgs = {
  sql: string;
  filepath: string;
  format?: "csv" | "json";
  options?: Record<string, unknown>;
};

export function registerExportQueryTool(
  server: {
    registerTool: (
      name: string,
      config: {
        description?: string;
        inputSchema: typeof exportQueryInputSchema;
      },
      handler: (args: ExportQueryArgs) => Promise<{
        content: Array<{ type: "text"; text: string }>;
      }>
    ) => void;
  },
  db: DBAdapter
): void {
  server.registerTool(
    "exportQuery",
    {
      description:
        "Execute a read-only SELECT SQL query and stream the results to a file. " +
        "Supports CSV and JSON output formats. " +
        "Designed for large datasets – results are streamed directly to disk without a row-count limit. " +
        "CSV options: delimiter (default ','), nullValue (default ''), bom (default false). " +
        "JSON options: pretty (default false). " +
        "Timeout is controlled by the EXPORT_QUERY_TIMEOUT environment variable (default: 300 s).",
      inputSchema: exportQueryInputSchema,
    },
    async ({ sql, filepath, format = "csv", options = {} }) => {
      validateSQL(sql);

      const resolvedPath = path.isAbsolute(filepath)
        ? filepath
        : path.resolve(process.cwd(), filepath);

      const exportTimeout =
        Number(process.env.EXPORT_QUERY_TIMEOUT) || DEFAULT_EXPORT_TIMEOUT_MS;

      const controller = new AbortController();
      const timeoutId = setTimeout(() => controller.abort(), exportTimeout);

      try {
        const rows = db.queryStream(sql, controller.signal);
        const result = await writeQueryResultToFile(
          rows,
          resolvedPath,
          format as ExportFormat,
          options,
          controller.signal
        );

        return {
          content: [{ type: "text", text: JSON.stringify(result) }],
        };
      } catch (err) {
        // Surface a user-friendly timeout message when the AbortSignal fired
        if (controller.signal.aborted) {
          throw new Error(
            `Export timed out after ${exportTimeout / 1000}s. ` +
              "Increase EXPORT_QUERY_TIMEOUT or narrow the query."
          );
        }
        throw err;
      } finally {
        clearTimeout(timeoutId);
      }
    }
  );
}
