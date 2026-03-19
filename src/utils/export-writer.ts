import * as fs from "fs";
import * as path from "path";
import { Readable, Transform, TransformCallback } from "stream";
import { pipeline } from "stream/promises";

export type ExportFormat = "csv" | "json";

/**
 * Options accepted by the CSV formatter.
 * Additional keys are allowed to support future options without breaking the interface.
 */
export interface CsvExportOptions {
  /** Column delimiter. Default: "," */
  delimiter?: string;
  /** Value to write for NULL / undefined cells. Default: "" */
  nullValue?: string;
  /** Prepend UTF-8 BOM (useful for Excel). Default: false */
  bom?: boolean;
  [key: string]: unknown;
}

/**
 * Options accepted by the JSON formatter.
 * Additional keys are allowed to support future options without breaking the interface.
 */
export interface JsonExportOptions {
  /** Pretty-print the JSON output. Default: false */
  pretty?: boolean;
  [key: string]: unknown;
}

/** Union type for all format-specific options plus arbitrary future keys. */
export type ExportOptions = CsvExportOptions & JsonExportOptions;

/** Return value of a successful export operation. */
export interface ExportResult {
  filepath: string;
  format: ExportFormat;
  rowCount: number;
}

// ---------------------------------------------------------------------------
// Internal Transform streams
// ---------------------------------------------------------------------------

class CsvTransform extends Transform {
  rowCount = 0;
  private columns: string[] | null = null;
  private readonly delimiter: string;
  private readonly nullValue: string;
  private readonly bom: boolean;

  constructor(options: CsvExportOptions) {
    super({ objectMode: true });
    this.delimiter = String(options.delimiter ?? ",");
    this.nullValue = String(options.nullValue ?? "");
    this.bom = Boolean(options.bom ?? false);
  }

  _transform(row: Record<string, unknown>, _enc: BufferEncoding, callback: TransformCallback): void {
    try {
      if (this.columns === null) {
        this.columns = Object.keys(row);
        const header = this.columns
          .map((c) => this.escape(c))
          .join(this.delimiter);
        if (this.bom) {
          this.push("\uFEFF");
        }
        this.push(header + "\n");
      }

      const line = this.columns
        .map((col) => this.escape(row[col]))
        .join(this.delimiter);
      this.push(line + "\n");
      this.rowCount++;
      callback();
    } catch (err) {
      callback(err instanceof Error ? err : new Error(String(err)));
    }
  }

  private escape(value: unknown): string {
    if (value === null || value === undefined) return this.nullValue;
    const str = String(value);
    if (
      str.includes(this.delimiter) ||
      str.includes('"') ||
      str.includes("\n") ||
      str.includes("\r")
    ) {
      return '"' + str.replace(/"/g, '""') + '"';
    }
    return str;
  }
}

class JsonTransform extends Transform {
  rowCount = 0;
  private isFirst = true;
  private readonly indent: number | undefined;

  constructor(options: JsonExportOptions) {
    super({ objectMode: true });
    this.indent = Boolean(options.pretty ?? false) ? 2 : undefined;
  }

  _transform(row: Record<string, unknown>, _enc: BufferEncoding, callback: TransformCallback): void {
    try {
      const json = JSON.stringify(row, null, this.indent);
      if (this.isFirst) {
        this.push("[\n" + json);
        this.isFirst = false;
      } else {
        this.push(",\n" + json);
      }
      this.rowCount++;
      callback();
    } catch (err) {
      callback(err instanceof Error ? err : new Error(String(err)));
    }
  }

  _flush(callback: TransformCallback): void {
    this.push(this.isFirst ? "[]" : "\n]");
    callback();
  }
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/**
 * Streams rows from an async iterable and writes them to `filepath` in the
 * requested `format`.
 *
 * - Uses Node.js stream `pipeline` for proper backpressure handling.
 * - Deletes the (possibly partial) output file if an error occurs.
 * - Pass an `AbortSignal` to cancel the operation (e.g. on timeout); the
 *   signal is forwarded to `pipeline` so it tears down the stream chain
 *   cleanly.
 *
 * @param rows     Async iterable of plain-object rows (e.g. from DB adapter stream).
 * @param filepath Destination file path.  Parent directories are created automatically.
 * @param format   "csv" or "json".
 * @param options  Format-specific options (see {@link CsvExportOptions} / {@link JsonExportOptions}).
 * @param signal   Optional AbortSignal to cancel the operation mid-stream.
 */
export async function writeQueryResultToFile(
  rows: AsyncIterable<Record<string, unknown>>,
  filepath: string,
  format: ExportFormat,
  options: ExportOptions = {},
  signal?: AbortSignal
): Promise<ExportResult> {
  // Ensure the parent directory exists
  await fs.promises.mkdir(path.dirname(filepath), { recursive: true });

  const writeStream = fs.createWriteStream(filepath, { encoding: "utf8" });

  let transform: CsvTransform | JsonTransform;
  if (format === "csv") {
    transform = new CsvTransform(options);
  } else {
    transform = new JsonTransform(options);
  }

  try {
    const pipelineOptions: Parameters<typeof pipeline>[1] = signal
      ? { signal }
      : {};
    await pipeline(Readable.from(rows), transform, writeStream, pipelineOptions);
  } catch (err) {
    // Clean up partial / empty file on any error
    try {
      await fs.promises.unlink(filepath);
    } catch {
      // Ignore – file may not have been created yet
    }
    throw err;
  }

  return {
    filepath,
    format,
    rowCount: transform.rowCount,
  };
}
