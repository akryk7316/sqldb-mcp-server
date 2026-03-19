import * as fs from "fs";
import * as os from "os";
import * as path from "path";
import { writeQueryResultToFile } from "../utils/export-writer";

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

async function* makeRows(
  rows: Record<string, unknown>[]
): AsyncGenerator<Record<string, unknown>> {
  for (const row of rows) {
    yield row;
  }
}

async function readFile(filepath: string): Promise<string> {
  return fs.promises.readFile(filepath, "utf8");
}

function tmpFile(name: string): string {
  return path.join(os.tmpdir(), `export-writer-test-${name}-${Date.now()}`);
}

// ---------------------------------------------------------------------------
// CSV tests
// ---------------------------------------------------------------------------

describe("writeQueryResultToFile – CSV", () => {
  it("writes header and rows", async () => {
    const fp = tmpFile("basic.csv");
    const result = await writeQueryResultToFile(
      makeRows([
        { id: 1, name: "Alice" },
        { id: 2, name: "Bob" },
      ]),
      fp,
      "csv"
    );

    expect(result.rowCount).toBe(2);
    expect(result.format).toBe("csv");
    expect(result.filepath).toBe(fp);

    const content = await readFile(fp);
    expect(content).toBe("id,name\n1,Alice\n2,Bob\n");

    await fs.promises.unlink(fp);
  });

  it("writes empty CSV (no rows, no header)", async () => {
    const fp = tmpFile("empty.csv");
    const result = await writeQueryResultToFile(makeRows([]), fp, "csv");

    expect(result.rowCount).toBe(0);
    const content = await readFile(fp);
    expect(content).toBe("");

    await fs.promises.unlink(fp);
  });

  it("uses custom delimiter", async () => {
    const fp = tmpFile("tab.csv");
    await writeQueryResultToFile(
      makeRows([{ a: 1, b: 2 }]),
      fp,
      "csv",
      { delimiter: "\t" }
    );

    const content = await readFile(fp);
    expect(content).toBe("a\tb\n1\t2\n");

    await fs.promises.unlink(fp);
  });

  it("quotes values that contain the delimiter", async () => {
    const fp = tmpFile("quote.csv");
    await writeQueryResultToFile(
      makeRows([{ value: "hello,world" }]),
      fp,
      "csv"
    );

    const content = await readFile(fp);
    expect(content).toBe('value\n"hello,world"\n');

    await fs.promises.unlink(fp);
  });

  it("doubles inner double-quotes during quoting", async () => {
    const fp = tmpFile("dquote.csv");
    await writeQueryResultToFile(
      makeRows([{ value: 'say "hi"' }]),
      fp,
      "csv"
    );

    const content = await readFile(fp);
    expect(content).toBe('value\n"say ""hi"""\n');

    await fs.promises.unlink(fp);
  });

  it("writes null/undefined with nullValue option", async () => {
    const fp = tmpFile("null.csv");
    await writeQueryResultToFile(
      makeRows([{ a: null, b: undefined, c: 0 }]),
      fp,
      "csv",
      { nullValue: "NULL" }
    );

    const content = await readFile(fp);
    expect(content).toBe("a,b,c\nNULL,NULL,0\n");

    await fs.promises.unlink(fp);
  });

  it("prepends BOM when bom option is true", async () => {
    const fp = tmpFile("bom.csv");
    await writeQueryResultToFile(
      makeRows([{ x: 1 }]),
      fp,
      "csv",
      { bom: true }
    );

    const raw = await fs.promises.readFile(fp);
    // BOM is the UTF-8 byte sequence EF BB BF
    expect(raw[0]).toBe(0xef);
    expect(raw[1]).toBe(0xbb);
    expect(raw[2]).toBe(0xbf);

    await fs.promises.unlink(fp);
  });

  it("creates parent directories if they do not exist", async () => {
    const dir = path.join(os.tmpdir(), `export-test-mkdir-${Date.now()}`);
    const fp = path.join(dir, "sub", "out.csv");

    await writeQueryResultToFile(makeRows([{ v: 1 }]), fp, "csv");

    const content = await readFile(fp);
    expect(content).toBe("v\n1\n");

    await fs.promises.rm(dir, { recursive: true });
  });
});

// ---------------------------------------------------------------------------
// JSON tests
// ---------------------------------------------------------------------------

describe("writeQueryResultToFile – JSON", () => {
  it("writes a JSON array", async () => {
    const fp = tmpFile("basic.json");
    const result = await writeQueryResultToFile(
      makeRows([
        { id: 1, name: "Alice" },
        { id: 2, name: "Bob" },
      ]),
      fp,
      "json"
    );

    expect(result.rowCount).toBe(2);
    expect(result.format).toBe("json");

    const content = await readFile(fp);
    const parsed = JSON.parse(content) as unknown[];
    expect(parsed).toEqual([
      { id: 1, name: "Alice" },
      { id: 2, name: "Bob" },
    ]);

    await fs.promises.unlink(fp);
  });

  it("writes an empty JSON array for no rows", async () => {
    const fp = tmpFile("empty.json");
    await writeQueryResultToFile(makeRows([]), fp, "json");

    const content = await readFile(fp);
    expect(content).toBe("[]");

    await fs.promises.unlink(fp);
  });

  it("pretty-prints when pretty option is true", async () => {
    const fp = tmpFile("pretty.json");
    await writeQueryResultToFile(
      makeRows([{ a: 1 }]),
      fp,
      "json",
      { pretty: true }
    );

    const content = await readFile(fp);
    // Pretty-printed output should contain newlines/spaces
    expect(content).toContain("\n");
    const parsed = JSON.parse(content) as unknown[];
    expect(parsed).toEqual([{ a: 1 }]);

    await fs.promises.unlink(fp);
  });
});

// ---------------------------------------------------------------------------
// Abort / error handling
// ---------------------------------------------------------------------------

describe("writeQueryResultToFile – abort and error handling", () => {
  it("removes partial file and rejects when AbortSignal is pre-aborted", async () => {
    const fp = tmpFile("aborted.csv");
    const controller = new AbortController();
    controller.abort();

    await expect(
      writeQueryResultToFile(makeRows([{ v: 1 }]), fp, "csv", {}, controller.signal)
    ).rejects.toThrow();

    // Partial file should have been cleaned up
    await expect(fs.promises.access(fp)).rejects.toThrow();
  });

  it("removes partial file when the source iterable throws", async () => {
    const fp = tmpFile("error.csv");

    async function* errorAfterFirst() {
      yield { a: 1 };
      throw new Error("stream error");
    }

    await expect(
      writeQueryResultToFile(errorAfterFirst(), fp, "csv")
    ).rejects.toThrow("stream error");

    await expect(fs.promises.access(fp)).rejects.toThrow();
  });
});
