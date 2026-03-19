import { toRowResult, MAX_ROWS } from "../utils/row-result";
import { QueryResult } from "../db/types";

describe("toRowResult", () => {
  it("returns columns and rows in compact format", () => {
    const result: QueryResult = {
      rows: [
        { id: 1, name: "Alice" },
        { id: 2, name: "Bob" },
      ],
      totalCount: 2,
    };

    const rowResult = toRowResult(result, 0, 50);

    expect(rowResult.columns).toEqual(["id", "name"]);
    expect(rowResult.rows).toEqual([
      [1, "Alice"],
      [2, "Bob"],
    ]);
    expect(rowResult.meta.totalCount).toBe(2);
    expect(rowResult.meta.returnedCount).toBe(2);
    expect(rowResult.meta.skip).toBe(0);
    expect(rowResult.meta.take).toBe(50);
  });

  it("caps rows at MAX_ROWS (100)", () => {
    const manyRows = Array.from({ length: 150 }, (_, i) => ({ id: i }));
    const result: QueryResult = { rows: manyRows, totalCount: 150 };

    const rowResult = toRowResult(result, 0, 150);

    expect(rowResult.rows.length).toBe(MAX_ROWS);
    expect(rowResult.meta.returnedCount).toBe(MAX_ROWS);
    expect(rowResult.meta.totalCount).toBe(150);
  });

  it("preserves totalCount from result even when rows are capped", () => {
    const manyRows = Array.from({ length: 200 }, (_, i) => ({ val: i }));
    const result: QueryResult = { rows: manyRows, totalCount: 5000 };

    const rowResult = toRowResult(result, 100, 100);

    expect(rowResult.meta.totalCount).toBe(5000);
    expect(rowResult.rows.length).toBe(MAX_ROWS);
  });

  it("returns empty columns and rows for empty result set", () => {
    const result: QueryResult = { rows: [], totalCount: 0 };
    const rowResult = toRowResult(result, 0, 50);

    expect(rowResult.columns).toEqual([]);
    expect(rowResult.rows).toEqual([]);
    expect(rowResult.meta.totalCount).toBe(0);
    expect(rowResult.meta.returnedCount).toBe(0);
  });
});
