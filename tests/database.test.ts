import { describe, expect, test } from "bun:test";
import { Database, PageType, parseSerialTypeCode, SerialType } from "../app/database";

describe("sample.db", async () => {
    const DB_PATH = "tests/sample.db";

    test("read header on open", async () => {
        const db = await Database.open(DB_PATH);
        expect(db.header).toBeObject();
        expect(db.header.pageSize).toEqual(4096);
        db.close();
    })

    test("read page 1", async () => {
        const db = await Database.open(DB_PATH);
        const p1 = await db.readPage(1);

        expect(p1).toBeObject();
        expect(p1.header.pageType).toEqual(PageType.LeafTable);

        db.close();
    })
    
    test("count tables", async () => {
        const db = await Database.open(DB_PATH);
        expect(await db.countTables()).toEqual(3);
        db.close();
    })
})

describe("Chinook_Sqlite.sqlite", async () => {
    const DB_PATH = "tests/Chinook_Sqlite.sqlite";

    test("read page 1", async () => {
        const db = await Database.open(DB_PATH);
        const p1 = await db.readPage(1);

        expect(p1).toBeObject();
        expect(p1.header.pageType).toEqual(PageType.InteriorTable);

        db.close();
    })

    test("count tables", async () => {
        const db = await Database.open(DB_PATH);
        expect(await db.countTables()).toEqual(11);
        db.close();
    })
})

describe('parseSerialTypeCode', () => {
  test('should correctly parse NULL type', () => {
    const result = parseSerialTypeCode(0);
    expect(result).toEqual({ type: SerialType.Null, size: 0 });
  });

  test('should correctly parse 8-bit signed integer', () => {
    const result = parseSerialTypeCode(1);
    expect(result).toEqual({ type: SerialType.Int8, size: 1 });
  });

  test('should correctly parse 16-bit signed integer', () => {
    const result = parseSerialTypeCode(2);
    expect(result).toEqual({ type: SerialType.Int16, size: 2 });
  });

  test('should correctly parse 24-bit signed integer', () => {
    const result = parseSerialTypeCode(3);
    expect(result).toEqual({ type: SerialType.Int24, size: 3 });
  });

  test('should correctly parse 32-bit signed integer', () => {
    const result = parseSerialTypeCode(4);
    expect(result).toEqual({ type: SerialType.Int32, size: 4 });
  });

  test('should correctly parse 48-bit signed integer', () => {
    const result = parseSerialTypeCode(5);
    expect(result).toEqual({ type: SerialType.Int48, size: 6 });
  });

  test('should correctly parse 64-bit signed integer', () => {
    const result = parseSerialTypeCode(6);
    expect(result).toEqual({ type: SerialType.Int64, size: 8 });
  });

  test('should correctly parse 64-bit floating point number', () => {
    const result = parseSerialTypeCode(7);
    expect(result).toEqual({ type: SerialType.Float, size: 8 });
  });

  test('should correctly parse constant 0', () => {
    const result = parseSerialTypeCode(8);
    expect(result).toEqual({ type: SerialType.Const0, size: 0 });
  });

  test('should correctly parse constant 1', () => {
    const result = parseSerialTypeCode(9);
    expect(result).toEqual({ type: SerialType.Const1, size: 0 });
  });

  test('should correctly parse BLOB type', () => {
    const result = parseSerialTypeCode(12);
    expect(result).toEqual({ type: SerialType.BLOB, size: 0 });
  });

  test('should correctly parse String type', () => {
    const result = parseSerialTypeCode(13);
    expect(result).toEqual({ type: SerialType.String, size: 0 });
  });

  test('should correctly parse larger BLOB type', () => {
    const result = parseSerialTypeCode(14);
    expect(result).toEqual({ type: SerialType.BLOB, size: 1 });
  });

  test('should correctly parse larger String type', () => {
    const result = parseSerialTypeCode(15);
    expect(result).toEqual({ type: SerialType.String, size: 1 });
  });

  test('should throw an error for invalid serial type code', () => {
    expect(() => parseSerialTypeCode(10)).toThrow('Invalid serial type code: 10');
  });
});