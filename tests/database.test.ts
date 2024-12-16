import { describe, expect, test } from "bun:test";
import { Database, PageType, } from "../app/database";

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