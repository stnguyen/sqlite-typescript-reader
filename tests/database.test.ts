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

    test("parse page 1 header", async () => {
        const db = await Database.open(DB_PATH);
        const p1Header = await db.parsePageHeader(1);

        expect(p1Header).toBeObject();
        expect(p1Header.pageType).toEqual(PageType.LeafTable);

        db.close();
    })
})

describe("Chinook_Sqlite.sqlite", async () => {
    const DB_PATH = "tests/Chinook_Sqlite.sqlite";

    test("parse page 1 header", async () => {
        const db = await Database.open(DB_PATH);
        const p1Header = await db.parsePageHeader(1);

        expect(p1Header).toBeObject();
        expect(p1Header.pageType).toEqual(PageType.InteriorTable);

        db.close();
    })
})