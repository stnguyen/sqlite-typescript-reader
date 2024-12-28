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

  test("get table names", async () => {
    const db = await Database.open(DB_PATH);
    const names = await db.getTableNames();
    expect(names).toEqual(["apples", "oranges"]);
    db.close();
  })

  test("count rows", async () => {
    const db = await Database.open(DB_PATH);
    expect(await db.countTableRows("apples")).toEqual(4);
    expect(await db.countTableRows("oranges")).toEqual(6);

    try {
      await db.countTableRows("notExist");
    } catch (error) {
      expect((error as any).message).toEqual("No such schema: notExist")
    }
    db.close();
  })

  test("get single column values", async () => {
    const db = await Database.open(DB_PATH);
    const values = await db.select("apples", ["name"]);
    expect(values.sort()).toEqual([["Fuji"], ["Golden Delicious"], ["Granny Smith"], ["Honeycrisp"]]);
    try {
      await db.select("apples", ["notExist"]);
    } catch (error) {
      expect((error as any).message).toEqual("No such column: notExist")
    }
    db.close();
    db.close();
  })

  test("get multi column values", async () => {
    const db = await Database.open(DB_PATH);
    const values = await db.select("apples", ["name", "color"]);
    expect(values.sort()).toEqual([["Fuji", "Red"], ["Golden Delicious", "Yellow"], ["Granny Smith", "Light Green"], ["Honeycrisp", "Blush Red"]]);
    try {
      await db.select("apples", ["name", "notExist"]);
    } catch (error) {
      expect((error as any).message).toEqual("No such column: notExist")
    }
    db.close();
    db.close();
  })

  test("get multi column values with where clause", async () => {
    const db = await Database.open(DB_PATH);
    const values = await db.select("apples", ["name", "color"], { column: "color", operator: "=", value: "Yellow"});
    expect(values.sort()).toEqual([["Golden Delicious", "Yellow"]]);
    try {
      await db.select("apples", ["name", "notExist"]);
    } catch (error) {
      expect((error as any).message).toEqual("No such column: notExist")
    }
    db.close();
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

  test("get table names", async () => {
    const db = await Database.open(DB_PATH);
    const names = await db.getTableNames();
    expect(names).toEqual([
      "Album", "Artist", "Customer", "Employee", "Genre", "Invoice", "InvoiceLine", "MediaType", "Playlist",
      "PlaylistTrack", "Track"
    ]);
    db.close();
  })

  test("count rows", async () => {
    const db = await Database.open(DB_PATH);
    expect(await db.countTableRows("Customer")).toEqual(59);
    expect(await db.countTableRows("Track")).toEqual(3503);
    db.close();
  })

  test("get single column values", async () => {
    const db = await Database.open(DB_PATH);
    const values = await db.select("Customer", ["FirstName"]);
    expect(values.sort()).toEqual([["Aaron"], ["Alexandre"], ["Astrid"], ["Bjørn"], ["Camille"], ["Daan"], ["Dan"], ["Diego"], ["Dominique"], ["Eduardo"], ["Edward"], ["Ellie"], ["Emma"], ["Enrique"], ["Fernanda"], ["Frank"], ["Frank"], ["František"], ["François"], ["Fynn"], ["Hannah"], ["Heather"], ["Helena"], ["Hugh"], ["Isabelle"], ["Jack"], ["Jennifer"], ["Joakim"], ["Johannes"], ["John"], ["João"], ["Julia"], ["Kara"], ["Kathy"], ["Ladislav"], ["Leonie"], ["Lucas"], ["Luis"], ["Luís"], ["Madalena"], ["Manoj"], ["Marc"], ["Mark"], ["Mark"], ["Martha"], ["Michelle"], ["Niklas"], ["Patrick"], ["Phil"], ["Puja"], ["Richard"], ["Robert"], ["Roberto"], ["Stanisław"], ["Steve"], ["Terhi"], ["Tim"], ["Victor"], ["Wyatt"]]);
    db.close();
  })


  test("get single column values", async () => {
    const db = await Database.open(DB_PATH);
    const values = await db.select("Customer", ["FirstName", "LastName"]);
    expect(values.sort()).toEqual([
      ["Aaron", "Mitchell"],
      ["Alexandre", "Rocha"],
      ["Astrid", "Gruber"],
      ["Bjørn", "Hansen"],
      ["Camille", "Bernard"],
      ["Daan", "Peeters"],
      ["Dan", "Miller"],
      ["Diego", "Gutiérrez"],
      ["Dominique", "Lefebvre"],
      ["Eduardo", "Martins"],
      ["Edward", "Francis"],
      ["Ellie", "Sullivan"],
      ["Emma", "Jones"],
      ["Enrique", "Muñoz"],
      ["Fernanda", "Ramos"],
      ["Frank", "Harris"],
      ["Frank", "Ralston"],
      ["František", "Wichterlová"],
      ["François", "Tremblay"],
      ["Fynn", "Zimmermann"],
      ["Hannah", "Schneider"],
      ["Heather", "Leacock"],
      ["Helena", "Holý"],
      ["Hugh", "O'Reilly"],
      ["Isabelle", "Mercier"],
      ["Jack", "Smith"],
      ["Jennifer", "Peterson"],
      ["Joakim", "Johansson"],
      ["Johannes", "Van der Berg"],
      ["John", "Gordon"],
      ["João", "Fernandes"],
      ["Julia", "Barnett"],
      ["Kara", "Nielsen"],
      ["Kathy", "Chase"],
      ["Ladislav", "Kovács"],
      ["Leonie", "Köhler"],
      ["Lucas", "Mancini"],
      ["Luis", "Rojas"],
      ["Luís", "Gonçalves"],
      ["Madalena", "Sampaio"],
      ["Manoj", "Pareek"],
      ["Marc", "Dubois"],
      ["Mark", "Philips"],
      ["Mark", "Taylor"],
      ["Martha", "Silk"],
      ["Michelle", "Brooks"],
      ["Niklas", "Schröder"],
      ["Patrick", "Gray"],
      ["Phil", "Hughes"],
      ["Puja", "Srivastava"],
      ["Richard", "Cunningham"],
      ["Robert", "Brown"],
      ["Roberto", "Almeida"],
      ["Stanisław", "Wójcik"],
      ["Steve", "Murray"],
      ["Terhi", "Hämäläinen"],
      ["Tim", "Goyer"],
      ["Victor", "Stevens"],
      ["Wyatt", "Girard"]
    ]);
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