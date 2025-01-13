import { describe, it, expect } from "bun:test";
import { parseTableSchemaSQL, parseIndexSchemaSQL } from "../app/parsers";

describe("parseTableSchemaSQL", () => {
  it("sample.db > apples", () => {
    const parsed = parseTableSchemaSQL(`
      CREATE TABLE apples
      (
        id integer primary key autoincrement,
        name text unique,
        color text
      );
      `)
    expect(parsed.columns).toEqual(["id", "name", "color"])
    expect(parsed.integerPrimaryKeyColIndex).toEqual(0)
    expect(parsed.autoIndexColumnToNMap).toEqual(new Map([["name", 1]]))
  })

  it("Chinook_Sqlite.sqlite > Customer", () => {
    expect(parseTableSchemaSQL(`
CREATE TABLE [Customer]
(
    [CustomerId] INTEGER  NOT NULL,
    [FirstName] NVARCHAR(40)  NOT NULL,
    [LastName] NVARCHAR(20)  NOT NULL,
    [Company] NVARCHAR(80),
    [Address] NVARCHAR(70),
    [City] NVARCHAR(40),
    [State] NVARCHAR(40),
    [Country] NVARCHAR(40),
    [PostalCode] NVARCHAR(10),
    [Phone] NVARCHAR(24),
    [Fax] NVARCHAR(24),
    [Email] NVARCHAR(60)  NOT NULL,
    [SupportRepId] INTEGER,
    CONSTRAINT [PK_Customer] PRIMARY KEY  ([CustomerId]),
    FOREIGN KEY ([SupportRepId]) REFERENCES [Employee] ([EmployeeId])
		ON DELETE NO ACTION ON UPDATE NO ACTION
);
`).columns).toEqual(["CustomerId", "FirstName", "LastName", "Company", "Address", "City", "State", "Country", "PostalCode", "Phone", "Fax", "Email", "SupportRepId"])
  })

  it("generated", () => {
    expect(parseTableSchemaSQL(`
CREATE TABLE apples (id integer primary key autoincrement, name text,   color text);
`).columns).toEqual(["id", "name", "color"])
  })

  it("superheroes", () => {
    expect(parseTableSchemaSQL(`
CREATE TABLE "superheroes" (id integer primary key autoincrement, name text not null, eye_color text, hair_color text, appearance_count integer, first_appearance text, first_appearance_year text)
`).columns).toEqual(["id", "name", "eye_color", "hair_color", "appearance_count", "first_appearance", "first_appearance_year"])
  })
})


describe("parseIndexSchemaSQL", () => {
  it("companies", () => {
    expect(parseIndexSchemaSQL(`
CREATE INDEX idx_companies_country
        on companies (country);
`)).toEqual({
      table: "companies",
      columns: ["country"]
    })
  })
})