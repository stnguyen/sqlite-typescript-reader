import { open } from 'fs/promises';
import type { FileHandle } from 'fs/promises';
import { constants } from 'fs';
import { decodeString, parseColumnsFromSchemaSQL, readVarInt } from './utils';

interface DatabaseHeader {
    pageSize: number
    numPages: number
}

export enum PageType {
    InteriorIndex = 2,
    InteriorTable = 5,
    LeafIndex = 10,
    LeafTable = 13
}

export enum SerialType {
    Null = 0,
    Int8, Int16, Int24, Int32, Int48, Int64,
    Float,
    Const0, Const1,
    Internal1, Internal2,
    BLOB,
    String
}

type ColumnValue = null | number | string;

interface SerialTypeWithSize {
    type: SerialType,
    size: number
}

const FIRST_PAGE_NUMBER = 1;
enum SqliteSchemaColumnIndices {
    type_0 = 0,
    name_1,
    tbl_name_2,
    rootpage_3,
    sql_4
}

interface Schema {
    type: "table" | "index" | "view" | "view" | "trigger"
    name: string,
    tbl_name: string
    rootPage: number
    sql: string
}

export function parseSerialTypeCode(code: number): SerialTypeWithSize {
    let type: number;
    if (code < SerialType.Internal1) {
        type = code;
    } else if (code >= 12 && code % 2 == 0) {
        type = SerialType.BLOB;
    } else if (code >= 13 && code % 2 == 1) {
        type = SerialType.String;
    } else {
        throw Error(`Invalid serial type code: ${code}`)
    }

    let size: number = -1;
    switch (type) {
        case SerialType.Null:
        case SerialType.Const0:
        case SerialType.Const1:
            size = 0;
            break;

        case SerialType.Int8:
        case SerialType.Int16:
        case SerialType.Int24:
        case SerialType.Int32:
            size = type;
            break;

        case SerialType.Int48:
            size = 6;
            break;

        case SerialType.Int64:
        case SerialType.Float:
            size = 8;
            break;

        case SerialType.BLOB:
        case SerialType.String:
            size = (code - type) / 2;
            break;
    }
    return { type, size }
}

function isInteriorPage(pageType: PageType): boolean {
    return pageType === PageType.InteriorIndex || pageType === PageType.InteriorTable;
}

function isTablePage(pageType: PageType): boolean {
    return pageType === PageType.InteriorTable || pageType == PageType.LeafTable;
}

function readColumnValue(serialTypeWithSize: SerialTypeWithSize, dataView: DataView, byteOffset: number): ColumnValue {
    switch (serialTypeWithSize.type) {
        case SerialType.Null:
            return null;
        case SerialType.Const0:
            return 0;
        case SerialType.Const1:
            return 1;
        case SerialType.Int8:
            return dataView.getInt8(byteOffset);
        case SerialType.Int16:
            return dataView.getInt16(byteOffset);
        case SerialType.Int32:
            return dataView.getInt32(byteOffset);
        case SerialType.Float:
            return dataView.getFloat64(byteOffset);
        case SerialType.String:
            return decodeString(dataView, byteOffset, serialTypeWithSize.size);

        default:
            throw new Error(`Not implemented: reading column value of type ${serialTypeWithSize.type}`);
    }
}

interface PageHeader {
    pageType: PageType,
    startFreeBlock: number,
    numCells: number,
    startCellContent: number,
    numFragmentedFreeBytes: number,
    rightMostPointer?: number
}

interface Page {
    header: PageHeader,
    startBody: number,
    dataView: DataView
}

interface WhereClause {
    column: string,
    operator: string,
    value: ColumnValue
}

const SIZE_DB_HEADER_BYTES = 100;

/**
 * A wrapper to interact with a SQLite database file.
 */
export class Database {
    private constructor(public fileHandle: FileHandle, public readonly header: DatabaseHeader) { }

    public static async open(filePath: string): Promise<Database> {
        const fileHandle = await open(filePath, constants.O_RDONLY);
        const header = await this.parseDatabaseHeader(fileHandle);
        return new Database(fileHandle, header);
    }

    private static async parseDatabaseHeader(fileHandle: FileHandle): Promise<DatabaseHeader> {
        const buffer: Uint8Array = new Uint8Array(100);
        await fileHandle.read(buffer, 0, buffer.length, 0);

        // Ues a DataView to read/write data in a raw binary buffer
        const dataView = new DataView(buffer.buffer, 0, buffer.byteLength);

        // The page size for a database file is determined by the 2-byte integer
        // located at an offset of 16 bytes from the beginning of the database file.
        const pageSize = dataView.getUint16(16);

        const numPages = dataView.getUint32(28);
        return { pageSize, numPages }
    }

    /**
     * Scan through leaf table pages
     * @param rootPageNumber root page number
     * @param leafTableReader callback to read each leaf table
     */
    private async scanTable(rootPageNumber: number, leafTableReader: (leafPage: Page) => void) {
        const _scanTablePage = async (pageNumber: number) => {
            const page = await this.readPage(pageNumber);
            // console.debug(`_countTable(${pageNumber}) -> header `, page.header, ", startBody ", page.startBody);

            if (page.header.pageType === PageType.LeafTable) {
                leafTableReader(page)
            } else if (page.header.pageType === PageType.InteriorTable) {
                for (let cellIdx = 0; cellIdx < page.header.numCells; cellIdx++) {
                    const cellAddr = page.dataView.getUint16(page.startBody + cellIdx * 2);
                    const leftChildPageNumber = page.dataView.getUint32(cellAddr);
                    await _scanTablePage(leftChildPageNumber);
                }
                await _scanTablePage(page.header.rightMostPointer!);
            } else {
                throw new Error(`Invalid page type encountered: page ${pageNumber} is of type ${page.header.pageType}, expected a table page`);
            }
        }

        return _scanTablePage(rootPageNumber);
    }

    private readCellColumns(page: Page, cellIdx: number, colIndicies: number[]): ColumnValue[] {
        const toColIdx = Math.max(...colIndicies)
        const values = new Array<ColumnValue>(colIndicies.length)
        const colIndexToValueSlotMap = colIndicies.reduce((map, colIdx, valSlot) => {
            if (map.has(colIdx)) {
                map.get(colIdx)?.push(valSlot);
            } else {
                map.set(colIdx, [valSlot]);
            }
            return map
        }, new Map<number, number[]>())

        const cellAddr = page.dataView.getUint16(page.startBody + cellIdx * 2);
        const [cellPayloadSize, rowidAddr] = readVarInt(page.dataView, cellAddr);
        const [rowid, payloadAddr] = readVarInt(page.dataView, rowidAddr);
        const [payloadHeaderSize, firstColSerialTypeAddr] = readVarInt(page.dataView, payloadAddr);
        let colSerialTypeAddr = firstColSerialTypeAddr;
        let byteOffset = payloadAddr + payloadHeaderSize;
        let serialTypeWithSize: SerialTypeWithSize | undefined;
        for (let i = 0; i <= toColIdx; i++) {
            const result = readVarInt(page.dataView, colSerialTypeAddr);
            serialTypeWithSize = parseSerialTypeCode(result[0]);
            if (colIndexToValueSlotMap.has(i)) {
                const value = readColumnValue(serialTypeWithSize, page.dataView, byteOffset);
                colIndexToValueSlotMap.get(i)!.forEach(slot => { values[slot] = value });
            }
            byteOffset += serialTypeWithSize.size;
            colSerialTypeAddr = result[1];
        }
        return values;
    }

    private async readSchema(schemaName: string): Promise<Schema> {
        let schema: Schema | undefined;
        await this.scanTable(FIRST_PAGE_NUMBER, (leafPage: Page) => {
            for (let cellIdx = 0; cellIdx < leafPage.header.numCells; cellIdx++) {
                const [type, name, tbl_name, rootPage, sql] = this.readCellColumns(leafPage, cellIdx, [
                    SqliteSchemaColumnIndices.type_0,
                    SqliteSchemaColumnIndices.name_1,
                    SqliteSchemaColumnIndices.tbl_name_2,
                    SqliteSchemaColumnIndices.rootpage_3,
                    SqliteSchemaColumnIndices.sql_4]) as [string, string, string, number, string];
                if (schemaName === name) {
                    schema = { type, name, tbl_name, rootPage, sql } as Schema;
                }
            }
        })
        if (!schema) {
            throw new Error(`No such schema: ${schemaName}`);
        }
        return schema;
    }

    async getTableNames(): Promise<string[]> {
        const tableNames: string[] = []
        await this.scanTable(FIRST_PAGE_NUMBER, (leafPage: Page) => {
            for (let cellIdx = 0; cellIdx < leafPage.header.numCells; cellIdx++) {
                const [type, name] = this.readCellColumns(leafPage, cellIdx, [SqliteSchemaColumnIndices.type_0, SqliteSchemaColumnIndices.name_1]) as [string, string];
                if (type === "table" && !name.startsWith("sqlite_")) {
                    tableNames.push(name)
                }
            }
        })
        return tableNames;
    }

    async countTableRows(tableName: string): Promise<number> {
        const schema = await this.readSchema(tableName);

        let numRows = 0;
        await this.scanTable(schema.rootPage, (leafPage: Page) => {
            numRows += leafPage.header.numCells;
        })
        return numRows;
    }

    async select(tableName: string, columnNames: string[], where?: WhereClause): Promise<ColumnValue[][]> {
        if (where && where.operator !== '=') {
            throw new Error(`where.operator is not supported: ${where.operator}`)
        }
        
        const schema = await this.readSchema(tableName);
        const schemaColumns = parseColumnsFromSchemaSQL(schema.sql);
        const readingColumns = where ? [...columnNames, where.column] : columnNames;

        const columnIndicies = readingColumns.map(colName => {
            const idx = schemaColumns.indexOf(colName)
            if (idx === -1) {
                throw new Error(`No such column: ${colName}`)
            }
            return idx
        })
        console.debug(columnIndicies)

        const values: ColumnValue[][] = [];
        await this.scanTable(schema.rootPage, (leafPage: Page) => {
            for (let cellIdx = 0; cellIdx < leafPage.header.numCells; cellIdx++) {
                const cellValues = this.readCellColumns(leafPage, cellIdx, columnIndicies);
                if (where) {
                    // Last column is for filtering
                    const whereValue = cellValues[cellValues.length - 1];
                    if (whereValue !== where.value) {
                        continue;
                    }
                    console.debug('cellValues before', cellValues);
                    cellValues.pop();
                    console.debug('cellValues after', cellValues);
                }
                values.push(cellValues);
            }
        })

        return values;
    }

    async countTables(): Promise<number> {
        let numTables = 0;
        await this.scanTable(FIRST_PAGE_NUMBER, (leafPage: Page) => {
            for (let cellIdx = 0; cellIdx < leafPage.header.numCells; cellIdx++) {
                const [type] = this.readCellColumns(leafPage, cellIdx, [SqliteSchemaColumnIndices.type_0]) as [string];
                if (type === "table") {
                    numTables += 1;
                }
            }
        });

        return numTables;
    }

    async readPage(pageNumber: number): Promise<Page> {
        const buffer: Uint8Array = new Uint8Array(this.header.pageSize);
        await this.fileHandle.read(buffer, 0, buffer.length, this.header.pageSize * (pageNumber - 1));

        // print first 100 bytes in hexa
        // console.debug(buffer.slice(0, 100).reduce((acc, v) => acc + v.toString(16).padStart(2, '0'), ''))

        const dataView = new DataView(buffer.buffer, 0, buffer.byteLength);

        const byteOffset = pageNumber === 1 ? SIZE_DB_HEADER_BYTES : 0;
        const pageType = dataView.getUint8(0 + byteOffset) as PageType;
        if (!(pageType in PageType)) {
            throw new TypeError(`Unknown page type: ${pageType}`);
        }

        const startFreeBlock = dataView.getUint16(1 + pageType);
        const numCells = dataView.getUint16(3 + byteOffset);
        const startCellConcentArea = dataView.getUint16(5 + byteOffset);
        const numFragmentedFreeBytes = dataView.getUint8(7 + byteOffset);
        const rightMostPointer = isInteriorPage(pageType) ? dataView.getUint32(8 + byteOffset) : undefined;

        return {
            header: {
                pageType,
                startFreeBlock,
                numCells,
                startCellContent: startCellConcentArea > 0 ? startCellConcentArea : 65536,
                numFragmentedFreeBytes,
                rightMostPointer
            },
            startBody: byteOffset + (isInteriorPage(pageType) ? 12 : 8),
            dataView
        }
    }

    async close() {
        await this.fileHandle.close();
    }
}