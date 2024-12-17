import { open } from 'fs/promises';
import type { FileHandle } from 'fs/promises';
import { constants } from 'fs';
import { decodeString, readVarInt } from './utils';

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

interface SerialTypeWithSize {
    type: SerialType,
    size: number
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

    async countTables(): Promise<number> {
        const _countTable = async (pageNumber: number) => {
            const page = await this.readPage(pageNumber);
            // console.debug(`_countTable(${pageNumber}) -> header `, page.header, ", startBody ", page.startBody);

            let numTables = 0;
            if (page.header.pageType === PageType.LeafTable) {
                // Each "table" row is represeneted by 1 cell in sqlite_schema leaf pages
                // Read each cell:
                //  - First varint is cell header size
                //  - Second varint is type & size of the first column which is "type"

                for(let cellIdx = 0; cellIdx < page.header.numCells; cellIdx++) {
                    const cellAddr = page.dataView.getUint16(page.startBody + cellIdx * 2);
                    const [ cellPayloadSize, rowidAddr] = readVarInt(page.dataView, cellAddr);
                    const [ rowid, payloadAddr] = readVarInt(page.dataView, rowidAddr);
                    const [ payloadHeaderSize, typeColSerialTypeAddr ] = readVarInt(page.dataView, payloadAddr);
                    const [ typeColSerialType, _ ] = readVarInt(page.dataView, typeColSerialTypeAddr);
                    const typeColSerialTypeWithSize = parseSerialTypeCode(typeColSerialType);
                    // TODO handle different string encoding https://www.sqlite.org/fileformat2.html#enc
                    // Assume it's UTF-8 for now
                    if (typeColSerialTypeWithSize.type !== SerialType.String) {
                        throw Error(`Invalid serial type for sqlite_schema.type column. Expected ${SerialType.String}, got ${typeColSerialTypeWithSize.type}`);
                    }
                    const firstColString = decodeString(page.dataView, payloadAddr + payloadHeaderSize, typeColSerialTypeWithSize.size);
                    if (firstColString === "table") {
                        numTables += 1;
                    }
                }
            } else {
                // Is an interior page, recursively visit all children
                for(let cellIdx = 0; cellIdx < page.header.numCells; cellIdx++) {
                    const cellAddr = page.dataView.getUint16(page.startBody + cellIdx * 2);
                    const leftChildPageNumber = page.dataView.getUint32(cellAddr);
                    console.log(cellAddr, leftChildPageNumber)
                    numTables += await _countTable(leftChildPageNumber);
                }
                numTables += await _countTable(page.header.rightMostPointer!);
            }
            return numTables;
        }

        return _countTable(1);
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