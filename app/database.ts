import { open } from 'fs/promises';
import type { FileHandle } from 'fs/promises';
import { constants } from 'fs';

interface DatabaseHeader {
    pageSize: number
}

export enum PageType {
    InteriorIndex = 2,
    InteriorTable = 5,
    LeafIndex = 10,
    LeafTable = 13
}

interface PageHeader {
    pageType: PageType,
    startFreeBlock: number,
    numCells: number,
    startCellConcentArea: number,
    numFragmentedFreeBytes: number,
    rightMostPointer?: number
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
        return { pageSize }
    }

    async parsePageHeader(pageNumber: number): Promise<PageHeader> {
        // TODO reuse this buffer. We properly needs a PageManager to page in-out pages, with LRU cache.
        const buffer: Uint8Array = new Uint8Array(this.header.pageSize);
        await this.fileHandle.read(buffer, this.header.pageSize * (pageNumber - 1), buffer.length, 0);

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
        const rightMostPointer = pageType <= PageType.InteriorTable ? dataView.getUint32(8 + byteOffset) : undefined;

        return {
            pageType,
            startFreeBlock,
            numCells,
            startCellConcentArea: startCellConcentArea > 0 ? startCellConcentArea : 65536,
            numFragmentedFreeBytes,
            rightMostPointer
        }
    }

    async close() {
        await this.fileHandle.close();
    }
}