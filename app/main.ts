import { open } from 'fs/promises';
import { constants } from 'fs';
import type { FileHandle } from 'fs/promises';
import type { DatabaseHeader } from './types'

const args = process.argv;
const databaseFilePath: string = args[2]
const command: string = args[3];

async function parseDatabaseHeader(fileHandle: FileHandle) : Promise<DatabaseHeader> {
    const buffer: Uint8Array = new Uint8Array(100);
    await fileHandle.read(buffer, 0, buffer.length, 0);

    // Ues a DataView to read/write data in a raw binary buffer
    const dataView = new DataView(buffer.buffer, 0, buffer.byteLength);
    
    // The page size for a database file is determined by the 2-byte integer
    // located at an offset of 16 bytes from the beginning of the database file.
    const pageSize = dataView.getUint16(16);
    return { pageSize }
}

function parsePageHeader(dataView: DataView) {

}

if (command === ".dbinfo") {
    const fileHandle = await open(databaseFilePath, constants.O_RDONLY);
    const dbHeader = await parseDatabaseHeader(fileHandle)
    console.log(`database page size: ${dbHeader.pageSize}`);

    await fileHandle.close();
} else {
    throw new Error(`Unknown command ${command}`);
}
