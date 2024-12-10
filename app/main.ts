import { open } from 'fs/promises';
import { constants } from 'fs';

const args = process.argv;
const databaseFilePath: string = args[2]
const command: string = args[3];

if (command === ".dbinfo") {
    const databaseFileHandler = await open(databaseFilePath, constants.O_RDONLY);
    // Use an Uint8Array to read data from the file, as requested by the fs/promises API
    const buffer: Uint8Array = new Uint8Array(100);
    await databaseFileHandler.read(buffer, 0, buffer.length, 0);

    // Ues a DataView to read/write data in a raw binary buffer
    const dataView = new DataView(buffer.buffer, 0, buffer.byteLength);
    
    // The page size for a database file is determined by the 2-byte integer
    // located at an offset of 16 bytes from the beginning of the database file.
    const pageSize = dataView.getUint16(16);
    console.log(`database page size: ${pageSize}`);

    await databaseFileHandler.close();
} else {
    throw new Error(`Unknown command ${command}`);
}
