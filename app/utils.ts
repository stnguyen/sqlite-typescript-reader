
const textDecoder = new TextDecoder();

/**
 * Read a signed 64bit variable-length integer starting at offset.
 * TODO will fail for ints more than 53 bits
 * @param dataView DataView
 * @param offset offset to start reading from
 * @returns two numbers: the decoded varint, and byte position right after it
 */
export function readVarInt(dataView: DataView, offset: number): [number, number] {
    const mask = 0x7f;
    const msb = 0x80;
    let value = 0;
    for (let i = 0; i < 8; i++) {
        const byte = dataView.getUint8(offset + i);
        if (i > 0) {
            value <<= 7;
        }
        value |= byte & mask;
        
        if ((byte & msb) === 0) {
            return [value, offset + i + 1]
        }
    }

    const lastByte = dataView.getUint8(offset + 8);
    value <<= 8;
    value |= lastByte;
    return [value, offset + 9];
}

/**
 * Decode a string value.
 * TODO assuming UTF-8 for now.
 * Need to support different string encoding https://www.sqlite.org/fileformat2.html#enc
 * @param dataView 
 * @param offset 
 * @param size 
 * @returns 
 */
export function decodeString(dataView: DataView, offset: number, size: number): string {
    return textDecoder.decode(dataView.buffer.slice(offset, offset + size));
}

/**
 * Export column names from a schema sql
 */
export function parseColumnsFromSchemaSQL(sql: string): string[] {
    // TODO need a much better parser

    // Sample SQL:
    // CREATE TABLE apples
    // (
    //     id integer primary key autoincrement,
    //     name text,
    //     color text
    // );
    const regex = /CREATE TABLE [\["]?\w+[\]"]?\s*\(\s*(?<columnDefinitions>(.+\s*)+)/;
    const match = sql.trim().match(regex);
    if (!match || !match.groups) {
        throw new Error("Failed to parse SQL");
    }
    const { columnDefinitions } = match.groups as any;
    const lines = (columnDefinitions as string).split(",").map(l => l.trim());
    const constraintStartIdx = lines.findIndex(l => l.startsWith("CONSTRAINT"));
    return lines.slice(0, constraintStartIdx > -1 ? constraintStartIdx : undefined).map(l => l.split(/\s+/)[0].replace("[", "").replace("]", ""));
}