
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
 * Parse table schema sql field
 */
export function parseTableSchemaSQL(sql: string): {
    columns: string[],
    integerPrimaryKeyColIndex?: number
    autoIndexColumnToNMap: Map<string, number>
} {
    // TODO need a much better parser

    // Sample SQL:
    // CREATE TABLE apples
    // (
    //     id integer primary key autoincrement,
    //     name text,
    //     color text
    // );
    const regex = /CREATE TABLE [\["]?\w+[\]"]?\s*\(\s*(?<content>(.+\s*)+)/;
    const match = sql.trim().match(regex);
    if (!match || !match.groups) {
        throw new Error("Failed to parse SQL");
    }
    const { content } = match.groups as any;
    const lines = (content as string).split(",").map(l => l.trim());
    const constraintStartIdx = lines.findIndex(l => l.match(/\s*(PRIMARY\s+KEY|FOREIGN\s+KEY|UNIQUE|CHECK|CONSTRAINT)\b/));
    const columnLines = lines.slice(0, constraintStartIdx > -1 ? constraintStartIdx : undefined);
    const columnParts = columnLines.map(l => l.split(/\s+/));
    const columns = columnParts.map(cp => cp[0].replace("[", "").replace("]", ""));

    // "Settings" include type-name and column-constraint, both optional
    // https://www.sqlite.org/syntax/column-def.html
    const columnSettings = columnParts.map(cp => cp.splice(1).join(" ").toLowerCase())
    const integerPrimaryKeyColIndex = columnSettings.findIndex(s => s.startsWith("integer primary key"));
    const autoIndexColumnToNMap = columnSettings.reduce((map, s, idx) => {
        if (s.match(/\bunique\b/s) || (s.match(/\bprimary\s+key\b/) && !s.match(/\binteger\sprimary\s+key\b/))) {
            map.set(columns[idx], map.size + 1);
        }
        return map;
    }, new Map<string, number>());

    // TODO parse table-constraint section

    return {
        columns,
        integerPrimaryKeyColIndex: integerPrimaryKeyColIndex > -1 ? integerPrimaryKeyColIndex : undefined,
        autoIndexColumnToNMap
    }
}

/**
 * Parse index schema sql field
 */
export function parseIndexSchemaSQL(sql: string): { table: string, columns: string[] } {
    // TODO need a much better parser
    const regex = /CREATE INDEX [\["]?\w+[\]"]?\s*on\s+(?<table>\w+)\s*\((?<content>(.+\s*)+)\)/;
    const match = sql.trim().match(regex);
    if (!match || !match.groups) {
        throw new Error("Failed to parse SQL");
    } 

    const { table, content } = match.groups as any;
    const parts = (content as string).split(",").map(l => l.trim());
    const columns = parts.map(p => p.replace("[", "").replace("]", ""));

    return {table, columns}
}