/**
 * Parse table schema SQL as stored in sqlite_schema table
 * @param sql 
 * @returns an object describing the table schema
 */
export function parseTableSchemaSQL(sql: string): {
    columns: string[],
    integerPrimaryKeyColIndex?: number
    autoIndexColumnToNMap: Map<string, number>
} {
    // TODO need a proper parser. This is very fragile.
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
 * Parse index schema SQL as stored in sqlite_schema table
 * @param sql
 * @returns an object describing the index schema
 */
export function parseIndexSchemaSQL(sql: string): { table: string, columns: string[] } {
    // TODO need a proper parser. This is very fragile.
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