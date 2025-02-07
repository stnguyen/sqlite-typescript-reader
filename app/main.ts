import { Database } from "./database";

const args = process.argv;
const databaseFilePath: string = args[2]
const command: string = args[3];


const db = await Database.open(databaseFilePath);
if (command === ".dbinfo") {
    console.log(`database page size: ${db.header.pageSize}`);
    console.log(`number of tables: ${await db.countTables()}`)
} else if (command === ".tables") {
    console.log((await db.getTableNames()).join("\t"));
} else if (command.toLowerCase().startsWith("select count(*) from ")) {
    const tableName = command.split("select count(*) from ")[1];
    console.log(await db.countTableRows(tableName))
} else {
    const selectColRegex = /select\s+(?<exprs>[\w\,*\s*]+)\s+from\s+(?<tableName>\w+)\s*((where)\s+(?<whereColumn>\w+)\s*=\s*['"](?<whereValue>.*)['"])?/i;
    const match = command.match(selectColRegex);
    if (match) {
        const { exprs, tableName, whereColumn, whereValue } = match.groups as any;
        const whereClause = whereColumn !== undefined && whereValue !== undefined ? { column: whereColumn, operator: "=", value: whereValue } : undefined
        const columnNames = (exprs as string).split(/\s*,\s*/)
        const rows = await db.select(tableName, columnNames, whereClause)
        const rowsFormatted = rows.map(r => r.join("|"))
        console.log(rowsFormatted.join("\n"))
    }
    else {
        throw new Error(`Unknown command ${command}`);
    }
}
await db.close();