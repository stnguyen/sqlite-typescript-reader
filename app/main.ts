import { Database } from "./database";

const args = process.argv;
const databaseFilePath: string = args[2]
const command: string = args[3].toLowerCase();


const db = await Database.open(databaseFilePath);
if (command === ".dbinfo") {
    console.log(`database page size: ${db.header.pageSize}`);
    console.log(`number of tables: ${await db.countTables()}`)
} else if (command === ".tables") {
    console.log((await db.getTableNames()).join("\t"));
} else if (command.startsWith("select count(*) from ")) {
    const tableName = command.split("select count(*) from ")[1];
    console.log(await db.countTableRows(tableName))
} else {
    throw new Error(`Unknown command ${command}`);
}
await db.close();