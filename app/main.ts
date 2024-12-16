import { Database } from "./database";

const args = process.argv;
const databaseFilePath: string = args[2]
const command: string = args[3];


if (command === ".dbinfo") {
    const db = await Database.open(databaseFilePath);
    console.log(`database page size: ${db.header.pageSize}`);
    console.log(`number of tables: ${await db.countTables()}`)
    await db.close();
} else {
    throw new Error(`Unknown command ${command}`);
}
