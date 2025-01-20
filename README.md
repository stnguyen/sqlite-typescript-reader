Educational project to understand SQLite internal file format. The challenge was designed by codecrafters.io

# Usage

The following commands are supported:

```
./sqlite3.sh sample.db .dbinfo
./sqlite3.sh sample.db .tables
./sqlite3.sh sample.db 'select count(*) from apples'
./sqlite3.sh sample.db 'select name, color from apples where color="Red"'
```

# Setup local development

```bash
npm install
```

# Test

## Test datasets

- Designed by codecrafters: `sample.db`, `companies.db`
- Additional: [Chinook_Sqlite v1.4.5](https://github.com/lerocha/chinook-database/releases/download/v1.4.5/Chinook_Sqlite.sqlite)

## Run test

```bash
bun test
```
