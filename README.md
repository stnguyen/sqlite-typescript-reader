[![progress-banner](https://backend.codecrafters.io/progress/sqlite/a32c983f-852f-4351-9b72-de0b2f7747ca)](https://app.codecrafters.io/users/codecrafters-bot?r=2qF)

Educational project to understand SQLite internal file format. The project is designed by [codecrafters](https://app.codecrafters.io/r/elegant-shark-164662) (referral link).

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
