#!/bin/sh

echo "Downloading Chinook_Sqlite.sqlite: ~1MB (used `bun test`)"
curl -Lo tests/Chinook_Sqlite.sqlite https://github.com/lerocha/chinook-database/releases/download/v1.4.5/Chinook_Sqlite.sqlite

echo "Downloading superheroes.db: ~1MB (used in stage 7)"
curl -Lo superheroes.db https://raw.githubusercontent.com/codecrafters-io/sample-sqlite-databases/master/superheroes.db

echo "Downloading companies.db: ~7MB (used in stage 8)"
curl -Lo companies.db https://raw.githubusercontent.com/codecrafters-io/sample-sqlite-databases/master/companies.db

echo "Sample databases downloaded."
