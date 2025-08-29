# NiceGUI Postgres Dump/Restore

## Config

Create a TOML file (default: `config.toml` in the same directory) with:

```toml
[connections.main] # "main" is the connection name here, you can set it to anything
host = "127.0.0.1"
port = 5432
dbname = "mydb"
user = "postgres"
password = "secret"
dump_path = "./dumps/main"

[connections.analytics]
host = "127.0.0.1"
port = 5432
dbname = "analytics"
user = "postgres"
password = "secret"
dump_path = "./dumps/analytics"
prevent_restore = true # completely disables restore for this connection
```

## Notes

- Uses `pg_dump` and `pg_restore` (must be on `PATH`).
- **Clean DB** manually drops all tables (`CASCADE`) before restore; it does **not** pass `--clean` to `pg_restore`.
