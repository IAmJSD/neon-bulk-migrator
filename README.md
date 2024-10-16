<h1 align="center">
    neon-bulk-migrator
</h1>

A tool to handle bulk database migrations. You can find the latest release in the Releases page. The following flags are provided for the CLI:

- `--rollback-mode` (optional): Defines if this is in rollback mode where the last state will be rolled back (defaults to false).
- `--main-connection-url` (required): Main Connection URL.
- `--table-name` (required): Raw SQL to handle Table Name.
- `--table-connection-column` (required): Raw SQL to handle Table Connection Column (can use `->` to handle JSON).
- `--table-branch-column` (required): Raw SQL to handle Table Branch Column.
- `--table-id-column` (required): Raw SQL to handle Table ID Column.
- `--migrate-command-name` (required): Command to do migration - takes a `CONNECTION_URL` environment variable.
- `--rollback-command-name` (required): Command to do rollback - takes a `CONNECTION_URL` environment variable.
- `--tenant-schema-connection-url` (required): Tenant Schema Connection URL.
- `--neon-role-name` (required): Neon Role Name.
- `--neon-role-password` (required): Neon Role Password.


## How Migrations are Handled

- **Migration Mode:** When the tool is run without the `--rollback-mode` flag, it operates in migration mode. It uses the `--migrate-command-name` to perform the migration. The command specified by `--migrate-command-name` should be able to handle a `CONNECTION_URL` environment variable.

- **Rollback Mode:** When the tool is run with the `--rollback-mode` flag, it operates in rollback mode. It uses the `--rollback-command-name` to perform the rollback. The command specified by `--rollback-command-name` should be able to handle a `CONNECTION_URL` environment variable.

The migration state and branches are stored in `state.msgpack`. This is updated during migration to ensure that if the application crashes mid migration it does not end up in a broken state.

## Rails Example

This is from [my tutorial on how to setup a Rails multi-tenant project](https://astrid.place/blog/neon):

```bash
neon-bulk-migrator \
    --migrate-command-name="bin/rails db:migrate RAILS_ENV=tenant" \
    --rollback-command-name="bin/rails db:rollback RAILS_ENV=tenant" \
    --main-connection-url=$USERS_DATABASE_URL \
    --neon-api-key=$NEON_API_TOKEN \
    --neon-project-id=$NEON_PROJECT_ID \
    --neon-role-name=$NEON_ROLE_NAME \
    --neon-role-password=$NEON_ROLE_PASSWORD \
    --neon-database-name=$USER_DATABASE_NAME \
    --table-branch-column="database_information->>'branch_id'" \
    --table-connection-column="database_information->>'database_url'" \
    --table-id-column="id" \
    --table-name="users" \
    --tenant-schema-connection-url=$USERS_DB_SCHEMA_URL
```
