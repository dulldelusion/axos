# Migrations

AXOS migrations are **code-first**: they live in the implementation, not
in this directory. The single authoritative migration list is
`MIGRATIONS` in `src/axos/store/migrations.py` (v1 → v12 in the frozen
release), documented table-by-table in `../schemas/schema-v12.md`.

## Guarantees (implemented in `migrate()`)

- **Atomic**: each migration runs inside one transaction. A failing
  migration rolls back fully, its version is not recorded, and
  `MigrationError` is raised — the database is left exactly as it was.
- **Deterministic**: pending versions apply in strictly increasing
  order; the SQL is fixed text plus one computed seed hash (the v9 head
  row).
- **Fresh-boot from zero**: `migrate()` on an empty database builds the
  full v12 schema.
- **Re-run is a no-op**: `applied_versions()` reads the
  `schema_migrations` table in the database itself; already-applied
  versions are skipped.
- **Privileged by design**: migrations run DDL on the store's private
  writable connection (`Store.conn` stays read-only — the F1 authority
  boundary).

## Exact invocation

```python
from axos.store.db import open_store
from axos.store.migrations import migrate, applied_versions

store = open_store(path)          # path: SQLite database file
applied_now = migrate(store)      # applies pending v1..v12, returns versions applied
print(applied_versions(store))    # [1, 2, ..., 12] when fully current
```

## Provenance

- Source: `src/axos/store/migrations.py`
- Release: `551d559cf3489148fe8966b56376dfab6c5b909278685bd623eed0e7cef4c192`
- Schema documentation: `../schemas/schema-v12.md`

There are no separate migration scripts in this directory by design:
the migration list inside the frozen source is the single source of
truth, and duplicating it here would create a second authority that
could drift.
