# Contributing to Hermes Analytics

Thanks for your interest in contributing! This is a small plugin, so the process is lightweight.

## Reporting Issues

Use [GitHub Issues](https://github.com/dyiapanis/hermes-analytics/issues). Include:
- Hermes Agent version (`hermes --version`)
- Plugin version (from `plugin.yaml`)
- The analytics DB schema version (run `sqlite3 analytics.db "SELECT * FROM _schema_meta"`)
- Error logs (the plugin logs to the Hermes gateway log with `[hermes-analytics]` prefix)

## Pull Requests

1. Fork the repo
2. Create a branch: `git checkout -b fix/your-fix`
3. Keep changes focused — one logical change per PR
4. Test that the plugin still loads: `python3 -c "import __init__"`
5. Test that the DB schema creates cleanly on a fresh profile
6. Commit with conventional commits: `fix:`, `feat:`, `docs:`, `refactor:`

## Code Style

- Single-file plugin (`__init__.py`) — no package structure
- stdlib `sqlite3` only for database access (no ORM, no external DB drivers)
- `pyyaml` is the only pip dependency — do not add others without strong justification
- All hook handlers use the fail-open decorator (`@_fail_open`)
- Background writer thread owns the sole write connection (WAL mode)
- Read-path tools open fresh read-only connections per query

## License

MIT — contributions are accepted under the same license.