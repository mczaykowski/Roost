from __future__ import annotations

import hashlib
from dataclasses import dataclass
from importlib import resources
from typing import Any


@dataclass(frozen=True)
class Migration:
    version: str
    name: str
    checksum: str
    sql: str


def list_migrations() -> list[Migration]:
    root = resources.files("roost.runtime.backends.postgres.migrations")
    migrations: list[Migration] = []
    for ref in sorted(root.iterdir(), key=lambda item: item.name):
        if not ref.name.endswith(".sql"):
            continue
        version = ref.name.split("_", 1)[0]
        sql = ref.read_text(encoding="utf-8")
        migrations.append(
            Migration(
                version=version,
                name=ref.name,
                checksum=hashlib.sha256(sql.encode("utf-8")).hexdigest(),
                sql=sql,
            )
        )
    return migrations


def _require_psycopg() -> Any:
    try:
        import psycopg
    except Exception as exc:
        raise RuntimeError(
            "Missing Postgres runtime dependency. Install with:\n"
            "  uv sync --extra postgres\n"
            "or:\n"
            "  pip install 'roost-runtime[postgres]'"
        ) from exc
    return psycopg


def apply_migrations(postgres_url: str) -> list[dict[str, Any]]:
    if not postgres_url:
        raise ValueError("Postgres URL is required")

    psycopg = _require_psycopg()
    applied: list[dict[str, Any]] = []
    with psycopg.connect(postgres_url) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS roost_schema_migrations (
              version TEXT PRIMARY KEY,
              name TEXT NOT NULL,
              checksum TEXT NOT NULL,
              applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
            )
            """
        )
        rows = conn.execute("SELECT version, checksum FROM roost_schema_migrations").fetchall()
        existing = {str(row[0]): str(row[1]) for row in rows}

        for migration in list_migrations():
            existing_checksum = existing.get(migration.version)
            if existing_checksum:
                if existing_checksum != migration.checksum:
                    raise RuntimeError(
                        f"Migration checksum mismatch for {migration.version}: "
                        f"database={existing_checksum} package={migration.checksum}"
                    )
                applied.append({"version": migration.version, "name": migration.name, "state": "already_applied"})
                continue

            conn.execute(migration.sql)
            conn.execute(
                """
                INSERT INTO roost_schema_migrations (version, name, checksum)
                VALUES (%s, %s, %s)
                """,
                (migration.version, migration.name, migration.checksum),
            )
            applied.append({"version": migration.version, "name": migration.name, "state": "applied"})

    return applied


def check_migrations(postgres_url: str) -> list[dict[str, Any]]:
    if not postgres_url:
        raise ValueError("Postgres URL is required")

    psycopg = _require_psycopg()
    with psycopg.connect(postgres_url) as conn:
        exists = conn.execute(
            """
            SELECT to_regclass('public.roost_schema_migrations')
            """
        ).fetchone()
        if not exists or exists[0] is None:
            return [
                {
                    "version": migration.version,
                    "name": migration.name,
                    "state": "missing",
                    "reason": "roost_schema_migrations table is missing",
                }
                for migration in list_migrations()
            ]

        rows = conn.execute("SELECT version, checksum FROM roost_schema_migrations").fetchall()

    existing = {str(row[0]): str(row[1]) for row in rows}
    out: list[dict[str, Any]] = []
    for migration in list_migrations():
        existing_checksum = existing.get(migration.version)
        if not existing_checksum:
            out.append({"version": migration.version, "name": migration.name, "state": "missing"})
        elif existing_checksum != migration.checksum:
            out.append({"version": migration.version, "name": migration.name, "state": "checksum_mismatch"})
        else:
            out.append({"version": migration.version, "name": migration.name, "state": "applied"})
    return out
