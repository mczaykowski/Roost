from __future__ import annotations

from roost.runtime.backends.postgres import list_migrations
from roost.runtime.backends.postgres import (
    PostgresArtifactMetadataStore,
    PostgresControlPlaneStore,
    PostgresLeaseStore,
    PostgresResourceStore,
    PostgresSnapshotStore,
    PostgresWorkerHeartbeatStore,
    PostgresWorkItemStore,
    build_postgres_durable_stores,
)
from roost.runtime.stores import ControlPlaneStore, LeaseStore, ResourceStore, SnapshotStore, WorkItemStore


def test_postgres_initial_migration_is_packaged():
    migrations = list_migrations()

    assert [migration.version for migration in migrations] == ["0001"]
    sql = migrations[0].sql
    assert "CREATE TABLE IF NOT EXISTS roost_work_items" in sql
    assert "CREATE TABLE IF NOT EXISTS roost_snapshots" in sql
    assert "CREATE TABLE IF NOT EXISTS roost_artifacts" in sql
    assert "CREATE TABLE IF NOT EXISTS roost_leases" in sql
    assert "CREATE TABLE IF NOT EXISTS roost_resource_claims" in sql
    assert "CREATE TABLE IF NOT EXISTS roost_events" in sql
    assert "CREATE TABLE IF NOT EXISTS roost_dlq" in sql


def test_postgres_stores_satisfy_runtime_store_protocols():
    assert issubclass(PostgresWorkItemStore, WorkItemStore)
    assert issubclass(PostgresSnapshotStore, SnapshotStore)
    assert issubclass(PostgresLeaseStore, LeaseStore)
    assert issubclass(PostgresResourceStore, ResourceStore)
    assert issubclass(PostgresControlPlaneStore, ControlPlaneStore)


def test_postgres_durable_store_factory_groups_memory_stores():
    stores = build_postgres_durable_stores(object())

    assert isinstance(stores.work_items, PostgresWorkItemStore)
    assert isinstance(stores.snapshots, PostgresSnapshotStore)
    assert isinstance(stores.artifacts, PostgresArtifactMetadataStore)
    assert isinstance(stores.leases, PostgresLeaseStore)
    assert isinstance(stores.resources, PostgresResourceStore)
    assert isinstance(stores.workers, PostgresWorkerHeartbeatStore)
    assert isinstance(stores.control, PostgresControlPlaneStore)
