"""Tests for cronwatch.job_grouping."""
from __future__ import annotations

import pytest

from cronwatch.job_grouping import GroupStore, JobGroup


@pytest.fixture()
def store(tmp_path) -> GroupStore:
    return GroupStore(str(tmp_path / "groups.db"))


def test_add_and_get_group(store: GroupStore) -> None:
    store.add("nightly", "backup", description="Nightly jobs")
    store.add("nightly", "cleanup")
    group = store.get_group("nightly")
    assert group is not None
    assert group.name == "nightly"
    assert "backup" in group.jobs
    assert "cleanup" in group.jobs


def test_get_unknown_group_returns_none(store: GroupStore) -> None:
    assert store.get_group("ghost") is None


def test_remove_job_from_group(store: GroupStore) -> None:
    store.add("weekly", "report")
    store.add("weekly", "digest")
    store.remove("weekly", "report")
    group = store.get_group("weekly")
    assert group is not None
    assert "report" not in group.jobs
    assert "digest" in group.jobs


def test_remove_last_job_makes_group_disappear(store: GroupStore) -> None:
    store.add("solo", "only-job")
    store.remove("solo", "only-job")
    assert store.get_group("solo") is None


def test_list_groups_returns_all(store: GroupStore) -> None:
    store.add("alpha", "job1")
    store.add("beta", "job2")
    names = store.list_groups()
    assert "alpha" in names
    assert "beta" in names


def test_list_groups_empty(store: GroupStore) -> None:
    assert store.list_groups() == []


def test_groups_for_job(store: GroupStore) -> None:
    store.add("g1", "shared")
    store.add("g2", "shared")
    store.add("g3", "other")
    groups = store.groups_for_job("shared")
    assert set(groups) == {"g1", "g2"}


def test_groups_for_unknown_job_returns_empty(store: GroupStore) -> None:
    assert store.groups_for_job("nobody") == []


def test_upsert_description(store: GroupStore) -> None:
    store.add("g", "j", description="old")
    store.add("g", "j", description="new")
    group = store.get_group("g")
    assert group is not None
    assert group.description == "new"


def test_all_groups_returns_mapping(store: GroupStore) -> None:
    store.add("a", "j1")
    store.add("a", "j2")
    store.add("b", "j3")
    mapping = store.all_groups()
    assert set(mapping.keys()) == {"a", "b"}
    assert set(mapping["a"].jobs) == {"j1", "j2"}
    assert mapping["b"].jobs == ["j3"]
