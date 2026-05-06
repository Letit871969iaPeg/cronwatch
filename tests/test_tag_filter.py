"""Tests for cronwatch.tag_filter."""

from __future__ import annotations

import pytest

from cronwatch.config import JobConfig
from cronwatch.tag_filter import TagFilter, parse_tags


def _job(name: str, tags=None) -> JobConfig:
    return JobConfig(
        name=name,
        schedule="* * * * *",
        command=f"echo {name}",
        tags=tags or [],
    )


# ---------------------------------------------------------------------------
# parse_tags
# ---------------------------------------------------------------------------


def test_parse_tags_basic():
    assert parse_tags("nightly,backup,db") == ["nightly", "backup", "db"]


def test_parse_tags_strips_whitespace():
    assert parse_tags(" nightly , backup ") == ["nightly", "backup"]


def test_parse_tags_none_returns_empty():
    assert parse_tags(None) == []


def test_parse_tags_empty_string_returns_empty():
    assert parse_tags("") == []


# ---------------------------------------------------------------------------
# TagFilter.matches
# ---------------------------------------------------------------------------


def test_empty_filter_matches_all():
    tf = TagFilter(tags=[])
    assert tf.matches(_job("j", ["x"]))
    assert tf.matches(_job("j", []))


def test_single_tag_match():
    tf = TagFilter(tags=["nightly"])
    assert tf.matches(_job("j", ["nightly", "backup"]))


def test_single_tag_no_match():
    tf = TagFilter(tags=["nightly"])
    assert not tf.matches(_job("j", ["backup"]))


def test_multi_tag_all_present():
    tf = TagFilter(tags=["nightly", "db"])
    assert tf.matches(_job("j", ["nightly", "db", "backup"]))


def test_multi_tag_partial_match_fails():
    tf = TagFilter(tags=["nightly", "db"])
    assert not tf.matches(_job("j", ["nightly"]))


def test_job_with_no_tags_does_not_match_filter():
    tf = TagFilter(tags=["nightly"])
    assert not tf.matches(_job("j", []))


# ---------------------------------------------------------------------------
# TagFilter.filter_jobs
# ---------------------------------------------------------------------------


def test_filter_jobs_returns_subset():
    jobs = [
        _job("a", ["nightly", "db"]),
        _job("b", ["nightly"]),
        _job("c", ["db"]),
    ]
    tf = TagFilter(tags=["nightly"])
    result = tf.filter_jobs(jobs)
    assert [j.name for j in result] == ["a", "b"]


def test_filter_jobs_empty_list():
    tf = TagFilter(tags=["nightly"])
    assert tf.filter_jobs([]) == []


def test_filter_jobs_no_match_returns_empty():
    jobs = [_job("a", ["backup"]), _job("b", [])]
    tf = TagFilter(tags=["nightly"])
    assert tf.filter_jobs(jobs) == []
