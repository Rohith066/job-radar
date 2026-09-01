"""Test isolation for user-owned state.

`user_state_path()` defaults to ~/.job-system/user_state.db. Without this
fixture every test that opens a Database would attach the developer's real
user-state file, so tests would contaminate each other and write fixture rows
into genuine application history. This redirects the whole session to a
temporary directory.
"""
from __future__ import annotations

import os
import tempfile

import pytest

from src.apply import user_state


@pytest.fixture(autouse=True)
def isolated_user_state(tmp_path, monkeypatch):
    """Point every test at a throwaway user-state database."""
    monkeypatch.setenv(user_state.ENV_VAR, str(tmp_path / "user_state.db"))
    yield


@pytest.fixture(autouse=True, scope="session")
def guard_real_user_state():
    """Fail loudly if a test ever writes to the real user-state location."""
    real = user_state.DEFAULT_DIR
    existed = real.exists()
    yield
    if not existed and real.exists():
        raise AssertionError(
            f"A test created {real} — tests must never touch real user state."
        )
