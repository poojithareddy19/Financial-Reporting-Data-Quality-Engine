"""dq_score: examples plus hypothesis properties."""

from __future__ import annotations

import pytest
from hypothesis import given
from hypothesis import strategies as st

from fin_dq_engine.quality.scoring import compute_dq_score

severities = st.sampled_from(["blocking", "warning", "info"])


def test_examples() -> None:
    assert compute_dq_score([]) == 100
    assert compute_dq_score(["warning"]) == 80
    assert compute_dq_score(["info", "info"]) == 90
    assert compute_dq_score(["warning", "blocking"]) == 0
    assert compute_dq_score(["warning"] * 10) == 0


def test_rejects_bad_input() -> None:
    with pytest.raises(ValueError):
        compute_dq_score(["severe"])
    with pytest.raises(ValueError):
        compute_dq_score([], warning_penalty=-1)


@given(st.lists(severities, max_size=30), st.integers(0, 100), st.integers(0, 100))
def test_score_bounded(sevs: list[str], w: int, i: int) -> None:
    assert 0 <= compute_dq_score(sevs, w, i) <= 100


@given(st.lists(severities, max_size=30))
def test_blocking_is_zero(sevs: list[str]) -> None:
    assert compute_dq_score([*sevs, "blocking"]) == 0


@given(st.lists(st.sampled_from(["warning", "info"]), max_size=20), st.sampled_from(["warning", "info"]))
def test_monotone_more_failures_never_raise_score(sevs: list[str], extra: str) -> None:
    assert compute_dq_score([*sevs, extra]) <= compute_dq_score(sevs)


@given(st.lists(st.sampled_from(["warning", "info"]), max_size=20))
def test_order_independent(sevs: list[str]) -> None:
    assert compute_dq_score(sevs) == compute_dq_score(list(reversed(sevs)))
