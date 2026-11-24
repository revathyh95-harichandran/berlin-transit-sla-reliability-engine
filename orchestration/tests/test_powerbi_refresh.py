"""Tests for orchestration/powerbi_refresh.py. No real HTTP request is ever sent: a fake opener stands in."""

from __future__ import annotations

import urllib.request

import pytest

from orchestration import powerbi_refresh


class FakeResponse:
    """Pretend to be the answer from Power BI (202 Accepted)."""

    status = 202

    def __enter__(self) -> "FakeResponse":
        return self

    def __exit__(self, *exc_info: object) -> None:
        return None


class FakeOpener:
    """Records every request it is asked to send, instead of sending it."""

    def __init__(self) -> None:
        self.requests: list[urllib.request.Request] = []

    def __call__(self, request: urllib.request.Request, timeout: int = 0) -> FakeResponse:
        self.requests.append(request)
        return FakeResponse()


def test_dry_run_is_the_default_and_sends_nothing() -> None:
    opener = FakeOpener()
    messages: list[str] = []

    result = powerbi_refresh.trigger_refresh({}, tests_passed=True, log=messages.append, opener=opener)

    assert result == {"outcome": "dry_run"}
    assert opener.requests == []
    assert "DRY_RUN is on" in messages[0]


@pytest.mark.parametrize("value", ["True", "1", "yes", "anything"])
def test_any_value_other_than_false_keeps_dry_run_on(value: str) -> None:
    assert powerbi_refresh.is_dry_run({"DRY_RUN": value}) is True


@pytest.mark.parametrize("value", ["False", "0", "no", "off"])
def test_explicit_false_words_turn_dry_run_off(value: str) -> None:
    assert powerbi_refresh.is_dry_run({"DRY_RUN": value}) is False


def test_failed_tests_block_the_refresh_even_when_dry_run_is_off() -> None:
    opener = FakeOpener()

    result = powerbi_refresh.trigger_refresh({"DRY_RUN": "False"}, tests_passed=False, opener=opener)

    assert result == {"outcome": "blocked"}
    assert opener.requests == []


def test_real_refresh_without_settings_fails_clearly() -> None:
    with pytest.raises(powerbi_refresh.PowerBIConfigError):
        powerbi_refresh.trigger_refresh({"DRY_RUN": "False"}, tests_passed=True, opener=FakeOpener())


def test_real_refresh_sends_one_post_with_bearer_token() -> None:
    env = {
        "DRY_RUN": "False",
        "POWERBI_GROUP_ID": "group-123",
        "POWERBI_DATASET_ID": "dataset-456",
        "POWERBI_ACCESS_TOKEN": "fake-token-for-tests",
    }
    opener = FakeOpener()

    result = powerbi_refresh.trigger_refresh(env, tests_passed=True, log=lambda _: None, opener=opener)

    assert result == {"outcome": "triggered"}
    assert len(opener.requests) == 1
    sent = opener.requests[0]
    assert sent.get_method() == "POST"
    assert sent.full_url.endswith("/groups/group-123/datasets/dataset-456/refreshes")
    assert sent.get_header("Authorization") == "Bearer fake-token-for-tests"
