import requests
import pytest

from ELT.config import settings
from ELT.extract import http_get_with_retry


class MockResponse:
    """Mock HTTP response object used for testing."""

    def raise_for_status(self):
        """Simulate a successful HTTP status check."""
        return None

    def json(self):
        """Return a mock JSON payload.

        Returns:
            A mock API response dictionary.
        """
        return {"ok": True}


def test_http_get_success(mocker, monkeypatch):
    """Test that an HTTP request succeeds on the first attempt."""

    monkeypatch.setattr(settings, "RETRY_COUNT", 3)
    monkeypatch.setattr(settings, "RETRY_SLEEP_SECONDS", 0)
    monkeypatch.setattr(settings, "TIMEOUT", 5)

    # Mock successful HTTP request
    mock_get = mocker.patch(
        "ELT.extract.requests.get",
        return_value=MockResponse(),
    )

    response = http_get_with_retry(
        "https://fake-url.com",
        source_name="test",
    )

    assert response.json() == {"ok": True}
    assert mock_get.call_count == 1


def test_http_get_retry_then_success(mocker, monkeypatch):
    """Test that the request retries after a failure and then succeeds."""

    monkeypatch.setattr(settings, "RETRY_COUNT", 3)
    monkeypatch.setattr(settings, "RETRY_SLEEP_SECONDS", 0)
    monkeypatch.setattr(settings, "TIMEOUT", 5)

    # Simulate one timeout followed by a successful response
    mock_get = mocker.patch(
        "ELT.extract.requests.get",
        side_effect=[
            requests.Timeout("timeout"),
            MockResponse(),
        ],
    )

    response = http_get_with_retry(
        "https://fake-url.com",
        source_name="test",
    )

    assert response.json() == {"ok": True}
    assert mock_get.call_count == 2


def test_http_get_fails_after_retries(mocker, monkeypatch):
    """Test that an error is raised after all retries fail."""

    monkeypatch.setattr(settings, "RETRY_COUNT", 2)
    monkeypatch.setattr(settings, "RETRY_SLEEP_SECONDS", 0)
    monkeypatch.setattr(settings, "TIMEOUT", 5)

    # Simulate repeated request failures
    mocker.patch(
        "ELT.extract.requests.get",
        side_effect=requests.Timeout("timeout"),
    )

    with pytest.raises(RuntimeError, match="HTTP failed"):
        http_get_with_retry(
            "https://fake-url.com",
            source_name="test",
        )