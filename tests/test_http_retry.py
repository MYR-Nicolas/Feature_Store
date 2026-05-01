import requests
import pytest
from ELT.extract import http_get_with_retry
from ELT.config import settings


class MockResponse:
    def raise_for_status(self):
        return None

    def json(self):
        return {"ok": True}


def test_http_get_success(mocker, monkeypatch):

    monkeypatch.setattr(settings, "RETRY_COUNT", 3)
    monkeypatch.setattr(settings, "RETRY_SLEEP_SECONDS", 0)
    monkeypatch.setattr(settings, "TIMEOUT", 5)

    mock_get = mocker.patch("ELT.extract.requests.get", return_value=MockResponse())

    response = http_get_with_retry("https://fake-url.com", source_name="test")

    assert response.json() == {"ok": True}
    assert mock_get.call_count == 1


def test_http_get_retry_then_success(mocker, monkeypatch):

    monkeypatch.setattr(settings, "RETRY_COUNT", 3)
    monkeypatch.setattr(settings, "RETRY_SLEEP_SECONDS", 0)
    monkeypatch.setattr(settings, "TIMEOUT", 5)

    mock_get = mocker.patch(
        "ELT.extract.requests.get",
        side_effect=[
            requests.Timeout("timeout"),
            MockResponse()
        ]
    )

    response = http_get_with_retry("https://fake-url.com", source_name="test")

    assert response.json() == {"ok": True}
    assert mock_get.call_count == 2


def test_http_get_fails_after_retries(mocker, monkeypatch):

    monkeypatch.setattr(settings, "RETRY_COUNT", 2)
    monkeypatch.setattr(settings, "RETRY_SLEEP_SECONDS", 0)
    monkeypatch.setattr(settings, "TIMEOUT", 5)

    mocker.patch("ELT.extract.requests.get", side_effect=requests.Timeout("timeout"))

    with pytest.raises(RuntimeError, match="HTTP failed"):
        http_get_with_retry("https://fake-url.com", source_name="test")