"""Alert payload contract, sanitising, classification and webhook sink (offline)."""

from __future__ import annotations

import json
import threading
from datetime import UTC, datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Any

import pytest

from lakehouse.orchestration import alert


KEYS = {
    "event",
    "schema_version",
    "kind",
    "dag_id",
    "run_id",
    "task_id",
    "brand_id",
    "reason",
    "try_number",
    "logical_date",
    "text",
}


def _payload(**over: Any) -> dict[str, Any]:
    base: dict[str, Any] = {
        "dag_id": "lakehouse_pipeline",
        "run_id": "manual__2026-01-05T00:00:00+00:00",
        "task_id": "landing_check",
        "kind": "failure",
        "reason": "brand_id=brand_beta injected failure",
        "try_number": 1,
        "logical_date": datetime(2026, 1, 5, tzinfo=UTC),
        "brand_id": "brand_beta",
    }
    base.update(over)
    return alert.build_alert_payload(**base)


def test_payload_has_the_contract_fields_and_is_json_serialisable() -> None:
    payload = _payload()
    assert set(payload) == KEYS
    assert json.loads(json.dumps(payload)) == payload
    assert payload["event"] == "lakehouse.alert"
    assert payload["kind"] == "failure"
    assert payload["brand_id"] == "brand_beta"
    assert payload["logical_date"] == "2026-01-05T00:00:00+00:00"
    assert "brand_beta" in payload["text"]


def test_logical_date_is_normalised_to_utc() -> None:
    berlin = timezone(timedelta(hours=1))
    assert _payload(logical_date=datetime(2026, 1, 5, 1, tzinfo=berlin))["logical_date"] == (
        "2026-01-05T00:00:00+00:00"
    )
    assert (
        _payload(logical_date=datetime(2026, 1, 5))["logical_date"] == "2026-01-05T00:00:00+00:00"
    )
    assert _payload(logical_date="2026-01-05T00:00:00Z")["logical_date"] == (
        "2026-01-05T00:00:00+00:00"
    )
    assert _payload(logical_date=None)["logical_date"] is None


def test_unknown_kind_is_rejected() -> None:
    with pytest.raises(ValueError, match="kind"):
        _payload(kind="warning")


def test_invalid_brand_is_dropped_not_echoed() -> None:
    payload = _payload(brand_id="jane.doe@example.com")
    assert payload["brand_id"] is None
    assert "jane" not in json.dumps(payload)
    assert _payload(brand_id=None)["brand_id"] is None


def test_reason_is_redacted_single_line_and_truncated() -> None:
    reason = "mail jane.doe@example.com failed\nat https://user:pa55@host.example/x " + "z" * 500
    safe = alert.sanitize_reason(reason)
    assert "jane" not in safe
    assert "pa55" not in safe
    assert "user:" not in safe
    assert "\n" not in safe
    assert len(safe) == alert.MAX_REASON_CHARS
    assert safe.endswith("...")


def test_classify_kind_and_extract_brand() -> None:
    assert alert.classify_kind("dlq_rate_breach brand_id=brand_beta: ...") == "threshold"
    assert alert.classify_kind("Task failed") == "failure"
    assert alert.extract_brand_id("x brand_id=brand_beta: boom") == "brand_beta"
    assert alert.extract_brand_id("brand_id=BRAND_X") is None
    assert alert.extract_brand_id("no brand here") is None


def test_threshold_payload() -> None:
    payload = _payload(
        kind="threshold",
        reason=f"{alert.THRESHOLD_MARKER} brand_id=brand_beta: DLQ rate above 0.15",
    )
    assert payload["kind"] == "threshold"
    assert payload["reason"].startswith("dlq_rate_breach")


def test_log_line_round_trip() -> None:
    payload = _payload()
    line = alert.format_log_line(payload)
    assert line.startswith("ALERT_PAYLOAD {")
    assert "\n" not in line
    assert alert.parse_log_line("2026-01-05 12:00:00 ERROR " + line) == payload
    assert alert.parse_log_line("nothing here") is None
    assert alert.parse_log_line("ALERT_PAYLOAD {broken") is None


class _Sink(BaseHTTPRequestHandler):
    received: list[bytes] = []  # noqa: RUF012
    status = 200

    def do_POST(self) -> None:
        length = int(self.headers.get("Content-Length", 0))
        type(self).received.append(self.rfile.read(length))
        self.send_response(type(self).status)
        self.end_headers()

    def log_message(self, *_args: object) -> None:
        pass


@pytest.fixture
def sink() -> Any:
    _Sink.received = []
    _Sink.status = 200
    server = HTTPServer(("127.0.0.1", 0), _Sink)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    yield server
    server.shutdown()
    server.server_close()


def test_webhook_receives_the_json_payload(sink: HTTPServer) -> None:
    url = f"http://127.0.0.1:{sink.server_address[1]}/hook"
    payload = _payload()
    assert alert.post_webhook(payload, url) is True
    assert json.loads(_Sink.received[0]) == payload


def test_webhook_failure_never_raises(sink: HTTPServer) -> None:
    _Sink.status = 500
    assert alert.post_webhook(_payload(), f"http://127.0.0.1:{sink.server_address[1]}/") is False
    assert alert.post_webhook(_payload(), "http://127.0.0.1:1/", timeout_s=0.5) is False
    assert alert.post_webhook(_payload(), "file:///etc/passwd") is False


def test_emit_alert_logs_always_and_posts_only_when_configured(
    sink: HTTPServer, caplog: pytest.LogCaptureFixture
) -> None:
    payload = _payload()
    with caplog.at_level("ERROR", logger="lakehouse.alert"):
        assert alert.emit_alert(payload, None) is False
        assert not _Sink.received
        assert alert.emit_alert(payload, f"http://127.0.0.1:{sink.server_address[1]}/") is True
    lines = [r.getMessage() for r in caplog.records]
    assert len(lines) == 2
    assert all(alert.parse_log_line(line) == payload for line in lines)
    assert len(_Sink.received) == 1
