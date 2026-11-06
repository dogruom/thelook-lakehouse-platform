"""docker-compose.yml policy (.claude/rules/infra.md): pinned images, loopback ports, no host
escalation, no secret defaults. The checkers are pure functions with negative tests."""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any

import pytest
import yaml


COMPOSE = Path(__file__).resolve().parents[3] / "docker-compose.yml"
SECRET_NAME = re.compile(r"PASSWORD|SECRET|KEY|TOKEN")
VAR_WITH_DEFAULT = re.compile(r"(?<!\$)\$\{([A-Za-z_][A-Za-z0-9_]*)(:?-)")
VAR = re.compile(r"(?<!\$)\$\{[^}]*\}")
LOOPBACK = "127.0.0.1"


def image_violations(services: dict[str, Any]) -> list[str]:
    out = []
    for name, svc in services.items():
        image = svc.get("image")
        if image is None:
            if "build" in svc:
                out.append(f"{name}: build without image tag")
            continue
        ref = image.split("@")[0]
        has_digest = "@sha256:" in image
        tag = ref.rsplit(":", 1)[1] if ":" in ref.rsplit("/", 1)[-1] else ""
        if has_digest and not tag:
            continue
        if not tag or tag == "latest" or "${" in tag:
            out.append(f"{name}: image {image!r} lacks an explicit non-latest tag")
    return out


def port_violations(services: dict[str, Any]) -> list[str]:
    out = []
    for name, svc in services.items():
        for port in svc.get("ports", []) or []:
            if isinstance(port, dict):
                if port.get("published") is not None and port.get("host_ip") != LOOPBACK:
                    out.append(f"{name}: long-syntax port {port} not bound to {LOOPBACK}")
                continue
            if isinstance(port, int):
                out.append(
                    f"{name}: unquoted port {port!r} (YAML may parse N:M as base-60); quote it"
                )
                continue
            parts = VAR.sub("X", str(port)).split(":")
            if len(parts) == 1:
                continue  # container-only, ephemeral host port is not published on a fixed address
            if len(parts) != 3 or parts[0] != LOOPBACK:
                out.append(f"{name}: port {port!r} not bound to {LOOPBACK}")
    return out


def escalation_violations(services: dict[str, Any]) -> list[str]:
    out = []
    for name, svc in services.items():
        if svc.get("network_mode") == "host":
            out.append(f"{name}: network_mode host")
        if svc.get("privileged") is True:
            out.append(f"{name}: privileged")
        for vol in svc.get("volumes", []) or []:
            text = (
                f"{vol.get('source', '')}{vol.get('target', '')}"
                if isinstance(vol, dict)
                else str(vol)
            )
            if "docker.sock" in text:
                out.append(f"{name}: mounts docker.sock")
    return out


def _strings(node: Any) -> list[str]:
    if isinstance(node, str):
        return [node]
    if isinstance(node, dict):
        return [s for v in node.values() for s in _strings(v)]
    if isinstance(node, list):
        return [s for v in node for s in _strings(v)]
    return []


def secret_default_violations(services: dict[str, Any]) -> list[str]:
    out = []
    for name, svc in services.items():
        for text in _strings(svc):
            for var, op in VAR_WITH_DEFAULT.findall(text):
                if SECRET_NAME.search(var):
                    out.append(f"{name}: ${{{var}{op}...}} gives a secret a default")
    return out


BARE_VAR = re.compile(r"(?<!\$)\$(?:\{([A-Za-z_][A-Za-z0-9_]*)\}|([A-Za-z_][A-Za-z0-9_]*))")


def bare_secret_violations(services: dict[str, Any]) -> list[str]:
    """``${SECRET}`` / ``$SECRET`` with no ``:?`` required form: silently empty when unset."""
    out = []
    for name, svc in services.items():
        for text in _strings(svc):
            for braced, plain in BARE_VAR.findall(text):
                var = braced or plain
                if SECRET_NAME.search(var):
                    out.append(f"{name}: bare ${var} without :? (use ${{{var}:?message}})")
    return out


def load_services() -> dict[str, Any]:
    data = yaml.safe_load(COMPOSE.read_text(encoding="utf-8"))
    assert data["services"], "no services parsed"
    return data["services"]  # type: ignore[no-any-return]


# ------------------------------------------------------------------ the real file


def test_compose_file_parses_with_services() -> None:
    assert len(load_services()) >= 5


def test_every_image_has_an_explicit_non_latest_tag() -> None:
    assert image_violations(load_services()) == []


def test_every_published_port_binds_loopback() -> None:
    assert port_violations(load_services()) == []


def test_no_host_network_privileged_or_docker_socket() -> None:
    assert escalation_violations(load_services()) == []


def test_no_secret_variable_has_a_default() -> None:
    assert secret_default_violations(load_services()) == []


def test_no_bare_secret_reference_without_required_form() -> None:
    assert bare_secret_violations(load_services()) == []


def test_policy_actually_sees_ports_and_secrets_in_the_real_file() -> None:
    """Guards against vacuous passes (e.g. a parser that returns nothing)."""
    services = load_services()
    assert sum(len(s.get("ports", []) or []) for s in services.values()) >= 4
    assert any("?" in t and "PASSWORD" in t for s in services.values() for t in _strings(s))


# ------------------------------------------------------------------ negative tests (checker has teeth)


@pytest.mark.parametrize("image", ["postgres", "postgres:latest", "redis:", "x/y:${TAG}"])
def test_bad_images_are_flagged(image: str) -> None:
    assert image_violations({"s": {"image": image}})


@pytest.mark.parametrize("image", ["crm/spark:3.5.9", "postgres:15.7-alpine", "reg:5000/a/b:1.2"])
def test_good_images_pass(image: str) -> None:
    assert image_violations({"s": {"image": image}}) == []


def test_build_without_image_is_flagged() -> None:
    assert image_violations({"s": {"build": {"context": "."}}})


@pytest.mark.parametrize(
    "port", ["5432:5432", "0.0.0.0:5432:5432", "${P:-1}:5432", "8080:80/tcp", "::1:80:80x"]
)
def test_unbound_or_public_short_ports_are_flagged(port: str) -> None:
    assert port_violations({"s": {"ports": [port]}})


@pytest.mark.parametrize(
    "port",
    [
        "127.0.0.1:${POSTGRES_HOST_PORT:-15432}:5432",
        "127.0.0.1:9000:9000",
        "5432",
        "127.0.0.1:80:80/tcp",
    ],
)
def test_loopback_and_container_only_short_ports_pass(port: str) -> None:
    assert port_violations({"s": {"ports": [port]}}) == []


def test_long_syntax_ports() -> None:
    assert port_violations({"s": {"ports": [{"target": 80, "published": 80}]}})
    assert port_violations(
        {"s": {"ports": [{"target": 80, "published": 80, "host_ip": "0.0.0.0"}]}}
    )
    assert (
        port_violations({"s": {"ports": [{"target": 80, "published": 80, "host_ip": LOOPBACK}]}})
        == []
    )
    assert port_violations({"s": {"ports": [{"target": 80}]}}) == []


@pytest.mark.parametrize(
    "svc",
    [
        {"network_mode": "host"},
        {"privileged": True},
        {"volumes": ["/var/run/docker.sock:/var/run/docker.sock"]},
        {"volumes": [{"type": "bind", "source": "/var/run/docker.sock", "target": "/s"}]},
    ],
)
def test_host_escalation_is_flagged(svc: dict[str, Any]) -> None:
    assert escalation_violations({"s": svc})


@pytest.mark.parametrize(
    "value",
    [
        "${DB_PASSWORD:-x}",
        "${API_KEY-x}",
        "${APP_SECRET:-}",
        "u:${AUTH_TOKEN:-t}@h",
        "${S3_SECRET_KEY:-k}",
    ],
)
def test_secret_defaults_are_flagged(value: str) -> None:
    assert secret_default_violations({"s": {"environment": {"X": value}}})
    assert secret_default_violations({"s": {"environment": [f"X={value}"]}})


@pytest.mark.parametrize(
    "value",
    [
        "${DB_PASSWORD:?missing}",
        "${POSTGRES_USER:-crm_admin}",
        "$${POSTGRES_PASSWORD:-x}",
        "${API_KEY?m}",
    ],
)
def test_required_form_non_secret_default_and_escaped_dollar_pass(value: str) -> None:
    assert secret_default_violations({"s": {"environment": {"X": value}}}) == []


def test_unquoted_sexagesimal_port_is_flagged() -> None:
    parsed = yaml.safe_load("ports:\n  - 22:22\n")  # PyYAML (YAML 1.1) reads this as int 1342
    assert isinstance(parsed["ports"][0], int)
    assert port_violations({"s": parsed})


@pytest.mark.parametrize(
    "value", ["${DB_PASSWORD}", "$API_TOKEN", "x=${APP_SECRET}/y", "${S3_SECRET_KEY}"]
)
def test_bare_secret_references_are_flagged(value: str) -> None:
    assert bare_secret_violations({"s": {"environment": {"X": value}}})


@pytest.mark.parametrize(
    "value",
    [
        "${DB_PASSWORD:?m}",
        "${API_KEY?m}",
        "$${POSTGRES_PASSWORD}",
        "${POSTGRES_USER}",
        "$$API_TOKEN",
    ],
)
def test_required_escaped_or_non_secret_references_are_not_bare_violations(value: str) -> None:
    assert bare_secret_violations({"s": {"environment": {"X": value}}}) == []
