#!/usr/bin/env python
"""Validate Docker Compose runtime-hardening invariants.

The check renders each committed stack with Docker Compose so anchors, profiles,
and override merges are validated before the normalized model is inspected.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
SERVER_DIR = ROOT / "deployments" / "server-stack"
EDGE_DIR = ROOT / "deployments" / "edge-stack"

EXPECTED_LOGGING = {
    "driver": "json-file",
    "options": {"max-file": "3", "max-size": "10m"},
}

SERVER_NETWORKS = {
    "redis": {"data"},
    "prefect-server": {"application"},
    "mlflow-server": {"application", "data"},
    "prometheus": {"observability"},
    "grafana": {"observability"},
    "redis-exporter": {"data", "observability"},
    "backend": {"application", "data", "observability"},
    "listener": {"application", "data", "observability"},
    "analytics": {"application", "data"},
}

SERVER_DEPENDENCIES = {
    "grafana": {"prometheus": "service_healthy"},
    "redis-exporter": {"redis": "service_healthy"},
    "backend": {"redis": "service_healthy"},
    "listener": {"redis": "service_healthy", "backend": "service_healthy"},
    "analytics": {
        "redis": "service_healthy",
        "backend": "service_healthy",
        "mlflow-server": "service_healthy",
        "prefect-server": "service_healthy",
    },
}

SERVER_PORTS = {
    "backend": {("0.0.0.0", 8080, 8080)},
    "prefect-server": {("127.0.0.1", 4200, 4200)},
    "mlflow-server": {("127.0.0.1", 5000, 5000)},
    "prometheus": {("127.0.0.1", 9090, 9090)},
    "grafana": {("127.0.0.1", 3000, 3000)},
}

EDGE_NETWORKS = {
    "redis": {"sniper_edge_network"},
    "listener": {"sniper_edge_network"},
}

EDGE_DEPENDENCIES = {
    "listener": {"redis": "service_healthy"},
}

EDGE_PORTS = {
    "redis": {("127.0.0.1", 6380, 6379)},
}


def render_compose(stack_dir: Path, *filenames: str, profiles: tuple[str, ...] = ()) -> dict[str, Any]:
    command = ["docker", "compose", "--project-directory", str(stack_dir)]
    for filename in filenames:
        command.extend(("-f", str(stack_dir / filename)))
    for profile in profiles:
        command.extend(("--profile", profile))
    command.extend(("config", "--format", "json"))

    environment = os.environ.copy()
    environment["COMPUTE_NODE_IP"] = "host.docker.internal"
    environment["LOCAL_POSTGRES_PORT"] = "5432"
    try:
        result = subprocess.run(
            command,
            check=True,
            capture_output=True,
            text=True,
            encoding="utf-8",
            env=environment,
        )
    except FileNotFoundError:
        print("ERROR: Docker Compose is required to validate deployment configuration.")
        raise SystemExit(2) from None
    except subprocess.CalledProcessError as exc:
        print(exc.stderr.strip())
        raise SystemExit(exc.returncode) from None

    return json.loads(result.stdout)


def normalized_ports(service: dict[str, Any]) -> set[tuple[str, int, int]]:
    return {
        (
            str(port.get("host_ip") or "0.0.0.0"),
            int(port["published"]),
            int(port["target"]),
        )
        for port in service.get("ports", [])
    }


def has_enabled_healthcheck(service: dict[str, Any]) -> bool:
    """Return whether a rendered service has an executable healthcheck."""
    healthcheck = service.get("healthcheck")
    if not isinstance(healthcheck, dict) or healthcheck.get("disable"):
        return False

    test = healthcheck.get("test")
    if not test:
        return False
    if isinstance(test, list) and str(test[0]).upper() == "NONE":
        return False
    return True


def validate_stack(
    label: str,
    config: dict[str, Any],
    expected_networks: dict[str, set[str]],
    expected_dependencies: dict[str, dict[str, str]],
    expected_ports: dict[str, set[tuple[str, int, int]]],
    healthcheck_exemptions: set[str] | None = None,
) -> list[str]:
    failures: list[str] = []
    services: dict[str, dict[str, Any]] = config["services"]
    exemptions = healthcheck_exemptions or set()

    for service_name, service in services.items():
        if service.get("logging") != EXPECTED_LOGGING:
            failures.append(f"{label}:{service_name} does not use bounded json-file logging")

        memory = service.get("deploy", {}).get("resources", {}).get("limits", {}).get("memory")
        if memory is None or int(memory) <= 0:
            failures.append(f"{label}:{service_name} has no positive memory limit")

        if service_name not in exemptions and not has_enabled_healthcheck(service):
            failures.append(f"{label}:{service_name} has no enabled healthcheck")

        actual_networks = set(service.get("networks", {}))
        wanted_networks = expected_networks.get(service_name)
        if wanted_networks is None:
            failures.append(f"{label}:{service_name} is missing from the expected network policy")
        elif actual_networks != wanted_networks:
            failures.append(
                f"{label}:{service_name} networks are {sorted(actual_networks)}, expected {sorted(wanted_networks)}"
            )

        actual_ports = normalized_ports(service)
        wanted_ports = expected_ports.get(service_name, set())
        if actual_ports != wanted_ports:
            failures.append(f"{label}:{service_name} ports are {sorted(actual_ports)}, expected {sorted(wanted_ports)}")

        for dependency, dependency_config in service.get("depends_on", {}).items():
            condition = dependency_config.get("condition")
            if condition != "service_healthy":
                failures.append(f"{label}:{service_name} waits for {dependency} with {condition!r}, expected 'service_healthy'")

    for service_name, dependencies in expected_dependencies.items():
        actual_dependencies = services[service_name].get("depends_on", {})
        for dependency, condition in dependencies.items():
            actual_condition = actual_dependencies.get(dependency, {}).get("condition")
            if actual_condition != condition:
                failures.append(
                    f"{label}:{service_name} waits for {dependency} with {actual_condition!r}, expected {condition!r}"
                )

    return failures


def main() -> int:
    server = render_compose(SERVER_DIR, "docker-compose.yml", profiles=("manual",))
    edge = render_compose(EDGE_DIR, "docker-compose.yml")
    server_override = render_compose(
        SERVER_DIR,
        "docker-compose.yml",
        "docker-compose.override.example.yml",
        profiles=("manual",),
    )
    edge_override = render_compose(EDGE_DIR, "docker-compose.yml", "docker-compose.override.example.yml")

    failures = validate_stack(
        "server",
        server,
        SERVER_NETWORKS,
        SERVER_DEPENDENCIES,
        SERVER_PORTS,
        healthcheck_exemptions={"analytics"},
    )
    failures.extend(validate_stack("edge", edge, EDGE_NETWORKS, EDGE_DEPENDENCIES, EDGE_PORTS))

    server_override_networks = {**SERVER_NETWORKS, "postgres": {"data"}}
    server_override_dependencies = {
        **SERVER_DEPENDENCIES,
        "mlflow-server": {"postgres": "service_healthy"},
        "backend": {"redis": "service_healthy", "postgres": "service_healthy"},
        "analytics": {
            **SERVER_DEPENDENCIES["analytics"],
            "postgres": "service_healthy",
        },
    }
    server_override_ports = {**SERVER_PORTS, "postgres": {("127.0.0.1", 5432, 5432)}}
    failures.extend(
        validate_stack(
            "server-override",
            server_override,
            server_override_networks,
            server_override_dependencies,
            server_override_ports,
            healthcheck_exemptions={"analytics"},
        )
    )
    failures.extend(validate_stack("edge-override", edge_override, EDGE_NETWORKS, EDGE_DEPENDENCIES, EDGE_PORTS))

    server_network_config = server["networks"]
    if not server_network_config["data"].get("internal"):
        failures.append("server:data must be an internal network")
    for network_name in ("application", "observability"):
        if server_network_config[network_name].get("internal"):
            failures.append(f"server:{network_name} must permit required host or outbound connectivity")

    if failures:
        for failure in failures:
            print(f"FAIL {failure}")
        print(f"ERROR: {len(failures)} Docker Compose hardening invariant(s) failed.")
        return 1

    print("All Docker Compose hardening invariants passed for base and example-override configurations.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
