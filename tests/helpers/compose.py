"""Thin wrapper around the `docker compose` CLI for the test fixtures.

Plain subprocess calls on purpose: the stack-ops contract allows exactly one
stack per machine (fixed ports and container names), so testcontainers would add
nothing but a second way to start the same services.
"""

from __future__ import annotations

import json
import subprocess
import time
from dataclasses import dataclass, field
from pathlib import Path

from tests.helpers.wait import wait_until


@dataclass
class ServiceState:
    name: str
    service: str
    state: str
    health: str
    exit_code: int

    @property
    def ok(self) -> bool:
        if self.state == "running":
            # one-shot init containers (`*-init`) are done only when they exited 0;
            # `docker compose up -d` re-runs them, and connect-init takes seconds since
            # stage 2 (it waits for the connector to be RUNNING)
            if self.service.endswith("-init"):
                return False
            return self.health in ("healthy", "")
        if self.state == "exited":
            return self.exit_code == 0
        return False


@dataclass
class ComposeStack:
    root: Path
    project: str = "wpdb"
    startup_seconds: float | None = field(default=None, init=False)

    # -- low level -----------------------------------------------------------
    def run(
        self, *args: str, check: bool = True, capture: bool = True
    ) -> subprocess.CompletedProcess:
        cmd = ["docker", "compose", *args]
        return subprocess.run(
            cmd,
            cwd=self.root,
            check=check,
            text=True,
            capture_output=capture,
        )

    def docker(self, *args: str, check: bool = True) -> subprocess.CompletedProcess:
        return subprocess.run(
            ["docker", *args], cwd=self.root, check=check, text=True, capture_output=True
        )

    # -- lifecycle -----------------------------------------------------------
    def up(
        self,
        services: list[str] | None = None,
        build: bool = False,
        profiles: list[str] | None = None,
    ) -> None:
        args: list[str] = []
        for p in profiles or []:
            args += ["--profile", p]
        args += ["up", "-d"]
        if build:
            args.append("--build")
        args += services or []
        self.run(*args)

    def down(self, volumes: bool = False) -> None:
        args = ["down", "--remove-orphans"]
        if volumes:
            args.append("-v")
        self.run(*args)

    def ps(self) -> list[ServiceState]:
        out = self.run("ps", "-a", "--format", "json").stdout
        rows: list[ServiceState] = []
        for line in out.splitlines():
            line = line.strip()
            if not line:
                continue
            d = json.loads(line)
            rows.append(
                ServiceState(
                    name=d["Name"],
                    service=d["Service"],
                    state=d["State"],
                    health=d.get("Health", "") or "",
                    exit_code=int(d.get("ExitCode", 0)),
                )
            )
        return rows

    def wait_healthy(self, timeout: float = 300) -> list[ServiceState]:
        """Port of .claude/skills/stack-ops/scripts/wait-healthy.sh."""

        def check() -> list[ServiceState] | None:
            rows = self.ps()
            if not rows:
                return None
            for r in rows:
                if r.state == "exited" and r.exit_code != 0:
                    raise RuntimeError(f"{r.name} exited with {r.exit_code}")
            return rows if all(r.ok for r in rows) else None

        return wait_until(check, timeout=timeout, every=3, desc="all services healthy")

    def up_and_wait(
        self, services: list[str] | None = None, build: bool = False, timeout: float = 300
    ) -> list[ServiceState]:
        t0 = time.monotonic()
        self.up(services, build=build)
        rows = self.wait_healthy(timeout)
        self.startup_seconds = time.monotonic() - t0
        return rows

    # -- helpers used by tests -------------------------------------------------
    def rerun_init(self, service: str, timeout: float = 180) -> int:
        """Re-run a one-shot init container and return its exit code."""
        self.run("up", "-d", "--force-recreate", "--no-deps", service)
        container = self.container_name(service)
        out = self.docker("wait", container).stdout.strip()
        return int(out)

    def container_name(self, service: str) -> str:
        for r in self.ps():
            if r.service == service:
                return r.name
        raise LookupError(f"no container for service {service}")

    def exec(self, service: str, *cmd: str, check: bool = True) -> subprocess.CompletedProcess:
        return self.run("exec", "-T", service, *cmd, check=check)

    def logs(self, service: str, tail: int | str = 200) -> str:  # tail="all" for everything
        return self.run("logs", "--no-color", "--tail", str(tail), service).stdout

    def kill(self, service: str) -> None:
        self.run("kill", service)

    def config_ok(self, profiles: list[str] | None = None) -> bool:
        args: list[str] = []
        for p in profiles or []:
            args += ["--profile", p]
        return self.run(*args, "config", "-q", check=False).returncode == 0
