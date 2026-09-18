"""Minimal Docker Engine API client over the unix socket.

The ops-agent needs three things from Docker and nothing else: start a container, stop a container,
and read one CPU/memory sample. Rather than add the `docker` SDK (and a `requests` dependency chain)
for that, this talks to the Engine API directly with the httpx client the service already uses.

Containers are found by their Compose labels, not by name, because Compose's generated names differ
between `docker compose` versions and between project directories.
"""
from __future__ import annotations

import json

import httpx

API_VERSION = "v1.44"


class DockerUnavailable(RuntimeError):
    """The Docker socket is not mounted or not reachable."""


class ContainerNotFound(LookupError):
    """No container matches the requested Compose service."""


class Docker:
    def __init__(self, socket_path: str, timeout: float = 30.0) -> None:
        self.socket_path = socket_path
        self._client = httpx.AsyncClient(
            transport=httpx.AsyncHTTPTransport(uds=socket_path),
            base_url=f"http://docker/{API_VERSION}",
            timeout=timeout,
        )

    async def aclose(self) -> None:
        await self._client.aclose()

    async def _request(self, method: str, path: str, **kw) -> httpx.Response:
        try:
            return await self._client.request(method, path, **kw)
        except httpx.HTTPError as exc:
            raise DockerUnavailable(f"docker socket {self.socket_path}: {type(exc).__name__}") from exc

    async def ping(self) -> bool:
        try:
            r = await self._request("GET", "/_ping")
        except DockerUnavailable:
            return False
        return r.status_code == 200

    async def find(self, service: str, *, all_states: bool = True) -> dict:
        """Return the container for a Compose service, newest first. Raises ContainerNotFound."""
        filters = json.dumps({"label": [f"com.docker.compose.service={service}"]})
        r = await self._request("GET", "/containers/json", params={"all": int(all_states), "filters": filters})
        r.raise_for_status()
        items = r.json()
        if not items:
            raise ContainerNotFound(service)
        items.sort(key=lambda c: c.get("Created", 0), reverse=True)
        return items[0]

    async def state(self, service: str) -> str | None:
        """'running', 'exited', … or None when no container exists for the service."""
        try:
            return (await self.find(service)).get("State")
        except ContainerNotFound:
            return None

    async def start(self, service: str) -> str:
        c = await self.find(service)
        if c.get("State") == "running":
            return c["Id"]
        r = await self._request("POST", f"/containers/{c['Id']}/start")
        if r.status_code not in (204, 304):
            raise RuntimeError(f"docker start {service}: HTTP {r.status_code} {r.text[:200]}")
        return c["Id"]

    async def stop(self, service: str, timeout_s: int = 40) -> str:
        c = await self.find(service)
        r = await self._request("POST", f"/containers/{c['Id']}/stop", params={"t": timeout_s},
                                timeout=timeout_s + 15)
        if r.status_code not in (204, 304):
            raise RuntimeError(f"docker stop {service}: HTTP {r.status_code} {r.text[:200]}")
        return c["Id"]

    async def kill(self, service: str, signal: str = "SIGKILL") -> str:
        """Kill a container so the Compose restart policy brings it back — the fault injection."""
        c = await self.find(service)
        if c.get("State") != "running":
            raise RuntimeError(f"container for {service} is {c.get('State')}, not running")
        r = await self._request("POST", f"/containers/{c['Id']}/kill", params={"signal": signal})
        if r.status_code != 204:
            raise RuntimeError(f"docker kill {service}: HTTP {r.status_code} {r.text[:200]}")
        return c["Id"]

    async def sample(self, service: str) -> dict | None:
        """One non-streaming CPU/memory sample. Returns None when the container is not running.

        Docker reports cumulative CPU nanoseconds; `stream=false` gives a single reading with a
        `precpu_stats` block taken ~1 s earlier, so the percentage is a real interval measurement.
        """
        try:
            c = await self.find(service)
        except ContainerNotFound:
            return None
        if c.get("State") != "running":
            return None
        r = await self._request("GET", f"/containers/{c['Id']}/stats", params={"stream": "false"}, timeout=20)
        if r.status_code != 200:
            return None
        s = r.json()
        cpu_pct = None
        try:
            delta = s["cpu_stats"]["cpu_usage"]["total_usage"] - s["precpu_stats"]["cpu_usage"]["total_usage"]
            sys_delta = s["cpu_stats"]["system_cpu_usage"] - s["precpu_stats"]["system_cpu_usage"]
            cpus = s["cpu_stats"].get("online_cpus") or len(s["cpu_stats"]["cpu_usage"].get("percpu_usage") or [1])
            if sys_delta > 0 and delta >= 0:
                cpu_pct = round(delta / sys_delta * cpus * 100.0, 1)
        except (KeyError, TypeError, ZeroDivisionError):
            cpu_pct = None
        mem_mb = None
        try:
            used = s["memory_stats"]["usage"] - s["memory_stats"].get("stats", {}).get("inactive_file", 0)
            mem_mb = round(used / (1024 * 1024), 1)
        except (KeyError, TypeError):
            mem_mb = None
        return {"container": service, "cpu_pct": cpu_pct, "mem_mb": mem_mb}
