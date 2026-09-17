"""Loading rows together with the access rule that applies to them."""
from __future__ import annotations

from .core import ApiError, Principal, fetch_one, require_owner, require_read


async def session_row(session_id: str) -> dict:
    row = await fetch_one("SELECT * FROM demo_sessions WHERE id = %s", (session_id,))
    if row is None:
        raise ApiError(404, "not_found", "session not found")
    return row


def readable(p: Principal, s: dict) -> None:
    """Experiment sessions hold published benchmark evidence and are public; others need a token."""
    if s["kind"] == "experiment":
        return
    require_read(p, s["id"])


async def read_session(p: Principal, session_id: str) -> dict:
    s = await session_row(session_id)
    readable(p, s)
    return s


async def run_row(run_id: str) -> dict:
    row = await fetch_one("SELECT * FROM runs WHERE id = %s", (run_id,))
    if row is None:
        raise ApiError(404, "not_found", "run not found")
    return row


async def read_run(p: Principal, run_id: str) -> tuple[dict, dict]:
    r = await run_row(run_id)
    s = await read_session(p, r["session_id"])
    return r, s


async def own_run(p: Principal, run_id: str) -> tuple[dict, dict]:
    r = await run_row(run_id)
    s = await session_row(r["session_id"])
    if s["kind"] == "experiment" or r["created_by"] == "experiment":
        if not p.operator:
            raise ApiError(403, "operator_required", "experiment runs are controlled by the experiment runner")
    else:
        require_owner(p, s["id"])
    return r, s
