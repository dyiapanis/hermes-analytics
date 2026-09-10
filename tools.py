"""Read-path reporting tools — analytics_query/digest/fleet_report/
export/pricing_config."""

from __future__ import annotations
import csv
import functools
import hashlib
import json
import logging
import os
import queue
import sqlite3
import threading
import time
from collections import deque
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)


# ── Read-path tools (reporting) ─────────────────────────────────────────────
# These provide structured analytics access to all agents without requiring
# the hermes-analytics skill or shelling out to scripts.

import glob as _glob
from datetime import datetime as _dt, timedelta as _td, timezone as _tz


def _discover_profiles() -> list[str]:
    """Auto-discover all profiles with analytics DBs.

    Resolution order:
    1. ANALYTICS_PROFILES env var (comma-separated explicit list)
    2. Glob ~/.hermes/profiles/*/analytics.db (Hermes fleet auto-discovery)
    """
    # Tier 0: explicit env var (standalone / non-Hermes deployments)
    profiles_env = os.environ.get("ANALYTICS_PROFILES")
    if profiles_env:
        return [p.strip() for p in profiles_env.split(",") if p.strip()]

    # Tier 1: Hermes fleet auto-discovery
    return sorted([
        os.path.basename(os.path.dirname(p))
        for p in _glob.glob(os.path.expanduser("~/.hermes/profiles/*/analytics.db"))
    ])


def _connect_read(profile: str):
    """Open a read-only SQLite connection to a profile's analytics DB.

    Path resolution:
    1. ANALYTICS_DB_PATH_{PROFILE} env var (per-profile override, uppercased)
    2. ANALYTICS_DB_PATH env var (single-DB mode)
    3. ~/.hermes/profiles/{profile}/analytics.db (Hermes fleet default)
    """
    # Tier 0: per-profile env var
    env_key = f"ANALYTICS_DB_PATH_{profile.upper()}"
    path = os.environ.get(env_key)

    # Tier 1: single-DB mode (when only one profile is configured)
    if not path:
        single = os.environ.get("ANALYTICS_DB_PATH")
        if single:
            path = os.path.expanduser(single)

    # Tier 2: Hermes fleet default
    if not path:
        path = os.path.expanduser(f"~/.hermes/profiles/{profile}/analytics.db")

    if not os.path.exists(path):
        return None
    conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    return conn


def _handle_fleet_report(args: dict, **kwargs) -> str:
    """Fleet-wide analytics summary across all agent profiles."""
    days = int(args.get("days", 7))
    since = _dt.now(_tz.utc) - _td(days=days)
    profiles = _discover_profiles()
    results = []

    since_ts = int(since.timestamp())
    for profile in profiles:
        db = _connect_read(profile)
        if db is None:
            continue
        try:
            r = db.execute(
                'SELECT COUNT(*) as calls, SUM(prompt_tokens) as pt, '
                'SUM(completion_tokens) as ct, SUM(cost_usd) as cost, '
                'AVG(duration_ms) as avg_ms '
                'FROM llm_calls WHERE timestamp >= ?',
                (since_ts,),
            ).fetchone()
            tools = db.execute(
                'SELECT COUNT(*) as calls, '
                'SUM(CASE WHEN status = "failure" THEN 1 ELSE 0 END) as fails '
                'FROM tool_calls WHERE timestamp >= ?',
                (since_ts,),
            ).fetchone()
            sess_count = db.execute(
                'SELECT COUNT(DISTINCT session_id) FROM llm_calls WHERE timestamp >= ?',
                (since_ts,),
            ).fetchone()[0]
            ctx = db.execute(
                'SELECT AVG(utilization_pct) as avg_util, '
                'MAX(utilization_pct) as max_util, '
                'SUM(compression_triggered) as compressions '
                'FROM context_pressure WHERE timestamp >= ?',
                (since_ts,),
            ).fetchone()
            l = dict(r) if r else {}
            t = dict(tools) if tools else {}
            c = dict(ctx) if ctx else {}
            results.append({
                "profile": profile,
                "sessions": sess_count,
                "llm_calls": int(l.get("calls") or 0),
                "tool_calls": int(t.get("calls") or 0),
                "tool_failures": int(t.get("fails") or 0),
                "prompt_tokens": int(l.get("pt") or 0),
                "completion_tokens": int(l.get("ct") or 0),
                "cost_usd": round(float(l.get("cost") or 0), 4),
                "avg_llm_ms": round(float(l.get("avg_ms") or 0)),
                "ctx_avg_pct": round(float(c.get("avg_util") or 0), 1),
                "ctx_peak_pct": int(c.get("max_util") or 0),
                "compressions": int(c.get("compressions") or 0),
            })
        except Exception as exc:
            results.append({"profile": profile, "error": str(exc)})
        finally:
            db.close()

    return json.dumps({"days": days, "agents": results}, default=str)


def _handle_digest(args: dict, **kwargs) -> str:
    """Per-profile deep-dive analytics digest."""
    profile = args.get("profile", "phoenix")
    days = int(args.get("days", 7))
    since = _dt.now(_tz.utc) - _td(days=days)
    db = _connect_read(profile)
    if db is None:
        return json.dumps({"error": f"No analytics DB found for profile '{profile}'"})

    since_ts = int(since.timestamp())
    sections = {}
    try:
        # Model distribution
        sections["models"] = [dict(r) for r in db.execute(
            'SELECT model, COUNT(*) as calls, SUM(prompt_tokens) as pt, '
            'SUM(completion_tokens) as ct, SUM(cost_usd) as cost, '
            'AVG(duration_ms) as avg_ms, MAX(duration_ms) as max_ms '
            'FROM llm_calls WHERE timestamp >= ? '
            'GROUP BY model ORDER BY calls DESC',
            (since_ts,),
        )]

        # Tool failures
        sections["tool_failures"] = [dict(r) for r in db.execute(
            'SELECT tool_name, status, COUNT(*) as fails '
            'FROM tool_calls WHERE status != "success" AND status IS NOT NULL '
            'AND timestamp >= ? '
            'GROUP BY tool_name, status ORDER BY fails DESC',
            (since_ts,),
        )]

        # Tool usage
        sections["tool_usage"] = [dict(r) for r in db.execute(
            'SELECT tool_name, COUNT(*) as total, '
            'SUM(CASE WHEN status = "success" THEN 1 ELSE 0 END) as ok, '
            'AVG(duration_ms) as avg_ms '
            'FROM tool_calls WHERE timestamp >= ? '
            'GROUP BY tool_name ORDER BY total DESC LIMIT 15',
            (since_ts,),
        )]

        # Context pressure
        sections["context_pressure"] = [dict(r) for r in db.execute(
            'SELECT COUNT(*) as samples, AVG(utilization_pct) as avg_util, '
            'MAX(utilization_pct) as peak_util, '
            'SUM(compression_triggered) as compressions '
            'FROM context_pressure WHERE timestamp >= ?',
            (since_ts,),
        )]

        # Daily trend
        sections["daily_trend"] = [dict(r) for r in db.execute(
            "SELECT strftime('%Y-%m-%d', timestamp, 'unixepoch') as day, "
            'COUNT(*) as calls, SUM(total_tokens) as tokens, '
            'SUM(cost_usd) as cost '
            'FROM llm_calls WHERE timestamp >= ? '
            'GROUP BY day ORDER BY day DESC',
            (since_ts,),
        )]

        # Platform distribution
        sections["platforms"] = [dict(r) for r in db.execute(
            'SELECT platform, COUNT(*) as sessions, '
            'SUM(total_llm_calls) as llm, '
            'SUM(total_tool_calls) as tools '
            'FROM session_summary WHERE start_time >= ? '
            'GROUP BY platform ORDER BY sessions DESC',
            (since_ts,),
        )]

        # Session health
        sections["session_health"] = [dict(r) for r in db.execute(
            'SELECT final_status, COUNT(*) as cnt, '
            'AVG(end_time - start_time) as avg_dur '
            'FROM session_summary WHERE start_time >= ? AND end_time IS NOT NULL '
            'GROUP BY final_status ORDER BY cnt DESC',
            (since_ts,),
        )]

        # Skill usage
        sections["skill_usage"] = [dict(r) for r in db.execute(
            'SELECT skill_name, COUNT(*) as views FROM skill_usage '
            'WHERE timestamp >= ? '
            'GROUP BY skill_name ORDER BY views DESC LIMIT 20',
            (since_ts,),
        )]

    except Exception as exc:
        return json.dumps({"error": str(exc)})
    finally:
        db.close()

    return json.dumps({"profile": profile, "days": days, "sections": sections}, default=str)


def _handle_query(args: dict, **kwargs) -> str:
    """Ad-hoc analytics query by type."""
    query_type = args.get("query_type", "tools")
    profile = args.get("profile", "phoenix")
    days = int(args.get("days", 7))
    since = _dt.now(_tz.utc) - _td(days=days)
    db = _connect_read(profile)
    if db is None:
        return json.dumps({"error": f"No analytics DB found for profile '{profile}'"})

    since_ts = int(since.timestamp())
    queries = {
        "tools": (
            'SELECT tool_name, COUNT(*) as total, '
            'SUM(CASE WHEN status = "failure" THEN 1 ELSE 0 END) as fails, '
            'AVG(duration_ms) as avg_ms, MAX(duration_ms) as max_ms '
            'FROM tool_calls WHERE timestamp >= ? '
            'GROUP BY tool_name ORDER BY total DESC'
        ),
        "cost": (
            'SELECT session_id, platform, total_llm_calls, total_prompt_tokens, '
            'total_completion_tokens, total_cost_usd, final_status '
            'FROM session_summary WHERE start_time >= ? '
            'ORDER BY total_cost_usd DESC'
        ),
        "sessions": (
            'SELECT session_id, platform, start_time, end_time, '
            'total_llm_calls, total_tool_calls, total_cost_usd, final_status '
            'FROM session_summary WHERE start_time >= ? '
            'ORDER BY start_time DESC'
        ),
        "duration": (
            'SELECT model, COUNT(*) as calls, AVG(duration_ms) as avg_ms, '
            'MAX(duration_ms) as max_ms, MIN(duration_ms) as min_ms '
            'FROM llm_calls WHERE timestamp >= ? '
            'GROUP BY model ORDER BY calls DESC'
        ),
        "platforms": (
            'SELECT platform, COUNT(*) as sessions, '
            'SUM(total_llm_calls) as llm, '
            'SUM(total_tool_calls) as tools '
            'FROM session_summary WHERE start_time >= ? '
            'GROUP BY platform ORDER BY sessions DESC'
        ),
        "latency": (
            'SELECT tool_name, COUNT(*) as calls, '
            'AVG(duration_ms) as avg_ms, MAX(duration_ms) as max_ms '
            'FROM tool_calls WHERE timestamp >= ? '
            'GROUP BY tool_name ORDER BY calls DESC LIMIT 10'
        ),
        "health": (
            'SELECT final_status, COUNT(*) as cnt, '
            'AVG(end_time - start_time) as avg_dur '
            'FROM session_summary WHERE start_time >= ? AND end_time IS NOT NULL '
            'GROUP BY final_status ORDER BY cnt DESC'
        ),
        "efficiency": (
            'SELECT platform, SUM(total_tool_calls) as tools, '
            'SUM(total_llm_calls) as llm '
            'FROM session_summary WHERE start_time >= ? '
            'GROUP BY platform'
        ),
        "models": (
            'SELECT model, COUNT(*) as calls, SUM(prompt_tokens) as pt, '
            'SUM(completion_tokens) as ct, SUM(cost_usd) as cost, '
            'AVG(duration_ms) as avg_ms '
            'FROM llm_calls WHERE timestamp >= ? '
            'GROUP BY model ORDER BY calls DESC'
        ),
        "cost_sources": (
            'SELECT cost_source, cost_status, COUNT(*) as calls, '
            'SUM(prompt_tokens) as pt, SUM(completion_tokens) as ct, '
            'SUM(cost_usd) as cost '
            'FROM llm_calls WHERE timestamp >= ? '
            'GROUP BY cost_source, cost_status ORDER BY cost DESC'
        ),
    }

    sql = queries.get(query_type)
    if not sql and query_type != "sql":
        return json.dumps({"error": f"Unknown query_type '{query_type}'. Valid: {sorted(list(queries.keys()) + ['sql'])}"})

    if query_type == "sql":
        # Read-only ad-hoc passthrough. DB is opened mode=ro, but fail closed
        # anyway: SELECT-only, single statement, bounded result set.
        raw = str(args.get("sql", "")).strip().rstrip(";")
        if not raw.lower().startswith("select"):
            return json.dumps({"error": "sql query_type only allows SELECT statements"})
        if ";" in raw:
            return json.dumps({"error": "Multiple SQL statements not allowed"})
        # ponytail: "has a LIMIT clause" via regex; a smarter cap can come later
        if not __import__("re").search(r"\blimit\s+\d+\s*$", raw, __import__("re").IGNORECASE):
            sql = raw + " LIMIT 500"
        else:
            sql = raw

    try:
        rows = [dict(r) for r in db.execute(sql, (since_ts,) if query_type != "sql" else ()).fetchall()]
        return json.dumps({"profile": profile, "query_type": query_type, "days": days, "rows": rows}, default=str)
    except Exception as exc:
        return json.dumps({"error": str(exc)})
    finally:
        db.close()


