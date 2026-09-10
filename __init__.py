"""Hermes Analytics — SQLite analytics plugin for Hermes Agent.

Built against the Hermes observer hook contract (hermes.observer.v1).
See docs/observability/README.md in the hermes-agent repo for the contract.

Uses SQLite in WAL mode (one .db file per profile, no daemon).
The writer thread owns the sole SQLite connection; all session-thread
code paths enqueue writes via _enqueue() or _enqueue_raw().

Standalone mode:
  Set ANALYTICS_DB_PATH to use a single database file outside Hermes.
  Set ANALYTICS_PROFILES to configure fleet report discovery.
  Set ANALYTICS_RETENTION_DAYS and ANALYTICS_CONTEXT_LENGTH for overrides.
  Without these env vars, the plugin auto-discovers Hermes profile paths.

Architecture: a single background writer thread owns the SQLite
connection. All session-thread code paths (hook handlers) enqueue writes
via _enqueue() (for typed rows) or _enqueue_raw() (for raw SQL).
The writer thread batches and flushes to SQLite every 0.25s.

Key features:
- Keyword-only hook handlers matching the v1 contract (**kwargs)
- Tool call status from contract `status` field (ok/error/blocked/cancelled)
- LLM calls recorded exclusively from post_api_request (no double-counting)
- Cost tracking via three-tier pricing: core's agent.usage_pricing,
  OpenRouter API (cache-aware), HF Router API (fallback). See model_aliases.yaml
- Correlation IDs: turn_id, api_request_id, tool_call_id
- Fail-open decorator on every handler
- No time.sleep in session end — flush queue before reconciliation
"""
from __future__ import annotations

import logging

logger = logging.getLogger(__name__)

# ponytail: sys.path hack keeps modules importable standalone; switch to a
# proper package install if the loader ever needs plugins imported as packages.
import os
import sys as _sys

_HERE = os.path.dirname(os.path.abspath(__file__))
if _HERE not in _sys.path:
    _sys.path.insert(0, _HERE)

from db import _SHUTDOWN, _start_retention_timer, _db, _get_db_path
from pricing import _load_pricing_config
from hooks import (api_request_error, on_kanban_task_blocked,
                  on_kanban_task_claimed, on_kanban_task_completed,
                  on_session_end, on_session_finalize, on_session_reset,
                  on_session_start, post_api_request, on_post_approval_response,
                  post_tool_call, pre_api_request, on_pre_gateway_dispatch,
                  pre_llm_call, pre_tool_call, on_pre_verify, on_pre_approval_request)
from tools import (_connect_read, _handle_query, _handle_digest,
                    _handle_fleet_report)


# ── Registration ───────────────────────────────────────────────────────────

def _handle_pricing_config(args: dict, **kwargs) -> str:
    """Read or update pricing_config.yaml via chat-callable tool.

    Actions:
    - action="get": return current config (source_priority + overrides)
    - action="set_override": add/update a per-model rate override
    - action="remove_override": remove a model override
    - action="reorder_sources": set source priority order
    """
    global _PRICING_CONFIG
    action = args.get("action", "get")
    config_path = os.path.join(
        os.path.dirname(os.path.abspath(__file__)), "pricing_config.yaml"
    )

    try:
        import yaml as _yaml_module

        if action == "get":
            config = _load_pricing_config()
            return json.dumps({"action": "get", "config": config}, default=str, indent=2)

        # For write actions, load the raw YAML first
        with open(config_path) as f:
            data = _yaml_module.safe_load(f) or {}

        if action == "set_override":
            model = args.get("model")
            if not model:
                return json.dumps({"error": "model is required for set_override"})
            overrides = data.setdefault("overrides", {})
            entry = {"input": float(args.get("input", 0)),
                     "output": float(args.get("output", 0))}
            if args.get("cache_read") is not None:
                entry["cache_read"] = float(args["cache_read"])
            if args.get("cache_write") is not None:
                entry["cache_write"] = float(args["cache_write"])
            overrides[model] = entry
            with open(config_path, "w") as f:
                _yaml_module.dump(data, f, default_flow_style=False)
            _PRICING_CONFIG = None  # Force reload
            return json.dumps({"action": "set_override", "model": model, "rates": entry,
                                "message": f"Override set for {model}. Reloaded config."})

        if action == "remove_override":
            model = args.get("model")
            if not model:
                return json.dumps({"error": "model is required for remove_override"})
            overrides = data.get("overrides", {})
            if model in overrides:
                del overrides[model]
                with open(config_path, "w") as f:
                    _yaml_module.dump(data, f, default_flow_style=False)
                _PRICING_CONFIG = None
                return json.dumps({"action": "remove_override", "model": model,
                                    "message": f"Removed override for {model}."})
            return json.dumps({"action": "remove_override", "model": model,
                                "message": f"No override found for {model}."})

        if action == "reorder_sources":
            new_order = args.get("source_priority")
            if not new_order or not isinstance(new_order, list):
                return json.dumps({"error": "source_priority (list) is required"})
            valid = {"override", "core", "openrouter", "hf_router"}
            invalid = [s for s in new_order if s not in valid]
            if invalid:
                return json.dumps({"error": f"Invalid sources: {invalid}. Valid: {list(valid)}"})
            data["source_priority"] = new_order
            with open(config_path, "w") as f:
                _yaml_module.dump(data, f, default_flow_style=False)
            _PRICING_CONFIG = None
            return json.dumps({"action": "reorder_sources", "source_priority": new_order,
                                "message": "Source priority updated."})

        return json.dumps({"error": f"Unknown action '{action}'. Valid: get, set_override, remove_override, reorder_sources"})

    except Exception as exc:
        return json.dumps({"error": str(exc)})


_EXPORT_TABLES = ("llm_calls", "tool_calls", "session_summary",
                  "context_pressure", "skill_usage")


def _handle_export(args: dict, **kwargs) -> str:
    """Export raw analytics rows from a table to CSV or JSON."""
    table = args.get("table")
    if table not in _EXPORT_TABLES:
        return json.dumps({"error": f"Invalid table '{table}'. Valid: {list(_EXPORT_TABLES)}"})
    profile = args.get("profile", "phoenix")
    days = int(args.get("days", 7))
    fmt = args.get("format", "csv")
    limit = int(args.get("limit", 1000))

    db = _connect_read(profile)
    if db is None:
        return json.dumps({"error": f"No analytics DB found for profile '{profile}'"})

    try:
        since_ts = int((_dt.now(_tz.utc) - _td(days=days)).timestamp())
        rows = [dict(r) for r in db.execute(
            "SELECT * FROM " + table + " WHERE timestamp >= ? ORDER BY timestamp DESC LIMIT ?",
            (since_ts, limit),
        ).fetchall()]
        if fmt == "json":
            return json.dumps(rows, default=str)
        # CSV
        if not rows:
            return ""
        headers = list(rows[0].keys())
        import io as _io
        buf = _io.StringIO()
        writer = csv.writer(buf)
        writer.writerow(headers)
        for row in rows:
            writer.writerow([row.get(h) if row.get(h) is not None else "" for h in headers])
        return buf.getvalue()
    except Exception as exc:
        return json.dumps({"error": str(exc)})
    finally:
        db.close()


def register(ctx) -> None:
    hooks_registered: list[str] = []

    def hook(name: str, fn) -> None:
        ctx.register_hook(name, fn)
        hooks_registered.append(name)
    # Hook registrations (write path)
    hook("on_session_start", on_session_start)
    hook("on_session_end", on_session_end)
    hook("on_session_finalize", on_session_finalize)
    hook("on_session_reset", on_session_reset)
    hook("pre_tool_call", pre_tool_call)
    hook("post_tool_call", post_tool_call)
    hook("pre_llm_call", pre_llm_call)
    hook("pre_api_request", pre_api_request)
    hook("post_api_request", post_api_request)
    hook("api_request_error", api_request_error)
    hook("pre_approval_request", on_pre_approval_request)
    hook("post_approval_response", on_post_approval_response)
    # subagent_start/subagent_stop removed — core doesn't fire these yet (no-ops)
    hook("pre_gateway_dispatch", on_pre_gateway_dispatch)
    hook("pre_verify", on_pre_verify)
    hook("kanban_task_claimed", on_kanban_task_claimed)
    hook("kanban_task_completed", on_kanban_task_completed)
    hook("kanban_task_blocked", on_kanban_task_blocked)
    # NOTE: post_llm_call deliberately NOT registered — LLM calls recorded
    # exclusively via post_api_request to avoid double-counting.

    # Start the periodic retention timer (daemon) so headless deployments
    # with no sessions still get telemetry retention.
    _start_retention_timer()

    # Tool registrations (read path)
    ctx.register_tool(
        name="analytics_fleet_report",
        toolset="hermes-analytics",
        schema={
            "type": "object",
            "properties": {
                "days": {
                    "type": "integer",
                    "description": "Number of days to look back (default: 7).",
                    "default": 7,
                },
            },
        },
        handler=_handle_fleet_report,
        emoji="📊",
        description=(
            "Get a fleet-wide analytics summary across all agent profiles. "
            "Returns per-agent LLM calls, tool calls, token counts, cost, "
            "context pressure, and session counts as structured JSON. "
            "Use when the user asks for fleet analytics, fleet health, "
            "or a cross-agent overview."
        ),
    )

    ctx.register_tool(
        name="analytics_digest",
        toolset="hermes-analytics",
        schema={
            "type": "object",
            "properties": {
                "profile": {
                    "type": "string",
                    "description": "Agent profile name (e.g. 'phoenix', 'warren'). Defaults to 'phoenix'.",
                    "default": "phoenix",
                },
                "days": {
                    "type": "integer",
                    "description": "Number of days to look back (default: 7).",
                    "default": 7,
                },
            },
        },
        handler=_handle_digest,
        emoji="📋",
        description=(
            "Get a detailed per-profile analytics digest. Returns model distribution, "
            "tool failures, tool usage, context pressure, daily trend, platform "
            "distribution, session health, and skill usage as structured JSON. "
            "Use when the user asks for a deep-dive on a specific agent's analytics."
        ),
    )

    ctx.register_tool(
        name="analytics_query",
        toolset="hermes-analytics",
        schema={
            "type": "object",
            "properties": {
                "query_type": {
                    "type": "string",
                    "description": "Type of query to run.",
                    "enum": ["tools", "cost", "sessions", "duration", "platforms",
                             "latency", "health", "efficiency", "models", "cost_sources"],
                },
                "profile": {
                    "type": "string",
                    "description": "Agent profile name (default: 'phoenix').",
                    "default": "phoenix",
                },
                "days": {
                    "type": "integer",
                    "description": "Number of days to look back (default: 7).",
                    "default": 7,
                },
            },
            "required": ["query_type"],
        },
        handler=_handle_query,
        emoji="🔍",
        description=(
            "Run a specific analytics query type against a single profile. "
            "Replaces the analytics_cli.py script. Use for targeted queries "
            "like 'show me tool failures for warren' or 'what models is sage using'."
        ),
    )

    ctx.register_tool(
        name="analytics_pricing_config",
        toolset="hermes-analytics",
        schema={
            "type": "object",
            "properties": {
                "action": {
                    "type": "string",
                    "enum": ["get", "set_override", "remove_override", "reorder_sources"],
                    "description": "Action to perform. 'get' returns current config. 'set_override' adds/updates a model's rates. 'remove_override' deletes a model override. 'reorder_sources' sets source priority order.",
                    "default": "get",
                },
                "model": {
                    "type": "string",
                    "description": "Model name for set_override/remove_override (e.g. 'glm-5.2').",
                },
                "input": {
                    "type": "number",
                    "description": "Input rate per million tokens (USD). For set_override.",
                },
                "output": {
                    "type": "number",
                    "description": "Output rate per million tokens (USD). For set_override.",
                },
                "cache_read": {
                    "type": "number",
                    "description": "Cache read rate per million tokens (USD). Optional.",
                },
                "cache_write": {
                    "type": "number",
                    "description": "Cache write rate per million tokens (USD). Optional.",
                },
                "source_priority": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "Ordered list of pricing sources. Valid: override, core, openrouter, hf_router. For reorder_sources.",
                },
            },
        },
        handler=_handle_pricing_config,
        emoji="💰",
        description=(
            "Read or update the analytics pricing configuration. "
            "Get current source priority and per-model overrides, "
            "set/remove model rate overrides, or reorder pricing sources. "
            "Changes take effect immediately (no restart needed)."
        ),
    )

    ctx.register_tool(
        name="analytics_export",
        toolset="hermes-analytics",
        schema={
            "type": "object",
            "properties": {
                "table": {
                    "type": "string",
                    "enum": ["llm_calls", "tool_calls", "session_summary",
                             "context_pressure", "skill_usage"],
                    "description": "Analytics table to export.",
                },
                "profile": {
                    "type": "string",
                    "description": "Agent profile name (default: 'phoenix').",
                    "default": "phoenix",
                },
                "days": {
                    "type": "integer",
                    "description": "Number of days to look back (default: 7).",
                    "default": 7,
                },
                "format": {
                    "type": "string",
                    "enum": ["csv", "json"],
                    "description": "Export format (default: csv).",
                    "default": "csv",
                },
                "limit": {
                    "type": "integer",
                    "description": "Maximum number of rows to export (default: 1000).",
                    "default": 1000,
                },
            },
            "required": ["table"],
        },
        handler=_handle_export,
        emoji="📤",
        description=(
            "Export raw analytics rows from a specified table (llm_calls, "
            "tool_calls, session_summary, context_pressure, skill_usage) to "
            "CSV or JSON. Use when you need to pull raw analytics data for a "
            "profile into a file for external analysis or backup."
        ),
    )

    print(f"[hermes-analytics] Registered {len(hooks_registered)} hooks + 5 read-path tools (SQLite v0.1.0)")


