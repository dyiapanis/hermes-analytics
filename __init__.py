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

import atexit
import csv
import functools
import hashlib
import json
import logging
import os
import queue
import threading
import time
from collections import deque
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

_PLUGIN_VERSION = "0.1.0"

import sqlite3

# ── Fail-open decorator ────────────────────────────────────────────────────

def _fail_open(fn):
    """Ensure hook handler exceptions never impact the agent loop."""
    @functools.wraps(fn)
    def wrapper(**kwargs):
        try:
            return fn(**kwargs)
        except Exception as exc:
            logger.warning("Analytics %s failed: %s", fn.__name__, exc)
    return wrapper


# ── Indicative cost tracking ───────────────────────────────────────────────
#
# Three-tier resolution for indicative cost (what usage would cost at published
# per-token rates), independent of billing model (flat-rate, per-token, local):
#
#   TIER 1: Core's agent.usage_pricing — handles recognized providers
#           (OpenAI, Anthropic, Google, DeepSeek, Bedrock, Fireworks).
#           Has cache_read + cache_write pricing. Most complete source.
#
#   TIER 2: OpenRouter API (via alias map) — handles open-weight models served
#           through aggregators like Ollama Cloud. Has cache_read pricing for
#           most models. Preferred over HF Router for this reason.
#
#   TIER 3: HF Router API (via alias map) — fallback for models not on OpenRouter.
#           Input/output only, NO cache pricing. Cache tokens priced at full
#           input rate (overstates cost). cost_source="hf_router" signals this.
#
#   TIER 4: None — no pricing available. Returns (None, "unknown", "none").
#
# The alias map (model_aliases.yaml) maps local model names to canonical IDs
# on OpenRouter and HF Router. It's the plugin's only configuration for pricing.
# Rates are fetched live and cached; no hardcoded rate tables.

import yaml as _yaml
import urllib.request as _urlreq


# ── Alias map loading ─────────────────────────────────────────────────────

_ALIASES: Optional[dict] = None
_ALIASES_LOCK = threading.Lock()


def _load_aliases() -> dict:
    """Load model_aliases.yaml, caching the result."""
    global _ALIASES
    with _ALIASES_LOCK:
        if _ALIASES is not None:
            return _ALIASES
        alias_path = os.path.join(
            os.path.dirname(os.path.abspath(__file__)), "model_aliases.yaml"
        )
        try:
            with open(alias_path) as f:
                data = _yaml.safe_load(f)
            _ALIASES = data or {}
        except Exception as exc:
            logger.warning("Could not load model_aliases.yaml: %s", exc)
            _ALIASES = {}
        return _ALIASES


def _resolve_alias(model: str) -> tuple[Optional[str], Optional[str]]:
    """Resolve a local model name to (openrouter_id, hf_id) via the alias map.

    Strips :cloud, -cloud, :675b suffixes iteratively for lookup.
    Returns (None, None) if the model is not in the alias map.
    """
    aliases = _load_aliases()
    key = model.lower().strip()

    # Direct lookup
    entry = aliases.get(key)
    if entry:
        return entry.get("openrouter"), entry.get("hf")

    # Try stripping suffixes iteratively
    candidates = [key]
    for sep in [":cloud", "-cloud", ":675b", ":0731", ":preview", ":397b"]:
        new_candidates = []
        for c in candidates:
            stripped = c.replace(sep, "")
            if stripped != c:
                new_candidates.append(stripped)
        candidates.extend(new_candidates)

    # Try each candidate
    for c in candidates:
        entry = aliases.get(c)
        if entry:
            return entry.get("openrouter"), entry.get("hf")

    return None, None


# ── Pricing config loading ─────────────────────────────────────────────────

_PRICING_CONFIG: Optional[dict] = None
_PRICING_CONFIG_LOCK = threading.Lock()

_DEFAULT_SOURCE_PRIORITY = ["override", "core", "openrouter", "hf_router"]


def _load_pricing_config() -> dict:
    """Load pricing_config.yaml, caching the result.

    Returns a dict with:
      - "source_priority": list of source names in resolution order
      - "overrides": {model_name: {input, output, cache_read?}} per-million USD
    """
    global _PRICING_CONFIG
    with _PRICING_CONFIG_LOCK:
        if _PRICING_CONFIG is not None:
            return _PRICING_CONFIG
        config_path = os.path.join(
            os.path.dirname(os.path.abspath(__file__)), "pricing_config.yaml"
        )
        try:
            with open(config_path) as f:
                data = _yaml.safe_load(f) or {}
            _PRICING_CONFIG = {
                "source_priority": data.get("source_priority") or _DEFAULT_SOURCE_PRIORITY,
                "overrides": data.get("overrides") or {},
            }
        except Exception as exc:
            logger.warning("Could not load pricing_config.yaml: %s", exc)
            _PRICING_CONFIG = {
                "source_priority": _DEFAULT_SOURCE_PRIORITY,
                "overrides": {},
            }
        return _PRICING_CONFIG


def _resolve_override(model: str, overrides: dict) -> Optional[dict]:
    """Look up user-supplied override rates for a model.

    Strips :cloud and other suffixes, same as _resolve_alias.
    Returns {input, output, cache_read?} or None.
    """
    key = model.lower().strip()
    entry = overrides.get(key)
    if entry:
        return entry
    # Try suffix stripping (same logic as _resolve_alias)
    candidates = [key]
    for sep in [":cloud", "-cloud", ":675b", ":0731", ":preview", ":397b"]:
        new_candidates = []
        for c in candidates:
            stripped = c.replace(sep, "")
            if stripped != c:
                new_candidates.append(stripped)
        candidates.extend(new_candidates)
    for c in candidates:
        entry = overrides.get(c)
        if entry:
            return entry
    return None


# ── OpenRouter pricing cache ───────────────────────────────────────────────

_OR_PRICING: Optional[dict] = None
_OR_PRICING_LOCK = threading.Lock()
_OR_PRICING_TTL = 3600  # 1 hour
_OR_PRICING_FETCHED = 0.0


def _fetch_openrouter_pricing() -> dict:
    """Fetch and cache OpenRouter model pricing. Returns {model_id: {pricing dict}}.

    Pricing values are per-million-token USD rates.
    """
    global _OR_PRICING, _OR_PRICING_FETCHED
    with _OR_PRICING_LOCK:
        now = time.time()
        if _OR_PRICING is not None and (now - _OR_PRICING_FETCHED) < _OR_PRICING_TTL:
            return _OR_PRICING
        try:
            req = _urlreq.Request(
                "https://openrouter.ai/api/v1/models",
                headers={"User-Agent": "hermes-analytics/1.0"},
            )
            with _urlreq.urlopen(req, timeout=15) as resp:
                data = json.loads(resp.read().decode("utf-8"))
            pricing = {}
            for m in data.get("data", []):
                mid = m.get("id", "")
                p = m.get("pricing", {})
                prompt = float(p.get("prompt", 0) or 0) * 1_000_000
                completion = float(p.get("completion", 0) or 0) * 1_000_000
                cache_read_raw = p.get("cache_read") or p.get("cached_prompt") or p.get("input_cache_read")
                cache_read = float(cache_read_raw) * 1_000_000 if cache_read_raw else None
                if prompt or completion:
                    pricing[mid] = {
                        "input": prompt,
                        "output": completion,
                        "cache_read": cache_read,
                    }
            _OR_PRICING = pricing
            _OR_PRICING_FETCHED = now
            logger.debug("Fetched OpenRouter pricing: %d models", len(pricing))
            return _OR_PRICING
        except Exception as exc:
            logger.warning("Failed to fetch OpenRouter pricing: %s", exc)
            return _OR_PRICING or {}


# ── HF Router pricing cache ───────────────────────────────────────────────

_HF_PRICING: Optional[dict] = None
_HF_PRICING_LOCK = threading.Lock()
_HF_PRICING_TTL = 3600  # 1 hour
_HF_PRICING_FETCHED = 0.0


def _fetch_hf_router_pricing() -> dict:
    """Fetch and cache HF Router model pricing. Returns {hf_model_id: {pricing dict}}.

    Uses cheapest provider per model. Pricing values are per-million-token USD rates.
    No cache pricing available from HF Router.
    """
    global _HF_PRICING, _HF_PRICING_FETCHED
    with _HF_PRICING_LOCK:
        now = time.time()
        if _HF_PRICING is not None and (now - _HF_PRICING_FETCHED) < _HF_PRICING_TTL:
            return _HF_PRICING
        try:
            req = _urlreq.Request(
                "https://router.huggingface.co/v1/models",
                headers={"User-Agent": "hermes-analytics/1.0"},
            )
            with _urlreq.urlopen(req, timeout=15) as resp:
                data = json.loads(resp.read().decode("utf-8"))
            pricing = {}
            for m in data.get("data", []):
                mid = m.get("id", "")
                providers = m.get("providers", [])
                # Pick cheapest provider by input rate
                best = None
                best_input = float("inf")
                for p in providers:
                    pr = p.get("pricing", {})
                    inp = float(pr.get("input", 0) or 0)
                    out = float(pr.get("output", 0) or 0)
                    if inp > 0 and inp < best_input:
                        best = {"input": inp, "output": out}
                        best_input = inp
                if best:
                    pricing[mid] = best
            _HF_PRICING = pricing
            _HF_PRICING_FETCHED = now
            logger.debug("Fetched HF Router pricing: %d models", len(pricing))
            return _HF_PRICING
        except Exception as exc:
            logger.warning("Failed to fetch HF Router pricing: %s", exc)
            return _HF_PRICING or {}


# ── Cost computation ──────────────────────────────────────────────────────

def _compute_cost(
    model: str, usage: dict, provider: str = "", base_url: str = ""
) -> tuple[Optional[float], str, str]:
    """Compute indicative cost via configurable source resolution.

    Returns (cost_usd, cost_status, cost_source):
      - cost_usd: estimated USD amount, or None if unpriced
      - cost_status: "actual" | "estimated" | "included" | "unknown"
      - cost_source: "override" | "core:official_docs_snapshot" |
                     "core:provider_models_api" | "openrouter" | "hf_router" | "none"

    Source priority and per-model overrides are configured in pricing_config.yaml.
    """
    if not model or not usage:
        return (None, "unknown", "none")

    config = _load_pricing_config()
    source_priority = config["source_priority"]
    overrides = config["overrides"]

    # Resolve alias once (used by openrouter and hf_router tiers)
    or_id, hf_id = _resolve_alias(model)

    for source in source_priority:
        if source == "override":
            rates = _resolve_override(model, overrides)
            if rates:
                amount = _compute_from_rates(usage, rates)
                if amount is not None:
                    return (amount, "actual", "override")

        elif source == "core":
            try:
                from agent.usage_pricing import estimate_usage_cost, normalize_usage
                canonical = normalize_usage(usage, provider=provider, api_mode="")
                result = estimate_usage_cost(
                    model, canonical, provider=provider, base_url=base_url, api_key=""
                )
                if result.amount_usd is not None and result.amount_usd > 0:
                    return (float(result.amount_usd), result.status, f"core:{result.source}")
            except Exception as exc:
                logger.debug("Core pricing failed for %s: %s", model, exc)

        elif source == "openrouter":
            if or_id:
                or_pricing = _fetch_openrouter_pricing()
                rates = or_pricing.get(or_id)
                if rates:
                    amount = _compute_from_rates(usage, rates)
                    if amount is not None:
                        return (amount, "estimated", "openrouter")

        elif source == "hf_router":
            if hf_id:
                hf_pricing = _fetch_hf_router_pricing()
                rates = hf_pricing.get(hf_id)
                if rates:
                    amount = _compute_from_rates(usage, rates)
                    if amount is not None:
                        return (amount, "estimated", "hf_router")

    return (None, "unknown", "none")


def _compute_from_rates(usage: dict, rates: dict) -> Optional[float]:
    """Compute cost from token counts and rate dict (per-million USD).

    Handles cache_read and cache_write tokens when rates are available.
    When cache_read rate is None, cached tokens are priced at full input
    rate (overstates cost). When cache_write rate is None, cache write
    tokens are priced at full input rate (conservative).
    """
    input_tokens = int(usage.get("input_tokens") or usage.get("prompt_tokens") or 0)
    output_tokens = int(usage.get("output_tokens") or usage.get("completion_tokens") or 0)
    cache_read = int(usage.get("cache_read_tokens") or 0)
    cache_write = int(usage.get("cache_write_tokens") or 0)

    input_rate = rates.get("input", 0)
    output_rate = rates.get("output", 0)
    cache_read_rate = rates.get("cache_read")  # May be None
    cache_write_rate = rates.get("cache_write")  # May be None

    amount = 0.0

    # Non-cached input
    amount += (input_tokens / 1_000_000) * input_rate

    # Cached input (read): at cache_read rate if available, else at full input rate
    if cache_read:
        effective_rate = cache_read_rate if cache_read_rate is not None else input_rate
        amount += (cache_read / 1_000_000) * effective_rate

    # Cache write: at cache_write rate if available, else at full input rate
    if cache_write:
        effective_rate = cache_write_rate if cache_write_rate is not None else input_rate
        amount += (cache_write / 1_000_000) * effective_rate

    # Output
    amount += (output_tokens / 1_000_000) * output_rate

    if amount == 0.0 and not (input_tokens or output_tokens or cache_read or cache_write):
        return None

    return round(amount, 6)


def _extract_usage(usage: Any) -> dict:
    if not usage:
        return {}
    if isinstance(usage, dict):
        return usage
    result = {}
    for attr in ("input_tokens", "output_tokens", "completion_tokens",
                 "prompt_tokens", "total_tokens", "cache_read_tokens",
                 "cache_write_tokens", "reasoning_tokens",
                 "cost", "request_cost", "total_cost", "amount_usd"):
        val = getattr(usage, attr, None)
        if val is not None:
            result[attr] = val
    return result


# ── Context window resolution ──────────────────────────────────────────────

_CTX_LIMIT_CACHE: Dict[str, int] = {}
_CTX_LIMIT_CACHE_LOCK = threading.Lock()


def _resolve_context_length(model: str) -> int:
    if not model:
        return 0
    with _CTX_LIMIT_CACHE_LOCK:
        if model in _CTX_LIMIT_CACHE:
            return _CTX_LIMIT_CACHE[model]

    # Tier 0: explicit env var (standalone / non-Hermes deployments)
    env_ctx = os.environ.get("ANALYTICS_CONTEXT_LENGTH")
    if env_ctx:
        try:
            ctx_len = int(env_ctx)
            if ctx_len > 0:
                with _CTX_LIMIT_CACHE_LOCK:
                    _CTX_LIMIT_CACHE[model] = ctx_len
                return ctx_len
        except ValueError:
            pass

    try:
        hermes_home = os.environ.get("HERMES_HOME", os.path.expanduser("~/.hermes"))
        parent_name = os.path.basename(os.path.dirname(hermes_home))
        if parent_name == "profiles":
            root = os.path.dirname(os.path.dirname(hermes_home))
        elif os.path.isdir(os.path.join(hermes_home, "profiles")):
            root = hermes_home
        else:
            root = os.path.expanduser("~/.hermes")
        active = os.environ.get("HERMES_PROFILE")
        if not active:
            active = os.path.basename(hermes_home) if parent_name == "profiles" else "phoenix"
        config_path = os.path.join(root, "profiles", active, "config.yaml")
        if os.path.exists(config_path):
            import yaml
            with open(config_path) as f:
                cfg = yaml.safe_load(f) or {}
            ctx_len = (cfg.get("model") or {}).get("context_length")
            if isinstance(ctx_len, int) and ctx_len > 0:
                with _CTX_LIMIT_CACHE_LOCK:
                    _CTX_LIMIT_CACHE[model] = ctx_len
                return ctx_len
    except Exception:
        pass
    try:
        from agent.model_metadata import get_model_context_length
        provider = os.environ.get("HERMES_PROVIDER", "")
        ctx_len = get_model_context_length(model, provider=provider)
        if isinstance(ctx_len, int) and ctx_len > 0:
            with _CTX_LIMIT_CACHE_LOCK:
                _CTX_LIMIT_CACHE[model] = ctx_len
            return ctx_len
    except Exception:
        pass
    with _CTX_LIMIT_CACHE_LOCK:
        _CTX_LIMIT_CACHE[model] = 0
    return 0


# ── Per-profile DB path ────────────────────────────────────────────────────

_DB_PATH: Optional[str] = None


def _get_db_path() -> str:
    global _DB_PATH
    if _DB_PATH is None:
        # Tier 0: explicit env var (standalone / non-Hermes deployments)
        env_path = os.environ.get("ANALYTICS_DB_PATH")
        if env_path:
            _DB_PATH = os.path.expanduser(env_path)
            return _DB_PATH

        # Tier 1: Hermes profile-aware path
        hermes_home = os.environ.get("HERMES_HOME", os.path.expanduser("~/.hermes"))
        parent_name = os.path.basename(os.path.dirname(hermes_home))
        if parent_name == "profiles":
            root = os.path.dirname(os.path.dirname(hermes_home))
        elif os.path.isdir(os.path.join(hermes_home, "profiles")):
            root = hermes_home
        else:
            root = os.path.expanduser("~/.hermes")
        active = os.environ.get("HERMES_PROFILE")
        if not active:
            active = os.path.basename(hermes_home) if parent_name == "profiles" else "phoenix"
        _DB_PATH = os.path.join(root, "profiles", active, "analytics.db")
    return _DB_PATH


# ── SQLite connection ─────────────────────────────────────────────────────

_DB_CONN: Optional[Any] = None
_SCHEMA_READY = False
_conn_lock = threading.Lock()

# SQLite — no namespace/database concept needed


def _ensure_schema(conn: Any) -> None:
    global _SCHEMA_READY
    if _SCHEMA_READY:
        return

    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")

    # Tables with explicit schema (allow NULLs for optional fields)
    conn.execute("""CREATE TABLE IF NOT EXISTS llm_calls (
        session_id TEXT, task_id TEXT, model TEXT,
        prompt_tokens INTEGER, completion_tokens INTEGER, total_tokens INTEGER,
        cache_read_tokens INTEGER, cost_usd REAL, cost_status TEXT, cost_source TEXT,
        duration_ms INTEGER, status TEXT, turn_id TEXT, api_request_id TEXT,
        provider TEXT, finish_reason TEXT,
        timestamp INTEGER DEFAULT (strftime('s','now'))
    )""")
    conn.execute("""CREATE TABLE IF NOT EXISTS tool_calls (
        session_id TEXT, task_id TEXT, tool_name TEXT, status TEXT,
        duration_ms INTEGER, args_hash TEXT, error_message TEXT,
        exit_code INTEGER, turn_id TEXT, tool_call_id TEXT, error_type TEXT,
        timestamp INTEGER DEFAULT (strftime('s','now'))
    )""")
    conn.execute("""CREATE TABLE IF NOT EXISTS context_pressure (
        session_id TEXT, turn_number INTEGER, context_used_chars INTEGER,
        context_limit_chars INTEGER, utilization_pct REAL,
        compression_triggered INTEGER, model TEXT,
        timestamp INTEGER DEFAULT (strftime('s','now'))
    )""")
    conn.execute("""CREATE TABLE IF NOT EXISTS session_summary (
        session_id TEXT PRIMARY KEY, platform TEXT,
        start_time INTEGER, end_time INTEGER,
        total_llm_calls INTEGER DEFAULT 0, total_tool_calls INTEGER DEFAULT 0,
        total_prompt_tokens INTEGER DEFAULT 0, total_completion_tokens INTEGER DEFAULT 0,
        total_cost_usd REAL DEFAULT 0, final_status TEXT
    )""")
    conn.execute("""CREATE TABLE IF NOT EXISTS skill_usage (
        session_id TEXT, task_id TEXT, skill_name TEXT, action TEXT,
        timestamp INTEGER DEFAULT (strftime('s','now'))
    )""")
    conn.execute("""CREATE TABLE IF NOT EXISTS approval_events (
        event_type TEXT, session_id TEXT, command_preview TEXT,
        platform TEXT, approved INTEGER,
        timestamp INTEGER DEFAULT (strftime('s','now'))
    )""")
    conn.execute("""CREATE TABLE IF NOT EXISTS kanban_events (
        event_type TEXT, task_id TEXT, board TEXT, assignee TEXT,
        run_id INTEGER, profile_name TEXT, summary TEXT, reason TEXT,
        timestamp INTEGER DEFAULT (strftime('s','now'))
    )""")
    conn.execute("""CREATE TABLE IF NOT EXISTS api_requests (
        session_id TEXT, task_id TEXT, turn_id TEXT, api_request_id TEXT,
        model TEXT, provider TEXT, api_mode TEXT, api_call_count INTEGER,
        message_count INTEGER, tool_count INTEGER, approx_input_tokens INTEGER,
        request_char_count INTEGER, max_tokens INTEGER, started_at REAL,
        timestamp INTEGER DEFAULT (strftime('s','now'))
    )""")
    conn.execute("""CREATE TABLE IF NOT EXISTS inbound_messages (
        platform TEXT, chat_id TEXT, user_id TEXT, user_name TEXT,
        thread_id TEXT, reply_to_id TEXT, message_length INTEGER,
        message_preview TEXT,
        timestamp INTEGER DEFAULT (strftime('s','now'))
    )""")
    conn.execute("""CREATE TABLE IF NOT EXISTS verify_events (
        session_id TEXT, model TEXT, platform TEXT, attempt INTEGER,
        edited_file_count INTEGER, edited_paths TEXT, response_length INTEGER,
        timestamp INTEGER DEFAULT (strftime('s','now'))
    )""")
    conn.execute("""CREATE TABLE IF NOT EXISTS _schema_meta (
        version TEXT, stamped_at INTEGER, plugin_version TEXT
    )""")

    # Schema version stamp
    _SCHEMA_VERSION = "1.0"
    existing = None
    try:
        row = conn.execute("SELECT version FROM _schema_meta LIMIT 1").fetchone()
        if row:
            existing = row[0]
    except Exception:
        pass
    if existing != _SCHEMA_VERSION:
        conn.execute("DELETE FROM _schema_meta")
        conn.execute("INSERT INTO _schema_meta (version, stamped_at, plugin_version) VALUES (?, ?, ?)",
                     (_SCHEMA_VERSION, int(time.time()), _PLUGIN_VERSION))

    # Indexes
    conn.execute("CREATE INDEX IF NOT EXISTS llm_calls_sid_idx ON llm_calls(session_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS llm_calls_ts_idx ON llm_calls(timestamp)")
    conn.execute("CREATE INDEX IF NOT EXISTS llm_calls_model_idx ON llm_calls(model)")
    conn.execute("CREATE INDEX IF NOT EXISTS llm_calls_sid_ts_idx ON llm_calls(session_id, timestamp)")
    conn.execute("CREATE INDEX IF NOT EXISTS tool_calls_sid_idx ON tool_calls(session_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS tool_calls_ts_idx ON tool_calls(timestamp)")
    conn.execute("CREATE INDEX IF NOT EXISTS tool_calls_tool_idx ON tool_calls(tool_name)")
    conn.execute("CREATE INDEX IF NOT EXISTS tool_calls_status_idx ON tool_calls(status)")
    conn.execute("CREATE INDEX IF NOT EXISTS ctx_sid_idx ON context_pressure(session_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS skill_sid_idx ON skill_usage(session_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS skill_name_idx ON skill_usage(skill_name)")
    conn.execute("CREATE INDEX IF NOT EXISTS api_req_sid_idx ON api_requests(session_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS api_req_req_id_idx ON api_requests(api_request_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS api_req_ts_idx ON api_requests(timestamp)")
    conn.execute("CREATE INDEX IF NOT EXISTS inbound_platform_idx ON inbound_messages(platform)")
    conn.execute("CREATE INDEX IF NOT EXISTS inbound_ts_idx ON inbound_messages(timestamp)")
    conn.execute("CREATE INDEX IF NOT EXISTS inbound_chat_idx ON inbound_messages(chat_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS verify_sid_idx ON verify_events(session_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS verify_model_idx ON verify_events(model)")

    conn.commit()
    _SCHEMA_READY = True


def _db() -> Any:
    global _DB_CONN
    if _DB_CONN is None:
        path = _get_db_path()
        os.makedirs(os.path.dirname(path), exist_ok=True)
        _DB_CONN = sqlite3.connect(path, timeout=10, check_same_thread=False)
        _DB_CONN.row_factory = sqlite3.Row
        _ensure_schema(_DB_CONN)
    return _DB_CONN


# ── Retention ────────────────────────────────────────────────────────────────

_RETENTION_DAYS: int | None = None
_RETAIN_INTERVAL_S = 3600  # Run at most once per hour
_last_retain_ts = 0.0
_retention_thread: Optional[threading.Thread] = None


def _get_retention_days() -> int:
    """Load retention_days from plugin.yaml (default) then $HERMES_HOME/analytics.yaml (override)."""
    global _RETENTION_DAYS
    if _RETENTION_DAYS is not None:
        return _RETENTION_DAYS

    # Tier 0: explicit env var (standalone / non-Hermes deployments)
    env_retention = os.environ.get("ANALYTICS_RETENTION_DAYS")
    if env_retention:
        try:
            days = int(env_retention)
            if days > 0:
                _RETENTION_DAYS = days
                return days
        except ValueError:
            pass

    default = 365
    # 1. Plugin default from plugin.yaml
    plugin_dir = os.path.dirname(os.path.abspath(__file__))
    plugin_yaml = os.path.join(plugin_dir, "plugin.yaml")
    try:
        import yaml
        with open(plugin_yaml) as f:
            cfg = yaml.safe_load(f) or {}
        default = (cfg.get("config") or {}).get("retention_days", 365)
        if not isinstance(default, int):
            default = 365
    except Exception:
        pass

    # 2. Profile-local override: $HERMES_HOME/analytics.yaml
    hermes_home = os.environ.get("HERMES_HOME", os.path.expanduser("~/.hermes"))
    user_yaml = os.path.join(hermes_home, "analytics.yaml")
    try:
        import yaml
        with open(user_yaml) as f:
            user_cfg = yaml.safe_load(f) or {}
        days = user_cfg.get("retention_days", default)
        if isinstance(days, int) and days > 0:
            default = days
    except Exception:
        pass

    _RETENTION_DAYS = default
    return _RETENTION_DAYS or 365


def _maybe_retain() -> None:
    """Delete raw event records older than retention_days.

    Runs at most once per hour (throttled by _RETAIN_INTERVAL_S).
    Session summaries, approval_events, and kanban_events are kept
    indefinitely (small, useful for long-term trends).
    """
    global _last_retain_ts
    now = time.time()
    if (now - _last_retain_ts) < _RETAIN_INTERVAL_S:
        return
    _last_retain_ts = now
    retention = _get_retention_days()
    try:
        conn = _db()
        cutoff_ts = int(now - retention * 86400)
        for table in ("llm_calls", "tool_calls", "context_pressure", "skill_usage",
                       "api_requests", "inbound_messages", "verify_events"):
            try:
                conn.execute(f"DELETE FROM {table} WHERE timestamp < ?", (cutoff_ts,))
            except Exception:
                pass
        conn.commit()
        logger.info("Analytics retention: deleted records older than %d days (ts < %d)", retention, cutoff_ts)
    except Exception as exc:
        logger.warning("Analytics retention failed: %s", exc)

    # Optimize after retention to reclaim space
    try:
        conn = _db()
        conn.execute("PRAGMA optimize")
        logger.info("Analytics optimized")
    except Exception as exc:
        logger.debug("Analytics optimize after retention: %s", exc)


def _retention_timer() -> None:
    """Background loop that runs retention periodically, independent of sessions.

    Headless deployments with no sessions never trigger _maybe_retain() via
    on_session_finalize. This daemon thread calls it every _RETAIN_INTERVAL_S
    (throttled internally to at most once per hour).
    """
    while not _SHUTDOWN:
        time.sleep(_RETAIN_INTERVAL_S)
        if _SHUTDOWN:
            break
        try:
            _maybe_retain()
        except Exception as exc:
            logger.debug("Analytics retention timer run failed: %s", exc)


def _start_retention_timer() -> None:
    """Start the periodic retention timer thread (idempotent, daemon)."""
    global _retention_thread
    if _retention_thread is not None and _retention_thread.is_alive():
        return
    _retention_thread = threading.Thread(target=_retention_timer, daemon=True,
                                         name="analytics-retention-timer")
    _retention_thread.start()


# ── Batched background writer ───────────────────────────────────────────────

_WriteItem = Tuple[str, List[str], List[Any]]
_write_queue: queue.Queue = queue.Queue()  # type: ignore
_writer_thread: Optional[threading.Thread] = None
_writer_lock = threading.Lock()
_FLUSH_INTERVAL_S = 0.25
_BATCH_SIZE = 25
_SHUTDOWN = False


def _writer_loop() -> None:
    buf: deque = deque()
    while not _SHUTDOWN:
        try:
            item = _write_queue.get(timeout=_FLUSH_INTERVAL_S)
            buf.append(item)
        except queue.Empty:
            if buf:
                try:
                    _flush_batch(buf)
                except Exception as exc:
                    logger.warning("Analytics flush raised (recovered): %s", exc)
                    buf.clear()
            continue
        if len(buf) >= _BATCH_SIZE:
            try:
                _flush_batch(buf)
            except Exception as exc:
                logger.warning("Analytics flush raised (recovered): %s", exc)
                buf.clear()
    # Drain remaining on shutdown
    while True:
        try:
            buf.append(_write_queue.get_nowait())
        except queue.Empty:
            break
    if buf:
        try:
            _flush_batch(buf)
        except Exception as exc:
            logger.warning("Analytics final flush raised: %s", exc)


def _flush_batch(buf: deque) -> None:
    if not buf:
        return
    raw_items: List[Tuple[str, tuple]] = []
    grouped: Dict[str, List[Tuple[List[str], List[Any]]]] = {}
    while buf:
        item = buf.popleft()
        if item[0] == "__raw__":
            raw_items.append((item[1], item[2]))
        else:
            table, fields, values = item
            grouped.setdefault(table, []).append((fields, values))
    try:
        with _conn_lock:
            conn = _db()
            for table, items in grouped.items():
                for fields, values in items:
                    placeholders = ", ".join(["?"] * len(fields))
                    col_names = ", ".join(fields)
                    try:
                        conn.execute(f"INSERT INTO {table} ({col_names}) VALUES ({placeholders})", values)
                    except Exception as exc:
                        logger.warning("Analytics insert %s failed: %s", table, exc)
            for sql, params in raw_items:
                try:
                    if isinstance(params, (list, tuple)):
                        conn.execute(sql, params)
                    elif isinstance(params, dict):
                        conn.execute(sql, params)
                    else:
                        conn.execute(sql)
                    conn.commit()
                except Exception as exc:
                    logger.debug("Analytics raw query failed: %s", exc)
    except Exception as exc:
        logger.warning("Analytics flush failed: %s", exc)


def _start_writer() -> None:
    global _writer_thread
    with _writer_lock:
        if _writer_thread is None or not _writer_thread.is_alive():
            _writer_thread = threading.Thread(target=_writer_loop, daemon=True, name="analytics-writer")
            _writer_thread.start()


def _enqueue(table: str, fields: List[str], values: List[Any]) -> None:
    _start_writer()
    _write_queue.put((table, fields, values))


def _enqueue_raw(sql: str, params: Any = None) -> None:
    """Enqueue a raw SQL statement for the writer thread.

    params can be a tuple/list (for ? placeholders), a dict (for :named), or None.
    """
    _start_writer()
    _write_queue.put(("__raw__", sql, params if params is not None else {}))


def _update_session(sid: str, delta_cost: float = 0.0,
                     delta_prompt: int = 0, delta_completion: int = 0,
                     llm_calls: int = 0, tool_calls: int = 0) -> None:
    _enqueue_raw(
        "INSERT INTO session_summary (session_id, start_time, total_llm_calls, "
        "total_tool_calls, total_prompt_tokens, total_completion_tokens, total_cost_usd) "
        "VALUES (?, strftime('%s','now'), ?, ?, ?, ?, ?) "
        "ON CONFLICT(session_id) DO UPDATE SET "
        "total_llm_calls = total_llm_calls + ?, "
        "total_tool_calls = total_tool_calls + ?, "
        "total_prompt_tokens = total_prompt_tokens + ?, "
        "total_completion_tokens = total_completion_tokens + ?, "
        "total_cost_usd = total_cost_usd + ?",
        (sid, llm_calls, tool_calls, delta_prompt, delta_completion, delta_cost,
         llm_calls, tool_calls, delta_prompt, delta_completion, delta_cost),
    )


def _flush_sync(timeout: float = 5.0) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        if _write_queue.empty():
            time.sleep(_FLUSH_INTERVAL_S + 0.05)
            if _write_queue.empty():
                return
        time.sleep(0.05)


# ── Helpers ─────────────────────────────────────────────────────────────────

def _args_hash(args: Any) -> str:
    if args is None:
        return ""
    try:
        payload = json.dumps(args, sort_keys=True, default=str)
        return hashlib.md5(payload[:4096].encode()).hexdigest()
    except Exception:
        return ""


def _extract_exit_code(result: Any) -> Optional[int]:
    parsed = None
    if isinstance(result, dict):
        parsed = result
    elif isinstance(result, str):
        try:
            parsed = json.loads(result)
        except (json.JSONDecodeError, ValueError):
            pass
    if isinstance(parsed, dict) and "exit_code" in parsed:
        val = parsed.get("exit_code")
        if isinstance(val, int):
            return val
    return None


# ── In-memory state ────────────────────────────────────────────────────────

class _State:
    __slots__ = ("lock", "turn_number", "session_platform", "model", "task_id", "api_request_start")
    def __init__(self):
        self.lock = threading.Lock()
        self.turn_number = 0
        self.session_platform = ""
        self.model = ""
        self.task_id = ""

_STATE: Dict[str, _State] = {}


def _state(sid: str) -> _State:
    if sid not in _STATE:
        _STATE[sid] = _State()
    return _STATE[sid]


# ── Context pressure dedup ─────────────────────────────────────────────────

_last_ctx_key: Tuple[str, int, int] = ("", 0, 0)
_last_ctx_ts = 0.0
_CTX_DEDUP_S = 1.5
_ctx_dedup_lock = threading.Lock()


# ── Hook callbacks (v1 contract) ───────────────────────────────────────────

@_fail_open
def on_session_start(*, session_id: str = "", model: str = "", platform: str = "",
                     sender_id: str = "", **kwargs) -> None:
    st = _state(session_id)
    with st.lock:
        st.session_platform = platform or ""
        st.model = model or ""
        st.turn_number = 0
    _enqueue_raw(
        "INSERT INTO session_summary (session_id, platform, start_time) "
        "VALUES (?, ?, strftime('%s','now')) "
        "ON CONFLICT(session_id) DO UPDATE SET "
        "platform = COALESCE(excluded.platform, session_summary.platform), "
        "start_time = COALESCE(session_summary.start_time, excluded.start_time)",
        (session_id, platform or ""),
    )


@_fail_open
def pre_llm_call(*, session_id: str = "", model: str = "", task_id: str = "",
                conversation_history: list = None, system_prompt: str = "",
                context_length: int = 0, **kwargs) -> None:
    st = _state(session_id)
    with st.lock:
        st.model = model or st.model
        st.turn_number += 1
        if task_id:
            st.task_id = task_id

    _ctx_limit = context_length or (model and _resolve_context_length(model)) or 0

    _ctx_len = 0
    if isinstance(conversation_history, list):
        try:
            _json = json.dumps(conversation_history, default=str)
            _ctx_len = len(_json.encode("utf-8"))
        except (TypeError, ValueError):
            pass

    _util = round((_ctx_len / 4.0 / _ctx_limit) * 100, 2) if _ctx_limit and _ctx_len else 0.0

    global _last_ctx_key, _last_ctx_ts
    now = time.time()
    key = (session_id or "", st.turn_number, _ctx_len)
    with _ctx_dedup_lock:
        if key == _last_ctx_key and (now - _last_ctx_ts) < _CTX_DEDUP_S:
            return
        _last_ctx_key = key
        _last_ctx_ts = now

    if _ctx_limit:
        _enqueue("context_pressure", [
            "session_id", "turn_number", "context_used_chars", "context_limit_chars",
            "utilization_pct", "compression_triggered", "model",
        ], [
            session_id or "", st.turn_number, _ctx_len, _ctx_limit,
            _util, 0, model or st.model,
        ])


@_fail_open
def post_api_request(*, session_id: str = "", task_id: str = "", model: str = "",
                     provider: str = "", base_url: str = "", api_mode: str = "",
                     api_duration: float = 0.0, usage: Any = None,
                     finish_reason: str = "", api_request_id: str = "",
                     turn_id: str = "", api_call_count: int = 0,
                     **kwargs) -> None:
    st = _state(session_id)
    duration_ms = int(api_duration * 1000) if api_duration else 0

    usage_dict = _extract_usage(usage)
    prompt_tokens = int(usage_dict.get("input_tokens") or usage_dict.get("prompt_tokens") or 0)
    completion_tokens = int(usage_dict.get("output_tokens") or usage_dict.get("completion_tokens") or 0)
    total_tokens = int(usage_dict.get("total_tokens") or (prompt_tokens + completion_tokens) or 0)
    cache_read = int(usage_dict.get("cache_read_tokens") or 0)

    # Provider-returned actual cost (e.g. OpenRouter's usage.cost)
    provider_cost = None
    for cost_key in ("cost", "request_cost", "total_cost", "amount_usd"):
        val = usage_dict.get(cost_key)
        if val is not None:
            try:
                provider_cost = float(val)
                if provider_cost > 0:
                    break
                provider_cost = None
            except (TypeError, ValueError):
                continue

    if provider_cost is not None:
        cost, cost_status, cost_source = provider_cost, "actual", "provider_response"
    elif prompt_tokens or completion_tokens:
        cost, cost_status, cost_source = _compute_cost(model or st.model, usage_dict, provider, base_url)
    else:
        cost, cost_status, cost_source = None, "unknown", "none"

    _enqueue("llm_calls", [
        "session_id", "task_id", "model", "prompt_tokens", "completion_tokens", "total_tokens",
        "cache_read_tokens", "cost_usd", "cost_status", "cost_source", "duration_ms", "status",
        "turn_id", "api_request_id", "provider", "finish_reason",
    ], [
        session_id, task_id or getattr(st, "task_id", "") or "", model or st.model,
        prompt_tokens, completion_tokens, total_tokens,
        cache_read, cost, cost_status, cost_source, duration_ms, "success",
        turn_id, api_request_id, provider, finish_reason,
    ])
    _update_session(session_id, delta_cost=cost or 0.0,
                    delta_prompt=prompt_tokens,
                    delta_completion=completion_tokens, llm_calls=1)


@_fail_open
def pre_tool_call(*, tool_name: str = "", session_id: str = "", task_id: str = "",
                  tool_call_id: str = "", turn_id: str = "", **kwargs) -> None:
    st = _state(session_id)
    with st.lock:
        if task_id:
            st.task_id = task_id


@_fail_open
def post_tool_call(*, tool_name: str = "", session_id: str = "", task_id: str = "",
                  result: Any = None, status: str = "", error_message: str = "",
                  error_type: str = "", duration_ms: int = 0, args: Any = None,
                  tool_call_id: str = "", turn_id: str = "", **kwargs) -> None:
    st = _state(session_id)

    if status == "ok":
        db_status = "success"
    elif status == "blocked":
        db_status = "policy_denied"
    elif status == "cancelled":
        db_status = "cancelled"
    elif status == "error":
        db_status = "failure"
    else:
        db_status = "failure" if error_message else "success"

    exit_code_val = _extract_exit_code(result) if tool_name in ("terminal", "execute_code") else None

    _enqueue("tool_calls", [
        "session_id", "task_id", "tool_name", "status", "duration_ms",
        "args_hash", "error_message", "exit_code",
        "turn_id", "tool_call_id", "error_type",
    ], [
        session_id, task_id or getattr(st, "task_id", "") or "", tool_name,
        db_status, duration_ms, _args_hash(args), error_message,
        exit_code_val, turn_id, tool_call_id, error_type,
    ])
    _update_session(session_id, tool_calls=1)

    if tool_name in ("skill_view", "skill_manage"):
        skill_name = None
        if isinstance(args, dict):
            skill_name = args.get("name")
        if not skill_name and isinstance(result, str):
            try:
                parsed = json.loads(result)
                if isinstance(parsed, dict) and parsed.get("success") and parsed.get("name"):
                    skill_name = parsed.get("name")
            except (json.JSONDecodeError, ValueError):
                pass
        if skill_name:
            _enqueue("skill_usage", [
                "session_id", "task_id", "skill_name", "action",
            ], [
                session_id, task_id or getattr(st, "task_id", "") or "",
                str(skill_name), "view" if tool_name == "skill_view" else "manage",
            ])


@_fail_open
def api_request_error(*, session_id: str = "", task_id: str = "", model: str = "",
                      provider: str = "", error: dict = None,
                      status_code: int = None, retry_count: int = None,
                      max_retries: int = None, retryable: bool = None,
                      reason: str = "", api_duration: float = 0.0,
                      turn_id: str = "", api_request_id: str = "",
                      **kwargs) -> None:
    st = _state(session_id)
    _error_type = ""
    _error_message = ""
    if isinstance(error, dict):
        _error_type = error.get("type", "") or ""
        _error_message = error.get("message", "") or ""
    if not _error_type and status_code:
        _error_type = f"HTTP{status_code}"
    if not _error_type and not _error_message:
        return

    duration_ms = int(api_duration * 1000) if api_duration else 0

    _enqueue("llm_calls", [
        "session_id", "task_id", "model", "prompt_tokens", "completion_tokens", "total_tokens",
        "cache_read_tokens", "cost_usd", "cost_status", "cost_source", "duration_ms", "status",
        "turn_id", "api_request_id", "provider", "finish_reason",
    ], [
        session_id, task_id or getattr(st, "task_id", "") or "", model or st.model,
        0, 0, 0, 0, 0.0, "unknown", "none", duration_ms, "error",
        turn_id, api_request_id, provider, _error_type,
    ])
    _update_session(session_id, llm_calls=1)


@_fail_open
def on_pre_approval_request(*, session_id: str = "", command_preview: str = "",
                             platform: str = "", **kwargs) -> None:
    _enqueue("approval_events", [
        "event_type", "session_id", "command_preview", "platform",
    ], [
        "request", session_id or "", command_preview or "", platform or "",
    ])


@_fail_open
def on_post_approval_response(*, session_id: str = "", command_preview: str = "",
                              platform: str = "", choice: str = "",
                              **kwargs) -> None:
    approved = choice in ("once", "session", "always")
    _enqueue("approval_events", [
        "event_type", "session_id", "command_preview", "platform", "approved",
    ], [
        "approved" if approved else "rejected",
        session_id or "", command_preview or "", platform or "",
        1 if approved else 0,
    ])


@_fail_open
def on_session_finalize(*, session_id: str = "", **kwargs) -> None:
    _flush_sync(timeout=5.0)
    _enqueue_raw(
        "UPDATE session_summary SET end_time = strftime('%s','now'), "
        "final_status = COALESCE(final_status, 'finalized') WHERE session_id = ?",
        (session_id,),
    )
    _STATE.pop(session_id, None)
    # Periodic retention — delete raw event records older than 90 days
    _maybe_retain()


@_fail_open
def on_session_reset(*, session_id: str = "", **kwargs) -> None:
    _enqueue_raw(
        "UPDATE session_summary SET final_status = 'reset' WHERE session_id = ?",
        (session_id,),
    )


@_fail_open
def on_session_end(*, session_id: str = "", interrupted: bool = False,
                   completed: bool = False, **kwargs) -> None:
    _flush_sync(timeout=5.0)
    status = "interrupted" if interrupted else ("completed" if completed else "unknown")

    # Reconcile from ground-truth event tables via raw SQL
    _enqueue_raw(
        "UPDATE session_summary SET "
        "end_time = strftime('%s','now'), "
        "final_status = ?, "
        "total_cost_usd = (SELECT COALESCE(SUM(cost_usd), 0) FROM llm_calls WHERE session_id = ?), "
        "total_prompt_tokens = (SELECT COALESCE(SUM(prompt_tokens), 0) FROM llm_calls WHERE session_id = ?), "
        "total_completion_tokens = (SELECT COALESCE(SUM(completion_tokens), 0) FROM llm_calls WHERE session_id = ?), "
        "total_llm_calls = (SELECT COUNT(*) FROM llm_calls WHERE session_id = ?), "
        "total_tool_calls = (SELECT COUNT(*) FROM tool_calls WHERE session_id = ?) "
        "WHERE session_id = ?",
        (status, session_id, session_id, session_id, session_id, session_id, session_id),
    )
    _STATE.pop(session_id, None)

# ── Pre-API-request: request-side analytics ────────────────────────────────

@_fail_open
def pre_api_request(*, session_id: str = "", task_id: str = "", turn_id: str = "",
                    api_request_id: str = "", model: str = "", provider: str = "",
                    base_url: str = "", api_mode: str = "", api_call_count: int = 0,
                    message_count: int = 0, tool_count: int = 0,
                    approx_input_tokens: int = 0, request_char_count: int = 0,
                    max_tokens: int = 0, started_at: float = 0.0,
                    request_messages: list = None, **kwargs) -> None:
    """Capture request-side metrics before the API call.

    Combined with post_api_request this gives us end-to-end latency
    and payload efficiency (tokens sent vs returned).
    """
    st = _state(session_id)
    st.api_request_start = started_at
    _enqueue("api_requests", [
        "session_id", "task_id", "turn_id", "api_request_id",
        "model", "provider", "api_mode", "api_call_count",
        "message_count", "tool_count", "approx_input_tokens",
        "request_char_count", "max_tokens", "started_at",
    ], [
        session_id, task_id or "", turn_id, api_request_id,
        model or st.model, provider, api_mode, api_call_count,
        message_count, tool_count, approx_input_tokens,
        request_char_count, max_tokens, started_at,
    ])


# ── Gateway dispatch: inbound message tracking ──────────────────────────────

@_fail_open
def on_pre_gateway_dispatch(*, event=None, gateway=None, session_store=None,
                            **kwargs) -> None:
    """Track every inbound message before it reaches the agent.

    Captures platform, chat ID, text length, reply context.
    Combined with on_session_start/end this gives us message volume
    and response time analytics.
    """
    if event is None:
        return
    _src = getattr(event, "source", None)
    _platform = getattr(_src, "platform", None) if _src else None
    _platform_name = getattr(_platform, "value", str(_platform)) if _platform else "unknown"
    _chat_id = getattr(_src, "chat_id", "") or "" if _src else ""
    _user_id = getattr(_src, "user_id", "") or "" if _src else ""
    _user_name = getattr(_src, "user_name", "") or "" if _src else ""
    _thread_id = getattr(_src, "thread_id", "") or "" if _src else ""
    _text = getattr(event, "text", "") or ""
    _reply_to_id = getattr(event, "reply_to_message_id", "") or ""
    _msg_len = len(_text)
    _msg_preview = _text[:80].replace("\n", " ") if _text else ""

    _enqueue("inbound_messages", [
        "platform", "chat_id", "user_id", "user_name",
        "thread_id", "reply_to_id", "message_length", "message_preview",
    ], [
        _platform_name, _chat_id, _user_id, _user_name,
        _thread_id, _reply_to_id, _msg_len, _msg_preview,
    ])


# ── Pre-verify: verification quality tracking ────────────────────────────────

@_fail_open
def on_pre_verify(*, session_id: str = "", model: str = "", platform: str = "",
                  attempt: int = 0, changed_paths: list = None,
                  final_response: str = "", **kwargs) -> None:
    """Track when the agent is about to finish after editing code.

    Records whether files were edited and which verification attempt
    this is. Combined with turn outcomes this gives us self-verification
    rates and model quality comparison.
    """
    _edited_count = len(changed_paths) if changed_paths else 0
    _edited_paths = ",".join(changed_paths[:10]) if changed_paths else ""

    _enqueue("verify_events", [
        "session_id", "model", "platform", "attempt",
        "edited_file_count", "edited_paths", "response_length",
    ], [
        session_id, model, platform, attempt,
        _edited_count, _edited_paths[:500], len(final_response),
    ])


# ── Kanban lifecycle hooks ──────────────────────────────────────────────────

@_fail_open
def on_kanban_task_claimed(*, task_id: str = "", board: str = "",
                           assignee: str = "", run_id: Optional[int] = None,
                           profile_name: str = "", **kwargs) -> None:
    _enqueue("kanban_events", [
        "event_type", "task_id", "board", "assignee", "run_id", "profile_name",
    ], [
        "claimed", task_id, board, assignee, run_id, profile_name,
    ])


@_fail_open
def on_kanban_task_completed(*, task_id: str = "", board: str = "",
                             assignee: str = "", run_id: Optional[int] = None,
                             profile_name: str = "", summary: str = "",
                             **kwargs) -> None:
    _enqueue("kanban_events", [
        "event_type", "task_id", "board", "assignee", "run_id",
        "profile_name", "summary",
    ], [
        "completed", task_id, board, assignee, run_id, profile_name, summary,
    ])


@_fail_open
def on_kanban_task_blocked(*, task_id: str = "", board: str = "",
                           assignee: str = "", run_id: Optional[int] = None,
                           profile_name: str = "", reason: str = "",
                           **kwargs) -> None:
    _enqueue("kanban_events", [
        "event_type", "task_id", "board", "assignee", "run_id",
        "profile_name", "reason",
    ], [
        "blocked", task_id, board, assignee, run_id, profile_name, reason,
    ])


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
    if not sql:
        return json.dumps({"error": f"Unknown query_type '{query_type}'. Valid: {list(queries.keys())}"})

    try:
        rows = [dict(r) for r in db.execute(sql, (since_ts,)).fetchall()]
        return json.dumps({"profile": profile, "query_type": query_type, "days": days, "rows": rows}, default=str)
    except Exception as exc:
        return json.dumps({"error": str(exc)})
    finally:
        db.close()


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
    # Hook registrations (write path)
    ctx.register_hook("on_session_start", on_session_start)
    ctx.register_hook("on_session_end", on_session_end)
    ctx.register_hook("on_session_finalize", on_session_finalize)
    ctx.register_hook("on_session_reset", on_session_reset)
    ctx.register_hook("pre_tool_call", pre_tool_call)
    ctx.register_hook("post_tool_call", post_tool_call)
    ctx.register_hook("pre_llm_call", pre_llm_call)
    ctx.register_hook("pre_api_request", pre_api_request)
    ctx.register_hook("post_api_request", post_api_request)
    ctx.register_hook("api_request_error", api_request_error)
    ctx.register_hook("pre_approval_request", on_pre_approval_request)
    ctx.register_hook("post_approval_response", on_post_approval_response)
    # subagent_start/subagent_stop removed — core doesn't fire these yet (no-ops)
    ctx.register_hook("pre_gateway_dispatch", on_pre_gateway_dispatch)
    ctx.register_hook("pre_verify", on_pre_verify)
    ctx.register_hook("kanban_task_claimed", on_kanban_task_claimed)
    ctx.register_hook("kanban_task_completed", on_kanban_task_completed)
    ctx.register_hook("kanban_task_blocked", on_kanban_task_blocked)
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

    print("[hermes-analytics] Registered 19 hooks + 5 read-path tools (SQLite v0.1.0)")


# ── Shutdown ────────────────────────────────────────────────────────────────

def _final_flush() -> None:
    global _SHUTDOWN
    _SHUTDOWN = True
    if _writer_thread is not None and _writer_thread.is_alive():
        _writer_thread.join(timeout=5.0)
    if _DB_CONN is not None:
        try:
            _DB_CONN.close()
        except Exception:
            pass


atexit.register(_final_flush)