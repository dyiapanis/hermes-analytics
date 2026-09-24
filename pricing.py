"""Indicative cost tracking — alias resolution, pricing config,
OpenRouter + HF Router fetchers, cost computation, context windows."""

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


def _invalidate_pricing_config() -> None:
    """Drop the cached pricing config so the next load re-reads the YAML."""
    global _PRICING_CONFIG
    with _PRICING_CONFIG_LOCK:
        _PRICING_CONFIG = None


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


