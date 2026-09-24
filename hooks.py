"""Observer hook callbacks — hermes.observer.v1 contract."""

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


from .db import (_enqueue, _enqueue_raw, _flush_sync, _maybe_retain,
                 _update_session, _args_hash, _extract_exit_code, _CTX_DEDUP_S,
                 _state, _ctx_dedup_lock, _STATE, _last_ctx_key, _last_ctx_ts)
from .pricing import _compute_cost, _extract_usage, _fail_open, _resolve_context_length


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


