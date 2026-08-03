# Hermes Analytics Plugin Specification

**Version:** 0.1.0
**Engine:** SQLite (WAL mode)
**Schema version:** 1.0

## Architecture

The plugin uses a single background writer thread that owns the sole SQLite
connection (WAL mode, `check_same_thread=False`). All session-thread code paths
(hook handlers) enqueue writes via `_enqueue()` or `_enqueue_raw()`. The writer
thread batches and flushes every 0.25s.

Read-path tools open fresh read-only connections per query.

## Tables

| Table | Purpose | Key columns |
|-------|---------|-------------|
| `llm_calls` | LLM API calls | model, prompt_tokens, completion_tokens, cost_usd, cost_status, cost_source, duration_ms |
| `tool_calls` | Tool invocations | tool_name, status, duration_ms, exit_code, error_message |
| `context_pressure` | Context window utilization | utilization_pct, compression_triggered, model |
| `session_summary` | Session-level aggregates | session_id (PK), total_cost_usd, total_prompt_tokens, final_status |
| `skill_usage` | Skill view/manage events | skill_name, action |
| `approval_events` | Approval lifecycle | event_type, command_preview, approved |
| `kanban_events` | Task lifecycle | event_type, task_id, assignee |
| `api_requests` | Request-side metrics | message_count, tool_count, approx_input_tokens |
| `inbound_messages` | Inbound message volume | platform, chat_id, message_length |
| `verify_events` | Verification quality | edited_file_count, attempt |

All event tables have `timestamp INTEGER DEFAULT (strftime('%s','now'))`.

## Hooks (19)

on_session_start, on_session_end, on_session_finalize, on_session_reset,
pre_tool_call, post_tool_call, pre_llm_call, pre_api_request, post_api_request,
api_request_error, subagent_start, subagent_stop, pre_gateway_dispatch, pre_verify,
pre_approval_request, post_approval_response, kanban_task_claimed,
kanban_task_completed, kanban_task_blocked.

`post_llm_call` is deliberately NOT registered — LLM calls are recorded
exclusively via `post_api_request` to avoid double-counting.

## Cost Resolution

Three-tier resolution (configurable via `pricing_config.yaml`):

1. **Override** — user-supplied per-model rates
2. **Core** — `agent.usage_pricing` (OpenAI, Anthropic, Google, etc.)
3. **OpenRouter API** — live pricing, cache-aware
4. **HF Router API** — fallback, no cache pricing

If a provider returns `usage.cost` (e.g. OpenRouter), it is used directly as
the actual cost, bypassing all tiers.

## Environment Variables

| Variable | Description |
|----------|-------------|
| `ANALYTICS_DB_PATH` | Single DB file path (standalone mode) |
| `ANALYTICS_DB_PATH_{PROFILE}` | Per-profile DB path override |
| `ANALYTICS_PROFILES` | Comma-separated fleet discovery override |
| `ANALYTICS_RETENTION_DAYS` | Retention period override |
| `ANALYTICS_CONTEXT_LENGTH` | Context length override |
