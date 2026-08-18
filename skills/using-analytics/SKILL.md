---
name: using-analytics
description: "Use when querying Hermes telemetry via the analytics plugin tools. Covers the 5 plugin tools, database schema, cost data concepts, and diagnostic query patterns."
version: 1.0.0
author: Hermes Analytics
license: MIT
metadata:
  hermes:
    tags: [analytics, telemetry, hermes, observability]
---

# Using Hermes Analytics

The analytics plugin captures LLM calls, tool usage, context pressure, session summaries, and other telemetry to a per-profile SQLite database. You interact with it through 5 tools.

## Tools

### `analytics_fleet_report` 📊
Fleet-wide summary across all profiles. Returns per-agent LLM/tool call counts, token usage, cost, context pressure, and session counts. Parameter: `days` (default 7).

Use this first when asked about overall fleet health or to compare agents.

### `analytics_digest` 📋
Per-profile deep dive. Returns models used, tool failures, tool usage breakdown, context pressure, daily trends, platforms, session health, and skill usage. Parameters: `profile` (default "phoenix"), `days` (default 7).

Use this when investigating a specific agent's behaviour or performance.

### `analytics_query` 🔍
Targeted single-query access. Parameters: `query_type` (required), `profile` (default "phoenix"), `days` (default 7).

Query types:
- `tools` — tool call counts and failure rates
- `cost` — total and per-model cost breakdown
- `cost_sources` — cost by pricing source and status (actual vs estimated vs unknown)
- `sessions` — session counts and depth distribution
- `duration` — latency statistics
- `platforms` — message volume by platform
- `latency` — LLM call latency distribution
- `health` — overall health indicators
- `efficiency` — tokens per session, cost per session
- `models` — model distribution and usage

Use this for specific questions that don't need the full digest.

### `analytics_pricing_config` 💰
Read and write pricing configuration. Actions: `get`, `set_override`, `remove_override`, `reorder_sources`.

Changes take effect immediately — no gateway restart needed. Use `set_override` when you know actual rates for a model (e.g. a negotiated deal or self-hosted inference).

### `analytics_export` 📤
Export raw rows from a table to CSV or JSON. Parameters: `table` (required: llm_calls|tool_calls|session_summary|context_pressure|skill_usage), `profile`, `days`, `format` (csv|json, default csv), `limit` (default 1000).

Use this when you need raw data for external analysis or when the structured tools don't cover your question.

## Database

SQLite in WAL mode. One file per profile at `~/.hermes/profiles/<profile>/analytics.db`. Created automatically on first LLM call.

### Schema

**`llm_calls`** — model, tokens (prompt/completion/cache_read/cache_write), cost_usd, cost_status, cost_source, duration_ms, status, turn_id, provider, finish_reason
**`tool_calls`** — tool_name, status, duration_ms, error_message, exit_code, turn_id, error_type
**`context_pressure`** — utilization_pct, compression_triggered, model
**`session_summary`** — total_prompt_tokens, total_completion_tokens, total_cost_usd (session_id is PRIMARY KEY)
**`skill_usage`** — skill_name, action (view/manage)
**`approval_events`** — approval request/response event_type
**`kanban_events`** — task lifecycle events
**`api_requests`** — message_count, tool_count, approx_input_tokens
**`inbound_messages`** — platform, chat_id, user_id, message_length
**`verify_events`** — verification quality (edited_file_count, attempt)

Timestamps are INTEGER (unixepoch seconds). Use `strftime('%Y-%m-%d', timestamp, 'unixepoch')` for date display.

### Direct queries

For questions not covered by the tools, connect directly:

```python
import sqlite3
conn = sqlite3.connect(path_to_db)
conn.row_factory = sqlite3.Row
rows = [dict(r) for r in conn.execute("SELECT * FROM llm_calls ORDER BY timestamp DESC LIMIT 5")]
conn.close()
```

Standard SQL works — `COUNT(*)`, `SUM()`, `AVG()`, `CASE WHEN`, `HAVING`, window functions, subqueries.

## Cost Data

`cost_usd` is an **indicative cost** — what usage would cost at published per-token rates, independent of billing model. Not actual billing unless the provider returns a real `usage.cost`.

### Cost resolution (three tiers)

1. **Provider-returned cost** — if the API response contains `usage.cost` (e.g. OpenRouter), used directly as `cost_status="actual"`, `cost_source="provider_response"`
2. **Core pricing** — Hermes's built-in `agent.usage_pricing` for recognized providers
3. **OpenRouter / HF Router API** — live pricing for open-weight models, cached 1 hour

### Cost fields

- `cost_usd` — the cost amount (or NULL if unpriced)
- `cost_status` — `"actual"` | `"estimated"` | `"included"` | `"unknown"`
- `cost_source` — `"core:..."` | `"openrouter"` | `"hf_router"` | `"override"` | `"provider_response"` | `"none"`

### Important

- **Always aggregate cost from `llm_calls`, not `session_summary`.** `session_summary` is written at session end; crashed sessions never get a summary row, causing 10-45% underreporting.
- **Unpriced models report as $0.00.** Check `cost_status="unknown"` to distinguish "free" from "no pricing data".
- **Cache tokens matter.** For a typical call with 60K cached tokens, cache_read pricing reduces indicative cost by 48-74%. When a source lacks cache pricing, cache tokens are priced at full input rate (conservative overstatement).

## Configuration

### Retention

```yaml
# plugin.yaml
config:
  retention_days: 365
```

Or env var: `ANALYTICS_RETENTION_DAYS=90`

### Pricing overrides

Edit `pricing_config.yaml` to set custom rates or reorder sources:

```yaml
source_priority:
  - override      # user-supplied rates (highest priority)
  - core          # agent.usage_pricing
  - openrouter    # OpenRouter API (cache-aware)
  - hf_router     # HF Router API (fallback)

overrides:
  my-model:
    input: 0.50
    output: 2.00
    cache_read: 0.10
    cache_write: 0.10
```

### Standalone mode

The plugin can run outside Hermes:

```bash
ANALYTICS_DB_PATH=/path/to/analytics.db
ANALYTICS_PROFILES=profile1,profile2
ANALYTICS_RETENTION_DAYS=90
ANALYTICS_CONTEXT_LENGTH=128000
```

## Diagnostic Patterns

- **Tool failure rate > 10%** — check `tool_calls` for `status = 'failure'`, group by `tool_name`. Exit codes: 124=timeout, 127=not found, 126=permission, 1=generic.
- **Context pressure > 90%** — investigate tool output chunking. `utilization_pct > 100` with `compression_triggered = 0` may indicate a stale context length config.
- **Zero-token calls** — run the zero-token cliff check before any token analysis: `SELECT COUNT(*) FROM llm_calls WHERE prompt_tokens = 0 AND completion_tokens = 0`. If > 50% of calls, data collection may be broken.
- **Unpriced models** — `SELECT model, COUNT(*), SUM(cost_usd) FROM llm_calls WHERE cost_status = 'unknown' GROUP BY model`. Add aliases to `model_aliases.yaml` or set overrides in `pricing_config.yaml`.