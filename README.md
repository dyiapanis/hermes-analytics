# Hermes Analytics

SQLite analytics for [Hermes Agent](https://github.com/NousResearch/hermes-agent) — a fleet of agents shouldn't be a black box. This plugin records every LLM call, tool invocation, and context event per profile, then hands that data back to the agents themselves as callable tools.

Runs on plain `sqlite3` from the standard library. The only dependency is `pyyaml`, and that's just for the pricing config.

## Why it exists

Hermes core already gives you `hermes insights` — sessions, tokens, models. But it can't tell you which tool failed most this week, how close agents run to their context ceiling, or what your fleet actually costs under different pricing assumptions. Those answers need per-call telemetry with latency, cache tokens, and failure status — which means hooking the call lifecycle, which means a plugin, not a skill.

So the plugin does two jobs:

1. **It watches.** Hooks capture each LLM call, tool call, session, approval, kanban event, and API request, writing one row per event. LLM calls record tokens, latency, cache reads/writes, finish reason, context pressure — the fields you need to answer "why is this agent slow/expensive/drifting?"
2. **It reports.** Five tools let any agent in the fleet query the telemetry — itself, its siblings, or the whole fleet — without shell access or SQL knowledge.

## What it tracks

- **LLM calls** — tokens (prompt, completion, cache read/write), latency, finish reason, cost (with a three-tier pricing resolver, more below)
- **Tool calls** — name, duration, success/failure, exit codes
- **Context pressure** — how full the context window is, compression events
- **Sessions & messages** — duration, inbound volume per platform and chat
- **Verification quality** — file edits and whether the edit was actually verified
- **Kanban & approvals** — task claims, completions, blocks; approval requests and outcomes

Data lands in one SQLite file per profile: `~/.hermes/profiles/<profile>/analytics.db` (WAL mode, schema versioned via `_schema_meta`).

## Install

```bash
hermes plugins install dyiapanis/hermes-analytics --enable
```

Or add it to your profile's `config.yaml`:

```yaml
plugins:
  enabled:
    - hermes-analytics
```

Restart your gateway and that's it — the database is created on the first LLM call, no configuration required.

## The five tools

Every tool is callable by the agents themselves, so an agent can pull its own numbers (or its whole fleet's) as part of its reasoning loop. `days` defaults to 7 everywhere.

- **`analytics_fleet_report`** — the fleet at a glance: sessions, calls, tokens, indicative cost, top tools, and context pressure, per agent. With `format='markdown'` it also renders live provider quota bars (see below).
- **`analytics_digest`** — one agent, in depth: models, tools, context, trends.
- **`analytics_query`** — one targeted question, ten answer types: `tools`, `cost`, `sessions`, `duration`, `platforms`, `latency`, `health`, `efficiency`, `models`, `cost_sources`. There's also a `sql` mode, read-only `SELECT` only.
- **`analytics_pricing_config`** — read and edit pricing overrides at runtime; changes take effect immediately, no restart.
- **`analytics_export`** — raw rows out to CSV or JSON, so a cron agent can archive or chart them.

## Pricing: three tiers, no guesses

Cost is the slippery number, so the pricing chain is explicit and ordered. Each call's cost resolves through, in order:

1. **Provider response** — if the API returns `usage.cost` (OpenRouter does), that's actual spend. Use it and done.
2. **Your overrides** — rates you set in `pricing_config.yaml`.
3. **Core pricing** — Hermes' own `usage_pricing` data.
4. **OpenRouter's public API** — cache-aware rates for the model.
5. **HF Router's API** — fallback for anything OpenRouter doesn't know.

Every row also records *which* tier it priced from (`cost_source` and `cost_status`) — so `analytics_query` with `query_type='cost_sources'` tells you how much of your "cost" is real provider data versus indicative estimates. A flat-rate plan (like Ollama Cloud) returns no cost field, so its numbers stay indicative rather than fake-precise.

### Setting your own rates

```yaml
# pricing_config.yaml
source_priority:
  - override      # your rates, highest priority
  - core          # agent.usage_pricing
  - openrouter    # OpenRouter API, cache-aware
  - hf_router     # fallback

overrides:
  my-model:
    input: 0.50
    output: 2.00
    cache_read: 0.10
    # cache_write: 0.10   # optional; falls back to input rate
```

## Provider usage quotas

`analytics_fleet_report` can render live quota bars for any flat-rate provider you use — Ollama Cloud's weekly window, HF Router's quota, anything with a usage page. Add a provider to `provider_usage.yaml`:

```yaml
ollama_cloud:
  label: Ollama Cloud
  usage_url: https://openrouter.ai/api/v1/...
  api_key: OPENROUTER_API_KEY
  extractor: fraction_windows
```

Two extractors are built in — `fraction_windows` (usage as a fraction of a rolling window) and `percent_windows` (percentage used per window). If your provider's API matches either shape, it's one YAML entry; if not, it's one small function in `tools.py`. `test_quota.py` has extractor tests.

## Configuration

Defaults are sane; the two things you might change:

**Retention** — records are purged after `retention_days` (default 365), enforced by a background timer that runs hourly, so headless installs stay clean even with quiet sessions.

```yaml
# plugin.yaml
config:
  retention_days: 365
```

Or `ANALYTICS_RETENTION_DAYS=90` as an environment variable.

**Standalone mode** — run the analytics DB outside Hermes entirely, useful for testing or side-by-side installs:

```bash
ANALYTICS_DB_PATH=/path/to/analytics.db
ANALYTICS_PROFILES=profile1,profile2
ANALYTICS_RETENTION_DAYS=90
ANALYTICS_CONTEXT_LENGTH=128000
```

## Database

One SQLite file per profile at `~/.hermes/profiles/<profile>/analytics.db`, WAL mode, timestamps as integer unixepoch seconds. Schema is versioned via `_schema_meta`, so migrations are a known quantity. `analytics_query` with `query_type='sql'` (SELECT only) is the escape hatch for anything the named query types don't cover.

## License

MIT