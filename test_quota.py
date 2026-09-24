#!/usr/bin/env python3
"""Offline tests for the provider-usage quota feature — no network, no deps.

Run: python3 test_quota.py
"""
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import tools  # noqa: E402


# (a) extractors normalize minimal synthetic payloads matching the real shapes
def test_extractors():
    # Ollama Cloud: limits.<scope>.usage is a fraction 0..1 with model breakdown
    ol = tools.fraction_windows({
        "limits": {
            "session": {"usage": 0.42, "models": [
                {"name": "gpt-oss:120b", "request_count": 75},
                {"name": "qwen3-coder", "request_count": 25},
            ]},
            "weekly": {"usage": 0.07, "models": []},
        }
    })
    assert [w["label"] for w in ol] == ["Session", "Weekly"], ol
    assert ol[0]["pct"] == 42.0, ol[0]
    assert ol[0]["models"] == [("gpt-oss:120b", 75.0, 75), ("qwen3-coder", 25.0, 25)], ol[0]["models"]
    assert abs(ol[1]["pct"] - 7.0) < 1e-9 and ol[1]["models"] == [], ol[1]

    # OpenCode Go: usage.<scope>.percent is 0..100 with a status
    oc = tools.percent_windows({
        "usage": {
            "rolling": {"percent": 61, "status": "warning"},
            "weekly": {"percent": 30, "status": "ok"},
            "monthly": {"percent": 85},
        }
    })
    assert [w["label"] for w in oc] == ["Rolling", "Weekly", "Monthly"], oc
    assert [w["pct"] for w in oc] == [61.0, 30.0, 85.0], oc
    assert oc[0]["status"] == "warning" and oc[0]["models"] == [], oc[0]
    assert oc[2]["status"] == "ok", oc[2]  # default status


# (b) renderer output via a monkeypatched registry + fetch helper
def _setup(monkey: dict):
    tools._load_registry = lambda: monkey["registry"]
    tools._fetch_provider_usage = lambda url, key: monkey["fetch"]()
    tools._secret = lambda name: "fake-key"


def _clear_env(monkeypatch=None):
    for var in ("OLLAMA_API_KEY", "OPENCODE_GO_API_KEY"):
        os.environ.pop(var, None)


def test_renderer():
    # icon thresholds: green < 50, yellow at 60, red at 85
    reg = {
        "a": {"label": "ProvA", "usage_url": "https://a/usage", "api_key": "A",
              "extractor": "percent_windows"},
        "b": {"label": "ProvB", "usage_url": "https://b/usage", "api_key": "B",
              "extractor": "fraction_windows", "note": "unit test"},
    }
    payload = {
        # ProvA: two windows → red (85) and green (30) icons/bars
        "mixed": {"usage": {"rolling": {"percent": 85}, "weekly": {"percent": 30}}},
        # ProvB: fraction 0.6 → 60% → yellow
        "fraction": {"limits": {"session": {"usage": 0.6}}},
    }
    seq = {"calls": 0}

    def fetch():
        i = seq["calls"]
        seq["calls"] += 1
        return [payload["mixed"], payload["fraction"]][i]  # A then B

    _setup({"registry": reg, "fetch": fetch})
    _clear_env()
    out = tools._quota_section()

    # ProvA rolling=85 → red icon + red bar; ProvA weekly=30 → green; ProvB session=60 → yellow
    assert "🔴" in out and "🟨" in out and "🟢" in out, out
    assert out.count("🔴") == 1, out
    # bar glyphs: 85% of 12 ≈ 10 red; 60% of 12 ≈ 7 yellow; 30% of 12 ≈ 4 green
    reds = out.count("🟥")
    yellows = out.count("🟨")
    greens = out.count("🟩")
    assert reds == 10, reds
    assert yellows == 7, yellows
    assert greens == 4, greens
    # 12-glyph bar: 10 red + 2 white fill
    line = next(l for l in out.splitlines() if "🟥" in l)
    assert line.count("⬜") == 2, line
    # "of 1 unit" only for the fraction (unit) provider; metered phrasing otherwise
    assert "of 1 unit" in out and "used ·" in out, out
    # no dollar figures anywhere
    assert "$" not in out, out
    # notes and labels survive
    assert "**ProvA**" in out and "**ProvB** *(unit test)*" in out, out

    # error path: fetch failure renders inline, other provider still renders
    seq2 = {"calls": 0}

    def fetch_err(url, key):
        i = seq2["calls"]
        seq2["calls"] += 1
        return [{"error": "HTTPError: 500"}, payload["fraction"]][i]

    tools._fetch_provider_usage = fetch_err
    out2 = tools._quota_section()
    assert "**ProvA**: unable to fetch (HTTPError: 500)" in out2, out2
    assert "🟨" in out2, out2  # ProvB still renders
    assert "$" not in out2, out2

    # (c) enabled: false entry is skipped
    called = []
    reg2 = {
        "off": {"label": "Disabled", "usage_url": "https://d/usage", "api_key": "D",
                "extractor": "percent_windows", "enabled": False},
        "on": {"label": "Enabled", "usage_url": "https://e/usage", "api_key": "E",
               "extractor": "percent_windows"},
    }
    tools._fetch_provider_usage = lambda url, key: called.append(url) or {
        "usage": {"rolling": {"percent": 30}}}
    tools._load_registry = lambda: reg2
    out3 = tools._quota_section()
    assert "Disabled" not in out3, out3
    assert "**Enabled**" in out3, out3
    assert called == ["https://e/usage"], called  # only the enabled provider fetched


if __name__ == "__main__":
    test_extractors()
    test_renderer()
    print("OK: all quota tests passed")