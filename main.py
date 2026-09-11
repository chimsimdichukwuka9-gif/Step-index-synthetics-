import os, json, time, traceback
from datetime import datetime, timezone
import numpy as np
import pandas as pd
import requests
import websocket

VERSION = "2026-09-STRUCTURED-V1"

WS_URL = os.getenv("DERIV_WS_URL", "wss://api.derivws.com/trading/v1/options/ws/public")
BOT = os.getenv("TELEGRAM_BOT_TOKEN", "")
CHAT = os.getenv("TELEGRAM_CHAT_ID", "")
INTERVAL = int(os.getenv("SCAN_INTERVAL_SECONDS", "60"))
STATE_FILE = os.getenv("STATE_FILE", "structured_state.json")

TARGETS = [
    ("STEP INDEX", "Step Index", "STEP"),
    ("STEP 200", "Step Index 200", "STEP"),
    ("STEP 300", "Step Index 300", "STEP"),
    ("STEP 400", "Step Index 400", "STEP"),
    ("STEP 500", "Step Index 500", "STEP"),
    ("MULTI STEP 2", "Multi Step 2 Index", "MULTI"),
    ("MULTI STEP 3", "Multi Step 3 Index", "MULTI"),
    ("MULTI STEP 4", "Multi Step 4 Index", "MULTI"),
    ("RANGE BREAK 100", "Range Break 100 Index", "RANGE"),
    ("RANGE BREAK 200", "Range Break 200 Index", "RANGE"),
    ("SKEW STEP 4 UP", "Skew Step 4 Up Index", "SKEW_UP"),
    ("SKEW STEP 4 DOWN", "Skew Step 4 Down Index", "SKEW_DOWN"),
    ("SKEW STEP 5 UP", "Skew Step 5 Up Index", "SKEW_UP"),
    ("SKEW STEP 5 DOWN", "Skew Step 5 Down Index", "SKEW_DOWN"),
]

TF5, TF15, TF1H, TF4H = 300, 900, 3600, 14400

http = requests.Session()
state = {}

try:
    with open(STATE_FILE, "r", encoding="utf-8") as f:
        state = json.load(f)
except Exception:
    pass


def save_state():
    with open(STATE_FILE + ".tmp", "w", encoding="utf-8") as f:
        json.dump(state, f, indent=2)
    os.replace(STATE_FILE + ".tmp", STATE_FILE)


def tg(text):
    if not BOT or not CHAT:
        print("\nTELEGRAM NOT CONFIGURED:\n" + text)
        return False

    try:
        r = http.post(
            f"https://api.telegram.org/bot{BOT}/sendMessage",
            data={"chat_id": CHAT, "text": text},
            timeout=20,
        )

        if not r.ok:
            print("Telegram error:", r.status_code, r.text[:300])
            return False

        return True

    except Exception as e:
        print("Telegram exception:", e)
        return False


def deriv(payload, timeout=20):
    ws = websocket.create_connection(
        WS_URL,
        timeout=timeout,
        origin="https://deriv.com"
    )

    try:
        ws.send(json.dumps(payload))

        end = time.time() + timeout

        while time.time() < end:
            raw = ws.recv()

            if not raw:
                continue

            data = json.loads(raw)

            if "error" in data:
                raise RuntimeError(
                    data["error"].get("message", str(data["error"]))
                )

            return data

        raise TimeoutError("Deriv request timeout")

    finally:
        ws.close()


def norm(s):
    s = str(s or "").upper()

    for c in " -_/().,":
        s = s.replace(c, "")

    return s.replace("INDEX", "")


def discover():
    # FIXED: product_type was rejected by the Deriv API.
    data = deriv({"active_symbols": "brief"})

    found = {}

    for x in data.get("active_symbols", []):
        name = (
            x.get("underlying_symbol_name")
            or x.get("display_name")
            or x.get("name")
        )

        code = (
            x.get("underlying_symbol")
            or x.get("symbol")
        )

        if name and code:
            found[norm(name)] = (code, name)

    out = {}

    print("\n=== SYMBOL DISCOVERY ===")

    for label, wanted, family in TARGETS:
        hit = found.get(norm(wanted))

        if hit:
            out[label] = {
                "code": hit[0],
                "display": hit[1],
                "family": family
            }

            print(
                "FOUND  ",
                label,
                "->",
                hit[1],
                "[" + hit[0] + "]"
            )
        else:
            print(
                "MISSING ",
                label,
                "->",
                wanted
            )

    print("========================\n")

    return out


def candles(symbol, seconds, count=260):
    data = deriv({
        "ticks_history": symbol,
        "adjust_start_time": 1,
        "count": count,
        "end": "latest",
        "granularity": seconds,
        "style": "candles",
    })

    rows = []

    for c in data.get("candles", []):
        try:
            rows.append({
                "time": pd.to_datetime(
                    int(c["epoch"]),
                    unit="s",
                    utc=True
                ),
                "open": float(c["open"]),
                "high": float(c["high"]),
                "low": float(c["low"]),
                "close": float(c["close"]),
            })

        except Exception:
            pass

    if not rows:
        raise RuntimeError(f"No candles for {symbol}")

    df = (
        pd.DataFrame(rows)
        .set_index("time")
        .sort_index()
        .drop_duplicates()
    )

    cutoff = (
        pd.Timestamp.now(tz="UTC")
        - pd.Timedelta(seconds=seconds)
    )

    df = df[df.index < cutoff]

    if len(df) < 70:
        raise RuntimeError(
            f"Only {len(df)} completed candles for {symbol}"
        )

    return df


def make12(df4):
    x = df4.resample(
        "12h",
        label="right",
        closed="right"
    ).agg({
        "open": "first",
        "high": "max",
        "low": "min",
        "close": "last"
    }).dropna()

    now = pd.Timestamp.now(tz="UTC")

    return x[x.index <= now]


def ema(s, n):
    return s.ewm(
        span=n,
        adjust=False
    ).mean()


def atr(df, n=14):
    pc = df.close.shift(1)

    tr = pd.concat([
        df.high - df.low,
        (df.high - pc).abs(),
        (df.low - pc).abs()
    ], axis=1).max(axis=1)

    return tr.ewm(
        alpha=1 / n,
        adjust=False
    ).mean()


def rsi(s, n=14):
    d = s.diff()

    g = d.clip(lower=0)
    l = -d.clip(upper=0)

    ag = g.ewm(
        alpha=1 / n,
        adjust=False
    ).mean()

    al = l.ewm(
        alpha=1 / n,
        adjust=False
    ).mean()

    rs = ag / al.replace(0, np.nan)

    return 100 - 100 / (1 + rs)


def ind(df):
    x = df.copy()

    x["e20"] = ema(x.close, 20)
    x["e50"] = ema(x.close, 50)
    x["atr"] = atr(x)
    x["rsi"] = rsi(x.close)

    x["bull"] = x.close > x.open
    x["bear"] = x.close < x.open

    return x


def direction(df):
    if len(df) < 55:
        return 0

    x = ind(df)

    a = x.iloc[-1]
    b = x.iloc[-4]

    bull = [
        a.close > a.e20,
        a.e20 > a.e50,
        a.e20 > b.e20
    ]

    bear = [
        a.close < a.e20,
        a.e20 < a.e50,
        a.e20 < b.e20
    ]

    if sum(bull) >= 2:
        return 1

    if sum(bear) >= 2:
        return -1

    return 0


def step_setup(m15, d):
    x = ind(m15)

    a = x.iloc[-1]

    if not np.isfinite(a.atr) or a.atr <= 0:
        return False, "ATR_INVALID"

    near = abs(a.close - a.e20) / a.atr <= 1.2

    recent = x.iloc[-5:]

    if d == 1:
        counter = recent.low.min() < recent.e20.max()
        ok = near and counter and a.close >= a.e20
    else:
        counter = recent.high.max() > recent.e20.min()
        ok = near and counter and a.close <= a.e20

    return (
        ok,
        "PULLBACK_READY" if ok else "PULLBACK_WAIT"
    )


def trigger5(m5, d):
    x = ind(m5)

    a = x.iloc[-1]
    p = x.iloc[-2]

    if not np.isfinite(a.atr) or a.atr <= 0:
        return False, "ATR_INVALID"

    controlled = (
        a.high - a.low
    ) <= 2 * a.atr

    if d == 1:
        score = sum([
            a.bull,
            a.close > a.e20,
            a.close > p.high,
            controlled
        ])
    else:
        score = sum([
            a.bear,
            a.close < a.e20,
            a.close < p.low,
            controlled
        ])

    return (
        score >= 3 and controlled,
        f"TRIGGER_{score}/4"
    )


def range_setup(m15, d):
    x = ind(m15)

    if len(x) < 70:
        return False, "NOT_ENOUGH_DATA", None

    recent = x.iloc[-11:-1]

    level = None

    for i in range(len(recent)):
        row = recent.iloc[i]

        prior = (
            x.loc[:recent.index[i]]
            .iloc[:-1]
            .tail(24)
        )

        if len(prior) < 24:
            continue

        if d == 1:
            lev = prior.high.max()

            if row.close > lev:
                level = float(lev)
                break

        else:
            lev = prior.low.min()

            if row.close < lev:
                level = float(lev)
                break

    if level is None:
        return False, "NO_BREAKOUT", None

    a = x.iloc[-1]

    near = (
        abs(a.close - level)
        <= 1.25 * a.atr
    )

    hold = (
        a.close >= level
        if d == 1
        else a.close <= level
    )

    candle = (
        bool(a.bull)
        if d == 1
        else bool(a.bear)
    )

    return (
        near and hold and candle,
        "RETEST_READY"
        if near and hold and candle
        else "RETEST_WAIT",
        level
    )


def scan_step(label, info, h12, h4, h1, m15, m5):
    d12 = direction(h12)
    d4 = direction(h4)
    d1 = direction(h1)

    if d12 == 0:
        return None, f"{label} NO SETUP 12H=NEUTRAL"

    if info["family"] == "SKEW_UP" and d12 != 1:
        return None, (
            f"{label} NO SETUP: "
            "12H conflicts with UP bias"
        )

    if info["family"] == "SKEW_DOWN" and d12 != -1:
        return None, (
            f"{label} NO SETUP: "
            "12H conflicts with DOWN bias"
        )

    if d4 != d12 or d1 != d12:
        return None, (
            f"{label} NO SETUP "
            f"12H={d12} 4H={d4} 1H={d1}"
        )

    pull, pp = step_setup(m15, d12)
    trig, tp = trigger5(m5, d12)

    if not pull:
        return None, f"{label} {pp}"

    if not trig:
        return None, f"{label} {tp}"

    score = 3 + 2 + 2 + 2 + 3

    return {
        "label": label,
        "display": info["display"],
        "direction": "BUY" if d12 == 1 else "SELL",
        "score": score,
        "max": 12,
        "d12": d12,
        "d4": d4,
        "d1": d1,
        "setup": pp,
        "trigger": tp,
        "time": m5.index[-1].isoformat(),
        "price": float(m5.close.iloc[-1]),
        "strategy": (
            "Structured Step Pullback"
            if info["family"] not in ["SKEW_UP", "SKEW_DOWN"]
            else "Skew-Step Trend Pullback"
        )
    }, None


def scan_range(label, info, h12, h4, h1, m15, m5):
    d12 = direction(h12)
    d4 = direction(h4)
    d1 = direction(h1)

    if d12 == 0:
        return None, f"{label} NO SETUP 12H=NEUTRAL"

    if d4 != d12 or d1 != d12:
        return None, (
            f"{label} NO SETUP "
            f"12H={d12} 4H={d4} 1H={d1}"
        )

    ok, phase, level = range_setup(m15, d12)

    trig, tp = trigger5(m5, d12)

    if not ok:
        return None, f"{label} {phase}"

    if not trig:
        return None, f"{label} {tp}"

    return {
        "label": label,
        "display": info["display"],
        "direction": "BUY" if d12 == 1 else "SELL",
        "score": 13,
        "max": 13,
        "d12": d12,
        "d4": d4,
        "d1": d1,
        "setup": phase,
        "trigger": tp,
        "time": m5.index[-1].isoformat(),
        "price": float(m5.close.iloc[-1]),
        "level": level,
        "strategy": "Range Breakout + Retest"
    }, None


def msg(s):
    e = "🟢" if s["direction"] == "BUY" else "🔴"

    t = (
        f"{e} {s['label']} — {s['direction']} SIGNAL\n\n"
        f"Technical score: {s['score']}/{s['max']}\n"
        f"Symbol: {s['display']}\n"
        f"Price: {s['price']:.6f}\n\n"
        f"MULTI-TIMEFRAME:\n"
        f"12H: {'BULLISH' if s['d12'] == 1 else 'BEARISH'}\n"
        f"4H: {'BULLISH' if s['d4'] == 1 else 'BEARISH'}\n"
        f"1H: {'BULLISH' if s['d1'] == 1 else 'BEARISH'}\n"
        f"15M: {s['setup']}\n"
        f"5M: {s['trigger']}\n\n"
        f"Strategy: {s['strategy']}\n"
        f"Signal candle: {s['time']}\n\n"
        f"Scanner: {VERSION}\n"
        f"Signal only — no automatic trading."
    )

    if "level" in s:
        t += (
            f"\nBreakout level: "
            f"{s['level']:.6f}"
        )

    return t


def scan_one(label, info):
    try:
        h4raw = candles(
            info["code"],
            TF4H
        )

        h12 = make12(h4raw)

        h4 = h4raw
        h1 = candles(
            info["code"],
            TF1H
        )
        m15 = candles(
            info["code"],
            TF15
        )
        m5 = candles(
            info["code"],
            TF5
        )

        if len(h12) < 30:
            print(
                label,
                "NOT ENOUGH 12H DATA"
            )
            return

        if info["family"] == "RANGE":
            signal, reason = scan_range(
                label,
                info,
                h12,
                h4,
                h1,
                m15,
                m5
            )
        else:
            signal, reason = scan_step(
                label,
                info,
                h12,
                h4,
                h1,
                m15,
                m5
            )

        if not signal:
            print(reason)
            return

        key = "STRUCTURED:" + label

        if state.get(key) == signal["time"]:
            print(
                label,
                "DUPLICATE"
            )
            return

        if tg(msg(signal)):
            state[key] = signal["time"]
            save_state()

            print(
                label,
                ">>>",
                signal["direction"],
                signal["score"],
                "/",
                signal["max"]
            )

    except Exception as e:
        print(
            label,
            "ERROR:",
            e
        )


def main():
    print(
        "Starting",
        VERSION
    )

    if not BOT or not CHAT:
        print(
            "WARNING: set TELEGRAM_BOT_TOKEN "
            "and TELEGRAM_CHAT_ID"
        )

    symbols = discover()

    if not symbols:
        raise RuntimeError(
            "No target symbols discovered."
        )

    print(
        f"Resolved {len(symbols)}/{len(TARGETS)} instruments."
    )

    while True:
        started = time.time()

        print(
            "\n=== SCAN",
            datetime.now(timezone.utc).isoformat(),
            "==="
        )

        for label, info in symbols.items():
            scan_one(
                label,
                info
            )

            time.sleep(0.5)

        wait = max(
            5,
            INTERVAL - (time.time() - started)
        )

        print(
            f"Cycle finished. "
            f"Next scan in {wait:.0f}s."
        )

        time.sleep(wait)


if __name__ == "__main__":
    main()
