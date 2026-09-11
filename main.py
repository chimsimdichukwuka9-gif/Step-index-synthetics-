import os
import json
import time
import traceback
from datetime import datetime, timezone

import numpy as np
import pandas as pd
import requests
import websocket


VERSION = "2026-09-PULLBACK-EARLY-V2"

WS_URL = os.getenv(
    "DERIV_WS_URL",
    "wss://api.derivws.com/trading/v1/options/ws/public"
)

BOT = os.getenv("TELEGRAM_BOT_TOKEN", "")
CHAT = os.getenv("TELEGRAM_CHAT_ID", "")

INTERVAL = int(os.getenv("SCAN_INTERVAL_SECONDS", "60"))
STATE_FILE = os.getenv("STATE_FILE", "pullback_state.json")


# ============================================================
# TARGETS
# ============================================================

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


# ============================================================
# TIMEFRAMES
# ============================================================

TF5 = 300
TF15 = 900
TF1H = 3600
TF4H = 14400


# ============================================================
# HTTP / STATE
# ============================================================

http = requests.Session()

state = {}

try:
    with open(STATE_FILE, "r", encoding="utf-8") as f:
        state = json.load(f)
except Exception:
    state = {}


def save_state():
    with open(STATE_FILE + ".tmp", "w", encoding="utf-8") as f:
        json.dump(state, f, indent=2)

    os.replace(STATE_FILE + ".tmp", STATE_FILE)


# ============================================================
# TELEGRAM
# ============================================================

def tg(text):
    if not BOT or not CHAT:
        print("\nTELEGRAM NOT CONFIGURED:\n")
        print(text)
        return False

    try:
        r = http.post(
            f"https://api.telegram.org/bot{BOT}/sendMessage",
            data={
                "chat_id": CHAT,
                "text": text,
            },
            timeout=20,
        )

        if not r.ok:
            print("Telegram error:", r.status_code, r.text[:500])
            return False

        return True

    except Exception as e:
        print("Telegram exception:", e)
        return False


# ============================================================
# DERIV WEBSOCKET
# ============================================================

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
                    data["error"].get(
                        "message",
                        str(data["error"])
                    )
                )

            return data

        raise TimeoutError("Deriv request timeout")

    finally:
        ws.close()


# ============================================================
# SYMBOL NORMALIZATION
# ============================================================

def norm(s):
    s = str(s or "").upper()

    for c in " -_/().,":
        s = s.replace(c, "")

    return s.replace("INDEX", "")


# ============================================================
# SYMBOL DISCOVERY
# ============================================================

def discover():
    # IMPORTANT:
    # product_type is intentionally NOT used here.
    data = deriv({
        "active_symbols": "brief"
    })

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
                "family": family,
            }

            print(
                "FOUND  ",
                label,
                "->",
                hit[1],
                "[",
                hit[0],
                "]"
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


# ============================================================
# CANDLES
# ============================================================

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
        raise RuntimeError(
            f"No candles for {symbol}"
        )

    df = (
        pd.DataFrame(rows)
        .set_index("time")
        .sort_index()
        .drop_duplicates()
    )

    # Remove the old completed candle boundary.
    # The 5M trigger is handled separately below.
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


# ============================================================
# 12H RESAMPLING
# ============================================================

def make12(df4):

    x = df4.resample(
        "12h",
        label="right",
        closed="right"
    ).agg({
        "open": "first",
        "high": "max",
        "low": "min",
        "close": "last",
    }).dropna()

    now = pd.Timestamp.now(tz="UTC")

    return x[x.index <= now]


# ============================================================
# INDICATORS
# ============================================================

def ema(s, n):
    return s.ewm(
        span=n,
        adjust=False
    ).mean()


def atr(df, n=14):

    pc = df.close.shift(1)

    tr = pd.concat(
        [
            df.high - df.low,
            (df.high - pc).abs(),
            (df.low - pc).abs(),
        ],
        axis=1
    ).max(axis=1)

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

    x["body"] = (x.close - x.open).abs()

    x["range"] = x.high - x.low

    return x


# ============================================================
# CLEAR DIRECTION
# ============================================================

def clear_direction(df):

    if len(df) < 55:
        return 0

    x = ind(df)

    a = x.iloc[-1]
    b = x.iloc[-4]

    if not np.isfinite(a.atr):
        return 0

    # Stronger than the old direction test.
    #
    # We want the 12H to be CLEAR, not merely slightly bullish
    # or bearish.

    bull_conditions = [
        a.close > a.e20,
        a.e20 > a.e50,
        a.e20 > b.e20,
        a.rsi >= 52,
    ]

    bear_conditions = [
        a.close < a.e20,
        a.e20 < a.e50,
        a.e20 < b.e20,
        a.rsi <= 48,
    ]

    bull_score = sum(bull_conditions)
    bear_score = sum(bear_conditions)

    if bull_score >= 3:
        return 1

    if bear_score >= 3:
        return -1

    return 0


# ============================================================
# NORMAL DIRECTION FOR LOWER TIMEFRAMES
# ============================================================

def direction(df):

    if len(df) < 55:
        return 0

    x = ind(df)

    a = x.iloc[-1]
    b = x.iloc[-4]

    bull = [
        a.close > a.e20,
        a.e20 > a.e50,
        a.e20 > b.e20,
    ]

    bear = [
        a.close < a.e20,
        a.e20 < a.e50,
        a.e20 < b.e20,
    ]

    if sum(bull) >= 2:
        return 1

    if sum(bear) >= 2:
        return -1

    return 0


# ============================================================
# TIMEFRAME ALIGNMENT
# ============================================================

def timeframe_alignment(d12, h4, h1, m15):

    directions = {
        "4H": h4,
        "1H": h1,
        "15M": m15,
    }

    aligned = sum(
        1
        for d in directions.values()
        if d == d12
    )

    return aligned, directions


# ============================================================
# PULLBACK DETECTION
# ============================================================

def pullback_setup(m15, d):

    x = ind(m15)

    if len(x) < 10:
        return False, "NOT_ENOUGH_15M"

    a = x.iloc[-1]

    if not np.isfinite(a.atr) or a.atr <= 0:
        return False, "ATR_INVALID"

    recent = x.iloc[-6:-1]

    if len(recent) < 3:
        return False, "NO_RECENT_DATA"

    distance = abs(a.close - a.e20) / a.atr

    near_ema = distance <= 1.5

    if d == 1:

        pullback_low = (
            recent.low.min() < recent.e20.max()
        )

        recovering = (
            a.close >= a.e20 * 0.999
        )

        ok = near_ema and pullback_low and recovering

        if ok:
            return True, "BULLISH_PULLBACK_READY"

        return False, "BULLISH_PULLBACK_WAIT"

    else:

        pullback_high = (
            recent.high.max() > recent.e20.min()
        )

        recovering = (
            a.close <= a.e20 * 1.001
        )

        ok = near_ema and pullback_high and recovering

        if ok:
            return True, "BEARISH_PULLBACK_READY"

        return False, "BEARISH_PULLBACK_WAIT"


# ============================================================
# EARLY NEW-CANDLE TRIGGER
#
# This function intentionally uses the CURRENT 5M CANDLE.
#
# We do NOT require it to be completed.
# ============================================================

def early_trigger5(symbol, d):

    # Request the latest candles directly.
    # The newest candle can still be forming.

    data = deriv({
        "ticks_history": symbol,
        "adjust_start_time": 1,
        "count": 80,
        "end": "latest",
        "granularity": TF5,
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

    if len(rows) < 25:
        return False, "NOT_ENOUGH_5M", None

    df = (
        pd.DataFrame(rows)
        .set_index("time")
        .sort_index()
        .drop_duplicates()
    )

    x = ind(df)

    # IMPORTANT:
    # -1 = newest CURRENT / FORMING candle
    # -2 = previous candle

    a = x.iloc[-1]
    p = x.iloc[-2]

    if not np.isfinite(a.atr) or a.atr <= 0:
        return False, "ATR_INVALID", None

    candle_range = a.high - a.low
    body = abs(a.close - a.open)

    if candle_range <= 0:
        return False, "NO_MOVEMENT", None

    body_ratio = body / candle_range

    # We want a candle that is actually showing direction.
    # Not simply one tick above/below its open.

    if d == 1:

        bullish = a.close > a.open

        above_ema = a.close > a.e20

        breaking_previous_high = (
            a.close > p.high
        )

        enough_body = (
            body >= 0.20 * a.atr
        )

        strong_body = (
            body_ratio >= 0.35
        )

        score = sum([
            bullish,
            above_ema,
            breaking_previous_high,
            enough_body,
            strong_body,
        ])

        if score >= 3:

            return (
                True,
                f"NEW_5M_BUY_{score}/5",
                a.name.isoformat()
            )

        return (
            False,
            f"NEW_5M_BUY_WAIT_{score}/5",
            a.name.isoformat()
        )

    else:

        bearish = a.close < a.open

        below_ema = a.close < a.e20

        breaking_previous_low = (
            a.close < p.low
        )

        enough_body = (
            body >= 0.20 * a.atr
        )

        strong_body = (
            body_ratio >= 0.35
        )

        score = sum([
            bearish,
            below_ema,
            breaking_previous_low,
            enough_body,
            strong_body,
        ])

        if score >= 3:

            return (
                True,
                f"NEW_5M_SELL_{score}/5",
                a.name.isoformat()
            )

        return (
            False,
            f"NEW_5M_SELL_WAIT_{score}/5",
            a.name.isoformat()
        )


# ============================================================
# RANGE BREAK / RETEST
# ============================================================

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
        <= 1.5 * a.atr
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

    ok = near and hold and candle

    if ok:
        return True, "RETEST_READY", level

    return False, "RETEST_WAIT", level


# ============================================================
# STEP SIGNAL
# ============================================================

def scan_step(
    label,
    info,
    h12,
    h4,
    h1,
    m15,
):

    d12 = clear_direction(h12)

    d4 = direction(h4)
    d1 = direction(h1)
    d15 = direction(m15)

    # --------------------------------------------------------
    # 12H MUST BE CLEAR
    # --------------------------------------------------------

    if d12 == 0:

        return (
            None,
            f"{label} NO SETUP 12H=NOT CLEAR"
        )

    # --------------------------------------------------------
    # SKEW DIRECTION FILTER
    # --------------------------------------------------------

    if (
        info["family"] == "SKEW_UP"
        and d12 != 1
    ):

        return (
            None,
            f"{label} NO SETUP: 12H NOT BULLISH"
        )

    if (
        info["family"] == "SKEW_DOWN"
        and d12 != -1
    ):

        return (
            None,
            f"{label} NO SETUP: 12H NOT BEARISH"
        )

    # --------------------------------------------------------
    # 2 OF 3 OR 3 OF 3
    # --------------------------------------------------------

    aligned, dirs = timeframe_alignment(
        d12,
        d4,
        d1,
        d15
    )

    if aligned < 2:

        return (
            None,
            f"{label} NO SETUP "
            f"12H={d12} "
            f"4H={d4} "
            f"1H={d1} "
            f"15M={d15} "
            f"ALIGN={aligned}/3"
        )

    # --------------------------------------------------------
    # PULLBACK
    # --------------------------------------------------------

    pull, pull_text = pullback_setup(
        m15,
        d12
    )

    if not pull:

        return (
            None,
            f"{label} {pull_text} "
            f"ALIGN={aligned}/3"
        )

    # --------------------------------------------------------
    # NEW 5M CANDLE
    # --------------------------------------------------------

    trigger, trigger_text, trigger_time = early_trigger5(
        info["code"],
        d12
    )

    if not trigger:

        return (
            None,
            f"{label} {trigger_text} "
            f"ALIGN={aligned}/3"
        )

    # --------------------------------------------------------
    # SCORE
    # --------------------------------------------------------

    # 12H clear = 4 points
    # 2/3 alignment = 2 points
    # 3/3 alignment = 3 points
    # Pullback = 2 points
    # New 5M candle = 3 points

    alignment_points = (
        3 if aligned == 3 else 2
    )

    score = (
        4
        + alignment_points
        + 2
        + 3
    )

    max_score = 12

    if aligned == 3:
        quality = "A+"
    else:
        quality = "A"

    return {
        "label": label,
        "display": info["display"],

        "direction": (
            "BUY"
            if d12 == 1
            else "SELL"
        ),

        "score": score,
        "max": max_score,
        "quality": quality,

        "d12": d12,
        "d4": d4,
        "d1": d1,
        "d15": d15,

        "aligned": aligned,

        "setup": pull_text,
        "trigger": trigger_text,

        "time": trigger_time,

        "price": float(
            m15.close.iloc[-1]
        ),

        "strategy":
            "12H Trend + Pullback + "
            "Early 5M Reversal",

    }, None


# ============================================================
# RANGE SIGNAL
# ============================================================

def scan_range(
    label,
    info,
    h12,
    h4,
    h1,
    m15,
):

    d12 = clear_direction(h12)

    d4 = direction(h4)
    d1 = direction(h1)
    d15 = direction(m15)

    # 12H required.

    if d12 == 0:

        return (
            None,
            f"{label} NO SETUP 12H=NOT CLEAR"
        )

    aligned, dirs = timeframe_alignment(
        d12,
        d4,
        d1,
        d15
    )

    if aligned < 2:

        return (
            None,
            f"{label} NO SETUP "
            f"12H={d12} "
            f"4H={d4} "
            f"1H={d1} "
            f"15M={d15} "
            f"ALIGN={aligned}/3"
        )

    ok, phase, level = range_setup(
        m15,
        d12
    )

    if not ok:

        return (
            None,
            f"{label} {phase} "
            f"ALIGN={aligned}/3"
        )

    trigger, trigger_text, trigger_time = early_trigger5(
        info["code"],
        d12
    )

    if not trigger:

        return (
            None,
            f"{label} {trigger_text} "
            f"ALIGN={aligned}/3"
        )

    alignment_points = (
        3 if aligned == 3 else 2
    )

    score = (
        4
        + alignment_points
        + 2
        + 3
    )

    return {
        "label": label,
        "display": info["display"],

        "direction": (
            "BUY"
            if d12 == 1
            else "SELL"
        ),

        "score": score,
        "max": 12,

        "quality": (
            "A+"
            if aligned == 3
            else "A"
        ),

        "d12": d12,
        "d4": d4,
        "d1": d1,
        "d15": d15,

        "aligned": aligned,

        "setup": phase,
        "trigger": trigger_text,

        "time": trigger_time,

        "price": float(
            m15.close.iloc[-1]
        ),

        "level": level,

        "strategy":
            "Range Break + Pullback + "
            "Early 5M Reversal",

    }, None


# ============================================================
# TELEGRAM MESSAGE
# ============================================================

def msg(s):

    emoji = (
        "🟢"
        if s["direction"] == "BUY"
        else "🔴"
    )

    direction_text = (
        "BULLISH"
        if s["direction"] == "BUY"
        else "BEARISH"
    )

    alignment_text = (
        f'{s["aligned"]}/3'
    )

    t = (
        f"{emoji} {s['label']} — "
        f"{s['direction']} SIGNAL\n\n"

        f"Quality: {s['quality']}\n"
        f"Technical score: "
        f"{s['score']}/{s['max']}\n\n"

        f"Symbol: {s['display']}\n"
        f"Price: {s['price']:.6f}\n\n"

        f"MULTI-TIMEFRAME:\n"

        f"12H: {direction_text} "
        f"✅ CLEAR\n"

        f"4H: "
        f"{'BULLISH' if s['d4']==1 else 'BEARISH' if s['d4']==-1 else 'NEUTRAL'}\n"

        f"1H: "
        f"{'BULLISH' if s['d1']==1 else 'BEARISH' if s['d1']==-1 else 'NEUTRAL'}\n"

        f"15M: "
        f"{'BULLISH' if s['d15']==1 else 'BEARISH' if s['d15']==-1 else 'NEUTRAL'}\n"

        f"Alignment: {alignment_text}\n\n"

        f"15M SETUP:\n"
        f"{s['setup']}\n\n"

        f"NEW 5M CANDLE:\n"
        f"{s['trigger']}\n\n"

        f"Strategy:\n"
        f"{s['strategy']}\n\n"

        f"Signal candle:\n"
        f"{s['time']}\n\n"

        f"Scanner: {VERSION}\n"
        f"Signal only — no automatic trading."
    )

    if "level" in s:
        t += (
            f"\n\nBreakout level: "
            f"{s['level']:.6f}"
        )

    return t


# ============================================================
# SCAN ONE
# ============================================================

def scan_one(label, info):

    try:

        # -----------------------------------------------
        # 4H
        # -----------------------------------------------

        h4raw = candles(
            info["code"],
            TF4H
        )

        # -----------------------------------------------
        # 12H
        # -----------------------------------------------

        h12 = make12(h4raw)

        # -----------------------------------------------
        # Other timeframes
        # -----------------------------------------------

        h4 = h4raw

        h1 = candles(
            info["code"],
            TF1H
        )

        m15 = candles(
            info["code"],
            TF15
        )

        # -----------------------------------------------
        # Enough 12H history
        # -----------------------------------------------

        if len(h12) < 30:

            print(
                label,
                "NOT ENOUGH 12H DATA"
            )

            return

        # -----------------------------------------------
        # RANGE or STEP
        # -----------------------------------------------

        if info["family"] == "RANGE":

            signal, reason = scan_range(
                label,
                info,
                h12,
                h4,
                h1,
                m15,
            )

        else:

            signal, reason = scan_step(
                label,
                info,
                h12,
                h4,
                h1,
                m15,
            )

        # -----------------------------------------------
        # NO SIGNAL
        # -----------------------------------------------

        if not signal:

            print(reason)

            return

        # -----------------------------------------------
        # DUPLICATE CONTROL
        # -----------------------------------------------

        key = (
            "PULLBACK:"
            + label
        )

        signal_time = signal["time"]

        if state.get(key) == signal_time:

            print(
                label,
                "DUPLICATE"
            )

            return

        # -----------------------------------------------
        # SEND TELEGRAM
        # -----------------------------------------------

        if tg(msg(signal)):

            state[key] = signal_time

            save_state()

            print(
                label,
                ">>>",
                signal["direction"],
                signal["quality"],
                signal["score"],
                "/",
                signal["max"],
            )

    except Exception as e:

        print(
            label,
            "ERROR:",
            e
        )

        traceback.print_exc()


# ============================================================
# MAIN LOOP
# ============================================================

def main():

    print(
        "Starting",
        VERSION
    )

    if not BOT or not CHAT:

        print(
            "WARNING: set "
            "TELEGRAM_BOT_TOKEN "
            "and "
            "TELEGRAM_CHAT_ID"
        )

    # -----------------------------------------------
    # DISCOVER SYMBOLS
    # -----------------------------------------------

    symbols = discover()

    if not symbols:

        raise RuntimeError(
            "No target symbols discovered."
        )

    print(
        f"Resolved "
        f"{len(symbols)}/"
        f"{len(TARGETS)} instruments."
    )

    # -----------------------------------------------
    # CONTINUOUS SCANNING
    # -----------------------------------------------

    while True:

        started = time.time()

        print(
            "\n=== SCAN",
            datetime.now(
                timezone.utc
            ).isoformat(),
            "==="
        )

        for label, info in symbols.items():

            scan_one(
                label,
                info
            )

            time.sleep(0.5)

        elapsed = (
            time.time()
            - started
        )

        wait = max(
            5,
            INTERVAL - elapsed
        )

        print(
            f"Cycle finished. "
            f"Next scan in "
            f"{wait:.0f}s."
        )

        time.sleep(wait)


# ============================================================
# START
# ============================================================

if __name__ == "__main__":
    main()
