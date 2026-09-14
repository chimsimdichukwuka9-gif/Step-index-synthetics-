import os
import json
import time
import traceback
from datetime import datetime, timezone

import numpy as np
import pandas as pd
import requests
import websocket


VERSION = "2026-09-VOLATILITY-PULLBACK-EARLY-V1"

WS_URL = os.getenv(
    "DERIV_WS_URL",
    "wss://api.derivws.com/trading/v1/options/ws/public"
)

BOT = os.getenv("TELEGRAM_BOT_TOKEN", "")
CHAT = os.getenv("TELEGRAM_CHAT_ID", "")

INTERVAL = int(os.getenv("SCAN_INTERVAL_SECONDS", "60"))
STATE_FILE = os.getenv(
    "STATE_FILE",
    "volatility_state.json"
)


# ============================================================
# VOLATILITY TARGETS
# ============================================================

TARGETS = [
    ("VOLATILITY 10", "Volatility 10 Index"),
    ("VOLATILITY 25", "Volatility 25 Index"),
    ("VOLATILITY 25 (1S)", "Volatility 25 (1s) Index"),
    ("VOLATILITY 50", "Volatility 50 Index"),
    ("VOLATILITY 75", "Volatility 75 Index"),
    ("VOLATILITY 100", "Volatility 100 Index"),

    ("VOLATILITY 10 (1S)", "Volatility 10 (1s) Index"),
    ("VOLATILITY 50 (1S)", "Volatility 50 (1s) Index"),
    ("VOLATILITY 75 (1S)", "Volatility 75 (1s) Index"),
    ("VOLATILITY 100 (1S)", "Volatility 100 (1s) Index"),
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

    with open(
        STATE_FILE + ".tmp",
        "w",
        encoding="utf-8"
    ) as f:

        json.dump(
            state,
            f,
            indent=2
        )

    os.replace(
        STATE_FILE + ".tmp",
        STATE_FILE
    )


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

            print(
                "Telegram error:",
                r.status_code,
                r.text[:500]
            )

            return False

        return True

    except Exception as e:

        print(
            "Telegram exception:",
            e
        )

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

        ws.send(
            json.dumps(payload)
        )

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

        raise TimeoutError(
            "Deriv request timeout"
        )

    finally:

        ws.close()


# ============================================================
# NORMALIZATION
# ============================================================

def norm(s):

    s = str(s or "").upper()

    for c in " -_/().,":

        s = s.replace(c, "")

    return s.replace(
        "INDEX",
        ""
    )


# ============================================================
# SYMBOL DISCOVERY
# ============================================================

def discover():

    # IMPORTANT:
    # Do NOT add product_type.
    data = deriv({
        "active_symbols": "brief"
    })

    found = {}

    for x in data.get(
        "active_symbols",
        []
    ):

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

            found[
                norm(name)
            ] = (
                code,
                name
            )

    out = {}

    print(
        "\n=== VOLATILITY SYMBOL DISCOVERY ==="
    )

    for label, wanted in TARGETS:

        hit = found.get(
            norm(wanted)
        )

        if hit:

            out[label] = {
                "code": hit[0],
                "display": hit[1],
            }

            print(
                "FOUND ",
                label,
                "->",
                hit[1],
                "[",
                hit[0],
                "]"
            )

        else:

            print(
                "MISSING",
                label,
                "->",
                wanted
            )

    print(
        "====================================\n"
    )

    return out


# ============================================================
# COMPLETED CANDLES
# ============================================================

def candles(
    symbol,
    seconds,
    count=260
):

    data = deriv({

        "ticks_history": symbol,

        "adjust_start_time": 1,

        "count": count,

        "end": "latest",

        "granularity": seconds,

        "style": "candles",

    })

    rows = []

    for c in data.get(
        "candles",
        []
    ):

        try:

            rows.append({

                "time":
                    pd.to_datetime(
                        int(c["epoch"]),
                        unit="s",
                        utc=True
                    ),

                "open":
                    float(c["open"]),

                "high":
                    float(c["high"]),

                "low":
                    float(c["low"]),

                "close":
                    float(c["close"]),

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

    # Remove the currently forming candle.
    # This function is used for 4H, 1H and 15M.
    cutoff = (
        pd.Timestamp.now(tz="UTC")
        -
        pd.Timedelta(
            seconds=seconds
        )
    )

    df = df[
        df.index < cutoff
    ]

    if len(df) < 70:

        raise RuntimeError(
            f"Only {len(df)} completed "
            f"candles for {symbol}"
        )

    return df


# ============================================================
# 12H FROM 4H
# ============================================================

def make12(df4):

    x = (
        df4
        .resample(
            "12h",
            label="right",
            closed="right"
        )
        .agg({

            "open": "first",

            "high": "max",

            "low": "min",

            "close": "last",

        })
        .dropna()
    )

    now = pd.Timestamp.now(
        tz="UTC"
    )

    return x[
        x.index <= now
    ]


# ============================================================
# EMA
# ============================================================

def ema(
    s,
    n
):

    return s.ewm(
        span=n,
        adjust=False
    ).mean()


# ============================================================
# ATR
# ============================================================

def atr(
    df,
    n=14
):

    pc = df.close.shift(1)

    tr = pd.concat(
        [

            df.high - df.low,

            (
                df.high - pc
            ).abs(),

            (
                df.low - pc
            ).abs(),

        ],
        axis=1
    ).max(axis=1)

    return tr.ewm(
        alpha=1 / n,
        adjust=False
    ).mean()


# ============================================================
# RSI
# ============================================================

def rsi(
    s,
    n=14
):

    d = s.diff()

    g = d.clip(
        lower=0
    )

    l = -d.clip(
        upper=0
    )

    ag = g.ewm(
        alpha=1 / n,
        adjust=False
    ).mean()

    al = l.ewm(
        alpha=1 / n,
        adjust=False
    ).mean()

    rs = (
        ag /
        al.replace(
            0,
            np.nan
        )
    )

    return (
        100 -
        100 / (1 + rs)
    )


# ============================================================
# INDICATORS
# ============================================================

def ind(df):

    x = df.copy()

    x["e20"] = ema(
        x.close,
        20
    )

    x["e50"] = ema(
        x.close,
        50
    )

    x["atr"] = atr(
        x
    )

    x["rsi"] = rsi(
        x.close
    )

    x["bull"] = (
        x.close >
        x.open
    )

    x["bear"] = (
        x.close <
        x.open
    )

    x["body"] = (
        x.close -
        x.open
    ).abs()

    x["range"] = (
        x.high -
        x.low
    )

    return x


# ============================================================
# CLEAR 12H DIRECTION
# ============================================================

def clear_direction(df):

    if len(df) < 55:

        return 0

    x = ind(df)

    a = x.iloc[-1]

    b = x.iloc[-4]

    if (
        not np.isfinite(a.atr)
        or
        a.atr <= 0
    ):

        return 0

    # --------------------------------------------------------
    # 12H BULLISH
    # --------------------------------------------------------

    bull_conditions = [

        a.close > a.e20,

        a.e20 > a.e50,

        a.e20 > b.e20,

        a.rsi >= 52,

    ]

    # --------------------------------------------------------
    # 12H BEARISH
    # --------------------------------------------------------

    bear_conditions = [

        a.close < a.e20,

        a.e20 < a.e50,

        a.e20 < b.e20,

        a.rsi <= 48,

    ]

    bull_score = sum(
        bull_conditions
    )

    bear_score = sum(
        bear_conditions
    )

    # 3/4 = clear direction

    if bull_score >= 3:

        return 1

    if bear_score >= 3:

        return -1

    return 0


# ============================================================
# LOWER-TIMEFRAME DIRECTION
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
# 4H + 1H + 15M ALIGNMENT
# ============================================================

def timeframe_alignment(
    d12,
    d4,
    d1,
    d15
):

    directions = {

        "4H": d4,

        "1H": d1,

        "15M": d15,

    }

    aligned = sum(

        1

        for d in directions.values()

        if d == d12

    )

    return (
        aligned,
        directions
    )


# ============================================================
# 15M PULLBACK DETECTION
# ============================================================

def pullback_setup(
    m15,
    d
):

    x = ind(m15)

    if len(x) < 15:

        return (
            False,
            "NOT_ENOUGH_15M"
        )

    a = x.iloc[-1]

    recent = x.iloc[-7:-1]

    if (
        not np.isfinite(a.atr)
        or
        a.atr <= 0
    ):

        return (
            False,
            "ATR_INVALID"
        )

    # Distance from EMA20

    distance = (
        abs(
            a.close -
            a.e20
        )
        /
        a.atr
    )

    near_ema = (
        distance <= 1.8
    )

    # ========================================================
    # BULLISH PULLBACK
    # ========================================================

    if d == 1:

        # Price must have pulled down
        # toward the EMA.

        pullback_low = (
            recent.low.min()
            <=
            recent.e20.max()
            +
            0.60 * a.atr
        )

        # The recent candles should show
        # some countertrend movement.

        countertrend = (
            (
                recent.close <
                recent.open
            ).sum()
            >= 1
        )

        # Current 15M candle does NOT need
        # to be strongly bullish yet.
        # 5M will be the trigger.

        ok = (
            near_ema
            and
            pullback_low
            and
            countertrend
        )

        if ok:

            return (
                True,
                "15M BULLISH PULLBACK READY"
            )

        return (
            False,
            "15M WAITING FOR BULLISH PULLBACK"
        )

    # ========================================================
    # BEARISH PULLBACK
    # ========================================================

    if d == -1:

        pullback_high = (
            recent.high.max()
            >=
            recent.e20.min()
            -
            0.60 * a.atr
        )

        countertrend = (
            (
                recent.close >
                recent.open
            ).sum()
            >= 1
        )

        ok = (
            near_ema
            and
            pullback_high
            and
            countertrend
        )

        if ok:

            return (
                True,
                "15M BEARISH PULLBACK READY"
            )

        return (
            False,
            "15M WAITING FOR BEARISH PULLBACK"
        )

    return (
        False,
        "INVALID_DIRECTION"
    )


# ============================================================
# CURRENT / FORMING 5M CANDLE
# ============================================================

def current5(symbol):

    data = deriv({

        "ticks_history": symbol,

        "adjust_start_time": 1,

        "count": 80,

        "end": "latest",

        "granularity": TF5,

        "style": "candles",

    })

    rows = []

    for c in data.get(
        "candles",
        []
    ):

        try:

            rows.append({

                "time":
                    pd.to_datetime(
                        int(c["epoch"]),
                        unit="s",
                        utc=True
                    ),

                "open":
                    float(c["open"]),

                "high":
                    float(c["high"]),

                "low":
                    float(c["low"]),

                "close":
                    float(c["close"]),

            })

        except Exception:

            pass

    if len(rows) < 30:

        return None

    df = (
        pd.DataFrame(rows)
        .set_index("time")
        .sort_index()
        .drop_duplicates()
    )

    return ind(df)


# ============================================================
# EARLY 5M TRIGGER
#
# Uses the CURRENT / FORMING candle.
#
# It does NOT wait for the 5M candle to close.
# ============================================================

def early_trigger5(
    symbol,
    d
):

    x = current5(
        symbol
    )

    if x is None:

        return (
            False,
            "NOT_ENOUGH_5M",
            None,
            None
        )

    if len(x) < 3:

        return (
            False,
            "NOT_ENOUGH_5M",
            None,
            None
        )

    # Current/forming candle

    a = x.iloc[-1]

    # Previous candle

    p = x.iloc[-2]

    if (
        not np.isfinite(a.atr)
        or
        a.atr <= 0
    ):

        return (
            False,
            "ATR_INVALID",
            None,
            None
        )

    candle_range = (
        a.high -
        a.low
    )

    body = abs(
        a.close -
        a.open
    )

    if candle_range <= 0:

        return (
            False,
            "NO_5M_MOVEMENT",
            None,
            None
        )

    body_ratio = (
        body /
        candle_range
    )

    body_atr = (
        body /
        a.atr
    )

    # ========================================================
    # BUY
    # ========================================================

    if d == 1:

        bullish = (
            a.close >
            a.open
        )

        above_ema = (
            a.close >
            a.e20
        )

        higher_than_previous = (
            a.close >
            p.close
        )

        meaningful_body = (
            body_atr >= 0.15
        )

        controlled_candle = (
            body_ratio >= 0.30
        )

        score = sum([

            bullish,

            above_ema,

            higher_than_previous,

            meaningful_body,

            controlled_candle,

        ])

        # 3/5 required.
        #
        # This intentionally does NOT require
        # a full breakout of the previous high.
        # That makes the trigger earlier.

        if score >= 3:

            return (

                True,

                f"NEW 5M BUY "
                f"MOVEMENT {score}/5",

                a.name.isoformat(),

                float(a.close)

            )

        return (

            False,

            f"NEW 5M BUY WAIT "
            f"{score}/5",

            a.name.isoformat(),

            float(a.close)

        )

    # ========================================================
    # SELL
    # ========================================================

    if d == -1:

        bearish = (
            a.close <
            a.open
        )

        below_ema = (
            a.close <
            a.e20
        )

        lower_than_previous = (
            a.close <
            p.close
        )

        meaningful_body = (
            body_atr >= 0.15
        )

        controlled_candle = (
            body_ratio >= 0.30
        )

        score = sum([

            bearish,

            below_ema,

            lower_than_previous,

            meaningful_body,

            controlled_candle,

        ])

        if score >= 3:

            return (

                True,

                f"NEW 5M SELL "
                f"MOVEMENT {score}/5",

                a.name.isoformat(),

                float(a.close)

            )

        return (

            False,

            f"NEW 5M SELL WAIT "
            f"{score}/5",

            a.name.isoformat(),

            float(a.close)

        )

    return (
        False,
        "INVALID_DIRECTION",
        None,
        None
    )


# ============================================================
# SCAN VOLATILITY INDEX
# ============================================================

def scan_volatility(
    label,
    info,
    h12,
    h4,
    h1,
    m15
):

    # ========================================================
    # 12H MAIN DIRECTION
    # ========================================================

    d12 = clear_direction(
        h12
    )

    # ========================================================
    # LOWER TIMEFRAMES
    # ========================================================

    d4 = direction(
        h4
    )

    d1 = direction(
        h1
    )

    d15 = direction(
        m15
    )

    # ========================================================
    # 12H MUST BE CLEAR
    # ========================================================

    if d12 == 0:

        return (
            None,
            f"{label} NO SETUP "
            f"12H=NOT CLEAR"
        )

    # ========================================================
    # 2 OF 3 OR 3 OF 3
    # ========================================================

    aligned, dirs = (
        timeframe_alignment(
            d12,
            d4,
            d1,
            d15
        )
    )

    if aligned < 2:

        return (

            None,

            f"{label} NO SETUP "

            f"12H="
            f"{d12} "

            f"4H="
            f"{d4} "

            f"1H="
            f"{d1} "

            f"15M="
            f"{d15} "

            f"ALIGN="
            f"{aligned}/3"

        )

    # ========================================================
    # 15M PULLBACK
    # ========================================================

    pull, pull_text = (
        pullback_setup(
            m15,
            d12
        )
    )

    if not pull:

        return (

            None,

            f"{label} "
            f"{pull_text} "

            f"ALIGN="
            f"{aligned}/3"

        )

    # ========================================================
    # CURRENT 5M CANDLE
    # ========================================================

    (
        trigger,
        trigger_text,
        trigger_time,
        trigger_price
    ) = early_trigger5(
        info["code"],
        d12
    )

    if not trigger:

        return (

            None,

            f"{label} "
            f"{trigger_text} "

            f"ALIGN="
            f"{aligned}/3"

        )

    # ========================================================
    # SCORE
    # ========================================================

    # 12H clear = 4 points
    #
    # 2/3 alignment = 2
    #
    # 3/3 alignment = 3
    #
    # 15M pullback = 2
    #
    # Current 5M trigger = 3

    alignment_points = (
        3
        if aligned == 3
        else 2
    )

    score = (
        4
        +
        alignment_points
        +
        2
        +
        3
    )

    max_score = 12

    quality = (
        "A+"
        if aligned == 3
        else "A"
    )

    return {

        "label":
            label,

        "display":
            info["display"],

        "direction":
            (
                "BUY"
                if d12 == 1
                else "SELL"
            ),

        "score":
            score,

        "max":
            max_score,

        "quality":
            quality,

        "d12":
            d12,

        "d4":
            d4,

        "d1":
            d1,

        "d15":
            d15,

        "aligned":
            aligned,

        "setup":
            pull_text,

        "trigger":
            trigger_text,

        "time":
            trigger_time,

        "price":
            trigger_price,

        "strategy":
            (
                "12H Direction + "
                "2/3 MTF Alignment + "
                "15M Pullback + "
                "Early Current 5M Reversal"
            ),

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

    def tf_text(v):

        if v == 1:
            return "BULLISH"

        if v == -1:
            return "BEARISH"

        return "NEUTRAL"

    text = (

        f"{emoji} "
        f"{s['label']} — "
        f"{s['direction']} SIGNAL\n\n"

        f"Quality: "
        f"{s['quality']}\n"

        f"Technical score: "
        f"{s['score']}/"
        f"{s['max']}\n\n"

        f"Symbol: "
        f"{s['display']}\n"

        f"Price: "
        f"{s['price']:.6f}\n\n"

        f"MULTI-TIMEFRAME:\n"

        f"12H: "
        f"{direction_text} "
        f"✅ CLEAR\n"

        f"4H: "
        f"{tf_text(s['d4'])}\n"

        f"1H: "
        f"{tf_text(s['d1'])}\n"

        f"15M: "
        f"{tf_text(s['d15'])}\n"

        f"Alignment: "
        f"{s['aligned']}/3\n\n"

        f"15M PULLBACK:\n"
        f"{s['setup']}\n\n"

        f"CURRENT 5M CANDLE:\n"
        f"{s['trigger']}\n\n"

        f"⚡ EARLY ENTRY:\n"
        f"Signal triggered from the "
        f"NEW / FORMING 5M candle.\n"

        f"The 5M candle does NOT need "
        f"to close first.\n\n"

        f"Strategy:\n"
        f"{s['strategy']}\n\n"

        f"Signal candle:\n"
        f"{s['time']}\n\n"

        f"Scanner: "
        f"{VERSION}\n"

        f"Signal only — "
        f"no automatic trading."
    )

    return text


# ============================================================
# SCAN ONE
# ============================================================

def scan_one(
    label,
    info
):

    try:

        # ====================================================
        # 4H
        # ====================================================

        h4raw = candles(
            info["code"],
            TF4H
        )

        # ====================================================
        # 12H
        # ====================================================

        h12 = make12(
            h4raw
        )

        # ====================================================
        # 1H
        # ====================================================

        h1 = candles(
            info["code"],
            TF1H
        )

        # ====================================================
        # 15M
        # ====================================================

        m15 = candles(
            info["code"],
            TF15
        )

        # ====================================================
        # DATA CHECK
        # ====================================================

        if len(h12) < 30:

            print(
                label,
                "NOT ENOUGH 12H DATA"
            )

            return

        # ====================================================
        # SCAN
        # ====================================================

        signal, reason = (
            scan_volatility(
                label,
                info,
                h12,
                h4raw,
                h1,
                m15
            )
        )

        # ====================================================
        # NO SIGNAL
        # ====================================================

        if not signal:

            print(reason)

            return

        # ====================================================
        # DUPLICATE CONTROL
        # ====================================================

        # One signal per exact current 5M candle.

        key = (
            "VOL_PULLBACK:"
            +
            label
        )

        signal_time = (
            signal["time"]
        )

        if state.get(key) == signal_time:

            print(
                label,
                "DUPLICATE"
            )

            return

        # ====================================================
        # TELEGRAM
        # ====================================================

        if tg(
            msg(signal)
        ):

            state[key] = (
                signal_time
            )

            save_state()

            print(

                label,

                ">>>",

                signal["direction"],

                signal["quality"],

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

        traceback.print_exc()


# ============================================================
# MAIN LOOP
# ============================================================

def main():

    print(
        "Starting",
        VERSION
    )

    print(
        "Strategy:"
    )

    print(
        "12H direction -> "
        "2/3 or 3/3 alignment -> "
        "15M pullback -> "
        "current 5M reversal"
    )

    if not BOT or not CHAT:

        print(
            "WARNING: set "
            "TELEGRAM_BOT_TOKEN "
            "and "
            "TELEGRAM_CHAT_ID"
        )

    # ========================================================
    # DISCOVER
    # ========================================================

    symbols = discover()

    if not symbols:

        raise RuntimeError(
            "No Volatility symbols discovered."
        )

    print(

        f"Resolved "
        f"{len(symbols)}/"
        f"{len(TARGETS)} "
        f"Volatility instruments."

    )

    # ========================================================
    # LOOP
    # ========================================================

    while True:

        started = time.time()

        print(

            "\n=== VOLATILITY SCAN",

            datetime.now(
                timezone.utc
            ).isoformat(),

            "==="

        )

        for label, info in (
            symbols.items()
        ):

            scan_one(
                label,
                info
            )

            time.sleep(
                0.5
            )

        elapsed = (
            time.time()
            -
            started
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

        time.sleep(
            wait
        )


# ============================================================
# START
# ============================================================

if __name__ == "__main__":

    main()
