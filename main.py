import os
import json
import time
import re
import math
import threading
from datetime import datetime, timezone

import requests
import pandas as pd
import numpy as np
import websocket


# ============================================================
# VERSION
# ============================================================

VERSION = "2026-09-VOLATILITY-PULLBACK-EARLY-V4"


# ============================================================
# ENVIRONMENT
# ============================================================

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()

SCAN_INTERVAL_SECONDS = int(
    os.getenv("SCAN_INTERVAL_SECONDS", "60")
)

STATE_FILE = os.getenv(
    "STATE_FILE",
    "volatility_state.json"
)


# ============================================================
# DERIV
# ============================================================

WS_URL = (
    "wss://api.derivws.com/"
    "trading/v1/options/ws/public"
)


# ============================================================
# TARGET VOLATILITY INDICES
# ============================================================

TARGETS = [
    ("10", False),
    ("25", False),
    ("25", True),
    ("50", False),
    ("75", False),
    ("100", False),

    ("10", True),
    ("50", True),
    ("75", True),
    ("100", True),
]


# ============================================================
# STRATEGY SETTINGS
# ============================================================

EMA_FAST = 20
EMA_SLOW = 50

ATR_LEN = 14
RSI_LEN = 14

# 12H must be clear.
MIN_12H_CONFIRMATIONS = 3

# At least 2 of 4H / 1H / 15M must agree.
MIN_LTF_ALIGNMENT = 2

# Pullback settings.
PULLBACK_LOOKBACK = 6
PULLBACK_ATR_DISTANCE = 2.0

# Early 5M trigger.
EARLY_BODY_ATR = 0.12
EARLY_BODY_RATIO = 0.28

# Prevent duplicate alerts.
DUPLICATE_COOLDOWN_SECONDS = 15 * 60


# ============================================================
# GLOBALS
# ============================================================

http = requests.Session()

state_lock = threading.Lock()

state = {}


# ============================================================
# LOGGING
# ============================================================

def log(message):
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{now} UTC] {message}", flush=True)


# ============================================================
# STATE
# ============================================================

def load_state():
    global state

    try:
        if os.path.exists(STATE_FILE):
            with open(STATE_FILE, "r", encoding="utf-8") as f:
                state = json.load(f)

            if not isinstance(state, dict):
                state = {}

        else:
            state = {}

    except Exception as e:
        log(f"STATE LOAD ERROR: {e}")
        state = {}


def save_state():
    try:
        with state_lock:
            with open(
                STATE_FILE,
                "w",
                encoding="utf-8"
            ) as f:
                json.dump(
                    state,
                    f,
                    indent=2
                )

    except Exception as e:
        log(f"STATE SAVE ERROR: {e}")


# ============================================================
# TELEGRAM
# ============================================================

def send_telegram(message):

    if not TELEGRAM_BOT_TOKEN:
        log("TELEGRAM_BOT_TOKEN missing")
        return False

    if not TELEGRAM_CHAT_ID:
        log("TELEGRAM_CHAT_ID missing")
        return False

    url = (
        "https://api.telegram.org/bot"
        f"{TELEGRAM_BOT_TOKEN}/sendMessage"
    )

    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": message
    }

    try:
        response = http.post(
            url,
            json=payload,
            timeout=20
        )

        if response.ok:
            log("Telegram alert sent")
            return True

        log(
            "TELEGRAM ERROR "
            f"{response.status_code}: "
            f"{response.text[:500]}"
        )

    except Exception as e:
        log(f"TELEGRAM REQUEST ERROR: {e}")

    return False


# ============================================================
# DERIV WEBSOCKET REQUEST
# ============================================================

def deriv_request(payload, timeout=30):

    ws = None

    try:

        ws = websocket.create_connection(
            WS_URL,
            timeout=timeout,
            enable_multithread=True
        )

        ws.send(
            json.dumps(payload)
        )

        deadline = time.time() + timeout

        while time.time() < deadline:

            raw = ws.recv()

            if not raw:
                continue

            data = json.loads(raw)

            if "error" in data:

                error = data.get(
                    "error",
                    {}
                )

                message = error.get(
                    "message",
                    "Unknown Deriv error"
                )

                raise RuntimeError(message)

            # We only need the response matching
            # the requested message type.
            requested_type = None

            if "active_symbols" in payload:
                requested_type = "active_symbols"

            elif "ticks_history" in payload:
                requested_type = (
                    "candles"
                    if payload.get("style") == "candles"
                    else "history"
                )

            elif "ticks" in payload:
                requested_type = "tick"

            if (
                requested_type is None
                or data.get("msg_type") == requested_type
            ):
                return data

        raise TimeoutError(
            "Timed out waiting for Deriv response"
        )

    except Exception as e:
        raise

    finally:
        try:
            if ws:
                ws.close()
        except Exception:
            pass


# ============================================================
# ACTIVE SYMBOL DISCOVERY
# ============================================================

def normalize_text(value):
    return re.sub(
        r"[^A-Z0-9]",
        "",
        str(value).upper()
    )


def volatility_info(item):

    fields = [
        item.get("underlying_symbol_name", ""),
        item.get("display_name", ""),
        item.get("name", ""),
        item.get("underlying_symbol", ""),
        item.get("symbol", "")
    ]

    text = " ".join(
        str(x)
        for x in fields
        if x
    ).upper()

    normalized = normalize_text(text)

    if "VOLATILITY" not in normalized:
        return None

    match = re.search(
        r"VOLATILITY\s*(\d+)",
        text
    )

    if not match:
        match = re.search(
            r"\bV\s*(\d+)\b",
            text
        )

    if not match:
        return None

    number = match.group(1)

    is_1s = bool(
        re.search(
            r"\b1\s*S\b",
            text
        )
        or
        re.search(
            r"\b1\s*SECOND",
            text
        )
        or
        "(1S)" in normalized
        or
        "1SECOND" in normalized
    )

    symbol = (
        item.get("underlying_symbol")
        or item.get("symbol")
    )

    name = (
        item.get("underlying_symbol_name")
        or item.get("display_name")
        or item.get("name")
        or symbol
    )

    if not symbol:
        return None

    return {
        "number": number,
        "is_1s": is_1s,
        "symbol": symbol,
        "name": name
    }


def discover_symbols():

    response = deriv_request({
        "active_symbols": "brief"
    })

    items = response.get(
        "active_symbols",
        []
    )

    log(
        f"Deriv returned {len(items)} active symbols"
    )

    found = {}

    for item in items:

        info = volatility_info(item)

        if not info:
            continue

        key = (
            info["number"],
            info["is_1s"]
        )

        found[key] = info

    selected = []

    for number, is_1s in TARGETS:

        key = (
            number,
            is_1s
        )

        if key in found:

            selected.append(
                found[key]
            )

        else:

            suffix = " (1s)" if is_1s else ""

            log(
                f"NOT AVAILABLE: "
                f"Volatility {number}{suffix}"
            )

    log(
        f"Selected {len(selected)} "
        f"Volatility symbols"
    )

    for item in selected:

        log(
            f"FOUND: {item['name']} "
            f"-> {item['symbol']}"
        )

    return selected


# ============================================================
# CANDLE DATA
# ============================================================

def fetch_candles(
    symbol,
    granularity,
    count=150,
    keep_current=False
):

    payload = {
        "ticks_history": symbol,
        "count": count,
        "end": "latest",
        "style": "candles",
        "granularity": granularity
    }

    response = deriv_request(payload)

    candles = response.get(
        "candles",
        []
    )

    if not candles:
        raise RuntimeError(
            f"No candles returned for {symbol}"
        )

    rows = []

    for c in candles:

        try:

            rows.append({
                "time": int(c["epoch"]),
                "open": float(c["open"]),
                "high": float(c["high"]),
                "low": float(c["low"]),
                "close": float(c["close"])
            })

        except Exception:
            continue

    if not rows:
        raise RuntimeError(
            f"Invalid candle data for {symbol}"
        )

    df = pd.DataFrame(rows)

    df = df.drop_duplicates(
        subset=["time"]
    )

    df = df.sort_values(
        "time"
    ).reset_index(drop=True)

    # For higher timeframes we use completed candles.
    if not keep_current:

        now = int(time.time())

        current_bucket = (
            now // granularity
        ) * granularity

        df = df[
            df["time"] < current_bucket
        ].copy()

    return df


# ============================================================
# INDICATORS
# ============================================================

def add_indicators(df):

    df = df.copy()

    df["ema20"] = (
        df["close"]
        .ewm(
            span=EMA_FAST,
            adjust=False
        )
        .mean()
    )

    df["ema50"] = (
        df["close"]
        .ewm(
            span=EMA_SLOW,
            adjust=False
        )
        .mean()
    )

    previous_close = (
        df["close"]
        .shift(1)
    )

    tr1 = (
        df["high"] -
        df["low"]
    )

    tr2 = (
        df["high"] -
        previous_close
    ).abs()

    tr3 = (
        df["low"] -
        previous_close
    ).abs()

    true_range = pd.concat(
        [tr1, tr2, tr3],
        axis=1
    ).max(axis=1)

    df["atr"] = (
        true_range
        .rolling(ATR_LEN)
        .mean()
    )

    delta = (
        df["close"]
        .diff()
    )

    gain = (
        delta.clip(lower=0)
        .rolling(RSI_LEN)
        .mean()
    )

    loss = (
        -delta.clip(upper=0)
        .rolling(RSI_LEN)
        .mean()
    )

    rs = gain / loss.replace(
        0,
        np.nan
    )

    df["rsi"] = (
        100 -
        (100 / (1 + rs))
    )

    df["body"] = (
        df["close"] -
        df["open"]
    ).abs()

    df["range"] = (
        df["high"] -
        df["low"]
    ).replace(
        0,
        np.nan
    )

    df["body_ratio"] = (
        df["body"] /
        df["range"]
    )

    df["green"] = (
        df["close"] >
        df["open"]
    )

    df["red"] = (
        df["close"] <
        df["open"]
    )

    return df


# ============================================================
# 12H CREATION FROM 4H
# ============================================================

def build_12h(df4h):

    if df4h.empty:
        return pd.DataFrame()

    temp = df4h.copy()

    temp["datetime"] = pd.to_datetime(
        temp["time"],
        unit="s",
        utc=True
    )

    temp = temp.set_index(
        "datetime"
    )

    result = (
        temp.resample("12h")
        .agg({
            "open": "first",
            "high": "max",
            "low": "min",
            "close": "last",
            "time": "last"
        })
    )

    counts = (
        temp["close"]
        .resample("12h")
        .count()
    )

    result["count"] = counts

    result = result[
        result["count"] >= 3
    ]

    result = result.reset_index(
        drop=True
    )

    result = result[
        [
            "time",
            "open",
            "high",
            "low",
            "close"
        ]
    ]

    return result


# ============================================================
# 12H DIRECTION
# ============================================================

def get_12h_direction(df):

    if len(df) < 60:
        return "NEUTRAL", 0

    df = add_indicators(df)

    row = df.iloc[-1]

    confirmations_buy = 0
    confirmations_sell = 0

    if row["close"] > row["ema20"]:
        confirmations_buy += 1

    if row["ema20"] > row["ema50"]:
        confirmations_buy += 1

    if (
        len(df) >= 5
        and row["ema20"] >
        df["ema20"].iloc[-5]
    ):
        confirmations_buy += 1

    if row["rsi"] >= 52:
        confirmations_buy += 1

    if row["close"] < row["ema20"]:
        confirmations_sell += 1

    if row["ema20"] < row["ema50"]:
        confirmations_sell += 1

    if (
        len(df) >= 5
        and row["ema20"] <
        df["ema20"].iloc[-5]
    ):
        confirmations_sell += 1

    if row["rsi"] <= 48:
        confirmations_sell += 1

    if confirmations_buy >= MIN_12H_CONFIRMATIONS:
        return "BUY", confirmations_buy

    if confirmations_sell >= MIN_12H_CONFIRMATIONS:
        return "SELL", confirmations_sell

    return "NEUTRAL", max(
        confirmations_buy,
        confirmations_sell
    )


# ============================================================
# LOWER TIMEFRAME DIRECTION
# ============================================================

def timeframe_direction(df):

    if len(df) < 60:
        return "NEUTRAL"

    df = add_indicators(df)

    row = df.iloc[-1]

    buy = 0
    sell = 0

    if row["close"] > row["ema20"]:
        buy += 1

    if row["ema20"] > row["ema50"]:
        buy += 1

    if (
        row["ema20"] >
        df["ema20"].iloc[-4]
    ):
        buy += 1

    if row["close"] < row["ema20"]:
        sell += 1

    if row["ema20"] < row["ema50"]:
        sell += 1

    if (
        row["ema20"] <
        df["ema20"].iloc[-4]
    ):
        sell += 1

    if buy >= 2:
        return "BUY"

    if sell >= 2:
        return "SELL"

    return "NEUTRAL"


# ============================================================
# PULLBACK DETECTION
# ============================================================

def pullback_detected(
    df,
    direction
):

    if len(df) < 30:
        return False

    df = add_indicators(df)

    recent = df.iloc[
        -PULLBACK_LOOKBACK:
    ]

    last = df.iloc[-1]

    if pd.isna(last["atr"]):
        return False

    atr = float(last["atr"])

    if atr <= 0:
        return False

    # Distance from EMA20.
    distance = abs(
        last["close"] -
        last["ema20"]
    )

    near_ema = (
        distance <=
        PULLBACK_ATR_DISTANCE * atr
    )

    if not near_ema:
        return False

    # Price must have interacted with EMA20.
    touched = (
        (
            recent["low"] <=
            recent["ema20"]
        )
        &
        (
            recent["high"] >=
            recent["ema20"]
        )
    ).any()

    if not touched:
        return False

    if direction == "BUY":

        countertrend = (
            recent["red"]
        ).any()

        structure_ok = (
            last["close"] >
            recent["low"].min()
        )

        return (
            countertrend
            and structure_ok
        )

    if direction == "SELL":

        countertrend = (
            recent["green"]
        ).any()

        structure_ok = (
            last["close"] <
            recent["high"].max()
        )

        return (
            countertrend
            and structure_ok
        )

    return False


# ============================================================
# CURRENT 5M EARLY TRIGGER
# ============================================================

def current_5m_trigger(
    df,
    direction
):

    if len(df) < 25:
        return False, 0

    df = add_indicators(df)

    current = df.iloc[-1]
    previous = df.iloc[-2]

    if pd.isna(current["atr"]):
        return False, 0

    atr = float(current["atr"])

    if atr <= 0:
        return False, 0

    score = 0

    # --------------------------------------------------------
    # BUY
    # --------------------------------------------------------

    if direction == "BUY":

        if current["green"]:
            score += 1

        if current["close"] > current["ema20"]:
            score += 1

        if current["close"] > previous["close"]:
            score += 1

        if current["body"] >= (
            EARLY_BODY_ATR * atr
        ):
            score += 1

        if current["body_ratio"] >= (
            EARLY_BODY_RATIO
        ):
            score += 1

        # Require at least 3 confirmations.
        return score >= 3, score

    # --------------------------------------------------------
    # SELL
    # --------------------------------------------------------

    if direction == "SELL":

        if current["red"]:
            score += 1

        if current["close"] < current["ema20"]:
            score += 1

        if current["close"] < previous["close"]:
            score += 1

        if current["body"] >= (
            EARLY_BODY_ATR * atr
        ):
            score += 1

        if current["body_ratio"] >= (
            EARLY_BODY_RATIO
        ):
            score += 1

        return score >= 3, score

    return False, 0


# ============================================================
# SIGNAL DUPLICATE CONTROL
# ============================================================

def signal_allowed(
    symbol,
    direction,
    setup_time
):

    key = (
        f"{symbol}|"
        f"{direction}|"
        f"{setup_time}"
    )

    now = int(time.time())

    with state_lock:

        previous = state.get(key)

        if previous:
            age = now - int(previous)

            if age < DUPLICATE_COOLDOWN_SECONDS:
                return False

        state[key] = now

    save_state()

    return True


# ============================================================
# FORMAT PRICE
# ============================================================

def format_price(price):

    try:

        if price >= 1000:
            return f"{price:.3f}"

        if price >= 100:
            return f"{price:.4f}"

        return f"{price:.5f}"

    except Exception:
        return str(price)


# ============================================================
# TELEGRAM MESSAGE
# ============================================================

def build_message(
    info,
    direction,
    score_12h,
    directions,
    current_5m,
    trigger_score
):

    symbol_name = info["name"]
    symbol_code = info["symbol"]

    suffix = (
        " (1s)"
        if info["is_1s"]
        else ""
    )

    price = current_5m.iloc[-1]["close"]

    rsi = current_5m.iloc[-1]["rsi"]

    candle_time = datetime.fromtimestamp(
        int(current_5m.iloc[-1]["time"]),
        tz=timezone.utc
    ).strftime(
        "%H:%M UTC"
    )

    emoji = (
        "🟢"
        if direction == "BUY"
        else "🔴"
    )

    return (
        f"{emoji} {symbol_name}{suffix} — "
        f"{direction} SIGNAL\n\n"

        f"Symbol: {symbol_code}\n"
        f"Price: {format_price(price)}\n\n"

        f"📊 MULTI-TIMEFRAME\n"
        f"12H: {direction} "
        f"({score_12h}/4)\n"
        f"4H: {directions['4H']}\n"
        f"1H: {directions['1H']}\n"
        f"15M: {directions['15M']}\n\n"

        f"🔄 Pullback: CONFIRMED\n"
        f"⚡ 5M: EARLY {direction}\n"
        f"5M trigger score: "
        f"{trigger_score}/5\n"
        f"5M RSI: {rsi:.1f}\n"
        f"5M candle: {candle_time}\n\n"

        f"Strategy: 12H trend + "
        f"multi-timeframe alignment + "
        f"pullback + early 5M confirmation.\n\n"

        f"⚠️ SIGNAL ONLY — "
        f"NO AUTO TRADING\n"
        f"Scanner {VERSION}"
    )


# ============================================================
# SCAN ONE SYMBOL
# ============================================================

def scan_symbol(info):

    symbol = info["symbol"]
    name = info["name"]

    try:

        # ----------------------------------------------------
        # 4H
        # ----------------------------------------------------

        df4h = fetch_candles(
            symbol,
            14400,
            count=180,
            keep_current=False
        )

        # ----------------------------------------------------
        # 12H
        # ----------------------------------------------------

        df12h = build_12h(df4h)

        direction_12h, score_12h = (
            get_12h_direction(df12h)
        )

        if direction_12h == "NEUTRAL":

            log(
                f"{name} NO SETUP "
                f"12H neutral"
            )

            return

        # ----------------------------------------------------
        # 1H
        # ----------------------------------------------------

        df1h = fetch_candles(
            symbol,
            3600,
            count=150,
            keep_current=False
        )

        direction_1h = (
            timeframe_direction(df1h)
        )

        # ----------------------------------------------------
        # 15M
        # ----------------------------------------------------

        df15m = fetch_candles(
            symbol,
            900,
            count=180,
            keep_current=False
        )

        direction_15m = (
            timeframe_direction(df15m)
        )

        # ----------------------------------------------------
        # 4H DIRECTION
        # ----------------------------------------------------

        direction_4h = (
            timeframe_direction(df4h)
        )

        directions = {
            "4H": direction_4h,
            "1H": direction_1h,
            "15M": direction_15m
        }

        aligned = sum(
            1
            for value in directions.values()
            if value == direction_12h
        )

        if aligned < MIN_LTF_ALIGNMENT:

            log(
                f"{name} NO SETUP "
                f"12H={direction_12h} "
                f"4H={direction_4h} "
                f"1H={direction_1h} "
                f"15M={direction_15m}"
            )

            return

        # ----------------------------------------------------
        # PULLBACK
        # ----------------------------------------------------

        if not pullback_detected(
            df15m,
            direction_12h
        ):

            log(
                f"{name} WAIT "
                f"{direction_12h} "
                f"alignment={aligned}/3 "
                f"pullback=no"
            )

            return

        # ----------------------------------------------------
        # CURRENT / FORMING 5M
        # ----------------------------------------------------

        df5m = fetch_candles(
            symbol,
            300,
            count=100,
            keep_current=True
        )

        triggered, trigger_score = (
            current_5m_trigger(
                df5m,
                direction_12h
            )
        )

        if not triggered:

            last = df5m.iloc[-1]

            candle = (
                "GREEN"
                if last["close"] > last["open"]
                else
                "RED"
                if last["close"] < last["open"]
                else
                "NEUTRAL"
            )

            log(
                f"{name} PULLBACK FOUND "
                f"but 5M not ready "
                f"candle={candle} "
                f"score={trigger_score}/5"
            )

            return

        # ----------------------------------------------------
        # DUPLICATE PROTECTION
        # ----------------------------------------------------

        setup_time = int(
            df15m.iloc[-1]["time"]
        )

        if not signal_allowed(
            symbol,
            direction_12h,
            setup_time
        ):

            log(
                f"{name} duplicate blocked"
            )

            return

        # ----------------------------------------------------
        # SEND
        # ----------------------------------------------------

        message = build_message(
            info,
            direction_12h,
            score_12h,
            directions,
            df5m,
            trigger_score
        )

        send_telegram(message)

        log(
            f"🚨 SIGNAL "
            f"{name} "
            f"{direction_12h} "
            f"alignment={aligned}/3 "
            f"5M={trigger_score}/5"
        )

    except Exception as e:

        log(
            f"{name} ERROR: {type(e).__name__}: {e}"
        )


# ============================================================
# MAIN SCANNER
# ============================================================

def main():

    log("=" * 60)
    log(f"STARTING {VERSION}")
    log("=" * 60)

    load_state()

    if not TELEGRAM_BOT_TOKEN:
        log(
            "WARNING: TELEGRAM_BOT_TOKEN "
            "is missing"
        )

    if not TELEGRAM_CHAT_ID:
        log(
            "WARNING: TELEGRAM_CHAT_ID "
            "is missing"
        )

    while True:

        cycle_start = time.time()

        try:

            symbols = discover_symbols()

            if not symbols:

                log(
                    "No target Volatility "
                    "symbols found."
                )

            else:

                for info in symbols:

                    scan_symbol(info)

                    # Small pause to avoid
                    # hammering the API.
                    time.sleep(0.4)

        except Exception as e:

            log(
                f"SCAN CYCLE ERROR: "
                f"{type(e).__name__}: {e}"
            )

        elapsed = time.time() - cycle_start

        sleep_time = max(
            5,
            SCAN_INTERVAL_SECONDS - elapsed
        )

        log(
            f"Cycle finished in "
            f"{elapsed:.1f}s. "
            f"Next scan in "
            f"{sleep_time:.1f}s."
        )

        time.sleep(sleep_time)


# ============================================================
# START
# ============================================================

if __name__ == "__main__":
    main()
