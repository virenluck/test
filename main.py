import hashlib
import hmac
import json
import os
import time
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from threading import Lock, Thread
from typing import Any

import pandas as pd
import requests
from fastapi import FastAPI, HTTPException, Query, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

# --- Configuration ---
ENTRY_RESOLUTION = os.getenv("ENTRY_RESOLUTION", os.getenv("DELTA_RESOLUTION", "3m")).strip()
_raw_mtf = os.getenv("MTF_RESOLUTIONS", f"{ENTRY_RESOLUTION},15m,1h").strip()
MTF_RESOLUTIONS: list[str] = []
for tf in [x.strip() for x in _raw_mtf.split(",") if x.strip()]:
    if tf not in MTF_RESOLUTIONS:
        MTF_RESOLUTIONS.append(tf)
if ENTRY_RESOLUTION not in MTF_RESOLUTIONS:
    MTF_RESOLUTIONS.insert(0, ENTRY_RESOLUTION)

POLL_SECONDS = int(os.getenv("POLL_SECONDS", "10"))
ATR_MULTIPLIER = float(os.getenv("ATR_MULTIPLIER", "1.5"))
BASE_LOOKBACK_HOURS = int(os.getenv("CANDLE_LOOKBACK_HOURS", "12"))
MIN_SIGNAL_SCORE = int(os.getenv("MIN_SIGNAL_SCORE", "4"))
REQUIRE_HTF_ALIGNMENT = os.getenv("REQUIRE_HTF_ALIGNMENT", "true").strip().lower() in ("1", "true", "yes")

DELTA_BASE_URL = os.getenv("DELTA_BASE_URL", "https://api.india.delta.exchange").rstrip("/")
DELTA_API_KEY = os.getenv("DELTA_API_KEY", "")
DELTA_API_SECRET = os.getenv("DELTA_API_SECRET", "")

API_PWD = os.getenv("API_PWD", "a1a1a1")
EXCEL_FILE = os.getenv("EXCEL_FILE", "data_Log.xlsx")

_raw_cors_origins = os.getenv("CORS_ALLOW_ORIGINS", "*").strip()
if not _raw_cors_origins or _raw_cors_origins == "*":
    CORS_ALLOW_ORIGINS: list[str] = ["*"]
else:
    CORS_ALLOW_ORIGINS = [
        origin.strip() for origin in _raw_cors_origins.split(",") if origin.strip()
    ] or ["*"]

DEFAULT_KEY_SYMBOLS: dict[str, str] = {
    "btc": os.getenv("BTC_SYMBOL", "BTCUSD"),
    "gold": os.getenv("GOLD_SYMBOL", "XAUTUSD"),
    "silver": os.getenv("SILVER_SYMBOL", "SILVONUSD"),
}

_raw_map = os.getenv("DELTA_SYMBOLS_MAP", "").strip()
if _raw_map:
    try:
        parsed_map = json.loads(_raw_map)
        if isinstance(parsed_map, dict) and parsed_map:
            KEY_TO_SYMBOL: dict[str, str] = {
                str(k).strip().lower(): str(v).strip().upper()
                for k, v in parsed_map.items()
                if str(k).strip() and str(v).strip()
            }
        else:
            KEY_TO_SYMBOL = DEFAULT_KEY_SYMBOLS.copy()
    except json.JSONDecodeError:
        KEY_TO_SYMBOL = DEFAULT_KEY_SYMBOLS.copy()
else:
    KEY_TO_SYMBOL = DEFAULT_KEY_SYMBOLS.copy()

SHEET_NAMES: dict[str, str] = {
    "btc": "BTC",
    "gold": "Gold",
    "silver": "Silver",
}

latest_data_by_key: dict[str, dict[str, Any]] = {}
data_lock = Lock()
excel_lock = Lock()


def verify_pwd(pwd: str) -> None:
    """Allow API response only when pwd equals API_PWD."""
    if pwd != API_PWD:
        raise HTTPException(status_code=401, detail="Unauthorized: invalid or missing pwd.")


def resolve_keyname(keyname: str | None) -> tuple[str, str]:
    """Normalize a requested keyname or symbol into (canonical_key, delta_symbol)."""
    if not keyname or not keyname.strip():
        raise HTTPException(
            status_code=400,
            detail=f"Query parameter 'keyname' is required. Supported keys: {sorted(KEY_TO_SYMBOL.keys())}",
        )

    cleaned = keyname.strip().lower()
    if cleaned in KEY_TO_SYMBOL:
        return cleaned, KEY_TO_SYMBOL[cleaned]

    for k, sym in KEY_TO_SYMBOL.items():
        if sym.lower() == cleaned:
            return k, sym

    raise HTTPException(
        status_code=400,
        detail=f"Unknown keyname '{keyname}'. Supported keys: {sorted(KEY_TO_SYMBOL.keys())}",
    )


def get_sheet_name(key: str) -> str:
    if key in SHEET_NAMES:
        return SHEET_NAMES[key]
    safe = "".join(ch if ch.isalnum() or ch in ("_", "-") else "_" for ch in key)
    return (safe or "DataLog")[:31]


def resolution_to_minutes(resolution: str) -> int:
    res = resolution.strip().lower()
    if res.endswith("m"):
        return max(1, int(res[:-1]))
    if res.endswith("h"):
        return max(60, int(res[:-1]) * 60)
    if res.endswith("d"):
        return max(1440, int(res[:-1]) * 1440)
    return 5


def lookback_hours_for_resolution(resolution: str, min_candles: int = 80) -> int:
    """Ensure enough historical window to compute EMA(50), MACD(12,26,9), RSI(14), and ATR(14)."""
    tf_minutes = resolution_to_minutes(resolution)
    needed_hours = int(((min_candles * tf_minutes) / 60.0) * 1.35) + 2
    return max(BASE_LOOKBACK_HOURS, needed_hours)


# --- Delta Exchange India API Helpers ---
def _sign_request(method: str, path: str, query_string: str = "", payload: str = "") -> dict[str, str]:
    if not DELTA_API_KEY or not DELTA_API_SECRET:
        raise RuntimeError("DELTA_API_KEY and DELTA_API_SECRET must be set for private endpoints.")

    timestamp = str(int(time.time()))
    signature_data = f"{method.upper()}{timestamp}{path}{query_string}{payload}"
    signature = hmac.new(
        DELTA_API_SECRET.encode("utf-8"),
        signature_data.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()

    return {
        "api-key": DELTA_API_KEY,
        "timestamp": timestamp,
        "signature": signature,
        "Content-Type": "application/json",
        "Accept": "application/json",
    }


def delta_private_get(path: str, params: dict[str, Any] | None = None) -> Any:
    query_string = ""
    if params:
        sorted_items = sorted((k, str(v)) for k, v in params.items() if v is not None)
        query_string = "?" + "&".join(f"{k}={v}" for k, v in sorted_items)

    headers = _sign_request("GET", path, query_string=query_string, payload="")
    url = f"{DELTA_BASE_URL}{path}"
    response = requests.get(url, headers=headers, params=params, timeout=15)
    response.raise_for_status()
    data = response.json()
    if isinstance(data, dict) and data.get("success") is False:
        raise RuntimeError(f"API error: {data.get('error', data)}")
    return data.get("result", data)


def fetch_candles(symbol: str, resolution: str = ENTRY_RESOLUTION) -> list[dict[str, float]]:
    now_utc = datetime.now(timezone.utc)
    lookback_hours = lookback_hours_for_resolution(resolution)
    end_ts = int(now_utc.timestamp())
    start_ts = int((now_utc - timedelta(hours=lookback_hours)).timestamp())

    url = f"{DELTA_BASE_URL}/v2/history/candles"
    params = {
        "symbol": symbol,
        "resolution": resolution,
        "start": start_ts,
        "end": end_ts,
    }
    response = requests.get(url, params=params, timeout=15)
    response.raise_for_status()
    body = response.json()
    raw_candles = body.get("result", []) if isinstance(body, dict) else []

    normalized: list[dict[str, float]] = []
    for c in raw_candles:
        normalized.append(
            {
                "time": float(int(c["time"])),
                "open": float(c["open"]),
                "high": float(c["high"]),
                "low": float(c["low"]),
                "close": float(c["close"]),
                "volume": float(c.get("volume", 0.0)),
            }
        )

    normalized.sort(key=lambda item: item["time"])
    return normalized


def get_ticker(symbol: str) -> dict[str, Any]:
    url = f"{DELTA_BASE_URL}/v2/tickers/{symbol}"
    response = requests.get(url, timeout=10)
    response.raise_for_status()
    body = response.json()
    return body.get("result", body) if isinstance(body, dict) else {}


def get_balances() -> Any:
    return delta_private_get("/v2/wallet/balances")


def get_positions() -> Any:
    return delta_private_get("/v2/positions/margined")


# --- Technical Indicators ---
def ema(values: list[float], period: int) -> list[float | None]:
    n = len(values)
    if period <= 0 or n < period:
        return [None] * n

    result: list[float | None] = [None] * n
    seed_sma = sum(values[:period]) / period
    result[period - 1] = seed_sma
    multiplier = 2.0 / (period + 1)

    prev = seed_sma
    for i in range(period, n):
        val = (values[i] - prev) * multiplier + prev
        result[i] = val
        prev = val

    return result


def rsi(closes: list[float], period: int = 14) -> float | None:
    if len(closes) <= period:
        return None

    gains: list[float] = []
    losses: list[float] = []
    for i in range(1, len(closes)):
        delta = closes[i] - closes[i - 1]
        gains.append(max(delta, 0.0))
        losses.append(max(-delta, 0.0))

    avg_gain = sum(gains[:period]) / period
    avg_loss = sum(losses[:period]) / period

    for i in range(period, len(gains)):
        avg_gain = (avg_gain * (period - 1) + gains[i]) / period
        avg_loss = (avg_loss * (period - 1) + losses[i]) / period

    if avg_loss == 0:
        return 100.0 if avg_gain > 0 else 50.0

    rs = avg_gain / avg_loss
    return 100.0 - (100.0 / (1.0 + rs))


def atr(candles: list[dict[str, float]], period: int = 14) -> float | None:
    if len(candles) <= period:
        return None

    true_ranges: list[float] = []
    for i in range(1, len(candles)):
        high = candles[i]["high"]
        low = candles[i]["low"]
        prev_close = candles[i - 1]["close"]
        tr = max(high - low, abs(high - prev_close), abs(low - prev_close))
        true_ranges.append(tr)

    current_atr = sum(true_ranges[:period]) / period
    for i in range(period, len(true_ranges)):
        current_atr = (current_atr * (period - 1) + true_ranges[i]) / period

    return current_atr


def vwap(candles: list[dict[str, float]]) -> float | None:
    cum_pv = 0.0
    cum_vol = 0.0
    for c in candles:
        typical_price = (c["high"] + c["low"] + c["close"]) / 3.0
        vol = c["volume"]
        cum_pv += typical_price * vol
        cum_vol += vol

    if cum_vol <= 0:
        return None
    return cum_pv / cum_vol


# --- Single-Timeframe Indicator & Score Analysis ---
def analyze_timeframe(candles: list[dict[str, float]], timeframe: str) -> dict[str, Any]:
    closed_candles = candles[:-1] if len(candles) > 61 else candles
    if len(closed_candles) < 60:
        return {
            "timeframe": timeframe,
            "status": "insufficient_data",
            "bias": "NEUTRAL",
            "buy_score": 0,
            "sell_score": 0,
            "detail": f"Need at least 60 closed candles on {timeframe}, got {len(closed_candles)}.",
        }

    closes = [c["close"] for c in closed_candles]
    volumes = [c["volume"] for c in closed_candles]
    price = closes[-1]
    prev_close = closes[-2]

    ema9 = ema(closes, 9)[-1]
    ema21 = ema(closes, 21)[-1]
    ema50 = ema(closes, 50)[-1]

    current_rsi = rsi(closes, 14)
    current_atr = atr(closed_candles, 14)

    fast_series = ema(closes, 12)
    slow_series = ema(closes, 26)

    macd_series: list[float | None] = [None] * len(closes)
    for i in range(len(closes)):
        if fast_series[i] is not None and slow_series[i] is not None:
            macd_series[i] = fast_series[i] - slow_series[i]

    first_valid_idx = next((i for i, v in enumerate(macd_series) if v is not None), None)
    signal_series: list[float | None] = [None] * len(closes)
    if first_valid_idx is not None:
        valid_vals = [v for v in macd_series[first_valid_idx:] if v is not None]
        computed_sig = ema(valid_vals, 9)
        for offset, sig_val in enumerate(computed_sig):
            signal_series[first_valid_idx + offset] = sig_val

    macd = macd_series[-1]
    prev_macd = macd_series[-2]
    macd_signal = signal_series[-1]
    prev_macd_signal = signal_series[-2]

    current_vwap = vwap(closed_candles)
    avg_volume = sum(volumes[-20:]) / 20.0
    current_volume = volumes[-1]

    if (
        None in (ema9, ema21, ema50, current_rsi, current_atr, macd, prev_macd, macd_signal, current_vwap)
        or current_atr <= 0
        or current_vwap <= 0
    ):
        return {
            "timeframe": timeframe,
            "status": "invalid_indicators",
            "bias": "NEUTRAL",
            "buy_score": 0,
            "sell_score": 0,
            "candle_time": int(closed_candles[-1]["time"]),
            "current_price": price,
            "detail": "Insufficient or invalid indicator data.",
        }

    full_bull_stack = ema9 > ema21 > ema50
    full_bear_stack = ema9 < ema21 < ema50
    soft_bull_trend = ema21 > ema50 and price >= ema21
    soft_bear_trend = ema21 < ema50 and price <= ema21

    buy_score = 0
    sell_score = 0
    buy_reasons: list[str] = []
    sell_reasons: list[str] = []

    if full_bull_stack:
        buy_score += 2
        buy_reasons.append("EMA 9>21>50")
    elif soft_bull_trend:
        buy_score += 1
        buy_reasons.append("EMA 21>50")

    if full_bear_stack:
        sell_score += 2
        sell_reasons.append("EMA 9<21<50")
    elif soft_bear_trend:
        sell_score += 1
        sell_reasons.append("EMA 21<50")

    if price > current_vwap:
        buy_score += 1
        buy_reasons.append("above VWAP")
    elif price < current_vwap:
        sell_score += 1
        sell_reasons.append("below VWAP")

    if 52 <= current_rsi <= 74:
        buy_score += 1
        buy_reasons.append(f"RSI {current_rsi:.1f}")
    elif 26 <= current_rsi <= 48:
        sell_score += 1
        sell_reasons.append(f"RSI {current_rsi:.1f}")

    macd_bull_cross = (
        prev_macd_signal is not None
        and prev_macd <= prev_macd_signal
        and macd > macd_signal
    )
    macd_bear_cross = (
        prev_macd_signal is not None
        and prev_macd >= prev_macd_signal
        and macd < macd_signal
    )

    if macd > macd_signal or macd_bull_cross:
        buy_score += 1
        buy_reasons.append("MACD cross up" if macd_bull_cross else "MACD > signal")
    if macd < macd_signal or macd_bear_cross:
        sell_score += 1
        sell_reasons.append("MACD cross down" if macd_bear_cross else "MACD < signal")

    vol_ok = avg_volume <= 0 or current_volume >= avg_volume * 0.85
    if vol_ok and price > prev_close:
        buy_score += 1
        buy_reasons.append("volume & green candle")
    if vol_ok and price < prev_close:
        sell_score += 1
        sell_reasons.append("volume & red candle")

    if buy_score >= MIN_SIGNAL_SCORE and buy_score > sell_score:
        bias = "BULLISH"
        detail = f"[{timeframe}] BUY {buy_score}/6: " + ", ".join(buy_reasons)
    elif sell_score >= MIN_SIGNAL_SCORE and sell_score > buy_score:
        bias = "BEARISH"
        detail = f"[{timeframe}] SELL {sell_score}/6: " + ", ".join(sell_reasons)
    elif buy_score > sell_score and buy_score >= 3:
        bias = "BULLISH"
        detail = f"[{timeframe}] Lean BULL {buy_score}/6: " + ", ".join(buy_reasons)
    elif sell_score > buy_score and sell_score >= 3:
        bias = "BEARISH"
        detail = f"[{timeframe}] Lean BEAR {sell_score}/6: " + ", ".join(sell_reasons)
    else:
        bias = "NEUTRAL"
        detail = f"[{timeframe}] Neutral (BUY {buy_score}/6, SELL {sell_score}/6)"

    return {
        "timeframe": timeframe,
        "status": "ok",
        "bias": bias,
        "buy_score": buy_score,
        "sell_score": sell_score,
        "candle_time": int(closed_candles[-1]["time"]),
        "current_price": price,
        "rsi": current_rsi,
        "atr": current_atr,
        "vwap": current_vwap,
        "ema9": ema9,
        "ema21": ema21,
        "ema50": ema50,
        "macd": macd,
        "macd_signal": macd_signal,
        "volume": current_volume,
        "average_volume": avg_volume,
        "detail": detail,
    }


# --- Multi-Timeframe Signal Synthesis ---
def analyze_mtf(
    candles_by_tf: dict[str, list[dict[str, float]]],
    key: str,
    symbol: str,
) -> dict[str, Any] | None:
    entry_candles = candles_by_tf.get(ENTRY_RESOLUTION)
    if not entry_candles or len(entry_candles) < 60:
        return None

    tf_reports: dict[str, dict[str, Any]] = {}
    for tf in MTF_RESOLUTIONS:
        if tf in candles_by_tf:
            tf_reports[tf] = analyze_timeframe(candles_by_tf[tf], tf)

    entry_report = tf_reports.get(ENTRY_RESOLUTION)
    if not entry_report or entry_report.get("status") != "ok":
        return None

    htf_list = [tf for tf in MTF_RESOLUTIONS if tf != ENTRY_RESOLUTION and tf in tf_reports]
    htf_bulls = [tf for tf in htf_list if tf_reports[tf].get("bias") == "BULLISH"]
    htf_bears = [tf for tf in htf_list if tf_reports[tf].get("bias") == "BEARISH"]

    entry_buy_ok = (
        entry_report["buy_score"] >= MIN_SIGNAL_SCORE
        and entry_report["buy_score"] > entry_report["sell_score"]
    )
    entry_sell_ok = (
        entry_report["sell_score"] >= MIN_SIGNAL_SCORE
        and entry_report["sell_score"] > entry_report["buy_score"]
    )

    htf_bull_aligned = len(htf_list) == 0 or (len(htf_bulls) == len(htf_list))
    htf_bear_aligned = len(htf_list) == 0 or (len(htf_bears) == len(htf_list))

    htf_summary_parts = [
        f"{tf}:{tf_reports[tf]['bias']}({tf_reports[tf]['buy_score']}B/{tf_reports[tf]['sell_score']}S)"
        for tf in MTF_RESOLUTIONS
        if tf in tf_reports
    ]
    htf_summary = " | ".join(htf_summary_parts)

    if entry_buy_ok and (htf_bull_aligned or not REQUIRE_HTF_ALIGNMENT):
        direction = "BUY"
        mtf_aligned = htf_bull_aligned
        detail = (
            f"MTF BUY [{ENTRY_RESOLUTION} {entry_report['buy_score']}/6; "
            f"aligned={mtf_aligned}; {htf_summary}]"
        )
    elif entry_sell_ok and (htf_bear_aligned or not REQUIRE_HTF_ALIGNMENT):
        direction = "SELL"
        mtf_aligned = htf_bear_aligned
        detail = (
            f"MTF SELL [{ENTRY_RESOLUTION} {entry_report['sell_score']}/6; "
            f"aligned={mtf_aligned}; {htf_summary}]"
        )
    else:
        direction = "NO TRADE"
        mtf_aligned = False
        detail = (
            f"No MTF setup [{ENTRY_RESOLUTION} B:{entry_report['buy_score']}/6 S:{entry_report['sell_score']}/6; "
            f"require_htf={REQUIRE_HTF_ALIGNMENT}; {htf_summary}]"
        )

    price = float(entry_report["current_price"])
    current_atr = entry_report.get("atr")

    entry: float | None = None
    stop: float | None = None
    risk: float | None = None
    targets: list[float | None] = [None, None, None]

    if direction in ("BUY", "SELL") and current_atr is not None:
        entry = round(price, 6)
        risk = round(float(current_atr) * ATR_MULTIPLIER, 6)
        if direction == "BUY":
            stop = round(price - risk, 6)
            targets = [
                round(price + risk * 1.0, 6),
                round(price + risk * 2.0, 6),
                round(price + risk * 3.0, 6),
            ]
        else:
            stop = round(price + risk, 6)
            targets = [
                round(price - risk * 1.0, 6),
                round(price - risk * 2.0, 6),
                round(price - risk * 3.0, 6),
            ]

    compact_timeframes = {
        tf: {
            "bias": rep.get("bias"),
            "buy_score": rep.get("buy_score"),
            "sell_score": rep.get("sell_score"),
            "candle_time": rep.get("candle_time"),
            "rsi": rep.get("rsi"),
            "vwap": rep.get("vwap"),
            "macd": rep.get("macd"),
            "macd_signal": rep.get("macd_signal"),
        }
        for tf, rep in tf_reports.items()
    }

    return {
        "keyname": key,
        "timeframe": ENTRY_RESOLUTION,
        "timeframes": MTF_RESOLUTIONS,
        "mtf_aligned": mtf_aligned,
        "mtf_summary": htf_summary,
        "datetime": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "candle_time": int(entry_report["candle_time"]),
        "symbol": symbol,
        "current_price": price,
        "trade": direction,
        "entry": entry,
        "stop_loss": stop,
        "risk_per_unit": risk,
        "target_1": targets[0],
        "target_2": targets[1],
        "target_3": targets[2],
        "rsi": entry_report.get("rsi"),
        "atr": current_atr,
        "vwap": entry_report.get("vwap"),
        "ema9": entry_report.get("ema9"),
        "ema21": entry_report.get("ema21"),
        "ema50": entry_report.get("ema50"),
        "macd": entry_report.get("macd"),
        "macd_signal": entry_report.get("macd_signal"),
        "volume": entry_report.get("volume"),
        "average_volume": entry_report.get("average_volume"),
        "per_timeframe": compact_timeframes,
        "detail": detail,
    }


# --- Excel Persistence ---
def save_to_excel(key: str, data: dict[str, Any]) -> None:
    sheet_name = get_sheet_name(key)
    columns = [
        "keyname",
        "timeframe",
        "mtf_aligned",
        "mtf_summary",
        "datetime",
        "candle_time",
        "symbol",
        "current_price",
        "trade",
        "entry",
        "stop_loss",
        "risk_per_unit",
        "target_1",
        "target_2",
        "target_3",
        "rsi",
        "atr",
        "vwap",
        "ema9",
        "ema21",
        "ema50",
        "macd",
        "macd_signal",
        "volume",
        "average_volume",
        "detail",
    ]

    flat_row = {col: data.get(col) for col in columns}
    row = pd.DataFrame([flat_row], columns=columns)

    with excel_lock:
        if os.path.exists(EXCEL_FILE):
            with pd.ExcelWriter(
                EXCEL_FILE,
                engine="openpyxl",
                mode="a",
                if_sheet_exists="overlay",
            ) as writer:
                startrow = writer.sheets[sheet_name].max_row if sheet_name in writer.sheets else 0
                row.to_excel(
                    writer,
                    sheet_name=sheet_name,
                    index=False,
                    header=(startrow == 0),
                    startrow=startrow,
                )
        else:
            row.to_excel(
                EXCEL_FILE,
                sheet_name=sheet_name,
                index=False,
            )


# --- Background Monitor ---
def market_monitor() -> None:
    last_signature_by_key: dict[str, tuple[tuple[str, int], ...]] = {}

    while True:
        for key, symbol in KEY_TO_SYMBOL.items():
            try:
                candles_by_tf: dict[str, list[dict[str, float]]] = {}
                candle_sig_items: list[tuple[str, int]] = []

                for tf in MTF_RESOLUTIONS:
                    candles = fetch_candles(symbol=symbol, resolution=tf)
                    if len(candles) >= 60:
                        candles_by_tf[tf] = candles
                        closed_candles = candles[:-1] if len(candles) > 61 else candles
                        candle_sig_items.append((tf, int(closed_candles[-1]["time"])))

                if ENTRY_RESOLUTION not in candles_by_tf:
                    continue

                signature = tuple(candle_sig_items)
                if last_signature_by_key.get(key) == signature:
                    continue

                signal = analyze_mtf(candles_by_tf, key=key, symbol=symbol)
                if signal:
                    last_signature_by_key[key] = signature
                    with data_lock:
                        latest_data_by_key[key] = signal.copy()

                    save_to_excel(key, signal)
                    print(
                        f"[{key.upper()} | MTF={','.join(MTF_RESOLUTIONS)}]",
                        signal["datetime"],
                        signal["symbol"],
                        f"Price={signal['current_price']}",
                        f"Trade={signal['trade']}",
                        f"Aligned={signal['mtf_aligned']}",
                        f"SL={signal['stop_loss']}",
                        f"T1={signal['target_1']}",
                        f"T2={signal['target_2']}",
                        f"T3={signal['target_3']}",
                        signal["detail"],
                    )
            except Exception as error:
                print(f"Monitor error [{key}:{symbol}]:", error)

        time.sleep(POLL_SECONDS)


@asynccontextmanager
async def lifespan(_: FastAPI):
    thread = Thread(target=market_monitor, daemon=True)
    thread.start()
    yield


app = FastAPI(
    title="Indic Multi-Key API",
    version="2.3",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=CORS_ALLOW_ORIGINS,
    allow_credentials=False,
    allow_methods=["GET", "OPTIONS"],
    allow_headers=["*"],
    expose_headers=["*"],
)


def _cors_headers_for_request(request: Request) -> dict[str, str]:
    origin = request.headers.get("origin")
    headers: dict[str, str] = {
        "Access-Control-Allow-Methods": "GET, OPTIONS",
        "Access-Control-Allow-Headers": "*",
    }
    if CORS_ALLOW_ORIGINS == ["*"]:
        headers["Access-Control-Allow-Origin"] = "*"
    elif origin and origin in CORS_ALLOW_ORIGINS:
        headers["Access-Control-Allow-Origin"] = origin
        headers["Vary"] = "Origin"
    return headers


@app.exception_handler(Exception)
async def unhandled_exception_handler(request: Request, exc: Exception) -> JSONResponse:
    if isinstance(exc, HTTPException):
        return JSONResponse(
            status_code=exc.status_code,
            content={"detail": exc.detail},
            headers=_cors_headers_for_request(request),
        )
    return JSONResponse(
        status_code=500,
        content={"detail": f"Internal server error: {exc}"},
        headers=_cors_headers_for_request(request),
    )


@app.options("/")
@app.options("/current")
@app.options("/checkdata")
@app.options("/details")
def preflight_options(request: Request) -> Response:
    return Response(status_code=204, headers=_cors_headers_for_request(request))


# --- Endpoints ---
@app.get("/")
def home(pwd: str = Query(...), keyname: str | None = Query(default=None)) -> dict[str, Any]:
    verify_pwd(pwd)
    if keyname is not None:
        key, symbol = resolve_keyname(keyname)
        return {
            "status": "running",
            "exchange": "Test Data",
            "entry_timeframe": ENTRY_RESOLUTION,
            "timeframes": MTF_RESOLUTIONS,
            "keyname": key,
            "symbol": symbol,
            "message": "API is active",
        }

    return {
        "status": "running",
        "exchange": "Test Data",
        "entry_timeframe": ENTRY_RESOLUTION,
        "timeframes": MTF_RESOLUTIONS,
        "keys": KEY_TO_SYMBOL,
        "message": "API is active. Pass ?keyname=btc|gold|silver&pwd=a1a1a1 to endpoints.",
    }


@app.get("/current")
def currentdata(pwd: str = Query(...), keyname: str = Query(...)) -> dict[str, Any]:
    verify_pwd(pwd)
    key, symbol = resolve_keyname(keyname)
    try:
        ticker = get_ticker(symbol)
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"Failed to fetch ticker for '{key}' ({symbol}): {exc}") from exc

    return {
        "keyname": key,
        "symbol": symbol,
        "current_price": ticker.get("close", ticker.get("mark_price")),
    }


@app.get("/checkdata")
def get_checkdata(pwd: str = Query(...), keyname: str = Query(...)) -> dict[str, Any]:
    verify_pwd(pwd)
    key, symbol = resolve_keyname(keyname)
    with data_lock:
        item = latest_data_by_key.get(key)
        if not item:
            return {
                "status": "waiting",
                "keyname": key,
                "symbol": symbol,
                "entry_timeframe": ENTRY_RESOLUTION,
                "timeframes": MTF_RESOLUTIONS,
                "message": f"MTF analysis not available yet for '{key}' ({symbol})",
            }
        return item.copy()


@app.get("/details")
def details(pwd: str = Query(...), keyname: str = Query(...), limit: int = 50) -> dict[str, Any]:
    verify_pwd(pwd)
    key, symbol = resolve_keyname(keyname)
    sheet_name = get_sheet_name(key)

    if not os.path.exists(EXCEL_FILE):
        return {"keyname": key, "symbol": symbol, "records": []}

    with excel_lock:
        try:
            df = pd.read_excel(EXCEL_FILE, sheet_name=sheet_name)
        except ValueError:
            return {"keyname": key, "symbol": symbol, "records": []}

    bounded_limit = max(1, min(limit, 1000))
    return {
        "keyname": key,
        "symbol": symbol,
        "records": df.tail(bounded_limit).to_dict(orient="records"),
    }