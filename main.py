import hashlib
import hmac
import os
import secrets
import time
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
from threading import Lock, Thread
from typing import Any

import pandas as pd
import requests
from fastapi import Depends, FastAPI, HTTPException, Request, status
from fastapi.security import APIKeyHeader

# --- Configuration ---
SYMBOL = os.getenv("DELTA_SYMBOL", "BTCUSD")
RESOLUTION = os.getenv("DELTA_RESOLUTION", "5m")
POLL_SECONDS = int(os.getenv("POLL_SECONDS", "15"))
ATR_MULTIPLIER = float(os.getenv("ATR_MULTIPLIER", "1.5"))
CANDLE_LOOKBACK_HOURS = int(os.getenv("CANDLE_LOOKBACK_HOURS", "12"))

DELTA_BASE_URL = os.getenv("DELTA_BASE_URL", "https://api.india.delta.exchange").rstrip("/")
DELTA_API_KEY = os.getenv("DELTA_API_KEY", "")
DELTA_API_SECRET = os.getenv("DELTA_API_SECRET", "")

# Static password for protecting FastAPI endpoints
API_PASSWORD = os.getenv("API_PASSWORD", "313131")

EXCEL_FILE = "BTC_Trading_Log.xlsx"
EXCEL_SHEET = "Trading_Log"

latest_data: dict[str, Any] = {}
data_lock = Lock()
excel_lock = Lock()

api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)


# --- Endpoint Password Dependency ---
def verify_static_password(
    request: Request,
    x_api_key: str | None = Depends(api_key_header),
) -> None:
    if not API_PASSWORD:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Server authentication password is not configured.",
        )

    provided_password = x_api_key
    if not provided_password:
        auth_header = request.headers.get("Authorization", "")
        if auth_header.lower().startswith("bearer "):
            provided_password = auth_header[7:].strip()

    if not provided_password or not secrets.compare_digest(provided_password, API_PASSWORD):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or missing API password.",
            headers={"WWW-Authenticate": "APIKey"},
        )


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
        raise RuntimeError(f"Delta API error: {data.get('error', data)}")
    return data.get("result", data)


def fetch_candles(symbol: str = SYMBOL, resolution: str = RESOLUTION) -> list[dict[str, float]]:
    end_ts = int(datetime.utcnow().timestamp())
    start_ts = int((datetime.utcnow() - timedelta(hours=CANDLE_LOOKBACK_HOURS)).timestamp())

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
                "time": int(c["time"]),
                "open": float(c["open"]),
                "high": float(c["high"]),
                "low": float(c["low"]),
                "close": float(c["close"]),
                "volume": float(c.get("volume", 0.0)),
            }
        )

    normalized.sort(key=lambda item: item["time"])
    return normalized


def get_ticker(symbol: str = SYMBOL) -> dict[str, Any]:
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


# --- Signal Analysis ---
def analyze(candles: list[dict[str, float]]) -> dict[str, Any] | None:
    if len(candles) < 60:
        return None

    closes = [c["close"] for c in candles]
    volumes = [c["volume"] for c in candles]
    price = closes[-1]

    ema9_series = ema(closes, 9)
    ema21_series = ema(closes, 21)
    ema50_series = ema(closes, 50)

    ema9 = ema9_series[-1]
    ema21 = ema21_series[-1]
    ema50 = ema50_series[-1]

    current_rsi = rsi(closes, 14)
    current_atr = atr(candles, 14)

    fast_series = ema(closes, 12)
    slow_series = ema(closes, 26)

    macd_series: list[float | None] = [None] * len(closes)
    for i in range(len(closes)):
        if fast_series[i] is not None and slow_series[i] is not None:
            macd_series[i] = fast_series[i] - slow_series[i]

    valid_macd_indices = [idx for idx, val in enumerate(macd_series) if val is not None]
    valid_macd_values = [macd_series[idx] for idx in valid_macd_indices]
    signal_subseries = ema(valid_macd_values, 9) if len(valid_macd_values) >= 9 else []

    macd = macd_series[-1]
    macd_signal = signal_subseries[-1] if signal_subseries else None

    current_vwap = vwap(candles)

    avg_volume = sum(volumes[-20:]) / 20.0
    current_volume = volumes[-1]

    if (
        None in (ema9, ema21, ema50, current_rsi, current_atr, macd, macd_signal, current_vwap)
        or current_atr <= 0
        or current_vwap <= 0
    ):
        direction = "NO TRADE"
        detail = "Insufficient or invalid indicator data (ATR/VWAP/EMA/MACD/RSI)."
    else:
        bullish_ema = ema9 > ema21 > ema50
        bearish_ema = ema9 < ema21 < ema50

        bullish_rsi = 50 < current_rsi < 70
        bearish_rsi = 30 < current_rsi < 50

        bullish_macd = macd > macd_signal
        bearish_macd = macd < macd_signal

        bullish_vwap = price > current_vwap
        bearish_vwap = price < current_vwap

        volume_confirmed = current_volume > avg_volume

        buy_conditions = [
            bullish_ema,
            bullish_rsi,
            bullish_macd,
            bullish_vwap,
            volume_confirmed,
        ]
        sell_conditions = [
            bearish_ema,
            bearish_rsi,
            bearish_macd,
            bearish_vwap,
            volume_confirmed,
        ]

        if all(buy_conditions):
            direction = "BUY"
            detail = (
                "Bullish EMA alignment, RSI bullish, "
                "MACD bullish, price above VWAP "
                "and volume confirmed."
            )
        elif all(sell_conditions):
            direction = "SELL"
            detail = (
                "Bearish EMA alignment, RSI bearish, "
                "MACD bearish, price below VWAP "
                "and volume confirmed."
            )
        else:
            direction = "NO TRADE"
            failed: list[str] = []

            if not (bullish_ema or bearish_ema):
                failed.append("EMA alignment not confirmed")
            if not (bullish_rsi or bearish_rsi):
                failed.append("RSI condition not satisfied")
            if not (bullish_macd or bearish_macd):
                failed.append("MACD confirmation missing")
            if not (bullish_vwap or bearish_vwap):
                failed.append("VWAP confirmation missing")
            if not volume_confirmed:
                failed.append("Volume below average")

            detail = "; ".join(failed)

    entry = price
    stop = None
    targets: list[float | None] = [None, None, None]

    if direction in ("BUY", "SELL") and current_atr is not None:
        risk = current_atr * ATR_MULTIPLIER
        if direction == "BUY":
            stop = price - risk
            targets = [price + risk, price + risk * 2, price + risk * 3]
        else:
            stop = price + risk
            targets = [price - risk, price - risk * 2, price - risk * 3]

    return {
        "datetime": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "candle_time": candles[-1]["time"],
        "symbol": SYMBOL,
        "current_price": price,
        "trade": direction,
        "entry": entry if direction != "NO TRADE" else None,
        "stop_loss": stop,
        "target_1": targets[0],
        "target_2": targets[1],
        "target_3": targets[2],
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


# --- Excel Persistence ---
def save_to_excel(data: dict[str, Any]) -> None:
    columns = [
        "datetime",
        "candle_time",
        "symbol",
        "current_price",
        "trade",
        "entry",
        "stop_loss",
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

    row = pd.DataFrame([data], columns=columns)

    with excel_lock:
        if os.path.exists(EXCEL_FILE):
            with pd.ExcelWriter(
                EXCEL_FILE,
                engine="openpyxl",
                mode="a",
                if_sheet_exists="overlay",
            ) as writer:
                startrow = writer.sheets[EXCEL_SHEET].max_row if EXCEL_SHEET in writer.sheets else 0
                row.to_excel(
                    writer,
                    sheet_name=EXCEL_SHEET,
                    index=False,
                    header=(startrow == 0),
                    startrow=startrow,
                )
        else:
            row.to_excel(
                EXCEL_FILE,
                sheet_name=EXCEL_SHEET,
                index=False,
            )


# --- Background Monitor ---
def market_monitor() -> None:
    global latest_data
    last_candle: int | None = None

    while True:
        try:
            candles = fetch_candles()
            if len(candles) < 60:
                time.sleep(POLL_SECONDS)
                continue

            candle_time = candles[-1]["time"]
            if candle_time == last_candle:
                time.sleep(POLL_SECONDS)
                continue

            last_candle = candle_time
            signal = analyze(candles)

            if signal:
                with data_lock:
                    latest_data = signal.copy()

                save_to_excel(signal)
                print(
                    signal["datetime"],
                    signal["current_price"],
                    signal["trade"],
                    signal["detail"],
                )
        except Exception as error:
            print("Monitor error:", error)

        time.sleep(POLL_SECONDS)


@asynccontextmanager
async def lifespan(_: FastAPI):
    thread = Thread(target=market_monitor, daemon=True)
    thread.start()
    yield


app = FastAPI(
    title="Test API",
    version="1.0",
    lifespan=lifespan,
    dependencies=[Depends(verify_static_password)],
)


# --- Endpoints ---
@app.get("/")
def home() -> dict[str, Any]:
    return {
        "status": "running",
        "exchange": "Delta Exchange India",
        "symbol": SYMBOL,
        "message": "BTC scalping API is active",
    }


@app.get("/current")
def market() -> dict[str, Any]:
    try:
        ticker = get_ticker()
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"Failed to fetch ticker: {exc}") from exc

    return {
        "symbol": SYMBOL,
        "current_price": ticker.get("close", ticker.get("mark_price")),
    }


@app.get("/check")
def get_signal() -> dict[str, Any]:
    with data_lock:
        if not latest_data:
            return {
                "status": "waiting",
                "message": "Market analysis not available yet",
            }
        return latest_data.copy()


@app.get("/details")
def history(limit: int = 50) -> dict[str, Any]:
    if not os.path.exists(EXCEL_FILE):
        return {"records": []}

    with excel_lock:
        df = pd.read_excel(EXCEL_FILE, sheet_name=EXCEL_SHEET)

    bounded_limit = max(1, min(limit, 1000))
    return {"records": df.tail(bounded_limit).to_dict(orient="records")}


# @app.get("/account/balance")
# def account_balance() -> Any:
#     try:
#         return get_balances()
#     except Exception as exc:
#         raise HTTPException(status_code=502, detail=f"Failed to fetch balance: {exc}") from exc


# @app.get("/account/positions")
# def account_positions() -> Any:
#     try:
#         return get_positions()
#     except Exception as exc:
#         raise HTTPException(status_code=502, detail=f"Failed to fetch positions: {exc}") from exc
