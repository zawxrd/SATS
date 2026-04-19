"""
BingX WebSocket K 線串流
支援多交易對、GZIP 解壓、自動重連、Ping/Pong 心跳、歷史資料預熱

主要差異（vs Binance）：
  - 連線後送 JSON 訂閱訊息（非 URL stream）
  - 所有 WebSocket 訊息以 GZIP 壓縮
  - 需手動回應 Pong
  - 幣種格式：BTC-USDT（非 BTCUSDT）
  - REST 週期格式：1m / 1h / 4h / 1d（短格式）
  - WebSocket 週期格式：1min / 60min / 4hour / 1day（實測驗證，與 REST 不同）
  - kline REST API 回傳 {"code":0, "data":[[open_time,o,h,l,c,vol,...], ...]} 格式
"""
from __future__ import annotations
import gzip
import io
import json
import logging
import threading
import time
import uuid
from typing import Callable, Dict, List, Optional

import requests
import websocket   # websocket-client

logger = logging.getLogger("sats.bingx")

BINGX_WS_URL  = "wss://open-api-ws.bingx.com/market"
BINGX_API_URL = "https://open-api.bingx.com"


# ══════════════════════════════════════════════════
# 格式轉換工具
# ══════════════════════════════════════════════════

# WebSocket 訂閱用的週期格式（BingX WS 使用分鐘制：1min、60min 等）
# REST API 直接用原始格式（1h、4h、1d），兩者不同
_WS_INTERVAL_MAP = {
    "1m":  "1min",
    "3m":  "3min",
    "5m":  "5min",
    "15m": "15min",
    "30m": "30min",
    "1h":  "60min",
    "2h":  "120min",
    "4h":  "240min",
    "6h":  "360min",
    "12h": "720min",
    "1d":  "1day",
    "1w":  "1week",
}


def to_bingx_symbol(symbol: str) -> str:
    """BTCUSDT → BTC-USDT"""
    symbol = symbol.upper()
    for quote in ("USDT", "USDC", "BTC", "ETH", "BNB"):
        if symbol.endswith(quote) and len(symbol) > len(quote):
            base = symbol[: -len(quote)]
            return f"{base}-{quote}"
    return symbol


def to_ws_interval(interval: str) -> str:
    """將週期格式轉換為 BingX WebSocket 訂閱格式。
    BingX WS 使用分鐘制：1h → 60min，4h → 240min，REST API 則直接用 1h。"""
    return _WS_INTERVAL_MAP.get(interval.lower(), interval)


def from_bingx_symbol(bingx_sym: str) -> str:
    """BTC-USDT → BTCUSDT"""
    return bingx_sym.replace("-", "")


# ══════════════════════════════════════════════════
# 歷史 K 棒抓取（預熱用）
# ══════════════════════════════════════════════════

def fetch_historical_klines(symbol: str, interval: str, limit: int = 300) -> Optional[list]:
    """
    從 BingX REST API 抓取歷史 K 棒，用來預熱引擎。
    BingX kline 回傳格式：直接回傳 list，每筆可能是:
      - list 格式: [open_time, open, high, low, close, volume]
      - dict 格式: {time, open, high, low, close, volume}
    """
    bx_sym = to_bingx_symbol(symbol)
    url    = f"{BINGX_API_URL}/openApi/spot/v1/market/kline"
    params = {"symbol": bx_sym, "interval": interval, "limit": limit}
    try:
        r   = requests.get(url, params=params, timeout=15)
        raw = r.json()

        # BingX 可能直接回傳 list，也可能用 {code, data} 包裝
        if isinstance(raw, dict):
            if raw.get("code") != 0:
                code = raw.get("code")
                msg  = raw.get("msg", "")
                logger.error(f"[{symbol}] BingX API 錯誤: {msg} (code={code})")
                return None if code in (100400, 100410) else []
            klines_raw = raw.get("data") or []
        elif isinstance(raw, list):
            klines_raw = raw
        else:
            logger.error(f"[{symbol}] BingX 未預期的回傳格式: {type(raw)}")
            return []

        result = []
        for k in klines_raw:
            if isinstance(k, list):
                # 陣列格式: [open_time, open, high, low, close, volume, ...]
                if len(k) < 6:
                    continue
                result.append({
                    "open_time": int(k[0]),
                    "open":      float(k[1]),
                    "high":      float(k[2]),
                    "low":       float(k[3]),
                    "close":     float(k[4]),
                    "volume":    float(k[5]),
                    "closed":    True,
                })
            elif isinstance(k, dict):
                # dict 格式
                result.append({
                    "open_time": int(k.get("time", k.get("openTime", 0))),
                    "open":      float(k["open"]),
                    "high":      float(k["high"]),
                    "low":       float(k["low"]),
                    "close":     float(k["close"]),
                    "volume":    float(k["volume"]),
                    "closed":    True,
                })
        logger.info(f"[{symbol}] BingX 已取得 {len(result)} 根歷史 K 棒（預熱用）")
        return result
    except Exception as e:
        logger.error(f"抓取歷史 K 棒失敗 [{symbol}]: {e}")
        return []


def validate_symbols(symbols: List[str], interval: str) -> tuple[List[str], List[str]]:
    """
    透過 BingX /openApi/spot/v1/common/symbols 驗證幣種。
    回傳 (valid_symbols, invalid_symbols)。
    API 失敗時視所有幣種為有效（保守處理）。
    """
    url = f"{BINGX_API_URL}/openApi/spot/v1/common/symbols"
    try:
        r = requests.get(url, timeout=15)
        data = r.json()
        if data.get("code") != 0:
            raise ValueError(f"BingX API: {data.get('msg')}")
        # 回傳的 symbols 是 BingX 格式（BTC-USDT）
        trading = {
            s["symbol"]
            for s in (data.get("data", {}).get("symbols") or [])
            if s.get("status") == 1   # 1 = TRADING
        }
        valid, invalid = [], []
        for sym in symbols:
            bx = to_bingx_symbol(sym)
            if bx in trading:
                valid.append(sym)
            else:
                invalid.append(sym)
        return valid, invalid
    except Exception as e:
        logger.warning(f"validate_symbols 無法連線 BingX，跳過驗證: {e}")
        return list(symbols), []


def fetch_top_symbols(
    top_n: int = 100,
    quote: str = "USDT",
) -> List[str]:
    """
    從 BingX 24hr Ticker API 取得成交額前 top_n 名的交易對。
    回傳格式為 BTCUSDT（統一格式，供引擎使用）。
    失敗時回傳空列表。
    """
    import time as _time
    url = f"{BINGX_API_URL}/openApi/spot/v1/ticker/24hr"
    try:
        params = {"timestamp": int(_time.time() * 1000)}
        r = requests.get(url, params=params, timeout=15)
        data = r.json()
        if data.get("code") != 0:
            logger.error(f"fetch_top_symbols BingX 錯誤: {data.get('msg')}")
            return []
        tickers = data.get("data") or []
        if isinstance(tickers, dict):
            tickers = tickers.get("tickers") or []
        # 篩選指定計價幣 + 有成交額的交易對
        quote_sfx = f"-{quote.upper()}"
        filtered = [
            t for t in tickers
            if t.get("symbol", "").endswith(quote_sfx)
            and float(t.get("quoteVolume", 0) or 0) > 0
        ]
        # 依 quoteVolume（計價幣成交額）降冪排序
        filtered.sort(key=lambda t: float(t.get("quoteVolume", 0) or 0), reverse=True)
        top = filtered[:top_n]
        # BTC-USDT → BTCUSDT
        result = [from_bingx_symbol(t["symbol"]) for t in top]
        logger.info(f"[BingX] 自動選取前 {len(result)} 個 {quote} 交易對（依成交額）")
        return result
    except Exception as e:
        logger.error(f"fetch_top_symbols 失敗: {e}")
        return []


# ══════════════════════════════════════════════════
# WebSocket 串流管理器
# ══════════════════════════════════════════════════

KlineCallback = Callable[[str, dict], None]


class BingXWSManager:
    """
    訂閱多個 BingX 交易對的 K 線 WebSocket。
    - 連線後逐一發送訂閱訊息
    - 自動解壓 GZIP
    - 回應 Ping → Pong 心跳
    - 自動重連
    - 以 open_time 變化判斷 K 棒是否已關閉
    """

    def __init__(
        self,
        symbols: List[str],
        interval: str,
        on_kline: KlineCallback,
        reconnect_delay: int = 5,
        max_reconnect: int = 10,
    ):
        self.symbols         = [s.upper() for s in symbols]
        self.interval        = interval
        self.on_kline        = on_kline
        self.reconnect_delay = reconnect_delay
        self.max_reconnect   = max_reconnect

        self._ws: Optional[websocket.WebSocketApp] = None
        self._thread: Optional[threading.Thread]   = None
        self._stop_event   = threading.Event()
        self._reconnect_count = 0
        self._running         = False

        # 每幣種的 open_time 追蹤（用來偵測 K 棒關閉）
        self._last_open_time: Dict[str, int] = {}
        # 每幣種最後一根 K 棒的暫存
        self._last_kline: Dict[str, dict] = {}

    # ── 對外介面 ──────────────────────────────────
    def start(self):
        self._stop_event.clear()
        self._running = True
        self._thread  = threading.Thread(target=self._run_loop, daemon=True)
        self._thread.start()
        logger.info(f"BingX WebSocket 管理器已啟動，交易對: {self.symbols}，週期: {self.interval}")

    def stop(self):
        self._stop_event.set()
        self._running = False
        if self._ws:
            self._ws.close()
        logger.info("BingX WebSocket 管理器已停止")

    # ── 內部 ──────────────────────────────────────
    def _run_loop(self):
        while self._running and not self._stop_event.is_set():
            logger.info(f"連線 BingX WebSocket: {BINGX_WS_URL}")
            self._ws = websocket.WebSocketApp(
                BINGX_WS_URL,
                on_open    = self._on_open,
                on_message = self._on_message,
                on_error   = self._on_error,
                on_close   = self._on_close,
            )
            self._ws.run_forever(ping_interval=0)   # 手動處理 Ping/Pong

            if self._stop_event.is_set():
                break

            self._reconnect_count += 1
            if self._reconnect_count > self.max_reconnect:
                logger.error("超過最大重連次數，停止重連")
                break

            wait = self.reconnect_delay * min(self._reconnect_count, 5)
            logger.warning(f"BingX WebSocket 斷線，{wait} 秒後重連（第 {self._reconnect_count} 次）")
            time.sleep(wait)

    def _on_open(self, ws):
        self._reconnect_count = 0
        logger.info("BingX WebSocket 已連線 ✅，發送訂閱訊息...")
        bx_itv = to_ws_interval(self.interval)   # WS 訂閱才需轉換格式
        for sym in self.symbols:
            bx_sym = to_bingx_symbol(sym)
            sub_msg = {
                "id":       str(uuid.uuid4()),
                "dataType": f"{bx_sym}@kline_{bx_itv}",
            }
            ws.send(json.dumps(sub_msg))
            logger.info(f"  訂閱: {bx_sym}@kline_{bx_itv}")
            time.sleep(0.1)   # 避免送太快

    def _on_message(self, ws, message):
        try:
            # BingX 所有訊息都是 GZIP 壓縮
            if isinstance(message, bytes):
                try:
                    raw = gzip.decompress(message).decode("utf-8")
                except Exception:
                    raw = message.decode("utf-8", errors="replace")
            else:
                raw = message

            # 心跳：Ping → Pong
            if raw == "Ping":
                ws.send("Pong")
                return

            data = json.loads(raw)

            # 訂閱確認
            if "code" in data and "dataType" not in data:
                if data.get("code") == 0:
                    logger.debug(f"訂閱確認: {data}")
                else:
                    logger.warning(f"訂閱失敗: {data}")
                return

            data_type = data.get("dataType", "")
            if "@kline_" not in data_type:
                return

            kdata = data.get("data", {})
            if not kdata:
                return

            # 解析K棒
            bx_sym = kdata.get("s", "")
            symbol = from_bingx_symbol(bx_sym)   # BTC-USDT → BTCUSDT

            open_time = int(kdata.get("t", 0))    # K棒開始時間(ms)
            kline = {
                "open_time": open_time,
                "open":      float(kdata.get("o", 0)),
                "high":      float(kdata.get("h", 0)),
                "low":       float(kdata.get("l", 0)),
                "close":     float(kdata.get("c", 0)),
                "volume":    float(kdata.get("v", 0)),
                "closed":    False,
            }

            # 判斷 K 棒是否「剛關閉」：
            # BingX 不像 Binance 有 x 欄位，改為偵測 open_time 變化
            prev_open = self._last_open_time.get(symbol, 0)
            if prev_open != 0 and prev_open != open_time:
                # open_time 換了 → 前一根 K 棒已確認關閉
                prev_kline = self._last_kline.get(symbol)
                if prev_kline:
                    closed_kline = {**prev_kline, "closed": True}
                    self.on_kline(symbol, closed_kline)

            # 更新暫存（實時推送，未關閉）
            self._last_open_time[symbol] = open_time
            self._last_kline[symbol]     = kline
            # 同時發送未關閉的即時更新
            self.on_kline(symbol, kline)

        except Exception as e:
            logger.debug(f"解析 BingX WebSocket 訊息失敗: {e}")

    def _on_error(self, ws, error):
        logger.error(f"BingX WebSocket 錯誤: {error}")

    def _on_close(self, ws, close_status_code, close_msg):
        logger.warning(f"BingX WebSocket 關閉 (code={close_status_code}, msg={close_msg})")
