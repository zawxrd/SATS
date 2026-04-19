"""
Binance WebSocket K 線串流
支援多交易對、自動重連、歷史資料預熱
"""
from __future__ import annotations
import json
import logging
import threading
import time
from typing import Callable, Dict, List, Optional

import requests
import websocket   # websocket-client

logger = logging.getLogger("sats.binance")

BINANCE_WS_BASE  = "wss://stream.binance.com:9443/stream"
BINANCE_API_BASE = "https://api.binance.com/api/v3"


# ══════════════════════════════════════════════════
# 歷史 K 棒抓取（預熱用）
# ══════════════════════════════════════════════════

def fetch_historical_klines(symbol: str, interval: str, limit: int = 300) -> list[dict]:
    """
    從 Binance REST API 抓取歷史 K 棒，用來預熱引擎。
    回傳格式：[{open, high, low, close, volume, open_time}, ...]
    幣種不存在時回傳 None（區別於網路錯誤的空列表）。
    """
    url = f"{BINANCE_API_BASE}/klines"
    params = {"symbol": symbol.upper(), "interval": interval, "limit": limit}
    try:
        r = requests.get(url, params=params, timeout=15)
        if r.status_code == 400:
            # 幣安對無效幣種回傳 400，例如 {"code":-1121,"msg":"Invalid symbol."}
            try:
                err = r.json()
            except Exception:
                err = r.text
            logger.error(f"[{symbol}] 幣種不存在或無效: {err}")
            return None   # None 表示「幣種本身有問題」
        r.raise_for_status()
        raw = r.json()
        result = []
        for k in raw:
            result.append({
                "open_time": k[0],
                "open":      float(k[1]),
                "high":      float(k[2]),
                "low":       float(k[3]),
                "close":     float(k[4]),
                "volume":    float(k[5]),
                "closed":    True,   # 歷史資料都是已確認的
            })
        logger.info(f"[{symbol}] 已取得 {len(result)} 根歷史 K 棒（預熱用）")
        return result
    except Exception as e:
        logger.error(f"抓取歷史 K 棒失敗 [{symbol}]: {e}")
        return []   # 空列表 = 網路/其他問題，保留幣種繼續試


def validate_symbols(symbols: List[str], interval: str) -> tuple[List[str], List[str]]:
    """
    透過 Binance exchangeInfo API 批次驗證幣種。
    回傳 (valid_symbols, invalid_symbols)。
    若 API 呼叫失敗則視所有幣種為有效（保守處理）。
    """
    url = f"{BINANCE_API_BASE}/exchangeInfo"
    try:
        r = requests.get(url, timeout=15)
        r.raise_for_status()
        data = r.json()
        trading_symbols = {
            s["symbol"]
            for s in data.get("symbols", [])
            if s.get("status") == "TRADING"
        }
        valid   = [s for s in symbols if s.upper() in trading_symbols]
        invalid = [s for s in symbols if s.upper() not in trading_symbols]
        return valid, invalid
    except Exception as e:
        logger.warning(f"validate_symbols 無法連線幣安，跳過驗證: {e}")
        return list(symbols), []   # 保守處理：視所有幣種為有效


def fetch_top_symbols(
    top_n: int = 100,
    quote: str = "USDT",
) -> List[str]:
    """
    從 Binance 24hr Ticker API 取得成交額前 top_n 名的交易對。
    回傳格式為 BTCUSDT。失敗時回傳空列表。
    """
    url = f"{BINANCE_API_BASE}/ticker/24hr"
    try:
        r = requests.get(url, timeout=15)
        r.raise_for_status()
        tickers = r.json()
        quote_sfx = quote.upper()
        filtered = [
            t for t in tickers
            if t.get("symbol", "").endswith(quote_sfx)
            and float(t.get("quoteVolume", 0) or 0) > 0
        ]
        filtered.sort(key=lambda t: float(t.get("quoteVolume", 0) or 0), reverse=True)
        result = [t["symbol"] for t in filtered[:top_n]]
        logger.info(f"[Binance] 自動選取前 {len(result)} 個 {quote} 交易對（依成交額）")
        return result
    except Exception as e:
        logger.error(f"fetch_top_symbols 失敗: {e}")
        return []


# ══════════════════════════════════════════════════
# WebSocket 串流管理器
# ══════════════════════════════════════════════════

KlineCallback = Callable[[str, dict], None]
"""callback(symbol, kline_data) — kline_data 含 open/high/low/close/volume/closed"""


class BinanceWSManager:
    """
    訂閱多個交易對的 K 線 WebSocket，
    收到已確認（closed）的 K 棒時呼叫 callback。
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
        self._stop_event = threading.Event()
        self._reconnect_count = 0
        self._running = False

    # ── 對外介面 ──────────────────────────────────
    def start(self):
        """啟動 WebSocket（非阻塞，在背景執行緒中執行）。"""
        self._stop_event.clear()
        self._running = True
        self._thread = threading.Thread(target=self._run_loop, daemon=True)
        self._thread.start()
        logger.info(f"WebSocket 管理器已啟動，交易對: {self.symbols}，週期: {self.interval}")

    def stop(self):
        """優雅關閉。"""
        self._stop_event.set()
        self._running = False
        if self._ws:
            self._ws.close()
        logger.info("WebSocket 管理器已停止")

    def join(self):
        """等待執行緒結束（主程式阻塞用）。"""
        if self._thread:
            self._thread.join()

    # ── 內部 ──────────────────────────────────────
    def _build_url(self) -> str:
        streams = "/".join(
            f"{s.lower()}@kline_{self.interval}" for s in self.symbols
        )
        return f"{BINANCE_WS_BASE}?streams={streams}"

    def _run_loop(self):
        while self._running and not self._stop_event.is_set():
            url = self._build_url()
            logger.info(f"連線 WebSocket: {url[:80]}...")
            self._ws = websocket.WebSocketApp(
                url,
                on_open    = self._on_open,
                on_message = self._on_message,
                on_error   = self._on_error,
                on_close   = self._on_close,
            )
            self._ws.run_forever(
                ping_interval = 20,
                ping_timeout  = 10,
            )

            if self._stop_event.is_set():
                break

            self._reconnect_count += 1
            if self._reconnect_count > self.max_reconnect:
                logger.error("超過最大重連次數，停止重連")
                break

            wait = self.reconnect_delay * min(self._reconnect_count, 5)
            logger.warning(f"WebSocket 斷線，{wait} 秒後重連（第 {self._reconnect_count} 次）")
            time.sleep(wait)

    def _on_open(self, ws):
        self._reconnect_count = 0
        logger.info("WebSocket 已連線 ✅")

    def _on_message(self, ws, message: str):
        try:
            data = json.loads(message)
            # 多 stream 格式：{"stream": "btcusdt@kline_1h", "data": {...}}
            if "stream" in data:
                payload = data["data"]
            else:
                payload = data

            if payload.get("e") != "kline":
                return

            k = payload["k"]
            symbol = k["s"]   # e.g. "BTCUSDT"
            kline = {
                "open_time": k["t"],
                "open":      float(k["o"]),
                "high":      float(k["h"]),
                "low":       float(k["l"]),
                "close":     float(k["c"]),
                "volume":    float(k["v"]),
                "closed":    bool(k["x"]),   # True = 這根 K 棒已關閉（最重要！）
            }
            self.on_kline(symbol, kline)

        except Exception as e:
            logger.debug(f"解析 WebSocket 訊息失敗: {e}")

    def _on_error(self, ws, error):
        logger.error(f"WebSocket 錯誤: {error}")

    def _on_close(self, ws, close_status_code, close_msg):
        logger.warning(f"WebSocket 關閉 (code={close_status_code}, msg={close_msg})")
