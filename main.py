"""
SATS Bot — 主程式 v2
Binance WebSocket + 多幣種 SATS 引擎 + Discord 通知
  ✅ 多幣種同時監控（單一 WebSocket 連線）
  ✅ 啟動 / 關閉時發送訊息
  ✅ 每小時狀態報告（所有幣種一覽）

使用方式：
    python main.py
    python main.py --config config/config.yaml
    python main.py --symbol BTCUSDT --symbol ETHUSDT --interval 4h
    python main.py --debug
"""
from __future__ import annotations

import argparse
import logging
import signal
import sys
import threading
import time
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional

import yaml

sys.path.insert(0, str(Path(__file__).parent))

from core.engine      import SATSEngine, SignalResult
from core.database    import SATSDatabase
from notifier.discord import (
    DiscordNotifier,
    COLOR_INFO, COLOR_WARN,
    build_signal_embed, build_open_embed, build_tp_hit_embed, build_close_embed,
)

# 交易所模組在 load_config 後動態選擇（見 _load_exchange_module）
_fetch_historical_klines = None   # type: ignore
_validate_symbols        = None   # type: ignore
_WSManagerClass          = None   # type: ignore
_fetch_top_symbols       = None   # type: ignore


def _load_exchange_module(exchange: str):
    """根據 config exchange 欄位動態載入對應模組。"""
    global _fetch_historical_klines, _validate_symbols, _WSManagerClass, _fetch_top_symbols
    ex = exchange.lower().strip()
    if ex == "bingx":
        from core.bingx_ws import (
            BingXWSManager         as _WS,
            fetch_historical_klines as _fhk,
            validate_symbols        as _vs,
            fetch_top_symbols       as _fts,
        )
        _WSManagerClass          = _WS
        _fetch_historical_klines = _fhk
        _validate_symbols        = _vs
        _fetch_top_symbols       = _fts
        logger.info("交易所模組：BingX ✅")
    else:
        if ex != "binance":
            logger.warning(f"未知交易所 '{exchange}'，使用預設 Binance")
        from core.binance_ws import (
            BinanceWSManager        as _WS,
            fetch_historical_klines as _fhk,
            validate_symbols        as _vs,
            fetch_top_symbols       as _fts,
        )
        _WSManagerClass          = _WS
        _fetch_historical_klines = _fhk
        _validate_symbols        = _vs
        _fetch_top_symbols       = _fts
        logger.info("交易所模組：Binance ✅")


# ══════════════════════════════════════════════════
# 日誌
# ══════════════════════════════════════════════════
def setup_logging(level: str, log_file: str):
    fmt = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
    # Windows cmd/PowerShell 預設 cp950，無法顯示 emoji → 強制 UTF-8
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")  # type: ignore[attr-defined]
    except AttributeError:
        pass  # Python < 3.7 或非 TextIOWrapper，略過
    handlers: list = [logging.StreamHandler(sys.stdout)]
    if log_file:
        handlers.append(logging.FileHandler(log_file, encoding="utf-8"))
    logging.basicConfig(
        level    = getattr(logging, level.upper(), logging.INFO),
        format   = fmt,
        handlers = handlers,
    )

logger = logging.getLogger("sats.main")


# ══════════════════════════════════════════════════
# 設定載入
# ══════════════════════════════════════════════════
def load_config(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


# ══════════════════════════════════════════════════
# 每幣種統計追蹤
# ══════════════════════════════════════════════════
class SymbolStats:
    """記錄每個幣種自啟動以來的訊號統計（含盈虧）"""
    def __init__(self):
        self.signals_total   = 0
        self.signals_buy     = 0
        self.signals_sell    = 0
        self.signals_skipped = 0          # 分數不足被過濾
        self.last_signal:  Optional[SignalResult] = None
        self.last_signal_time: Optional[datetime] = None
        self.last_price: float = 0.0
        self.last_tqi:   float = 0.0
        self.last_trend: str   = "—"
        # ── 盈虧追蹤 ──────────────────────────────
        self.last_entry_price: float = 0.0   # 上一筆訊號的進場價格
        self.last_entry_dir:   str   = ""    # 上一筆方向 "BUY" / "SELL"
        self.realized_pnl:     float = 0.0   # 累積已實現盈虧 %（加總）
        self.trade_count:      int   = 0     # 完成交易次數（有進有出）
        self.win_count:        int   = 0     # 獲利次數

    def record_signal(self, sig: SignalResult, sent: bool):
        """記錄訊號。"""
        if sent:
            self.signals_total += 1
            if sig.direction == "BUY":
                self.signals_buy += 1
            else:
                self.signals_sell += 1
            self.last_signal      = sig
            self.last_signal_time = datetime.now(timezone.utc)
        else:
            self.signals_skipped += 1

    @property
    def win_rate(self) -> Optional[float]:
        """勝率 0~100%；無完成交易時回傳 None。"""
        return (self.win_count / self.trade_count * 100) if self.trade_count > 0 else None

    def update_state(self, price: float, tqi: float, trend: str):
        self.last_price = price
        self.last_tqi   = tqi
        self.last_trend = trend

    def reset(self):
        """重置所有統計與盈虧數據（用於預熱後清空）"""
        self.signals_total   = 0
        self.signals_buy     = 0
        self.signals_sell    = 0
        self.signals_skipped = 0
        self.last_signal     = None
        self.last_signal_time = None
        self.last_entry_price = 0.0
        self.last_entry_dir   = ""
        self.realized_pnl     = 0.0
        self.trade_count      = 0
        self.win_count        = 0



# ══════════════════════════════════════════════════
# 每小時報告產生器
# ══════════════════════════════════════════════════
def _tqi_bar(tqi: float, width: int = 8) -> str:
    filled = round(tqi * width)
    return "█" * filled + "░" * (width - filled)

def _trend_emoji(trend: str) -> str:
    return "🟢" if trend == "Bullish" else "🔴"

def _regime_label(er: float) -> str:
    if er >= 0.50: return "📈 Trending"
    if er >= 0.25: return "〰️ Mixed"
    return "🌀 Choppy"


# ══════════════════════════════════════════════════
# 持倉總覽 Embed
# ══════════════════════════════════════════════════
def build_positions_embed(
    symbols: List[str],
    engines: Dict[str, SATSEngine],
    stats:   Dict[str, SymbolStats],
) -> dict:
    """產生目前所有活躍持倉的 Discord Embed。"""
    now = datetime.now(timezone.utc)

    # 收集有持倉的幣種
    active: List[tuple] = [
        (sym, engines[sym].position, stats[sym])
        for sym in symbols
        if engines[sym].position is not None
    ]

    if not active:
        return {
            "title":       "📋  目前持倉",
            "description": "目前無任何開倉位",
            "color":       COLOR_INFO,
            "timestamp":   now.isoformat(),
            "footer":      {"text": "SATS Bot v1.9.0"},
        }

    fields = []
    for sym, pos, stat in active:
        direction = pos["direction"]
        entry     = pos["entry"]
        current   = stat.last_price
        dir_emoji = "🟢" if direction == "BUY" else "🔴"

        # 未實現盈虧
        if entry > 0 and current > 0:
            if direction == "BUY":
                upnl = (current - entry) / entry * 100
            else:
                upnl = (entry - current) / entry * 100
            upnl_sign  = "+" if upnl >= 0 else ""
            upnl_emoji = "📈" if upnl >= 0 else "📉"
            upnl_str   = f"{upnl_emoji} `{upnl_sign}{upnl:.2f}%`"
        else:
            upnl_str = "—"

        # TP 命中狀態
        tp1_e = "✅" if pos["hit_tp1"] else "⬜"
        tp2_e = "✅" if pos["hit_tp2"] else "⬜"
        tp3_e = "✅" if pos["hit_tp3"] else "⬜"

        value_lines = [
            f"{dir_emoji} **{direction}**  |  已開 `{pos['bars_open']}` 根",
            f"進場 `{entry:.6g}`  →  現價 `{current:.6g}`",
            f"止損 `{pos['sl']:.6g}`",
            (
                f"TP1 {tp1_e} `{pos['tp1']:.6g}` ({pos['tp1r']:.1f}R)  "
                f"TP2 {tp2_e} `{pos['tp2']:.6g}` ({pos['tp2r']:.1f}R)  "
                f"TP3 {tp3_e} `{pos['tp3']:.6g}` ({pos['tp3r']:.1f}R)"
            ),
            f"未實盈虧 {upnl_str}",
        ]
        fields.append({
            "name":   f"{dir_emoji} {sym}",
            "value":  "\n".join(value_lines),
            "inline": False,
        })

    return {
        "title":       f"📋  目前持倉  ({len(active)} 筆)  —  {now.strftime('%H:%M UTC')}",
        "description": f"共 **{len(active)}** 個幣種有開倉（監控 `{len(symbols)}` 對）",
        "color":       COLOR_INFO,
        "fields":      fields[:25],   # Discord 上限 25 個 field
        "timestamp":   now.isoformat(),
        "footer":      {"text": "SATS Bot v1.9.0"},
    }


def build_hourly_report(
    symbols: List[str],
    engines: Dict[str, SATSEngine],
    stats:   Dict[str, SymbolStats],
    interval: str,
    uptime_seconds: float,
) -> dict:
    """產生每小時 Discord Embed 報告（摘要版，適用任意幣種數量）"""
    now = datetime.now(timezone.utc)

    hours      = int(uptime_seconds // 3600)
    minutes    = int((uptime_seconds % 3600) // 60)
    uptime_str = f"{hours}h {minutes}m"

    # ── 整體統計 ─────────────────────────────────
    total_signals = sum(s.signals_total   for s in stats.values())
    total_skipped = sum(s.signals_skipped for s in stats.values())
    total_pnl     = sum(s.realized_pnl   for s in stats.values())
    total_trades  = sum(s.trade_count    for s in stats.values())
    total_wins    = sum(s.win_count      for s in stats.values())
    overall_wr    = f"{total_wins/total_trades*100:.0f}%" if total_trades > 0 else "—"
    pnl_sign      = "+" if total_pnl >= 0 else ""
    pnl_emoji     = "📈" if total_pnl >= 0 else "📉"
    bullish_n     = sum(1 for s in symbols if engines[s].trend == "Bullish")
    bearish_n     = len(symbols) - bullish_n

    # ── Description：一行概覽 ────────────────────
    description = (
        f"監控 **{len(symbols)}** 對  |  🟢 {bullish_n} 看漲  🔴 {bearish_n} 看跌\n"
        f"累積盈虧 {pnl_emoji} **{pnl_sign}{total_pnl:.2f}%**（{total_trades} 筆  勝率 {overall_wr}）"
    )

    # ── Field 1：整體摘要 ─────────────────────────
    fields = [
        {
            "name": "📊 整體摘要",
            "value": (
                f"幣種 `{len(symbols)}`  |  週期 `{interval}`  |  運行 `{uptime_str}`\n"
                f"🟢 {bullish_n} 看漲  🔴 {bearish_n} 看跌\n"
                f"訊號 `{total_signals}` 已發  `{total_skipped}` 過濾\n"
                f"盈虧 {pnl_emoji} `{pnl_sign}{total_pnl:.2f}%`  |  {total_trades} 筆  |  勝率 `{overall_wr}`"
            ),
            "inline": False,
        }
    ]

    # ── Field 2：最近有訊號的幣種（最多 8 個）────
    recent = sorted(
        [(s, stats[s]) for s in symbols if stats[s].last_signal is not None],
        key=lambda x: x[1].last_signal_time or now,
        reverse=True,
    )[:8]

    if recent:
        lines = []
        for sym, st in recent:
            mins_ago = int((now - st.last_signal_time).total_seconds() / 60) if st.last_signal_time else 0
            sig_e = "🟢" if st.last_signal.direction == "BUY" else "🔴"
            te    = _trend_emoji(engines[sym].trend)
            lines.append(
                f"{sig_e} `{sym}` {te}  TQI `{engines[sym].tqi*100:.0f}%`  "
                f"Score `{st.last_signal.score:.0f}`  ({mins_ago}m ago)"
            )
        fields.append({
            "name":   "🕒 最近訊號",
            "value":  "\n".join(lines),
            "inline": False,
        })

    # ── Field 3：表現最佳 Top 5 ──────────────────
    top_performers = sorted(
        [s for s in symbols if stats[s].trade_count > 0],
        key=lambda s: stats[s].realized_pnl,
        reverse=True,
    )[:5]

    if top_performers:
        lines = []
        for sym in top_performers:
            st = stats[sym]
            p_sign = "+" if st.realized_pnl >= 0 else ""
            lines.append(
                f"`{sym}`  盈虧 `{p_sign}{st.realized_pnl:.2f}%`  "
                f"({st.trade_count} 筆, 勝率 {st.win_rate:.0f}%)"
            )
        fields.append({
            "name":   "⭐ 表現最佳",
            "value":  "\n".join(lines),
            "inline": False,
        })

    # ── Field 4：TQI 最高 Top 5 ──────────────────
    top_tqi = sorted(symbols, key=lambda s: engines[s].tqi, reverse=True)[:5]
    lines = []
    for sym in top_tqi:
        eng = engines[sym]
        te  = _trend_emoji(eng.trend)
        # 安全檢查：若 last_close 尚未定義，則顯示 N/A
        price_str = f"{eng.last_close:.6g}" if hasattr(eng, "last_close") else "N/A"
        lines.append(f"`{sym}` {te}  {_tqi_bar(eng.tqi)}  `{eng.tqi*100:.0f}%`  `{price_str}`")
    fields.append({
        "name":   "🔥 TQI Top 5",
        "value":  "\n".join(lines),
        "inline": False,
    })

    return {
        "title":       f"⏰  每小時狀態報告  —  {now.strftime('%H:%M UTC')}",
        "description": description,
        "color":       0x5C6BC0,
        "fields":      fields,
        "timestamp":   now.isoformat(),
        "footer":      {"text": f"SATS Bot v1.9.0  •  下次報告於 1 小時後"},
    }


# ══════════════════════════════════════════════════
# 每小時報告執行緒
# ══════════════════════════════════════════════════
class HourlyReporter(threading.Thread):
    def __init__(self, notifier, symbols, engines, stats, interval, start_time, interval_sec=3600):
        super().__init__(daemon=True)
        self.notifier     = notifier
        self.symbols      = symbols
        self.engines      = engines
        self.stats        = stats
        self.interval     = interval
        self.start_time   = start_time
        self.interval_sec = interval_sec
        self._stop        = threading.Event()

    def stop(self):
        self._stop.set()

    def run(self):
        # 啟動後稍微延遲，確保預熱完成且數據初始化
        logger.info("[HourlyReporter] 執行緒已啟動，等待 30 秒進行首次報告...")
        if self._stop.wait(30):
            return
        
        while not self._stop.is_set():
            logger.info("[HourlyReporter] 正在發送報告...")
            try:
                self._send_report()
            except Exception as e:
                logger.error(f"[HourlyReporter] 報告發送失敗: {e}")

            # 計算下次發送的等待時間（對齊間隔）
            now = time.time()
            wait_time = self.interval_sec - (now % self.interval_sec)
            # 確保等待時間至少有 10 秒，避免極短間隔導致的無限循環
            if wait_time < 10:
                wait_time += self.interval_sec
                
            logger.info(f"[HourlyReporter] 報告發送完成。下次將在 {wait_time/60:.1f} 分後發送。")
            
            # 執行等待，若期間收到停止訊號則跳出
            if self._stop.wait(wait_time):
                break

    def _send_report(self):
        uptime = time.time() - self.start_time
        embed  = build_hourly_report(
            self.symbols, self.engines, self.stats, self.interval, uptime
        )
        pos_embed = build_positions_embed(self.symbols, self.engines, self.stats)

        payload = {
            "username": self.notifier.username,
            "embeds":   [embed, pos_embed],   # 兩個 embed 一起發送
        }
        if self.notifier.avatar_url:
            payload["avatar_url"] = self.notifier.avatar_url
        try:
            import requests as req
            r = req.post(self.notifier.webhook_url, json=payload, timeout=10)
            if r.status_code in (200, 204):
                logger.info("[HourlyReporter] 每小時報告＋持倉已發送 ✅")
            else:
                logger.warning(f"[HourlyReporter] 發送失敗 HTTP {r.status_code}")
        except Exception as e:
            logger.error(f"[HourlyReporter] 例外: {e}")


# ══════════════════════════════════════════════════
# 啟動 / 關閉通知
# ══════════════════════════════════════════════════
def build_startup_embed(symbols: List[str], interval: str, min_score: int,
                         warmup_results: dict) -> dict:
    now = datetime.now(timezone.utc)

    # 統計牛/熊/不明
    bullish = sum(1 for s in symbols if warmup_results.get(s, {}).get("trend") == "Bullish")
    bearish = len(symbols) - bullish
    avg_tqi = (
        sum(warmup_results.get(s, {}).get("tqi", 0.0) for s in symbols) / len(symbols)
        if symbols else 0.0
    )

    return {
        "title": "🚀  SATS Bot 已啟動",
        "description": (
            f"正在監控 **{len(symbols)}** 個交易對，週期 `{interval}`\n"
            f"預熱完成：🟢 {bullish} 看漲  🔴 {bearish} 看跌  |  平均 TQI `{avg_tqi:.2f}`"
        ),
        "color": 0x00E676,
        "fields": [
            {"name": "週期",     "value": f"`{interval}`",    "inline": True},
            {"name": "最低分數", "value": f"`{min_score}`",   "inline": True},
            {"name": "幣種數",   "value": f"`{len(symbols)}`","inline": True},
        ],
        "footer": {"text": "SATS Bot v1.9.2  •  每小時將自動發送狀態報告"},
        "timestamp": now.isoformat(),
    }


def build_shutdown_embed(
    symbols:    List[str],
    stats:      Dict[str, SymbolStats],
    uptime_sec: float,
    reason:     str = "手動停止",
) -> dict:
    now = datetime.now(timezone.utc)
    hours   = int(uptime_sec // 3600)
    minutes = int((uptime_sec % 3600) // 60)

    total_buy  = sum(s.signals_buy   for s in stats.values())
    total_sell = sum(s.signals_sell  for s in stats.values())
    total_skip = sum(s.signals_skipped for s in stats.values())

    sym_lines = []
    for sym in symbols:
        s = stats[sym]
        sym_lines.append(
            f"`{sym}`  🟢{s.signals_buy} 🔴{s.signals_sell}  "
            f"（過濾 {s.signals_skipped}）"
        )

    return {
        "title": "🔴  SATS Bot 已關閉",
        "description": f"**原因：** {reason}\n\n" + "\n".join(sym_lines),
        "color": 0xFF5252,
        "fields": [
            {"name": "運行時間",   "value": f"`{hours}h {minutes}m`", "inline": True},
            {"name": "總 BUY",    "value": f"`{total_buy}`",          "inline": True},
            {"name": "總 SELL",   "value": f"`{total_sell}`",         "inline": True},
            {"name": "總過濾訊號", "value": f"`{total_skip}`",         "inline": True},
        ],
        "footer": {"text": "SATS Bot v1.9.0"},
        "timestamp": now.isoformat(),
    }


# ══════════════════════════════════════════════════
# Bot 主體
# ══════════════════════════════════════════════════
class SATSBot:
    def __init__(self, cfg: dict, symbols: List[str] | None = None, interval: str | None = None):
        self.cfg      = cfg
        self.symbols  = [s.upper() for s in (symbols or cfg["symbols"])]
        self.interval = interval or cfg["interval"]
        self.min_score = cfg["filters"]["min_score"]

        # 資料庫
        self.db = SATSDatabase()

        # 每幣種引擎
        self.engines: Dict[str, SATSEngine] = {
            sym: SATSEngine(sym, self.interval, cfg)
            for sym in self.symbols
        }

        # 每幣種統計
        self.stats: Dict[str, SymbolStats] = {
            sym: SymbolStats() for sym in self.symbols
        }

        # 追蹤每個幣種當前的 signal_id
        self.current_signal_ids: Dict[str, int] = {
            sym: 0 for sym in self.symbols
        }

        # Discord 通知器
        dc = cfg["discord"]
        self.notifier = DiscordNotifier(
            webhook_url  = dc["webhook_url"],
            username     = dc.get("username", "SATS Bot 🤖"),
            avatar_url   = dc.get("avatar_url", ""),
            mention_role = dc.get("mention_role_id", ""),
        )

        # WebSocket（所有幣種共用一條連線）
        sys_cfg = cfg["system"]
        self.ws_manager = _WSManagerClass(
            symbols         = self.symbols,
            interval        = self.interval,
            on_kline        = self._on_kline,
            reconnect_delay = sys_cfg.get("reconnect_delay", 5),
            max_reconnect   = sys_cfg.get("max_reconnect", 10),
        )

        self._start_time: float  = 0.0
        self._shutdown           = False
        self._reporter: Optional[HourlyReporter] = None
        self._shutdown_reason    = "手動停止"

    # ── 啟動 ──────────────────────────────────────
    def start(self):
        logger.info("=" * 60)
        logger.info("  SATS Bot 啟動中")
        logger.info(f"  幣種: {self.symbols}")
        logger.info(f"  週期: {self.interval}  |  最低分: {self.min_score}")
        logger.info("=" * 60)

        self._start_time = time.time()

        # ── 幣種驗證：過濾掉找不到的交易對 ──────────
        valid, invalid = _validate_symbols(self.symbols, self.interval)
        if invalid:
            logger.warning(f"以下幣種在幣安找不到，已自動移除: {invalid}")
            # 發送 Discord 警告
            warn_embed = {
                "title": "⚠️  無效幣種警告",
                "description": (
                    f"以下幣種在幣安**找不到或已下架**，已自動移除：\n"
                    + "\n".join(f"• `{s}`" for s in invalid)
                ),
                "color": COLOR_WARN,
                "footer": {"text": "請檢查 config.yaml 的 symbols 設定"},
            }
            self._send_embed(warn_embed)
            # 從各字典移除無效幣種
            for sym in invalid:
                self.engines.pop(sym, None)
                self.stats.pop(sym, None)
            self.symbols = valid
            self.ws_manager.symbols = valid  # 同步更新 WS 管理器

        if not self.symbols:
            logger.error("所有幣種均無效，Bot 無法啟動！")
            return

        # 預熱所有引擎
        warmup_results = self._warmup_all()

        # ── 預熱後重置統計數據（確保歷史數據不計入盈虧） ──
        for st in self.stats.values():
            st.reset()

        # ── 啟動通知 ──────────────────────────────
        embed = build_startup_embed(
            self.symbols, self.interval, self.min_score, warmup_results
        )
        self._send_embed(embed)

        # ── 啟動每小時報告執行緒 ──────────────────
        report_sec = self.cfg["system"].get("hourly_report_interval", 3600)
        self._reporter = HourlyReporter(
            notifier     = self.notifier,
            symbols      = self.symbols,
            engines      = self.engines,
            stats        = self.stats,
            interval     = self.interval,
            start_time   = self._start_time,
            interval_sec = report_sec,
        )
        self._reporter.start()

        # ── 啟動 WebSocket ────────────────────────
        self.ws_manager.start()

        logger.info(f"✅ 正在監控 {len(self.symbols)} 個幣種，等待訊號...")

        try:
            while not self._shutdown:
                time.sleep(1)
        except KeyboardInterrupt:
            self.shutdown("鍵盤中斷 (Ctrl+C)")

    def shutdown(self, reason: str = "手動停止"):
        if self._shutdown:
            return
        self._shutdown_reason = reason
        logger.info(f"收到關閉訊號（{reason}），正在停止...")
        self._shutdown = True

        if self._reporter:
            self._reporter.stop()
        self.ws_manager.stop()

        # ── 關閉通知 ──────────────────────────────
        uptime = time.time() - self._start_time
        embed  = build_shutdown_embed(self.symbols, self.stats, uptime, reason)
        self._send_embed(embed)
        logger.info("已關閉 ✅")

    # ── 預熱 ──────────────────────────────────────
    def _warmup_all(self) -> dict:
        limit = min(
            max(self.cfg["system"]["warmup_bars"],
                max(e._warmup for e in self.engines.values())) + 20,
            999,
        )
        results = {}
        failed_syms = []
        for sym, engine in list(self.engines.items()):
            logger.info(f"[{sym}] 預熱中（抓取 {limit} 根 K 棒）...")
            klines = _fetch_historical_klines(sym, self.interval, limit=limit)

            # None = 幣種無效（HTTP 400），預熱階段第二道防線
            if klines is None:
                logger.warning(f"[{sym}] 預熱失敗（幣種無效），已移除")
                failed_syms.append(sym)
                self.engines.pop(sym, None)
                self.stats.pop(sym, None)
                if sym in self.symbols:
                    self.symbols.remove(sym)
                continue

            for kl in klines:
                engine.update(
                    open_     = kl["open"],
                    high      = kl["high"],
                    low       = kl["low"],
                    close     = kl["close"],
                    volume    = kl["volume"],
                    is_closed = True,
                )
            logger.info(
                f"[{sym}] 預熱完成  bars={engine.bar_index}"
                f"  trend={engine.trend}  TQI={engine.tqi:.3f}"
            )
            results[sym] = {
                "trend": engine.trend,
                "tqi":   engine.tqi,
                "bars":  engine.bar_index,
            }
            # 更新初始狀態統計
            self.stats[sym].update_state(
                price = klines[-1]["close"] if klines else 0.0,
                tqi   = engine.tqi,
                trend = engine.trend,
            )

        # 同步更新 WS 管理器，排除預熱失敗的幣種
        if failed_syms:
            self.ws_manager.symbols = self.symbols

        return results

    # ── WebSocket 回調 ────────────────────────────
    def _on_kline(self, symbol: str, kline: dict):
        engine = self.engines.get(symbol)
        stat   = self.stats.get(symbol)
        if engine is None or stat is None:
            return

        # ✅ 在 update() 之前快照持倉狀態
        had_position_before = engine.position is not None
        had_position_dir    = engine.position["direction"] if had_position_before else None

        sig = engine.update(
            open_     = kline["open"],
            high      = kline["high"],
            low       = kline["low"],
            close     = kline["close"],
            volume    = kline["volume"],
            is_closed = kline["closed"],
        )

        # ── 交易事件（TP / SL / Timeout）────────────
        # 修正：不論 K 棒是否關閉，只要有事件就處理（例如即時觸發的 TP/SL）
        if engine.trade_events:
            self._handle_trade_events(symbol, engine, stat)
            engine._trade_events = []  # 處理完後手動清空，避免重複發送

        # 更新即時狀態
        stat.update_state(kline["close"], engine.tqi, engine.trend)

        closed_mark = "✅" if kline["closed"] else "🔄"
        logger.debug(
            f"{closed_mark} [{symbol}] "
            f"close={kline['close']:.4f}  "
            f"TQI={engine.tqi:.3f}  trend={engine.trend}"
        )

        if sig is None:
            return   # 無翻轉訊號

        # ── 1. 分數過濾（優先）────────────────────────
        # 分數不足直接 skip，不論持倉狀態，也不發 DC 通知
        if sig.score < self.min_score:
            reason = f"分數 {sig.score:.0f} < {self.min_score}"
            logger.info(f"[{symbol}] {sig.direction} 跳過（{reason}）")
            stat.signals_skipped += 1
            return

        # ── 2. 持倉檢查：用 update() 呼叫「之前」的狀態判斷 ──
        # ✅ 不能用 engine.position，那已是新訊號寫入後的狀態
        if had_position_before:
            # 同方向：確實是重複訊號，過濾（分數已足夠，才值得通知被過濾）
            if had_position_dir == sig.direction:
                reason = f"目前已有 {had_position_dir} 持倉中"
                logger.info(f"⚠️ [{symbol}] {sig.direction} 訊號已過濾 （{reason}）")
                stat.signals_skipped += 1
                self.notifier.send_skipped_signal(sig, reason)
                return
            # 反方向：ST 翻轉，舊倉已由 _check_hits 關閉（或將由 timeout 處理）
            # 允許繼續往下發出新訊號通知

        # ── 記錄訊號 ────
        stat.record_signal(sig, sent=True)
        pnl_field = None  # 訊號通知中暫不顯示 PnL，由關倉通知負責

        # ── 記錄訊號到資料庫 ────────────────────────────
        signal_data = {
            "symbol": sig.symbol,
            "interval": sig.interval,
            "direction": sig.direction,
            "price": sig.price,
            "sl": sig.sl,
            "tp1": sig.tp1,
            "tp2": sig.tp2,
            "tp3": sig.tp3,
            "tp1_r": sig.tp1_r,
            "tp2_r": sig.tp2_r,
            "tp3_r": sig.tp3_r,
            "score": sig.score,
            "tqi": sig.tqi,
            "er": sig.er,
            "rsi": sig.rsi,
            "vol_z": sig.vol_z,
            "preset": sig.preset,
            "tp_mode": sig.tp_mode,
            "dyn_scale": sig.dyn_scale,
            "bar_index": sig.bar_index,
        }
        signal_id = self.db.record_signal(signal_data, sent=True)
        self.current_signal_ids[symbol] = signal_id

        # ── 更新資料庫統計 ──────────────────────────────
        self.db.update_symbol_stats(
            symbol=sig.symbol,
            interval=sig.interval,
            signal_sent=True,
            direction=sig.direction,
            entry_price=sig.price,
            entry_dir=sig.direction,
        )

        # ── 發送 Discord 通知（合併 Signal 與 Open 以避免速率限制） ──
        logger.info(
            f"🔔 [{symbol}] {sig.direction}  "
            f"price={sig.price:.6g}  sl={sig.sl:.6g}  "
            f"TQI={sig.tqi:.3f}  score={sig.score:.0f}"
        )
        
        # 為了減少 Discord Webhook 調用次數並確保不漏發，合併兩個 embed
        try:
            signal_embed = build_signal_embed(sig, self.notifier.mention_role)
            if pnl_field:
                signal_embed["fields"].insert(0, pnl_field)
            
            # 判斷是否需要開倉通知 (只有當 engine 真的開倉時才發送)
            embeds = [signal_embed]
            if engine.position is not None:
                open_embed = build_open_embed(sig, self.notifier.mention_role)
                embeds.append(open_embed)
            
            from datetime import datetime, timezone
            ts = datetime.now(timezone.utc).isoformat()
            for em in embeds:
                em["timestamp"] = ts
                
            payload = {
                "username": self.notifier.username,
                "embeds": embeds
            }
            if self.notifier.avatar_url:
                payload["avatar_url"] = self.notifier.avatar_url
                
            import requests as req
            r = req.post(self.notifier.webhook_url, json=payload, timeout=10)
            if r.status_code in (200, 204):
                logger.info(f"[{symbol}] 訊號與開倉通知已發送 (共 {len(embeds)} 個 Embed) ✅")
            else:
                logger.warning(f"[{symbol}] 通知發送失敗 HTTP {r.status_code}")
        except Exception as e:
            logger.error(f"[{symbol}] 通知發送過程發生錯誤: {e}", exc_info=True)

    # ── 交易事件處理（TP 命中 / 關倉）────────────────
    def _handle_trade_events(self, symbol: str, engine: "SATSEngine", stat: "SymbolStats"):
        for evt in engine.trade_events:
            evt_type = evt["type"]
            logger.info(f"[{symbol}] 交易事件: {evt_type}")

            if evt_type in ("tp1_hit", "tp2_hit"):
                # 記錄 TP 命中事件
                signal_id = self.current_signal_ids.get(symbol, 0)
                if not signal_id:
                    signal_id = self._get_latest_signal_id(symbol)
                
                if signal_id:
                    # 1. 檢查是否已記錄過，防止重複處理
                    existing = self.db.get_tp_sl_event(signal_id, evt_type)
                    if existing:
                        logger.info(f"[{symbol}] {evt_type} 已處理過，跳過")
                        continue
                    
                    hit_tp = None
                    if evt_type == "tp1_hit":
                        hit_tp = 1
                    elif evt_type == "tp2_hit":
                        hit_tp = 2
                    
                    # 2. 寫入資料庫鎖定狀態
                    self.db.record_tp_sl_event(
                        signal_id=signal_id,
                        symbol=symbol,
                        event_type=evt_type,
                        hit_price=evt["hit_price"],
                        hit_tp=hit_tp,
                        hit_r=evt.get("hit_r"),
                    )
                    
                    # 3. 發送通知
                    ok = self.notifier.send_tp_hit(evt, symbol, self.interval)
                    logger.info(f"[{symbol}] {evt_type} 通知 {'✅' if ok else '❌'}")

            elif evt_type in ("tp3_hit", "sl_hit", "timeout"):
                # 計算本次盈虧
                entry  = evt["entry"]
                exit_p = evt["exit_price"]
                if evt["direction"] == "BUY":
                    pnl = (exit_p - entry) / entry * 100
                else:
                    pnl = (entry - exit_p) / entry * 100

                # 修正：確保勝率計算反映的是「利潤是否大於 0」
                stat.realized_pnl += pnl
                stat.trade_count  += 1
                if pnl > 0.000001:  # 避免浮點數微小誤差
                    stat.win_count += 1

                pnl_sign = "+" if pnl >= 0 else ""
                cum_sign = "+" if stat.realized_pnl >= 0 else ""
                logger.info(
                    f"[{symbol}] 關倉 {evt_type}  盈虧={pnl_sign}{pnl:.2f}%  "
                    f"累積={cum_sign}{stat.realized_pnl:.2f}%  "
                    f"勝率={stat.win_rate:.0f}%" if stat.win_rate else ""
                )

                # 取得 signal_id 用於記錄
                signal_id = self.current_signal_ids.get(symbol, 0)
                if not signal_id:
                    signal_id = self._get_latest_signal_id(symbol)
                
                # 記錄平倉交易到資料庫
                if signal_id:
                    # 先記錄 TP/SL 事件
                    self.db.record_tp_sl_event(
                        signal_id=signal_id,
                        symbol=symbol,
                        event_type=evt_type,
                        hit_price=exit_p,
                        hit_tp=None,
                        hit_r=None,
                    )
                    
                    # 記錄完整的平倉交易
                    self.db.record_trade_close(
                        signal_id=signal_id,
                        symbol=symbol,
                        entry_price=entry,
                        exit_price=exit_p,
                        direction=evt["direction"],
                        pnl_percent=pnl,
                        close_reason=evt_type,
                        entry_timestamp=evt.get("entry_time", ""),
                        bars_held=evt.get("bars_held", 0),
                    )

                # 更新資料庫統計
                self.db.update_symbol_stats(
                    symbol=symbol,
                    interval=self.interval,
                    signal_sent=False,  # 這是平倉事件，不是新訊號
                    is_win=(pnl > 0.000001),
                    pnl=pnl
                )

                ok = self.notifier.send_close(
                    evt, symbol, self.interval,
                    realized_pnl = stat.realized_pnl,
                    trade_count  = stat.trade_count,
                    win_rate     = stat.win_rate,
                )
                logger.info(f"[{symbol}] 關倉通知 {'✅' if ok else '❌'}")

    # ── 工具 ──────────────────────────────────────
    def _get_latest_signal_id(self, symbol: str) -> int:
        """取得該幣種最新的訊號 ID（用於關聯 TP/SL 事件）"""
        signals = self.db.get_recent_signals(symbol=symbol, limit=1)
        return signals[0]["id"] if signals else 0

    def _send_embed(self, embed: dict):
        """直接發送 embed（不透過 DiscordNotifier 的 rate-limit 等待）"""
        import requests as req
        payload = {"username": self.notifier.username, "embeds": [embed]}
        if self.notifier.avatar_url:
            payload["avatar_url"] = self.notifier.avatar_url
        try:
            r = req.post(self.notifier.webhook_url, json=payload, timeout=10)
            if r.status_code not in (200, 204):
                logger.warning(f"[embed] 發送失敗 HTTP {r.status_code}: {r.text[:100]}")
        except Exception as e:
            logger.error(f"[embed] 例外: {e}")

    def send_positions(self):
        """立即發送一次持倉總覽至 Discord（可從外部呼叫）。"""
        embed = build_positions_embed(self.symbols, self.engines, self.stats)
        self._send_embed(embed)
        active_n = sum(1 for s in self.symbols if self.engines[s].position is not None)
        logger.info(f"[Positions] 持倉報告已發送（{active_n} 筆開倉）✅")


# ══════════════════════════════════════════════════
# CLI 入口
# ══════════════════════════════════════════════════
def main():
    parser = argparse.ArgumentParser(description="SATS Bot — 多幣種本地監控系統")
    parser.add_argument("--config",    default="config/config.yaml")
    parser.add_argument("--symbol",    action="append", dest="symbols",
                         help="交易對（可多次，例如 --symbol BTCUSDT --symbol ETHUSDT）")
    parser.add_argument("--interval",  default=None, help="K 線週期（例如 1h 4h）")
    parser.add_argument("--debug",     action="store_true")
    parser.add_argument("--positions", action="store_true",
                         help="預熱後立即發送一次持倉總覽至 Discord，然後正常運行")
    args = parser.parse_args()

    cfg     = load_config(args.config)
    sys_cfg = cfg["system"]
    log_lvl = "DEBUG" if args.debug else sys_cfg.get("log_level", "INFO")
    setup_logging(log_lvl, sys_cfg.get("log_file", ""))

    # 動態載入交易所模組
    exchange = cfg.get("exchange", "binance")
    _load_exchange_module(exchange)

    # ── 自動選取交易對 ────────────────────────────────
    auto_cfg = cfg.get("auto_symbols", {})
    cli_symbols = args.symbols                  # CLI --symbol 優先級最高

    if not cli_symbols and auto_cfg.get("enabled", False):
        top_n = int(auto_cfg.get("top_n", 100))
        quote = auto_cfg.get("quote", "USDT")
        logger.info(f"[auto_symbols] 正在抓取成交額前 {top_n} 名 {quote} 交易對...")
        auto_syms = _fetch_top_symbols(top_n=top_n, quote=quote)
        if auto_syms:
            logger.info(f"[auto_symbols] 成功取得 {len(auto_syms)} 個交易對")
            cli_symbols = auto_syms
        else:
            logger.warning("[auto_symbols] 自動取得失敗，退回 config 手動清單")

    bot = SATSBot(cfg=cfg, symbols=cli_symbols, interval=args.interval)

    def _sig_handler(signum, frame):
        bot.shutdown("系統信號中斷")
        sys.exit(0)

    signal.signal(signal.SIGINT,  _sig_handler)
    signal.signal(signal.SIGTERM, _sig_handler)

    # ── --positions：預熱後立即發送持倉，然後繼續 ──
    if args.positions:
        # 在 start() 之前 hook：先 warmup，發送持倉，再跑主迴圈
        # 用 threading 讓 bot.start() 啟動後延遲發送
        def _delayed_positions():
            time.sleep(3)   # 等 WS 連線穩定
            bot.send_positions()
        threading.Thread(target=_delayed_positions, daemon=True).start()

    bot.start()


if __name__ == "__main__":
    main()
