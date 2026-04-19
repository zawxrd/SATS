"""
Discord Webhook 通知模組
"""
from __future__ import annotations
import time
import requests
from typing import Optional
from core.engine import SignalResult



# ══════════════════════════════════════════════════
# 顏色常數 (Discord embed color)
# ══════════════════════════════════════════════════
COLOR_BUY    = 0x00E676   # 綠
COLOR_SELL   = 0xFF5252   # 紅
COLOR_INFO   = 0x2196F3   # 藍
COLOR_WARN   = 0xFFEB3B   # 黃

COLOR_CLOSE_WIN  = 0x00E676   # 綠（獲利關倉）
COLOR_CLOSE_LOSS = 0xFF5252   # 紅（虧損關倉）
COLOR_TP_HIT     = 0xFFD600   # 金黃（TP 命中里程碑）
COLOR_OPEN       = 0x29B6F6   # 天藍（開倉確認）


# ══════════════════════════════════════════════════
# 工具函數  ← 必須在所有 build_*embed 之前定義
# ══════════════════════════════════════════════════
def _tqi_bar(tqi: float, width: int = 8) -> str:
    filled = round(tqi * width)
    return "█" * filled + "░" * (width - filled)


def _regime_label(er: float) -> str:
    if er >= 0.50:
        return "📈 Trending"
    elif er >= 0.25:
        return "〰️ Mixed"
    return "🌀 Choppy"


def _vol_label(vol_z: float) -> str:
    if vol_z > 2.0:
        return "🔥 High"
    elif vol_z > 0.5:
        return "✅ Normal"
    return "😴 Low"


# ══════════════════════════════════════════════════
# 開倉通知 Embed
# ══════════════════════════════════════════════════
def build_open_embed(sig: "SignalResult", mention_role: str = "") -> dict:
    """訊號確認開倉時發送的輕量 Embed（區別於原始訊號 Embed）。"""
    from datetime import datetime, timezone
    is_buy = sig.direction == "BUY"
    emoji  = "🟢" if is_buy else "🔴"
    risk   = abs(sig.price - sig.sl)

    desc_lines = []
    if mention_role:
        desc_lines.append(f"<@&{mention_role}>")
    desc_lines.append(f"**{emoji} {sig.symbol} 開倉 — {'BUY ▲' if is_buy else 'SELL ▼'}**")
    desc_lines.append(f"*{sig.preset} | {sig.interval}*")

    return {
        "title":       f"📂  開倉  •  {sig.symbol}  •  {sig.interval}",
        "description": "\n".join(desc_lines),
        "color":       COLOR_OPEN,
        "fields": [
            {"name": "📍 進場價", "value": f"`{sig.price:.6g}`",                             "inline": True},
            {"name": "🛡️ 止損",  "value": f"`{sig.sl:.6g}`  ({risk:.6g} risk)",              "inline": True},
            {"name": "\u200b",   "value": "\u200b",                                           "inline": True},
            {"name": "🎯 TP1",   "value": f"`{sig.tp1:.6g}`  ({sig.tp1_r:.1f}R)",            "inline": True},
            {"name": "🎯 TP2",   "value": f"`{sig.tp2:.6g}`  ({sig.tp2_r:.1f}R)",            "inline": True},
            {"name": "🎯 TP3",   "value": f"`{sig.tp3:.6g}`  ({sig.tp3_r:.1f}R)",            "inline": True},
            {"name": "TQI",      "value": f"`{_tqi_bar(sig.tqi)}` {sig.tqi*100:.0f}%",       "inline": True},
            {"name": "Score",    "value": f"`{sig.score:.0f} / 102`",                         "inline": True},
            {"name": "Bar #",    "value": f"`{sig.bar_index}`",                               "inline": True},
        ],
        "footer":    {"text": "SATS v1.9.0  •  開倉確認"},
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


# ══════════════════════════════════════════════════
# TP 里程碑通知 Embed
# ══════════════════════════════════════════════════
def build_tp_hit_embed(evt: dict, symbol: str, interval: str) -> dict:
    """TP1 / TP2 首次命中通知（里程碑，倉位繼續開）。"""
    from datetime import datetime, timezone
    tp_num = {"tp1_hit": 1, "tp2_hit": 2}.get(evt["type"], 1)
    is_buy = evt["direction"] == "BUY"
    emoji  = "🟢" if is_buy else "🔴"

    entry     = evt["entry"]
    exit_p    = evt["exit_price"]
    pnl_pct   = ((exit_p - entry) / entry * 100) if is_buy else ((entry - exit_p) / entry * 100)
    pnl_sign  = "+" if pnl_pct >= 0 else ""

    hit_icons = (
        ("✅" if evt.get("hit_tp1") else "⬜") +
        ("✅" if evt.get("hit_tp2") else "⬜") +
        ("⬜")
    )

    # ── Breakeven 提示 ─────────────────────
    sl_val = evt["sl"]
    is_be  = evt.get("is_breakeven", False)
    sl_str = f"`{sl_val:.6g}`"
    if is_be:
        if tp_num == 1:
            sl_str = f"🛡️ `{entry:.6g}` (已保本)"
        elif tp_num == 2:
            tp1_val = evt.get("tp1", 0)
            sl_str = f"🔒 `{tp1_val:.6g}` (鎖利 TP1)"
    # ──────────────────────────────────────

    return {
        "title":       f"🏆  TP{tp_num} 命中  •  {symbol}  •  {interval}",
        "description": f"{emoji} **{evt['direction']}**  |  TP{tp_num} 已達到！",
        "color":       COLOR_TP_HIT,
        "fields": [
            {"name": "📍 進場價",      "value": f"`{entry:.6g}`",                            "inline": True},
            {"name": f"🎯 TP{tp_num}", "value": f"`{exit_p:.6g}`",                           "inline": True},
            {"name": "📈 浮盈",        "value": f"`{pnl_sign}{pnl_pct:.2f}%`",               "inline": True},
            {"name": "🛡️ 止損",       "value": sl_str,                                      "inline": True},
            {"name": "已命中 TP",      "value": hit_icons,                                   "inline": True},
            {"name": "開倉根數",       "value": f"`{evt['bars_open']}`",                     "inline": True},
        ],
        "footer":    {"text": "SATS v1.9.0  •  倉位持續，等待 TP3 或止損"},
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


# ══════════════════════════════════════════════════
# 關倉通知 Embed
# ══════════════════════════════════════════════════
_CLOSE_TYPE_LABEL = {
    "tp3_hit": ("🏆", "TP3 全部達到，完美獲利！"),
    "sl_hit":  ("🛑", "止損觸發，倉位已平倉"),
    "timeout": ("⏱️", "交易超時，倉位已平倉"),
}

def build_close_embed(evt: dict, symbol: str, interval: str,
                      realized_pnl: float, trade_count: int,
                      win_rate: "Optional[float]" = None) -> dict:
    """TP3 / SL / Timeout 關倉通知。"""
    from datetime import datetime, timezone
    close_type = evt["type"]
    icon, subtitle = _CLOSE_TYPE_LABEL.get(close_type, ("🔒", "倉位已平倉"))
    is_buy    = evt["direction"] == "BUY"
    d_emoji   = "🟢" if is_buy else "🔴"

    entry  = evt["entry"]
    exit_p = evt["exit_price"]
    pnl    = ((exit_p - entry) / entry * 100) if is_buy else ((entry - exit_p) / entry * 100)
    pnl_sign = "+" if pnl >= 0 else ""
    cum_sign  = "+" if realized_pnl >= 0 else ""
    is_win    = pnl > 0
    color     = COLOR_CLOSE_WIN if is_win else COLOR_CLOSE_LOSS
    pnl_emoji = "📈" if is_win else "📉"

    hit_icons = (
        ("✅" if evt.get("hit_tp1") else "⬜") +
        ("✅" if evt.get("hit_tp2") else "⬜") +
        ("✅" if evt.get("hit_tp3") else "⬜")
    )

    wr_str = f"  勝率 `{win_rate:.0f}%`" if win_rate is not None else ""

    return {
        "title":       f"{icon}  關倉  •  {symbol}  •  {interval}",
        "description": f"{d_emoji} **{evt['direction']}**  |  {subtitle}",
        "color":       color,
        "fields": [
            {"name": "📍 進場價",         "value": f"`{entry:.6g}`",                         "inline": True},
            {"name": "🚪 出場價",         "value": f"`{exit_p:.6g}`",                        "inline": True},
            {"name": f"{pnl_emoji} 盈虧", "value": f"`{pnl_sign}{pnl:.2f}%`",               "inline": True},
            {"name": "🛡️ 止損",          "value": f"`{evt['sl']:.6g}`",                     "inline": True},
            {"name": "TP 命中",           "value": hit_icons,                                "inline": True},
            {"name": "開倉根數",          "value": f"`{evt['bars_open']}`",                  "inline": True},
            {
                "name":   "📊 累積統計",
                "value":  (
                    f"累積盈虧 `{cum_sign}{realized_pnl:.2f}%`"
                    f"  /  {trade_count} 筆{wr_str}"
                ),
                "inline": False,
            },
        ],
        "footer":    {"text": "SATS v1.9.0  •  關倉"},
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


# ══════════════════════════════════════════════════
# 訊號 Embed
# ══════════════════════════════════════════════════
def build_signal_embed(sig: SignalResult, mention_role: str = "") -> dict:
    is_buy = sig.direction == "BUY"
    color  = COLOR_BUY if is_buy else COLOR_SELL
    emoji  = "🟢" if is_buy else "🔴"
    direction_label = "BUY ▲" if is_buy else "SELL ▼"

    tqi_pct   = f"{sig.tqi * 100:.0f}%"
    tqi_bar   = _tqi_bar(sig.tqi)
    score_pct = f"{sig.score:.0f} / 102"
    regime    = _regime_label(sig.er)
    vol_lbl   = _vol_label(sig.vol_z)

    tp_mode_label = f"Dynamic ×{sig.dyn_scale:.2f}" if sig.tp_mode == "Dynamic" else "Fixed"
    risk = abs(sig.price - sig.sl)

    description_lines = []
    if mention_role:
        description_lines.append(f"<@&{mention_role}>")
    description_lines.append(f"**{emoji} {sig.symbol} — {direction_label}**")
    description_lines.append(f"*Preset: {sig.preset} | TF: {sig.interval} | TP: {tp_mode_label}*")
    description = "\n".join(description_lines)

    embed = {
        "title":       f"{emoji}  SATS Signal  •  {sig.symbol}  •  {sig.interval}",
        "description": description,
        "color":       color,
        "fields": [
            {"name": "📍 Entry",      "value": f"`{sig.price:.6g}`",                         "inline": True},
            {"name": "🛡️ Stop Loss", "value": f"`{sig.sl:.6g}`  ({risk:.6g} risk)",          "inline": True},
            {"name": "\u200b",        "value": "\u200b",                                      "inline": True},
            {"name": "🎯 TP1",        "value": f"`{sig.tp1:.6g}`  ({sig.tp1_r:.1f}R)",       "inline": True},
            {"name": "🎯 TP2",        "value": f"`{sig.tp2:.6g}`  ({sig.tp2_r:.1f}R)",       "inline": True},
            {"name": "🎯 TP3",        "value": f"`{sig.tp3:.6g}`  ({sig.tp3_r:.1f}R)",       "inline": True},
            {"name": f"📐 TQI  {tqi_pct}", "value": f"`{tqi_bar}`",                          "inline": False},
            {"name": "📊 Score",      "value": score_pct,                                    "inline": True},
            {"name": "📡 Regime",     "value": regime,                                       "inline": True},
            {"name": "📦 Volume",     "value": vol_lbl + f"  (Z={sig.vol_z:.2f})",           "inline": True},
            {"name": "ER",            "value": f"`{sig.er:.3f}`",                            "inline": True},
            {"name": "RSI",           "value": f"`{sig.rsi:.1f}`",                           "inline": True},
            {"name": "Bar #",         "value": f"`{sig.bar_index}`",                         "inline": True},
        ],
        "footer":    {"text": "SATS v1.9.0  •  WillyAlgoTrader (Python port)"},
        "timestamp": None,  # 由呼叫端填入 ISO 時間
    }
    return embed


# ══════════════════════════════════════════════════
# 訊號過濾（跳過）Embed
# ══════════════════════════════════════════════════
def build_skipped_embed(sig: "SignalResult", reason: str) -> dict:
    """當訊號被過濾時發送的簡短通知。"""
    from datetime import datetime, timezone
    is_buy = sig.direction == "BUY"
    emoji  = "🟢" if is_buy else "🔴"
    
    # 根據原因選擇圖示
    reason_icon = "⚠️" if "持倉" in reason else "📉"
    
    return {
        "title":       f"{reason_icon}  訊號過濾  •  {sig.symbol}  •  {sig.interval}",
        "description": f"**{emoji} {sig.symbol} {sig.direction}**\n原因：`{reason}`",
        "color":       COLOR_WARN,
        "fields": [
            {"name": "📍 價格",   "value": f"`{sig.price:.6g}`",         "inline": True},
            {"name": "📊 分數",   "value": f"`{sig.score:.0f}`",         "inline": True},
            {"name": "📐 TQI",    "value": f"`{sig.tqi*100:.0f}%`",      "inline": True},
        ],
        "footer":    {"text": "SATS v1.9.0  •  訊號已跳過"},
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


# ══════════════════════════════════════════════════
# 通知器
# ══════════════════════════════════════════════════
class DiscordNotifier:
    def __init__(self, webhook_url: str, username: str = "SATS Bot 🤖",
                 avatar_url: str = "", mention_role: str = ""):
        self.webhook_url   = webhook_url
        self.username      = username
        self.avatar_url    = avatar_url
        self.mention_role  = mention_role
        self._last_sent: float = 0.0
        self._rate_limit_delay = 1.0   # Discord rate: 30 msg/min 安全起見 1 秒一條

    def send_signal(self, sig: SignalResult, pnl_field: Optional[dict] = None) -> bool:
        """發送訊號通知。若有 pnl_field 則插入 embed 頂部。成功回傳 True。"""
        now  = time.time()
        wait = self._rate_limit_delay - (now - self._last_sent)
        if wait > 0:
            time.sleep(wait)

        from datetime import datetime, timezone
        ts = datetime.now(timezone.utc).isoformat()

        embed = build_signal_embed(sig, self.mention_role)
        embed["timestamp"] = ts

        if pnl_field is not None:
            embed["fields"].insert(0, pnl_field)

        payload = {"username": self.username, "embeds": [embed]}
        if self.avatar_url:
            payload["avatar_url"] = self.avatar_url

        try:
            r = requests.post(self.webhook_url, json=payload, timeout=10)
            self._last_sent = time.time()
            if r.status_code in (200, 204):
                return True
            print(f"[Discord] 發送失敗 HTTP {r.status_code}: {r.text[:200]}")
            return False
        except Exception as e:
            print(f"[Discord] 發送例外: {e}")
            return False

    def send_open(self, sig: SignalResult) -> bool:
        """發送開倉確認通知。"""
        embed = build_open_embed(sig, self.mention_role)
        return self._post_embed(embed)

    def send_tp_hit(self, evt: dict, symbol: str, interval: str) -> bool:
        """發送 TP1 / TP2 里程碑通知。"""
        embed = build_tp_hit_embed(evt, symbol, interval)
        return self._post_embed(embed)

    def send_close(self, evt: dict, symbol: str, interval: str,
                   realized_pnl: float, trade_count: int,
                   win_rate: Optional[float] = None) -> bool:
        """發送關倉（TP3 / SL / Timeout）通知。"""
        embed = build_close_embed(evt, symbol, interval, realized_pnl, trade_count, win_rate)
        return self._post_embed(embed)

    def send_info(self, title: str, message: str) -> bool:
        """發送一般資訊訊息。"""
        from datetime import datetime, timezone
        ts = datetime.now(timezone.utc).isoformat()
        payload = {
            "username": self.username,
            "embeds": [{
                "title":       title,
                "description": message,
                "color":       COLOR_INFO,
                "timestamp":   ts,
                "footer":      {"text": "SATS Bot"},
            }],
        }
        if self.avatar_url:
            payload["avatar_url"] = self.avatar_url
        try:
            r = requests.post(self.webhook_url, json=payload, timeout=10)
            return r.status_code in (200, 204)
        except Exception as e:
            print(f"[Discord] send_info 例外: {e}")
            return False

    def send_error(self, title: str, message: str) -> bool:
        from datetime import datetime, timezone
        ts = datetime.now(timezone.utc).isoformat()
        payload = {
            "username": self.username,
            "embeds": [{
                "title":       f"⚠️ {title}",
                "description": message,
                "color":       COLOR_WARN,
                "timestamp":   ts,
                "footer":      {"text": "SATS Bot"},
            }],
        }
        if self.avatar_url:
            payload["avatar_url"] = self.avatar_url
        try:
            r = requests.post(self.webhook_url, json=payload, timeout=10)
            return r.status_code in (200, 204)
        except Exception as e:
            print(f"[Discord] send_error 例外: {e}")
            return False

    def send_skipped_signal(self, sig: SignalResult, reason: str) -> bool:
        """發送訊號過濾通知。"""
        embed = build_skipped_embed(sig, reason)
        return self._post_embed(embed)

    def _post_embed(self, embed: dict) -> bool:
        """內部：直接 POST 一個 embed，帶速率限制。"""
        now  = time.time()
        wait = self._rate_limit_delay - (now - self._last_sent)
        if wait > 0:
            time.sleep(wait)
        payload = {"username": self.username, "embeds": [embed]}
        if self.avatar_url:
            payload["avatar_url"] = self.avatar_url
        try:
            r = requests.post(self.webhook_url, json=payload, timeout=10)
            self._last_sent = time.time()
            if r.status_code in (200, 204):
                return True
            print(f"[Discord] _post_embed 失敗 HTTP {r.status_code}: {r.text[:200]}")
            return False
        except Exception as e:
            print(f"[Discord] _post_embed 例外: {e}")
            return False