"""
SATS Bot — Discord 指令模組
使用 discord.py 提供雙向互動 Slash Commands

安裝依賴：
    pip install "discord.py>=2.3.0"

config.yaml 需新增：
    discord:
        bot_token: "YOUR_BOT_TOKEN"       # Bot Token（非 Webhook URL）
        command_channel_id: 123456789     # 允許接受指令的頻道 ID
        admin_role_id: 987654321          # 可執行管理指令的身份組 ID（可選）

支援指令：
    /status          — 整體運行狀態摘要
    /positions       — 目前所有開倉
    /stats [symbol]  — 幣種績效統計（不填則顯示前 10）
    /tqi [top]       — TQI 排行榜（預設前 10）
    /signal [symbol] — 查詢某幣種最近訊號
    /report          — 立即觸發一次績效報告（等同每小時報告）
    /pause           — 暫停發送 Discord 通知（引擎繼續運行）
    /resume          — 恢復發送 Discord 通知
    /watchlist       — 顯示目前監控幣種清單
"""
from __future__ import annotations

import asyncio
import logging
import threading
import time
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Dict, List, Optional

import discord
from discord import app_commands
from discord.ext import commands

if TYPE_CHECKING:
    from main import SATSBot

logger = logging.getLogger("sats.discord_bot")

COLOR_BUY       = 0x00E676
COLOR_SELL      = 0xFF5252
COLOR_INFO      = 0x2196F3
COLOR_WARN      = 0xFFEB3B
COLOR_TP_HIT    = 0xFFD600
COLOR_CLOSE_WIN = 0x00E676
COLOR_CLOSE_LOSS= 0xFF5252


def _tqi_bar(tqi: float, width: int = 10) -> str:
    filled = round(tqi * width)
    return "█" * filled + "░" * (width - filled)

def _trend_emoji(trend: str) -> str:
    return "🟢" if trend == "Bullish" else "🔴"

def _pnl_str(pnl: float) -> str:
    sign = "+" if pnl >= 0 else ""
    emoji = "📈" if pnl >= 0 else "📉"
    return f"{emoji} `{sign}{pnl:.2f}%`"


# ══════════════════════════════════════════════════
# Discord Bot 類別
# ══════════════════════════════════════════════════
class SATSDiscordBot(commands.Bot):
    """
    雙向 Discord Bot，透過 Slash Commands 查詢 SATSBot 狀態。
    在獨立執行緒中運行，不阻塞主程式的 WebSocket 迴圈。
    """

    def __init__(self, sats_bot: "SATSBot", cfg: dict):
        intents = discord.Intents.default()
        super().__init__(command_prefix="!", intents=intents)

        self.sats_bot   = sats_bot
        self.cfg        = cfg
        self.dc_cfg     = cfg.get("discord", {})
        self.allowed_ch = int(self.dc_cfg.get("command_channel_id", 0))
        self.admin_role = int(self.dc_cfg.get("admin_role_id", 0))
        self._paused    = False   # 通知暫停旗標（同步給 SATSBot 使用）
        self._start_time = time.time()

        self._register_commands()

    # ── 生命週期 ──────────────────────────────────
    async def on_ready(self):
        logger.info(f"[DiscordBot] 已登入：{self.user} (id={self.user.id})")
        try:
            synced = await self.tree.sync()
            logger.info(f"[DiscordBot] 已同步 {len(synced)} 個 Slash Commands")
        except Exception as e:
            logger.error(f"[DiscordBot] Slash Command 同步失敗：{e}")

    # ── 頻道 / 權限 檢查 ─────────────────────────
    def _check_channel(self, interaction: discord.Interaction) -> bool:
        """若設定了允許頻道，則只在該頻道回應。"""
        if self.allowed_ch and interaction.channel_id != self.allowed_ch:
            return False
        return True

    def _is_admin(self, interaction: discord.Interaction) -> bool:
        """檢查是否擁有管理身份組（未設定則允許所有人）。"""
        if not self.admin_role:
            return True
        if isinstance(interaction.user, discord.Member):
            return any(r.id == self.admin_role for r in interaction.user.roles)
        return False

    async def _deny(self, interaction: discord.Interaction, msg: str):
        await interaction.response.send_message(
            embed=discord.Embed(description=f"⛔ {msg}", color=COLOR_WARN),
            ephemeral=True,
        )

    # ══════════════════════════════════════════════
    # 指令註冊
    # ══════════════════════════════════════════════
    def _register_commands(self):

        async def _safe_reply(interaction: discord.Interaction, build_fn, *args, **kwargs):
            """defer → build embed → followup，任何例外都確保使用者收到回應。"""
            try:
                await interaction.response.defer()
            except discord.NotFound:
                logger.warning("[DiscordBot] Interaction 已過期，無法 defer")
                return
            except Exception as e:
                logger.error(f"[DiscordBot] defer 失敗：{e}")
                return
            try:
                embed_dict = build_fn(*args, **kwargs)
                await interaction.followup.send(embed=discord.Embed.from_dict(embed_dict))
            except Exception as e:
                logger.error(f"[DiscordBot] 指令執行失敗：{e}", exc_info=True)
                try:
                    await interaction.followup.send(f"❌ 發生錯誤：`{e}`", ephemeral=True)
                except Exception:
                    pass

        # ── /status ───────────────────────────────
        @self.tree.command(name="status", description="顯示 SATS Bot 整體運行狀態")
        async def cmd_status(interaction: discord.Interaction):
            if not self._check_channel(interaction):
                return await self._deny(interaction, "請在指定頻道使用指令")
            await _safe_reply(interaction, self._build_status_embed)

        # ── /positions ────────────────────────────
        @self.tree.command(name="positions", description="顯示目前所有開倉")
        async def cmd_positions(interaction: discord.Interaction):
            if not self._check_channel(interaction):
                return await self._deny(interaction, "請在指定頻道使用指令")
            await _safe_reply(interaction, self._build_positions_embed)

        # ── /stats ────────────────────────────────
        @self.tree.command(name="stats", description="查詢幣種績效統計")
        @app_commands.describe(symbol="幣種符號（留空顯示全部前 10）")
        async def cmd_stats(interaction: discord.Interaction, symbol: str = ""):
            if not self._check_channel(interaction):
                return await self._deny(interaction, "請在指定頻道使用指令")
            sym = symbol.upper().strip() if symbol else None
            await _safe_reply(interaction, self._build_stats_embed, sym)

        # ── /tqi ──────────────────────────────────
        @self.tree.command(name="tqi", description="TQI 品質指數排行榜")
        @app_commands.describe(top="顯示前幾名（預設 10，最多 25）")
        async def cmd_tqi(interaction: discord.Interaction, top: int = 10):
            if not self._check_channel(interaction):
                return await self._deny(interaction, "請在指定頻道使用指令")
            await _safe_reply(interaction, self._build_tqi_embed, max(1, min(top, 25)))

        # ── /signal ───────────────────────────────
        @self.tree.command(name="signal", description="查詢某幣種最近一筆訊號資訊")
        @app_commands.describe(symbol="幣種符號，例如 BTCUSDT")
        async def cmd_signal(interaction: discord.Interaction, symbol: str):
            if not self._check_channel(interaction):
                return await self._deny(interaction, "請在指定頻道使用指令")
            await _safe_reply(interaction, self._build_signal_embed, symbol.upper().strip())

        # ── /report ───────────────────────────────
        @self.tree.command(name="report", description="立即觸發一次績效報告")
        async def cmd_report(interaction: discord.Interaction):
            if not self._check_channel(interaction):
                return await self._deny(interaction, "請在指定頻道使用指令")
            if not self._is_admin(interaction):
                return await self._deny(interaction, "需要管理員身份組")
            try:
                await interaction.response.send_message(
                    embed=discord.Embed(description="📊 正在產生報告，請稍候...", color=COLOR_INFO),
                    ephemeral=True,
                )
                threading.Thread(target=self.sats_bot._reporter._send_report, daemon=True).start()
            except Exception as e:
                logger.error(f"[DiscordBot] /report 失敗：{e}", exc_info=True)

        # ── /pause ────────────────────────────────
        @self.tree.command(name="pause", description="暫停發送 Discord 通知（引擎繼續運行）")
        async def cmd_pause(interaction: discord.Interaction):
            if not self._check_channel(interaction):
                return await self._deny(interaction, "請在指定頻道使用指令")
            if not self._is_admin(interaction):
                return await self._deny(interaction, "需要管理員身份組")
            try:
                self._paused = True
                self.sats_bot._notifications_paused = True
                await interaction.response.send_message(
                    embed=discord.Embed(
                        description="⏸️ 通知已暫停。引擎繼續運行，但不會發送任何 Discord 訊息。\n使用 `/resume` 恢復。",
                        color=COLOR_WARN,
                    ),
                )
            except Exception as e:
                logger.error(f"[DiscordBot] /pause 失敗：{e}", exc_info=True)

        # ── /resume ───────────────────────────────
        @self.tree.command(name="resume", description="恢復發送 Discord 通知")
        async def cmd_resume(interaction: discord.Interaction):
            if not self._check_channel(interaction):
                return await self._deny(interaction, "請在指定頻道使用指令")
            if not self._is_admin(interaction):
                return await self._deny(interaction, "需要管理員身份組")
            try:
                self._paused = False
                self.sats_bot._notifications_paused = False
                await interaction.response.send_message(
                    embed=discord.Embed(description="▶️ 通知已恢復。", color=COLOR_BUY),
                )
            except Exception as e:
                logger.error(f"[DiscordBot] /resume 失敗：{e}", exc_info=True)

        # ── /watchlist ────────────────────────────
        @self.tree.command(name="watchlist", description="顯示目前監控的幣種清單")
        async def cmd_watchlist(interaction: discord.Interaction):
            if not self._check_channel(interaction):
                return await self._deny(interaction, "請在指定頻道使用指令")
            await _safe_reply(interaction, self._build_watchlist_embed)

    # ══════════════════════════════════════════════
    # Embed 建構
    # ══════════════════════════════════════════════
    def _build_status_embed(self) -> dict:
        bot   = self.sats_bot
        stats = bot.stats
        engines = bot.engines
        now   = datetime.now(timezone.utc)

        uptime_sec = time.time() - bot._start_time
        hours   = int(uptime_sec // 3600)
        minutes = int((uptime_sec % 3600) // 60)

        total_signals = sum(s.signals_total   for s in stats.values())
        total_trades  = sum(s.trade_count      for s in stats.values())
        total_wins    = sum(s.win_count        for s in stats.values())
        total_pnl     = sum(s.realized_pnl    for s in stats.values())
        bullish_n     = sum(1 for s in bot.symbols if engines[s].trend == "Bullish")
        bearish_n     = len(bot.symbols) - bullish_n
        active_pos    = sum(1 for s in bot.symbols if engines[s].position is not None)
        wr_str        = f"{total_wins/total_trades*100:.0f}%" if total_trades > 0 else "—"
        paused_str    = "⏸️ 通知已暫停" if getattr(bot, "_notifications_paused", False) else "▶️ 正常運行"

        return {
            "title": f"🤖  SATS Bot 狀態  —  {now.strftime('%H:%M UTC')}",
            "color": COLOR_INFO,
            "fields": [
                {"name": "⏱️ 運行時間",  "value": f"`{hours}h {minutes}m`",       "inline": True},
                {"name": "📡 狀態",      "value": paused_str,                      "inline": True},
                {"name": "🔄 週期",      "value": f"`{bot.interval}`",             "inline": True},
                {"name": "🪙 監控幣種", "value": f"`{len(bot.symbols)}`",          "inline": True},
                {"name": "📂 開倉數",   "value": f"`{active_pos}`",                "inline": True},
                {"name": "🎯 最低分數", "value": f"`{bot.min_score}`",             "inline": True},
                {"name": "🟢 看漲",     "value": f"`{bullish_n}`",                 "inline": True},
                {"name": "🔴 看跌",     "value": f"`{bearish_n}`",                 "inline": True},
                {"name": "⠀",           "value": "⠀",                             "inline": True},
                {"name": "📊 訊號數",   "value": f"`{total_signals}`",             "inline": True},
                {"name": "💹 交易次數", "value": f"`{total_trades}`",              "inline": True},
                {"name": "🏆 勝率",     "value": f"`{wr_str}`",                    "inline": True},
                {
                    "name":   "📈 累積盈虧",
                    "value":  _pnl_str(total_pnl),
                    "inline": False,
                },
            ],
            "footer":    {"text": "SATS Bot v1.9.0  •  /positions 查看持倉詳情"},
            "timestamp": now.isoformat(),
        }

    def _build_positions_embed(self) -> dict:
        bot     = self.sats_bot
        engines = bot.engines
        stats   = bot.stats
        now     = datetime.now(timezone.utc)

        active = [
            (sym, engines[sym].position, stats[sym])
            for sym in bot.symbols
            if engines[sym].position is not None
        ]

        if not active:
            return {
                "title":       "📋  目前持倉",
                "description": "目前無任何開倉",
                "color":       COLOR_INFO,
                "timestamp":   now.isoformat(),
                "footer":      {"text": "SATS Bot v1.9.0"},
            }

        fields = []
        for sym, pos, stat in active:
            direction = pos["direction"]
            entry     = pos["entry"]
            current   = stat.last_price
            d_emoji   = "🟢" if direction == "BUY" else "🔴"

            if entry > 0 and current > 0:
                raw = (current - entry) / entry * 100 if direction == "BUY" else (entry - current) / entry * 100
                upnl_str = _pnl_str(raw)
            else:
                upnl_str = "—"

            tp1_e = "✅" if pos["hit_tp1"] else "⬜"
            tp2_e = "✅" if pos["hit_tp2"] else "⬜"
            tp3_e = "✅" if pos["hit_tp3"] else "⬜"

            fields.append({
                "name": f"{d_emoji} {sym}",
                "value": (
                    f"{d_emoji} **{direction}**  |  已開 `{pos['bars_open']}` 根\n"
                    f"進場 `{entry:.6g}`  現價 `{current:.6g}`\n"
                    f"SL `{pos['sl']:.6g}`\n"
                    f"TP1 {tp1_e}`{pos['tp1']:.6g}`({pos['tp1r']:.1f}R)  "
                    f"TP2 {tp2_e}`{pos['tp2']:.6g}`({pos['tp2r']:.1f}R)  "
                    f"TP3 {tp3_e}`{pos['tp3']:.6g}`({pos['tp3r']:.1f}R)\n"
                    f"未實盈虧 {upnl_str}"
                ),
                "inline": False,
            })

        return {
            "title":       f"📋  目前持倉  ({len(active)} 筆)",
            "description": f"共 **{len(active)}** 個幣種有開倉",
            "color":       COLOR_INFO,
            "fields":      fields[:25],
            "timestamp":   now.isoformat(),
            "footer":      {"text": "SATS Bot v1.9.0"},
        }

    def _build_stats_embed(self, symbol: Optional[str]) -> dict:
        bot   = self.sats_bot
        now   = datetime.now(timezone.utc)

        if symbol:
            # 單一幣種
            if symbol not in bot.stats:
                return {
                    "title":       f"❌ 找不到 {symbol}",
                    "description": f"`{symbol}` 不在監控清單中",
                    "color":       COLOR_WARN,
                    "timestamp":   now.isoformat(),
                }
            st  = bot.stats[symbol]
            eng = bot.engines[symbol]
            pos = eng.position

            wr_str = f"{st.win_rate:.0f}%" if st.win_rate is not None else "—"
            pos_str = (
                f"{_trend_emoji(pos['direction'] == 'BUY' and 'Bullish' or 'Bearish')} "
                f"{pos['direction']}  已開 `{pos['bars_open']}` 根"
                if pos else "無"
            )

            return {
                "title": f"📊  {symbol} 統計",
                "color": COLOR_INFO,
                "fields": [
                    {"name": "趨勢",      "value": f"{_trend_emoji(eng.trend)} {eng.trend}", "inline": True},
                    {"name": "TQI",       "value": f"`{_tqi_bar(eng.tqi)}` {eng.tqi*100:.0f}%", "inline": True},
                    {"name": "現價",      "value": f"`{getattr(eng, 'last_close', 0):.6g}`",  "inline": True},
                    {"name": "訊號數",    "value": f"`{st.signals_total}`",                    "inline": True},
                    {"name": "BUY",       "value": f"`{st.signals_buy}`",                      "inline": True},
                    {"name": "SELL",      "value": f"`{st.signals_sell}`",                     "inline": True},
                    {"name": "交易次數",  "value": f"`{st.trade_count}`",                      "inline": True},
                    {"name": "勝率",      "value": f"`{wr_str}`",                              "inline": True},
                    {"name": "累積盈虧",  "value": _pnl_str(st.realized_pnl),                  "inline": True},
                    {"name": "目前持倉",  "value": pos_str,                                    "inline": False},
                ],
                "timestamp": now.isoformat(),
                "footer":    {"text": "SATS Bot v1.9.0"},
            }
        else:
            # 前 10 幣種（依累積盈虧排序）
            ranked = sorted(
                bot.symbols,
                key=lambda s: bot.stats[s].realized_pnl,
                reverse=True,
            )[:10]

            lines = []
            for i, sym in enumerate(ranked, 1):
                st  = bot.stats[sym]
                eng = bot.engines[sym]
                te  = _trend_emoji(eng.trend)
                wr  = f"{st.win_rate:.0f}%" if st.win_rate is not None else "—"
                pnl_s = ("+" if st.realized_pnl >= 0 else "") + f"{st.realized_pnl:.2f}%"
                lines.append(
                    f"`{i:>2}.` {te} **{sym}**  盈虧 `{pnl_s}`  "
                    f"({st.trade_count}筆 勝率{wr})  TQI`{eng.tqi*100:.0f}%`"
                )

            return {
                "title":       "📊  幣種績效統計 Top 10",
                "description": "\n".join(lines) if lines else "尚無統計數據",
                "color":       COLOR_INFO,
                "timestamp":   now.isoformat(),
                "footer":      {"text": "SATS Bot v1.9.0  •  使用 /stats BTCUSDT 查看單一幣種"},
            }

    def _build_tqi_embed(self, top: int) -> dict:
        bot = self.sats_bot
        now = datetime.now(timezone.utc)

        ranked = sorted(bot.symbols, key=lambda s: bot.engines[s].tqi, reverse=True)[:top]

        fields = []
        for i, sym in enumerate(ranked, 1):
            eng = bot.engines[sym]
            te  = _trend_emoji(eng.trend)
            price = f"`{getattr(eng, 'last_close', 0):.6g}`"
            fields.append({
                "name":   f"`{i:>2}.` {te} {sym}",
                "value":  f"`{_tqi_bar(eng.tqi)}` **{eng.tqi*100:.0f}%**  {price}",
                "inline": False,
            })

        return {
            "title":       f"🔥  TQI 排行榜 Top {top}",
            "description": "品質指數越高，代表趨勢越清晰、動能越強",
            "color":       COLOR_TP_HIT,
            "fields":      fields,
            "timestamp":   now.isoformat(),
            "footer":      {"text": "SATS Bot v1.9.0"},
        }

    def _build_signal_embed(self, symbol: str) -> dict:
        bot = self.sats_bot
        now = datetime.now(timezone.utc)

        if symbol not in bot.engines:
            return {
                "title":       f"❌ 找不到 {symbol}",
                "description": f"`{symbol}` 不在監控清單中",
                "color":       COLOR_WARN,
                "timestamp":   now.isoformat(),
            }

        st  = bot.stats[symbol]
        eng = bot.engines[symbol]
        sig = st.last_signal

        if sig is None:
            return {
                "title":       f"🔔  {symbol} — 尚無訊號",
                "description": "自啟動後此幣種尚未產生任何訊號",
                "color":       COLOR_INFO,
                "timestamp":   now.isoformat(),
            }

        is_buy  = sig.direction == "BUY"
        color   = COLOR_BUY if is_buy else COLOR_SELL
        d_emoji = "🟢" if is_buy else "🔴"
        mins_ago = int((now - st.last_signal_time).total_seconds() / 60) if st.last_signal_time else 0
        risk = abs(sig.price - sig.sl)

        return {
            "title":       f"🔔  {symbol} 最近訊號",
            "description": f"{d_emoji} **{sig.direction}**  |  {mins_ago} 分鐘前",
            "color":       color,
            "fields": [
                {"name": "📍 Entry",    "value": f"`{sig.price:.6g}`",                   "inline": True},
                {"name": "🛡️ SL",      "value": f"`{sig.sl:.6g}` ({risk:.6g})",          "inline": True},
                {"name": "⠀",          "value": "⠀",                                    "inline": True},
                {"name": "🎯 TP1",     "value": f"`{sig.tp1:.6g}` ({sig.tp1_r:.1f}R)",   "inline": True},
                {"name": "🎯 TP2",     "value": f"`{sig.tp2:.6g}` ({sig.tp2_r:.1f}R)",   "inline": True},
                {"name": "🎯 TP3",     "value": f"`{sig.tp3:.6g}` ({sig.tp3_r:.1f}R)",   "inline": True},
                {"name": "📐 TQI",     "value": f"`{_tqi_bar(sig.tqi)}` {sig.tqi*100:.0f}%", "inline": True},
                {"name": "📊 Score",   "value": f"`{sig.score:.0f}/102`",                "inline": True},
                {"name": "Bar#",       "value": f"`{sig.bar_index}`",                    "inline": True},
                {"name": "現價",       "value": f"`{getattr(eng, 'last_close', 0):.6g}`","inline": True},
                {"name": "趨勢",       "value": f"{_trend_emoji(eng.trend)} {eng.trend}","inline": True},
                {"name": "TQI 現況",   "value": f"`{eng.tqi*100:.0f}%`",                 "inline": True},
            ],
            "timestamp": now.isoformat(),
            "footer":    {"text": "SATS Bot v1.9.0"},
        }

    def _build_watchlist_embed(self) -> dict:
        bot = self.sats_bot
        now = datetime.now(timezone.utc)

        # 每行 4 個幣種
        syms = bot.symbols
        chunk_size = 4
        lines = []
        for i in range(0, len(syms), chunk_size):
            chunk = syms[i:i + chunk_size]
            lines.append("  ".join(f"`{s}`" for s in chunk))

        return {
            "title":       f"🪙  監控清單  ({len(syms)} 對)",
            "description": "\n".join(lines) if lines else "清單為空",
            "color":       COLOR_INFO,
            "fields": [
                {"name": "週期",     "value": f"`{bot.interval}`",   "inline": True},
                {"name": "最低分數", "value": f"`{bot.min_score}`",  "inline": True},
            ],
            "timestamp": now.isoformat(),
            "footer":    {"text": "SATS Bot v1.9.0"},
        }


# ══════════════════════════════════════════════════
# 啟動函數（在背景執行緒中執行）
# ══════════════════════════════════════════════════
def start_discord_bot(sats_bot: "SATSBot", cfg: dict) -> Optional[SATSDiscordBot]:
    """
    在獨立執行緒中啟動 Discord Bot。
    回傳 bot 實例（可用於外部呼叫 stop_discord_bot）。
    若未設定 bot_token 則直接回傳 None（靜默略過）。
    """
    token = cfg.get("discord", {}).get("bot_token", "")
    if not token:
        logger.info("[DiscordBot] 未設定 bot_token，跳過指令功能")
        return None

    bot = SATSDiscordBot(sats_bot, cfg)

    def _run():
        # 每個執行緒需要自己的 event loop
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            loop.run_until_complete(bot.start(token))
        except Exception as e:
            logger.error(f"[DiscordBot] 執行錯誤：{e}")
        finally:
            loop.close()

    t = threading.Thread(target=_run, name="DiscordBotThread", daemon=True)
    t.start()
    logger.info("[DiscordBot] 指令 Bot 執行緒已啟動")
    return bot


def stop_discord_bot(bot: Optional[SATSDiscordBot]):
    """安全關閉 Discord Bot（在主執行緒呼叫）。"""
    if bot is None:
        return
    try:
        future = asyncio.run_coroutine_threadsafe(bot.close(), bot.loop)
        future.result(timeout=5)
        logger.info("[DiscordBot] 已關閉")
    except Exception as e:
        logger.warning(f"[DiscordBot] 關閉時發生例外：{e}")
