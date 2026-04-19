"""
SATS Database — SQLite 數據持久化
記錄交易訊號、止損/止盈命中、平倉事件與統計數據
"""
from __future__ import annotations

import sqlite3
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, List, Dict, Any
from contextlib import contextmanager

logger = logging.getLogger("sats.database")


class SATSDatabase:
    """SQLite 資料庫管理，用於持久化交易數據"""

    def __init__(self, db_path: str = "sats_bot.db"):
        self.db_path = Path(db_path)
        self._init_db()
        logger.info(f"資料庫已初始化：{self.db_path.absolute()}")

    @contextmanager
    def _get_connection(self):
        """取得資料庫連線的上下文管理器"""
        conn = sqlite3.connect(str(self.db_path))
        conn.row_factory = sqlite3.Row
        try:
            yield conn
            conn.commit()
        except Exception as e:
            conn.rollback()
            logger.error(f"資料庫事務錯誤：{e}")
            raise
        finally:
            conn.close()

    def _init_db(self):
        """初始化資料庫結構"""
        with self._get_connection() as conn:
            cursor = conn.cursor()

            # 交易訊號表
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS signals (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    symbol TEXT NOT NULL,
                    interval TEXT NOT NULL,
                    direction TEXT NOT NULL,
                    price REAL NOT NULL,
                    sl REAL NOT NULL,
                    tp1 REAL NOT NULL,
                    tp2 REAL NOT NULL,
                    tp3 REAL NOT NULL,
                    tp1_r REAL NOT NULL,
                    tp2_r REAL NOT NULL,
                    tp3_r REAL NOT NULL,
                    score REAL NOT NULL,
                    tqi REAL NOT NULL,
                    er REAL NOT NULL,
                    rsi REAL NOT NULL,
                    vol_z REAL NOT NULL,
                    preset TEXT NOT NULL,
                    tp_mode TEXT NOT NULL,
                    dyn_scale REAL NOT NULL,
                    bar_index INTEGER NOT NULL,
                    timestamp TEXT NOT NULL,
                    sent INTEGER NOT NULL DEFAULT 1
                )
            """)

            # TP/SL 命中事件表
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS tp_sl_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    signal_id INTEGER NOT NULL,
                    symbol TEXT NOT NULL,
                    event_type TEXT NOT NULL,
                    hit_price REAL NOT NULL,
                    hit_tp REAL,
                    hit_r REAL,
                    timestamp TEXT NOT NULL,
                    FOREIGN KEY (signal_id) REFERENCES signals(id)
                )
            """)

            # 交易平倉記錄表
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS trade_closes (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    signal_id INTEGER NOT NULL,
                    symbol TEXT NOT NULL,
                    entry_price REAL NOT NULL,
                    exit_price REAL NOT NULL,
                    direction TEXT NOT NULL,
                    pnl_percent REAL NOT NULL,
                    is_win INTEGER NOT NULL,
                    close_reason TEXT NOT NULL,
                    entry_timestamp TEXT NOT NULL,
                    close_timestamp TEXT NOT NULL,
                    bars_held INTEGER NOT NULL,
                    FOREIGN KEY (signal_id) REFERENCES signals(id)
                )
            """)

            # 幣種統計表（快取用，可從_signals 重新計算）
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS symbol_stats (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    symbol TEXT NOT NULL UNIQUE,
                    interval TEXT NOT NULL,
                    total_signals INTEGER NOT NULL DEFAULT 0,
                    buy_signals INTEGER NOT NULL DEFAULT 0,
                    sell_signals INTEGER NOT NULL DEFAULT 0,
                    skipped_signals INTEGER NOT NULL DEFAULT 0,
                    total_trades INTEGER NOT NULL DEFAULT 0,
                    win_trades INTEGER NOT NULL DEFAULT 0,
                    realized_pnl REAL NOT NULL DEFAULT 0.0,
                    last_signal_time TEXT,
                    last_entry_price REAL,
                    last_entry_dir TEXT,
                    updated_at TEXT NOT NULL
                )
            """)

            # 系統運行日誌表
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS system_logs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    event_type TEXT NOT NULL,
                    message TEXT NOT NULL,
                    details TEXT,
                    timestamp TEXT NOT NULL
                )
            """)

            # 建立索引以加速查詢
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_signals_symbol 
                ON signals(symbol, timestamp)
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_signals_direction 
                ON signals(symbol, direction)
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_tp_sl_events_signal 
                ON tp_sl_events(signal_id)
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_trade_closes_signal 
                ON trade_closes(signal_id)
            """)

            logger.debug("資料庫表格與索引已建立")

    # ── 訊號記錄 ─────────────────────────────────────
    def record_signal(self, signal_data: Dict[str, Any], sent: bool = True) -> int:
        """
        記錄一筆新的交易訊號
        回傳 signal_id
        """
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO signals (
                    symbol, interval, direction, price, sl, tp1, tp2, tp3,
                    tp1_r, tp2_r, tp3_r, score, tqi, er, rsi, vol_z,
                    preset, tp_mode, dyn_scale, bar_index, timestamp, sent
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                signal_data["symbol"],
                signal_data["interval"],
                signal_data["direction"],
                signal_data["price"],
                signal_data["sl"],
                signal_data["tp1"],
                signal_data["tp2"],
                signal_data["tp3"],
                signal_data["tp1_r"],
                signal_data["tp2_r"],
                signal_data["tp3_r"],
                signal_data["score"],
                signal_data["tqi"],
                signal_data["er"],
                signal_data["rsi"],
                signal_data["vol_z"],
                signal_data["preset"],
                signal_data["tp_mode"],
                signal_data["dyn_scale"],
                signal_data["bar_index"],
                datetime.now(timezone.utc).isoformat(),
                1 if sent else 0,
            ))
            return cursor.lastrowid

    # ── TP/SL 事件記錄 ───────────────────────────────
    def record_tp_sl_event(
        self,
        signal_id: int,
        symbol: str,
        event_type: str,
        hit_price: float,
        hit_tp: Optional[float] = None,
        hit_r: Optional[float] = None,
    ):
        """記錄止損或止盈命中事件"""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO tp_sl_events (
                    signal_id, symbol, event_type, hit_price, hit_tp, hit_r, timestamp
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (
                signal_id,
                symbol,
                event_type,
                hit_price,
                hit_tp,
                hit_r,
                datetime.now(timezone.utc).isoformat(),
            ))

    def get_tp_sl_event(self, signal_id: int, event_type: str) -> Optional[Dict[str, Any]]:
        """查詢是否已存在指定的 TP/SL 事件記錄"""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT * FROM tp_sl_events
                WHERE signal_id = ? AND event_type = ?
            """, (signal_id, event_type))
            row = cursor.fetchone()
            if row:
                return dict(row)
            return None

    # ── 交易平倉記錄 ─────────────────────────────────
    def record_trade_close(
        self,
        signal_id: int,
        symbol: str,
        entry_price: float,
        exit_price: float,
        direction: str,
        pnl_percent: float,
        close_reason: str,
        entry_timestamp: str,
        bars_held: int,
    ):
        """記錄一筆完整的交易平倉"""
        is_win = 1 if pnl_percent > 0 else 0
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO trade_closes (
                    signal_id, symbol, entry_price, exit_price, direction,
                    pnl_percent, is_win, close_reason, entry_timestamp,
                    close_timestamp, bars_held
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                signal_id,
                symbol,
                entry_price,
                exit_price,
                direction,
                pnl_percent,
                is_win,
                close_reason,
                entry_timestamp,
                datetime.now(timezone.utc).isoformat(),
                bars_held,
            ))

    # ── 統計更新 ─────────────────────────────────────
    def update_symbol_stats(
        self,
        symbol: str,
        interval: str,
        signal_sent: bool,
        direction: Optional[str] = None,
        pnl: Optional[float] = None,
        is_win: Optional[bool] = None,
        entry_price: Optional[float] = None,
        entry_dir: Optional[str] = None,
    ):
        """更新幣種統計數據"""
        with self._get_connection() as conn:
            cursor = conn.cursor()

            # 檢查是否已存在
            cursor.execute(
                "SELECT * FROM symbol_stats WHERE symbol = ?", (symbol,)
            )
            row = cursor.fetchone()

            now_str = datetime.now(timezone.utc).isoformat()

            if row is None:
                # 新增記錄
                cursor.execute("""
                    INSERT INTO symbol_stats (
                        symbol, interval, total_signals, buy_signals, sell_signals,
                        skipped_signals, total_trades, win_trades, realized_pnl,
                        last_signal_time, last_entry_price, last_entry_dir, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    symbol,
                    interval,
                    1 if signal_sent else 0,
                    1 if signal_sent and direction == "BUY" else 0,
                    1 if signal_sent and direction == "SELL" else 0,
                    0 if signal_sent else 1,
                    0,
                    0,
                    0.0,
                    now_str if signal_sent else None,
                    entry_price,
                    entry_dir,
                    now_str,
                ))
            else:
                # 更新現有記錄
                updates = ["updated_at = ?"]
                params: List[Any] = [now_str]

                if signal_sent:
                    updates.append("total_signals = total_signals + 1")
                    updates.append("last_signal_time = ?")
                    params.append(now_str)
                    if direction == "BUY":
                        updates.append("buy_signals = buy_signals + 1")
                    elif direction == "SELL":
                        updates.append("sell_signals = sell_signals + 1")

                if not signal_sent:
                    updates.append("skipped_signals = skipped_signals + 1")

                if pnl is not None:
                    updates.append("realized_pnl = realized_pnl + ?")
                    params.append(pnl)

                if is_win is not None and is_win:
                    updates.append("win_trades = win_trades + 1")

                if pnl is not None:  # 有 PnL 代表完成一筆交易
                    updates.append("total_trades = total_trades + 1")

                if entry_price is not None:
                    updates.append("last_entry_price = ?")
                    params.append(entry_price)

                if entry_dir is not None:
                    updates.append("last_entry_dir = ?")
                    params.append(entry_dir)

                query = f"""
                    UPDATE symbol_stats
                    SET {", ".join(updates)}
                    WHERE symbol = ?
                """
                params.append(symbol)
                cursor.execute(query, params)

    # ── 查詢統計 ─────────────────────────────────────
    def get_symbol_stats(self, symbol: str) -> Optional[Dict[str, Any]]:
        """取得單一幣種的統計數據"""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT * FROM symbol_stats WHERE symbol = ?", (symbol,)
            )
            row = cursor.fetchone()
            if row:
                return dict(row)
            return None

    def get_all_stats(self) -> List[Dict[str, Any]]:
        """取得所有幣種的統計數據"""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM symbol_stats ORDER BY symbol")
            return [dict(row) for row in cursor.fetchall()]

    # ── 查詢歷史訊號 ─────────────────────────────────
    def get_recent_signals(
        self, symbol: Optional[str] = None, limit: int = 50
    ) -> List[Dict[str, Any]]:
        """取得最近的訊號記錄"""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            if symbol:
                cursor.execute(
                    """
                    SELECT * FROM signals
                    WHERE symbol = ?
                    ORDER BY timestamp DESC
                    LIMIT ?
                    """,
                    (symbol, limit),
                )
            else:
                cursor.execute(
                    """
                    SELECT * FROM signals
                    ORDER BY timestamp DESC
                    LIMIT ?
                    """,
                    (limit,),
                )
            return [dict(row) for row in cursor.fetchall()]

    def get_trade_history(
        self, symbol: Optional[str] = None, limit: int = 100
    ) -> List[Dict[str, Any]]:
        """取得交易平倉歷史"""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            if symbol:
                cursor.execute(
                    """
                    SELECT tc.*, s.direction as signal_direction
                    FROM trade_closes tc
                    JOIN signals s ON tc.signal_id = s.id
                    WHERE tc.symbol = ?
                    ORDER BY tc.close_timestamp DESC
                    LIMIT ?
                    """,
                    (symbol, limit),
                )
            else:
                cursor.execute(
                    """
                    SELECT tc.*, s.direction as signal_direction
                    FROM trade_closes tc
                    JOIN signals s ON tc.signal_id = s.id
                    ORDER BY tc.close_timestamp DESC
                    LIMIT ?
                    """,
                    (limit,),
                )
            return [dict(row) for row in cursor.fetchall()]

    # ── 系統日誌 ─────────────────────────────────────
    def log_system_event(
        self, event_type: str, message: str, details: Optional[str] = None
    ):
        """記錄系統事件"""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                INSERT INTO system_logs (event_type, message, details, timestamp)
                VALUES (?, ?, ?, ?)
                """,
                (event_type, message, details, datetime.now(timezone.utc).isoformat()),
            )

    # ── 報表生成 ─────────────────────────────────────
    def generate_performance_report(
        self, days: int = 7
    ) -> Dict[str, Any]:
        """生成績效報告"""
        with self._get_connection() as conn:
            cursor = conn.cursor()

            # 計算日期範圍
            from datetime import timedelta
            cutoff = (
                datetime.now(timezone.utc) - timedelta(days=days)
            ).isoformat()

            # 總體統計
            cursor.execute(
                """
                SELECT
                    COUNT(*) as total_trades,
                    SUM(CASE WHEN is_win = 1 THEN 1 ELSE 0 END) as wins,
                    SUM(pnl_percent) as total_pnl,
                    AVG(pnl_percent) as avg_pnl,
                    MAX(pnl_percent) as max_pnl,
                    MIN(pnl_percent) as min_pnl
                FROM trade_closes
                WHERE close_timestamp >= ?
                """,
                (cutoff,),
            )
            overall = dict(cursor.fetchone())

            # 各幣種統計
            cursor.execute(
                """
                SELECT
                    symbol,
                    COUNT(*) as trades,
                    SUM(CASE WHEN is_win = 1 THEN 1 ELSE 0 END) as wins,
                    SUM(pnl_percent) as pnl,
                    AVG(pnl_percent) as avg_pnl
                FROM trade_closes
                WHERE close_timestamp >= ?
                GROUP BY symbol
                ORDER BY pnl DESC
                """,
                (cutoff,),
            )
            by_symbol = [dict(row) for row in cursor.fetchall()]

            # 每日盈虧
            cursor.execute(
                """
                SELECT
                    DATE(close_timestamp) as date,
                    SUM(pnl_percent) as daily_pnl,
                    COUNT(*) as trades
                FROM trade_closes
                WHERE close_timestamp >= ?
                GROUP BY DATE(close_timestamp)
                ORDER BY date DESC
                """,
                (cutoff,),
            )
            daily = [dict(row) for row in cursor.fetchall()]

            return {
                "period_days": days,
                "overall": overall,
                "by_symbol": by_symbol,
                "daily": daily,
            }

    # ── 工具方法 ─────────────────────────────────────
    def clear_all_data(self):
        """清除所有數據（慎用）"""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("DELETE FROM trade_closes")
            cursor.execute("DELETE FROM tp_sl_events")
            cursor.execute("DELETE FROM signals")
            cursor.execute("DELETE FROM symbol_stats")
            cursor.execute("DELETE FROM system_logs")
            logger.warning("所有資料庫數據已清除")

    def export_to_csv(self, output_dir: str = "exports"):
        """匯出數據到 CSV 文件"""
        import csv
        from pathlib import Path

        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)

        with self._get_connection() as conn:
            cursor = conn.cursor()

            # 匯出訊號
            cursor.execute("SELECT * FROM signals ORDER BY timestamp")
            with open(output_path / "signals.csv", "w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=cursor.description)
                writer.writeheader()
                writer.writerows([dict(row) for row in cursor.fetchall()])

            # 匯出交易
            cursor.execute("SELECT * FROM trade_closes ORDER BY close_timestamp")
            with open(output_path / "trades.csv", "w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=cursor.description)
                writer.writeheader()
                writer.writerows([dict(row) for row in cursor.fetchall()])

            logger.info(f"數據已匯出至 {output_path.absolute()}")
