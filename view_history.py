#!/usr/bin/env python3
"""
SATS Bot 歷史數據查看工具
提供多種查詢方式來檢視交易歷史與績效統計
"""

import sqlite3
import shutil
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd

# 嘗試載入 SATSDatabase（若在專案根目錄外執行則 fallback 到直接 sqlite3）
try:
    sys.path.insert(0, str(Path(__file__).parent.parent))
    from core.database import SATSDatabase
    _HAS_DB_CLASS = True
except ImportError:
    _HAS_DB_CLASS = False

# 資料庫路徑
DB_PATH = Path("sats_bot.db")

def get_db_connection():
    """建立資料庫連線"""
    if not DB_PATH.exists():
        print(f"❌ 錯誤：資料庫檔案 {DB_PATH} 不存在")
        print("請先執行交易機器人產生數據")
        sys.exit(1)
    
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def fix_database_structure():
    """檢查並修復資料庫結構，自動新增缺失的欄位"""
    if not DB_PATH.exists():
        print(f"❌ 錯誤：找不到資料庫檔案 {DB_PATH}")
        return False

    # 自動備份
    backup_path = f"{DB_PATH}.backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    try:
        shutil.copy2(DB_PATH, backup_path)
        print(f"💾 已建立備份：{backup_path}")
    except Exception as e:
        print(f"⚠️ 備份失敗：{e}")

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # 定義所有需要檢查的欄位 (根據 core/database.py 的結構)
    # 格式: (table, column, dtype, default_value_if_needed)
    fixes = [
        # signals 表
        ("signals", "sent", "INTEGER DEFAULT 1"),
        ("signals", "interval", "TEXT"),
        ("signals", "tp1_r", "REAL"),
        ("signals", "tp2_r", "REAL"),
        ("signals", "tp3_r", "REAL"),
        ("signals", "tqi", "REAL"),
        
        # trade_closes 表
        ("trade_closes", "is_win", "INTEGER"),
        ("trade_closes", "bars_held", "INTEGER"),
        
        # symbol_stats 表
        ("symbol_stats", "buy_signals", "INTEGER DEFAULT 0"),
        ("symbol_stats", "sell_signals", "INTEGER DEFAULT 0"),
        ("symbol_stats", "realized_pnl", "REAL DEFAULT 0.0"),
    ]

    print("🔍 開始檢查並修復資料庫結構...\n")
    
    fixed_count = 0
    for table, column, dtype in fixes:
        try:
            # 檢查欄位是否存在
            cursor.execute(f"PRAGMA table_info({table})")
            columns = [info[1] for info in cursor.fetchall()]
            
            if column not in columns:
                cursor.execute(f"ALTER TABLE {table} ADD COLUMN {column} {dtype}")
                print(f"➕ 新增 {table}.{column} ({dtype})...")
                fixed_count += 1
            else:
                # print(f"✓ {table}.{column} 已存在")
                pass
                
        except sqlite3.OperationalError as e:
            if "no such table" in str(e).lower():
                print(f"⚠️ 表 {table} 不存在，跳過")
            else:
                print(f"⚠️ 錯誤 {table}.{column}: {e}")
    
    conn.commit()
    conn.close()
    
    print("\n" + "="*50)
    if fixed_count > 0:
        print(f"✅ 成功修復 {fixed_count} 個欄位！")
    else:
        print("✅ 資料庫結構已是最新，無需修復。")
    print("="*50)
    return True

def view_symbol_stats():
    """查看所有幣種的統計數據"""
    print("\n" + "="*80)
    print("📊 幣種績效統計")
    print("="*80)
    
    conn = get_db_connection()
    # 根據 core/database.py 的欄位名稱
    query = """
        SELECT 
            symbol,
            interval,
            total_signals,
            buy_signals,
            sell_signals,
            total_trades,
            win_trades,
            (total_trades - win_trades) as lose_trades,
            CASE WHEN total_trades > 0 THEN ROUND(CAST(win_trades AS REAL) / total_trades * 100, 2) ELSE 0.0 END as win_rate,
            ROUND(realized_pnl, 4) as realized_pnl,
            updated_at
        FROM symbol_stats
        ORDER BY realized_pnl DESC
    """
    
    try:
        df = pd.read_sql_query(query, conn)
        conn.close()
        
        if df.empty:
            print("暫無統計數據")
            return
        
        # 格式化顯示
        pd.set_option('display.max_columns', None)
        pd.set_option('display.width', None)
        pd.set_option('display.float_format', lambda x: f'{x:.4f}')
        
        print(df.to_string(index=False))
        print(f"\n總計幣種數量：{len(df)}")
    except Exception as e:
        print(f"❌ 查詢錯誤：{e}")
        conn.close()

def view_recent_signals(limit=20):
    """查看最近的交易訊號"""
    print("\n" + "="*80)
    print(f"🔔 最近 {limit} 筆交易訊號")
    print("="*80)
    
    conn = get_db_connection()
    query = """
        SELECT 
            id,
            symbol,
            interval,
            direction,
            price,
            score,
            tqi,
            CASE WHEN sent = 1 THEN 'SENT' ELSE 'SKIPPED' END as status,
            timestamp
        FROM signals
        ORDER BY timestamp DESC
        LIMIT ?
    """
    
    try:
        df = pd.read_sql_query(query, conn, params=(limit,))
        conn.close()
        
        if df.empty:
            print("暫無訊號記錄")
            return
        
        pd.set_option('display.max_columns', None)
        pd.set_option('display.width', None)
        
        print(df.to_string(index=False))
    except Exception as e:
        print(f"❌ 查詢錯誤：{e}")
        conn.close()

def view_trade_history(symbol=None, limit=50):
    """查看交易歷史（含止盈止損）"""
    print("\n" + "="*80)
    if symbol:
        print(f"💹 {symbol} 交易歷史")
    else:
        print(f"💹 全部交易歷史 (最近 {limit} 筆)")
    print("="*80)
    
    conn = get_db_connection()
    
    try:
        if symbol:
            query = """
                SELECT 
                    symbol,
                    direction,
                    entry_price,
                    exit_price,
                    pnl_percent,
                    close_reason,
                    entry_timestamp,
                    close_timestamp,
                    bars_held
                FROM trade_closes
                WHERE symbol = ?
                ORDER BY close_timestamp DESC
                LIMIT ?
            """
            df = pd.read_sql_query(query, conn, params=(symbol, limit))
        else:
            query = """
                SELECT 
                    symbol,
                    direction,
                    entry_price,
                    exit_price,
                    pnl_percent,
                    close_reason,
                    entry_timestamp,
                    close_timestamp,
                    bars_held
                FROM trade_closes
                ORDER BY close_timestamp DESC
                LIMIT ?
            """
            df = pd.read_sql_query(query, conn, params=(limit,))
        
        conn.close()
        
        if df.empty:
            print("暫無交易歷史")
            return
        
        pd.set_option('display.max_columns', None)
        pd.set_option('display.width', None)
        pd.set_option('display.float_format', lambda x: f'{x:.4f}')
        
        print(df.to_string(index=False))
    except Exception as e:
        print(f"❌ 查詢錯誤：{e}")
        conn.close()

def view_tp_sl_events(symbol=None, limit=30):
    """查看止盈止損命中事件"""
    print("\n" + "="*80)
    if symbol:
        print(f"🎯 {symbol} 止盈止損事件")
    else:
        print(f"🎯 全部止盈止損事件 (最近 {limit} 筆)")
    print("="*80)
    
    conn = get_db_connection()
    
    try:
        if symbol:
            query = """
                SELECT 
                    symbol,
                    event_type,
                    hit_price,
                    hit_tp,
                    hit_r,
                    timestamp
                FROM tp_sl_events
                WHERE symbol = ?
                ORDER BY timestamp DESC
                LIMIT ?
            """
            df = pd.read_sql_query(query, conn, params=(symbol, limit))
        else:
            query = """
                SELECT 
                    symbol,
                    event_type,
                    hit_price,
                    hit_tp,
                    hit_r,
                    timestamp
                FROM tp_sl_events
                ORDER BY timestamp DESC
                LIMIT ?
            """
            df = pd.read_sql_query(query, conn, params=(limit,))
        
        conn.close()
        
        if df.empty:
            print("暫無止盈止損事件")
            return
        
        pd.set_option('display.max_columns', None)
        pd.set_option('display.width', None)
        pd.set_option('display.float_format', lambda x: f'{x:.4f}')
        
        print(df.to_string(index=False))
    except Exception as e:
        print(f"❌ 查詢錯誤：{e}")
        conn.close()

def generate_performance_report(days=7):
    """生成績效報告"""
    print("\n" + "="*80)
    print(f"📈 績效報告 (過去 {days} 天)")
    print("="*80)
    
    conn = get_db_connection()
    
    # 計算時間範圍 (ISO 格式)
    start_date = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()
    
    try:
        # 總體統計
        summary_query = """
            SELECT 
                COUNT(*) as total_trades,
                SUM(CASE WHEN pnl_percent > 0 THEN 1 ELSE 0 END) as winning_trades,
                SUM(CASE WHEN pnl_percent <= 0 THEN 1 ELSE 0 END) as losing_trades,
                SUM(pnl_percent) as total_pnl,
                AVG(pnl_percent) as avg_pnl,
                MAX(pnl_percent) as max_profit,
                MIN(pnl_percent) as max_loss
            FROM trade_closes
            WHERE close_timestamp >= ?
        """
        summary_df = pd.read_sql_query(summary_query, conn, params=(start_date,))
        
        if summary_df.empty or summary_df.iloc[0]['total_trades'] == 0:
            print(f"過去 {days} 天無交易記錄")
            conn.close()
            return
        
        # 計算勝率
        row = summary_df.iloc[0]
        total = row['total_trades']
        win_rate = (row['winning_trades'] / total * 100) if total > 0 else 0
        
        print("\n【總體表現】")
        print(f"  交易次數：{int(total)}")
        print(f"  獲利次數：{int(row['winning_trades'])}")
        print(f"  虧損次數：{int(row['losing_trades'])}")
        print(f"  勝率：{win_rate:.2f}%")
        print(f"  總盈虧：{row['total_pnl']:.4f}%")
        print(f"  平均盈虧：{row['avg_pnl']:.4f}%")
        print(f"  最大獲利：{row['max_profit']:.4f}%")
        print(f"  最大虧損：{row['max_loss']:.4f}%")
        
        # 每日盈虧
        daily_query = """
            SELECT 
                DATE(close_timestamp) as trade_date,
                COUNT(*) as trades,
                SUM(pnl_percent) as daily_pnl,
                SUM(CASE WHEN pnl_percent > 0 THEN pnl_percent ELSE 0 END) as profit,
                SUM(CASE WHEN pnl_percent <= 0 THEN pnl_percent ELSE 0 END) as loss
            FROM trade_closes
            WHERE close_timestamp >= ?
            GROUP BY DATE(close_timestamp)
            ORDER BY trade_date DESC
        """
        daily_df = pd.read_sql_query(daily_query, conn, params=(start_date,))
        
        print("\n【每日盈虧】")
        print(daily_df.to_string(index=False))
        
        # 幣種貢獻度
        symbol_query = """
            SELECT 
                symbol,
                COUNT(*) as trades,
                SUM(pnl_percent) as total_pnl,
                AVG(pnl_percent) as avg_pnl
            FROM trade_closes
            WHERE close_timestamp >= ?
            GROUP BY symbol
            ORDER BY total_pnl DESC
        """
        symbol_df = pd.read_sql_query(symbol_query, conn, params=(start_date,))
        
        print("\n【幣種貢獻度】")
        print(symbol_df.to_string(index=False))
        
        conn.close()
    except Exception as e:
        print(f"❌ 報告生成錯誤：{e}")
        conn.close()

def export_to_csv(output_dir="exports"):
    """匯出所有數據到 CSV"""
    print("\n" + "="*80)
    print("💾 匯出數據到 CSV")
    print("="*80)
    
    try:
        conn = get_db_connection()
        
        # 建立輸出目錄
        output_path = Path(output_dir)
        output_path.mkdir(exist_ok=True)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # 匯出各表
        tables = ['signals', 'trade_closes', 'tp_sl_events', 'symbol_stats', 'system_logs']
        
        for table in tables:
            query = f"SELECT * FROM {table}"
            try:
                df = pd.read_sql_query(query, conn)
                filename = f"{table}_{timestamp}.csv"
                filepath = output_path / filename
                df.to_csv(filepath, index=False, encoding='utf-8-sig')
                print(f"✓ 已匯出：{filepath}")
            except Exception as e:
                print(f"⚠️ 匯出表 {table} 失敗：{e}")
        
        conn.close()
        print(f"\n所有檔案已儲存至：{output_path.absolute()}")
    except Exception as e:
        print(f"❌ 匯出錯誤：{e}")

# ══════════════════════════════════════════════════
# 重置功能
# ══════════════════════════════════════════════════
def _backup_db() -> str:
    """建立備份，回傳備份路徑。"""
    backup_path = f"{DB_PATH}.backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    try:
        shutil.copy2(DB_PATH, backup_path)
        print(f"💾 已建立備份：{backup_path}")
    except Exception as e:
        print(f"⚠️  備份失敗：{e}")
        backup_path = ""
    return backup_path


def _confirm(prompt: str) -> bool:
    """要求使用者輸入 YES 確認。"""
    ans = input(f"{prompt}\n⚠️  此操作無法還原（已自動備份）。輸入 YES 確認：").strip()
    return ans == "YES"


def reset_history():
    """互動式重置選單。"""
    if not DB_PATH.exists():
        print(f"❌ 找不到資料庫 {DB_PATH}")
        return

    # 取得各表記錄數預覽
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    counts = {}
    for t in ["signals", "tp_sl_events", "trade_closes", "symbol_stats", "system_logs"]:
        try:
            cursor.execute(f"SELECT COUNT(*) FROM {t}")
            counts[t] = cursor.fetchone()[0]
        except Exception:
            counts[t] = 0
    conn.close()

    print("\n" + "="*60)
    print("🗑️   歷史資料重置")
    print("="*60)
    print("目前資料量：")
    print(f"  signals      : {counts.get('signals', 0):>8,} 筆")
    print(f"  tp_sl_events : {counts.get('tp_sl_events', 0):>8,} 筆")
    print(f"  trade_closes : {counts.get('trade_closes', 0):>8,} 筆")
    print(f"  symbol_stats : {counts.get('symbol_stats', 0):>8,} 筆")
    print(f"  system_logs  : {counts.get('system_logs', 0):>8,} 筆")
    print("="*60)
    print("請選擇重置範圍：")
    print("  1. 全部清除（保留資料庫結構）")
    print("  2. 只重置統計數據（symbol_stats 歸零，保留訊號與交易記錄）")
    print("  3. 只清除交易記錄（trade_closes + tp_sl_events，保留 signals）")
    print("  4. 清除指定幣種的所有記錄")
    print("  5. 清除指定日期之前的記錄")
    print("  0. 返回")
    print("="*60)

    choice = input("請選擇 (0-5): ").strip()

    if choice == "0":
        return

    # 全部清除
    if choice == "1":
        if not _confirm("即將清除所有歷史資料（signals / tp_sl_events / trade_closes / symbol_stats / system_logs）。"):
            print("已取消")
            return
        _backup_db()
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        for t in ["trade_closes", "tp_sl_events", "signals", "symbol_stats", "system_logs"]:
            cursor.execute(f"DELETE FROM {t}")
        conn.commit()
        conn.close()
        print("✅ 全部資料已清除")

    # 只重置統計
    elif choice == "2":
        if not _confirm("即將將 symbol_stats 所有欄位歸零（訊號與交易記錄保留）。"):
            print("已取消")
            return
        _backup_db()
        now_str = datetime.now(timezone.utc).isoformat()
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute("""
            UPDATE symbol_stats SET
                total_signals=0, buy_signals=0, sell_signals=0, skipped_signals=0,
                total_trades=0, win_trades=0, realized_pnl=0.0,
                last_signal_time=NULL, last_entry_price=NULL, last_entry_dir=NULL,
                updated_at=?
        """, (now_str,))
        affected = cursor.rowcount
        conn.commit()
        conn.close()
        print(f"✅ symbol_stats 已歸零（{affected} 個幣種）")

    # 只清除交易記錄
    elif choice == "3":
        if not _confirm("即將清除所有 trade_closes 與 tp_sl_events（signals 保留）。"):
            print("已取消")
            return
        _backup_db()
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute("DELETE FROM trade_closes")
        tc = cursor.rowcount
        cursor.execute("DELETE FROM tp_sl_events")
        te = cursor.rowcount
        conn.commit()
        conn.close()
        print(f"✅ 已清除 trade_closes ({tc} 筆) 與 tp_sl_events ({te} 筆)")

    # 清除指定幣種
    elif choice == "4":
        symbol = input("輸入幣種符號（例如 BTCUSDT）：").strip().upper()
        if not symbol:
            print("已取消")
            return
        # 預覽
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM signals WHERE symbol=?", (symbol,))
        sig_cnt = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(*) FROM trade_closes WHERE symbol=?", (symbol,))
        tc_cnt = cursor.fetchone()[0]
        conn.close()
        if sig_cnt == 0 and tc_cnt == 0:
            print(f"⚠️  找不到 {symbol} 的任何記錄")
            return
        print(f"找到 {symbol}：signals={sig_cnt} 筆，trade_closes={tc_cnt} 筆")
        if not _confirm(f"即將刪除 {symbol} 的所有記錄。"):
            print("已取消")
            return
        _backup_db()
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute("SELECT id FROM signals WHERE symbol=?", (symbol,))
        sig_ids = [r[0] for r in cursor.fetchall()]
        if sig_ids:
            ph = ",".join("?"*len(sig_ids))
            cursor.execute(f"DELETE FROM trade_closes WHERE signal_id IN ({ph})", sig_ids)
            cursor.execute(f"DELETE FROM tp_sl_events WHERE signal_id IN ({ph})", sig_ids)
        cursor.execute("DELETE FROM signals      WHERE symbol=?", (symbol,))
        cursor.execute("DELETE FROM symbol_stats WHERE symbol=?", (symbol,))
        conn.commit()
        conn.close()
        print(f"✅ {symbol} 的所有記錄已刪除（{len(sig_ids)} 筆訊號）")

    # 清除指定日期前的記錄
    elif choice == "5":
        date_str = input("清除哪個日期之前的記錄（格式 YYYY-MM-DD，例如 2026-01-01）：").strip()
        try:
            datetime.strptime(date_str, "%Y-%m-%d")
        except ValueError:
            print("❌ 日期格式錯誤")
            return
        # 預覽
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM signals WHERE timestamp < ?", (date_str,))
        s_cnt = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(*) FROM trade_closes WHERE close_timestamp < ?", (date_str,))
        tc_cnt = cursor.fetchone()[0]
        conn.close()
        print(f"將刪除：signals={s_cnt} 筆，trade_closes={tc_cnt} 筆（{date_str} 之前）")
        if s_cnt == 0 and tc_cnt == 0:
            print("⚠️  該日期前無任何記錄")
            return
        if not _confirm(f"即將刪除 {date_str} 之前的所有記錄。"):
            print("已取消")
            return
        _backup_db()
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute("DELETE FROM trade_closes WHERE close_timestamp < ?", (date_str,))
        cursor.execute("DELETE FROM tp_sl_events WHERE timestamp       < ?", (date_str,))
        cursor.execute("DELETE FROM signals       WHERE timestamp       < ?", (date_str,))
        cursor.execute("DELETE FROM system_logs   WHERE timestamp       < ?", (date_str,))
        conn.commit()
        conn.close()
        print(f"✅ {date_str} 之前的記錄已清除")

    else:
        print("❌ 無效選項")


def show_menu():
    """顯示選單"""
    print("\n" + "="*80)
    print("🔍 SATS Bot 歷史數據查看工具")
    print("="*80)
    print("1. 查看幣種績效統計 (Symbol Stats)")
    print("2. 查看最近交易訊號 (Signals)")
    print("3. 查看交易歷史 (Trade History)")
    print("4. 查看止盈止損事件 (TP/SL Events)")
    print("5. 生成績效報告 (Performance Report)")
    print("6. 匯出數據到 CSV")
    print("7. 自訂 SQL 查詢")
    print("8. 修復資料庫結構 (Fix DB Schema)")
    print("9. 重置歷史資料 (Reset History)")
    print("0. 退出")
    print("="*80)

def custom_sql_query():
    """執行自訂 SQL 查詢"""
    print("\n輸入 SQL 查詢語句 (僅支援 SELECT):")
    print("範例：SELECT symbol, COUNT(*) FROM signals GROUP BY symbol")
    
    sql = input("> ").strip()
    
    if not sql:
        return
        
    if not sql.upper().startswith('SELECT'):
        print("❌ 錯誤：僅支援 SELECT 查詢")
        return
    
    try:
        conn = get_db_connection()
        df = pd.read_sql_query(sql, conn)
        conn.close()
        
        if df.empty:
            print("查詢結果為空")
        else:
            pd.set_option('display.max_columns', None)
            pd.set_option('display.width', None)
            print(df.to_string(index=False))
    except Exception as e:
        print(f"❌ 查詢錯誤：{e}")

def main():
    """主程式"""
    # 檢查 pandas 是否安裝
    try:
        import pandas as pd
    except ImportError:
        print("❌ 錯誤：缺少 pandas 庫。請執行: pip install pandas")
        return

    while True:
        show_menu()
        choice = input("請選擇功能 (0-9): ").strip()
        
        if choice == '1':
            view_symbol_stats()
        elif choice == '2':
            try:
                limit_input = input("輸入查詢筆數 (預設 20): ").strip()
                limit = int(limit_input) if limit_input else 20
                view_recent_signals(limit)
            except ValueError:
                view_recent_signals(20)
        elif choice == '3':
            symbol = input("輸入幣種符號 (留空查看全部): ").strip().upper()
            try:
                limit_input = input("輸入查詢筆數 (預設 50): ").strip()
                limit = int(limit_input) if limit_input else 50
            except ValueError:
                limit = 50
            view_trade_history(symbol if symbol else None, limit)
        elif choice == '4':
            symbol = input("輸入幣種符號 (留空查看全部): ").strip().upper()
            try:
                limit_input = input("輸入查詢筆數 (預設 30): ").strip()
                limit = int(limit_input) if limit_input else 30
            except ValueError:
                limit = 30
            view_tp_sl_events(symbol if symbol else None, limit)
        elif choice == '5':
            try:
                days_input = input("輸入天數 (預設 7): ").strip()
                days = int(days_input) if days_input else 7
            except ValueError:
                days = 7
            generate_performance_report(days)
        elif choice == '6':
            export_to_csv()
        elif choice == '7':
            custom_sql_query()
        elif choice == '8':
            fix_database_structure()
        elif choice == '9':
            reset_history()
        elif choice == '0':
            print("👋 再見！")
            break
        else:
            print("❌ 無效選項，請重新選擇")

if __name__ == "__main__":
    main()
