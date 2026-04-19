#!/usr/bin/env python3
"""
SATS Bot 歷史數據查看工具
提供多種查詢方式來檢視交易歷史與績效統計
"""

import sqlite3
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path
import sys
import shutil

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
    fixes = [
        # signals 表 - 檢查可能缺失的欄位
        ("signals", "signal_type", "TEXT"),
        ("signals", "entry_price", "REAL"),
        ("signals", "status", "TEXT DEFAULT 'ACTIVE'"),
        
        # trade_closes 表 - 檢查可能缺失的欄位
        ("trade_closes", "exit_reason", "TEXT"),
        ("trade_closes", "closed_at", "TIMESTAMP"),
        ("trade_closes", "pnl", "REAL"),
        
        # symbol_stats 表 - 檢查可能缺失的欄位
        ("symbol_stats", "winning_trades", "INTEGER DEFAULT 0"),
        ("symbol_stats", "losing_trades", "INTEGER DEFAULT 0"),
        ("symbol_stats", "win_rate", "REAL DEFAULT 0.0"),
        ("symbol_stats", "total_pnl", "REAL DEFAULT 0.0"),
        ("symbol_stats", "avg_pnl", "REAL DEFAULT 0.0"),
        ("symbol_stats", "max_profit", "REAL DEFAULT 0.0"),
        ("symbol_stats", "max_loss", "REAL DEFAULT 0.0"),
        ("symbol_stats", "last_updated", "TIMESTAMP"),
        
        # tp_sl_events 表 - 檢查可能缺失的欄位
        ("tp_sl_events", "tp_level", "TEXT"),
        ("tp_sl_events", "trigger_price", "REAL"),
        ("tp_sl_events", "pnl_locked", "REAL"),
        ("tp_sl_events", "triggered_at", "TIMESTAMP"),
    ]

    print("🔍 開始檢查並修復資料庫結構...\n")
    
    fixed_count = 0
    for table, column, dtype in fixes:
        try:
            # 嘗試新增欄位
            cursor.execute(f"ALTER TABLE {table} ADD COLUMN {column} {dtype}")
            print(f"➕ 新增 {table}.{column} ({dtype})...")
            
            # 根據欄位類型更新舊數據
            now = datetime.now().isoformat()
            update_sql = None
            if "TIMESTAMP" in dtype:
                update_sql = f"UPDATE {table} SET {column} = ? WHERE {column} IS NULL"
                params = (now,)
            elif "DEFAULT 0" in dtype or "INTEGER" in dtype:
                update_sql = f"UPDATE {table} SET {column} = 0 WHERE {column} IS NULL"
                params = ()
            elif "DEFAULT 0.0" in dtype or "REAL" in dtype:
                update_sql = f"UPDATE {table} SET {column} = 0.0 WHERE {column} IS NULL"
                params = ()
            elif "DEFAULT 'ACTIVE'" in dtype:
                update_sql = f"UPDATE {table} SET {column} = 'ACTIVE' WHERE {column} IS NULL"
                params = ()
            
            if update_sql:
                if params:
                    cursor.execute(update_sql, params)
                else:
                    cursor.execute(update_sql)
                print(f"   📝 已更新舊數據預設值")
            
            conn.commit()
            fixed_count += 1
            
        except sqlite3.OperationalError as e:
            if "duplicate column" in str(e).lower():
                print(f"✓ {table}.{column} 已存在，跳過")
            elif "no such table" in str(e).lower():
                print(f"⚠️ 表 {table} 不存在，跳過")
            else:
                print(f"⚠️ 錯誤 {table}.{column}: {e}")
    
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
    query = """
        SELECT 
            symbol,
            total_trades,
            win_trades as winning_trades,
            (total_trades - win_trades) as losing_trades,
            CASE WHEN total_trades > 0 THEN ROUND(CAST(win_trades AS REAL) / total_trades * 100, 2) ELSE 0.0 END as win_rate,
            realized_pnl as total_pnl,
            CASE WHEN total_trades > 0 THEN ROUND(realized_pnl / total_trades, 2) ELSE 0.0 END as avg_pnl,
            0.0 as max_profit,
            0.0 as max_loss,
            updated_at as last_updated
        FROM symbol_stats
        ORDER BY realized_pnl DESC
    """
    
    df = pd.read_sql_query(query, conn)
    conn.close()
    
    if df.empty:
        print("暫無統計數據")
        return
    
    # 格式化顯示
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', None)
    pd.set_option('display.float_format', lambda x: f'{x:.2f}')
    
    print(df.to_string(index=False))
    print(f"\n總計幣種數量：{len(df)}")

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
            direction as signal_type,
            price as entry_price,
            score,
            CASE WHEN sent = 1 THEN 'SENT' ELSE 'SKIPPED' END as status,
            timestamp as created_at
        FROM signals
        ORDER BY timestamp DESC
        LIMIT ?
    """
    
    df = pd.read_sql_query(query, conn, params=(limit,))
    conn.close()
    
    if df.empty:
        print("暫無訊號記錄")
        return
    
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', None)
    
    print(df.to_string(index=False))

def view_trade_history(symbol=None, limit=50):
    """查看交易歷史（含止盈止損）"""
    print("\n" + "="*80)
    if symbol:
        print(f"💹 {symbol} 交易歷史")
    else:
        print(f"💹 全部交易歷史 (最近 {limit} 筆)")
    print("="*80)
    
    conn = get_db_connection()
    
    if symbol:
        query = """
            SELECT 
                s.symbol,
                s.direction as signal_type,
                s.price as entry_price,
                tc.exit_price,
                tc.pnl_percent as pnl,
                tc.pnl_percent,
                tc.close_reason as exit_reason,
                s.entry_timestamp as entry_time,
                tc.close_timestamp as exit_time
            FROM signals s
            JOIN trade_closes tc ON s.id = tc.signal_id
            WHERE s.symbol = ?
            ORDER BY tc.close_timestamp DESC
            LIMIT ?
        """
        df = pd.read_sql_query(query, conn, params=(symbol, limit))
    else:
        query = """
            SELECT 
                s.symbol,
                s.direction as signal_type,
                s.price as entry_price,
                tc.exit_price,
                tc.pnl_percent as pnl,
                tc.pnl_percent,
                tc.close_reason as exit_reason,
                s.timestamp as entry_time,
                tc.close_timestamp as exit_time
            FROM signals s
            JOIN trade_closes tc ON s.id = tc.signal_id
            ORDER BY tc.close_timestamp DESC
            LIMIT ?
        """
        df = pd.read_sql_query(query, conn, params=(limit,))
    
    conn.close()
    
    if df.empty:
        print("暫無交易歷史")
        return
    
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', None)
    pd.set_option('display.float_format', lambda x: f'{x:.2f}')
    
    print(df.to_string(index=False))

def view_tp_sl_events(symbol=None, limit=30):
    """查看止盈止損命中事件"""
    print("\n" + "="*80)
    if symbol:
        print(f"🎯 {symbol} 止盈止損事件")
    else:
        print(f"🎯 全部止盈止損事件 (最近 {limit} 筆)")
    print("="*80)
    
    conn = get_db_connection()
    
    if symbol:
        query = """
            SELECT 
                symbol,
                event_type as tp_level,
                hit_price as trigger_price,
                hit_r as pnl_locked,
                timestamp as triggered_at
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
                event_type as tp_level,
                hit_price as trigger_price,
                hit_r as pnl_locked,
                timestamp as triggered_at
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
    pd.set_option('display.float_format', lambda x: f'{x:.2f}')
    
    print(df.to_string(index=False))

def generate_performance_report(days=7):
    """生成績效報告"""
    print("\n" + "="*80)
    print(f"📈 績效報告 (過去 {days} 天)")
    print("="*80)
    
    conn = get_db_connection()
    
    # 計算時間範圍
    start_date = datetime.now() - timedelta(days=days)
    
    # 總體統計
    summary_query = """
        SELECT 
            COUNT(*) as total_trades,
            SUM(CASE WHEN tc.pnl > 0 THEN 1 ELSE 0 END) as winning_trades,
            SUM(CASE WHEN tc.pnl <= 0 THEN 1 ELSE 0 END) as losing_trades,
            SUM(tc.pnl) as total_pnl,
            AVG(tc.pnl) as avg_pnl,
            MAX(tc.pnl) as max_profit,
            MIN(tc.pnl) as max_loss
        FROM trade_closes tc
        JOIN signals s ON tc.signal_id = s.id
        WHERE tc.closed_at >= ?
    """
    
    summary_df = pd.read_sql_query(summary_query, conn, params=(start_date,))
    
    if summary_df.iloc[0]['total_trades'] == 0:
        print(f"過去 {days} 天無交易記錄")
        conn.close()
        return
    
    # 計算勝率
    row = summary_df.iloc[0]
    win_rate = (row['winning_trades'] / row['total_trades'] * 100) if row['total_trades'] > 0 else 0
    
    print("\n【總體表現】")
    print(f"  交易次數：{int(row['total_trades'])}")
    print(f"  獲利次數：{int(row['winning_trades'])}")
    print(f"  虧損次數：{int(row['losing_trades'])}")
    print(f"  勝率：{win_rate:.2f}%")
    print(f"  總盈虧：{row['total_pnl']:.2f}")
    print(f"  平均盈虧：{row['avg_pnl']:.2f}")
    print(f"  最大獲利：{row['max_profit']:.2f}")
    print(f"  最大虧損：{row['max_loss']:.2f}")
    
    # 每日盈虧
    daily_query = """
        SELECT 
            DATE(tc.closed_at) as trade_date,
            COUNT(*) as trades,
            SUM(tc.pnl) as daily_pnl,
            SUM(CASE WHEN tc.pnl > 0 THEN tc.pnl ELSE 0 END) as profit,
            SUM(CASE WHEN tc.pnl <= 0 THEN tc.pnl ELSE 0 END) as loss
        FROM trade_closes tc
        JOIN signals s ON tc.signal_id = s.id
        WHERE tc.closed_at >= ?
        GROUP BY DATE(tc.closed_at)
        ORDER BY trade_date DESC
    """
    
    daily_df = pd.read_sql_query(daily_query, conn, params=(start_date,))
    
    print("\n【每日盈虧】")
    pd.set_option('display.float_format', lambda x: f'{x:.2f}')
    print(daily_df.to_string(index=False))
    
    # 幣種貢獻度
    symbol_query = """
        SELECT 
            s.symbol,
            COUNT(*) as trades,
            SUM(tc.pnl) as total_pnl,
            AVG(tc.pnl) as avg_pnl
        FROM trade_closes tc
        JOIN signals s ON tc.signal_id = s.id
        WHERE tc.closed_at >= ?
        GROUP BY s.symbol
        ORDER BY total_pnl DESC
    """
    
    symbol_df = pd.read_sql_query(symbol_query, conn, params=(start_date,))
    
    print("\n【幣種貢獻度】")
    print(symbol_df.to_string(index=False))
    
    conn.close()

def export_to_csv(output_dir="exports"):
    """匯出所有數據到 CSV"""
    print("\n" + "="*80)
    print("💾 匯出數據到 CSV")
    print("="*80)
    
    conn = get_db_connection()
    
    # 建立輸出目錄
    output_path = Path(output_dir)
    output_path.mkdir(exist_ok=True)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # 匯出各表
    tables = {
        'signals': f'signals_{timestamp}.csv',
        'trade_closes': f'trade_closes_{timestamp}.csv',
        'tp_sl_events': f'tp_sl_events_{timestamp}.csv',
        'symbol_stats': f'symbol_stats_{timestamp}.csv'
    }
    
    for table, filename in tables.items():
        query = f"SELECT * FROM {table}"
        df = pd.read_sql_query(query, conn)
        filepath = output_path / filename
        df.to_csv(filepath, index=False, encoding='utf-8-sig')
        print(f"✓ 已匯出：{filepath}")
    
    conn.close()
    print(f"\n所有檔案已儲存至：{output_path.absolute()}")

def show_menu():
    """顯示選單"""
    print("\n" + "="*80)
    print("🔍 SATS Bot 歷史數據查看工具")
    print("="*80)
    print("1. 查看幣種績效統計")
    print("2. 查看最近交易訊號")
    print("3. 查看交易歷史")
    print("4. 查看止盈止損事件")
    print("5. 生成績效報告")
    print("6. 匯出數據到 CSV")
    print("7. 自訂 SQL 查詢")
    print("8. 修復資料庫結構")
    print("0. 退出")
    print("="*80)

def custom_sql_query():
    """執行自訂 SQL 查詢"""
    print("\n輸入 SQL 查詢语句 (僅支援 SELECT):")
    print("範例：SELECT symbol, COUNT(*) FROM signals GROUP BY symbol")
    
    sql = input("> ").strip()
    
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
    while True:
        show_menu()
        choice = input("請選擇功能 (0-8): ").strip()
        
        if choice == '1':
            view_symbol_stats()
        elif choice == '2':
            try:
                limit = int(input("輸入查詢筆數 (預設 20): ").strip() or "20")
                view_recent_signals(limit)
            except ValueError:
                view_recent_signals(20)
        elif choice == '3':
            symbol = input("輸入幣種符號 (留空查看全部): ").strip().upper()
            try:
                limit = int(input("輸入查詢筆數 (預設 50): ").strip() or "50")
            except ValueError:
                limit = 50
            view_trade_history(symbol if symbol else None, limit)
        elif choice == '4':
            symbol = input("輸入幣種符號 (留空查看全部): ").strip().upper()
            try:
                limit = int(input("輸入查詢筆數 (預設 30): ").strip() or "30")
            except ValueError:
                limit = 30
            view_tp_sl_events(symbol if symbol else None, limit)
        elif choice == '5':
            try:
                days = int(input("輸入天數 (預設 7): ").strip() or "7")
            except ValueError:
                days = 7
            generate_performance_report(days)
        elif choice == '6':
            export_to_csv()
        elif choice == '7':
            custom_sql_query()
        elif choice == '8':
            fix_database_structure()
        elif choice == '0':
            print("👋 再見！")
            break
        else:
            print("❌ 無效選項，請重新選擇")

if __name__ == "__main__":
    main()
