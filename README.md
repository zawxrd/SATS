# SATS Bot — 本地版 Self-Aware Trend System

**Pine Script v1.9.0 完整移植 → Python + Binance WebSocket + Discord 通知**

---

## 📁 專案結構

```
sats_bot/
├── main.py                  ← 主程式入口
├── view_history.py          ← 歷史訊號查詢工具
├── diagnose_db.py           ← 資料庫診斷工具
├── sats_bot.db              ← SQLite 資料庫（自動建立）
├── requirements.txt
├── config/
│   └── config.yaml          ← 所有設定在這裡
├── core/
│   ├── engine.py            ← SATS 核心引擎（完整移植）
│   ├── database.py          ← SQLite 資料庫操作
│   ├── binance_ws.py        ← Binance WebSocket + REST 預熱
│   ├── bingx_ws.py          ← BingX WebSocket 支援
│   └── __init__.py
└── notifier/
    ├── discord.py           ← Discord Webhook 通知
    ├── discord_bot.py       ← Discord Bot 整合
    └── __init__.py
```

---

## ⚡ 快速開始

### 1. 安裝依賴

```bash
pip install -r requirements.txt
```

### 2. 設定配置檔

編輯 `config/config.yaml`：

#### 2.1 選擇交易所

```yaml
exchange: "binance"   # binance 或 bingx
```

#### 2.2 設定 Discord 通知

**方式一：Webhook（簡單）**
```yaml
discord:
  webhook_url: "https://discord.com/api/webhooks/XXXXXXXXXX/XXXXXX"
```

**方式二：Discord Bot（進階，支援指令互動）**
```yaml
discord:
  bot_token: "YOUR_BOT_TOKEN"
  channel_id: "123456789012345678"
  mention_role_id: "123456789012345678"   # 可選，訊號會 @該角色
```

> 💡 **取得 Webhook URL**：Discord 頻道右鍵 → **編輯頻道** → **整合** → **Webhook** → 建立新 Webhook → 複製 URL

### 3. 選擇交易對與週期

```yaml
symbols:
  - BTCUSDT
  - ETHUSDT
  - SOLUSDT

interval: "1h"   # 1m 5m 15m 30m 1h 4h 1d
```

### 4. 初始化資料庫（首次執行）

```bash
# 自動建立資料庫與資料表（執行 main.py 時會自動完成）
# 或手動診斷資料庫狀態
python diagnose_db.py
```

### 5. 執行

```bash
# 一般執行（讀取 config/config.yaml）
python main.py

# 指定設定檔
python main.py --config config/config.yaml

# 覆蓋交易對與週期（CLI 參數優先）
python main.py --symbol BTCUSDT --symbol SOLUSDT --interval 4h

# 開啟 DEBUG 日誌
python main.py --debug

# 指定交易所
python main.py --exchange bingx
```

### 6. 查詢歷史訊號（選用）

```bash
# 查看所有歷史訊號
python view_history.py

# 查詢特定交易對
python view_history.py --symbol BTCUSDT

# 查詢最近 N 筆
python view_history.py --limit 10
```

---

## 🔔 Discord 通知格式

每次訊號會發送一個 Embed，包含：

| 欄位 | 說明 |
|------|------|
| Entry | 進場價 |
| Stop Loss | 停損（ATR 基礎 + Pivot 支撐/壓力）|
| TP1 / TP2 / TP3 | 三個目標（R-倍數）|
| TQI | 趨勢品質指數（0~100%，附圖示）|
| Score | 訊號分數（/102）|
| Regime | 市場狀態（Trending / Mixed / Choppy）|
| Volume Z | 成交量 Z-score |

---

## ⚙️ 主要設定說明

### 基本設定

```yaml
main:
  use_preset: "Auto"   # Auto 自動選 Preset（依週期）
                       # Scalping / Default / Swing / Crypto 24/7 / Custom
  base_mult: 2.0       # 超趨勢帶寬（xATR）
```

### 訊號過濾

```yaml
filters:
  min_score: 60        # 低於此分數的訊號不發通知（0~102）
```

### TP 模式

```yaml
risk:
  tp_mode: "Fixed"     # Fixed = 固定 R 倍數
                       # Dynamic = 依 TQI + Vol 動態調整

  tp1_r: 1.0
  tp2_r: 2.0
  tp3_r: 3.0
```

### 動態 TP（tp_mode: Dynamic 時）

```yaml
dynamic_tp:
  tqi_weight: 0.6      # TQI 對 TP 擴縮的影響比重
  vol_weight: 0.4      # 波動率的影響比重
  min_scale: 0.5       # TP 最小縮放（不會縮到 50% 以下）
  max_scale: 2.0       # TP 最大擴張（不會超過 200%）
```

### @mention 功能

```yaml
discord:
  mention_role_id: "123456789012345678"   # 填 Role ID，訊號會 @該角色
```

---

## 🔄 運作原理

1. **啟動時**：從 Binance REST API 抓取歷史 K 棒（最多 999 根）預熱引擎
2. **即時串流**：透過 WebSocket 接收每根 K 棒
3. **已確認 K 棒**（`closed=true`）才觸發訊號判斷
4. **訊號產生**：SuperTrend 翻轉時計算 TQI、分數、SL/TP
5. **分數過濾**：低於 `min_score` 的訊號靜默丟棄
6. **Discord 通知**：格式化 Embed 發送到你的頻道

---

## 🛡️ 注意事項

- 本程式為**純指標通知工具**，不會自動下單
- 請勿將此訊號視為財務建議
- Binance WebSocket 斷線會自動重連（最多 10 次，可調整）
- `closed=false` 的即時 K 棒只更新引擎狀態，不發出訊號（等 K 棒收盤確認）
