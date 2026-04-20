"""
SATS Core — Self-Aware Trend System
完整移植自 Pine Script v1.9.0
"""
from __future__ import annotations
import math
from collections import deque
from dataclasses import dataclass, field
from typing import Optional


# ══════════════════════════════════════════════════
# 常數
# ══════════════════════════════════════════════════
ER_LOW_THRESH     = 0.25
ER_HIGH_THRESH    = 0.50
VOL_LOW_THRESH    = 0.7
VOL_HIGH_THRESH   = 1.3
EWMA_ALPHA        = 0.2
MULT_SMOOTH_ALPHA = 0.15
MAX_HISTORY_SIGS  = 100
BYPASS_SCORE      = 12.0
MAX_SCORE_REF     = 102


# ══════════════════════════════════════════════════
# 工具函數
# ══════════════════════════════════════════════════
def safe_div(num: float, den: float, fallback: float = 0.0) -> float:
    if den != 0 and not math.isnan(num) and not math.isnan(den):
        return num / den
    return fallback


def clamp(v: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, v))


def map_clamp(v: float, in_lo: float, in_hi: float, out_lo: float, out_hi: float) -> float:
    t = clamp(safe_div(v - in_lo, in_hi - in_lo, 0.0), 0.0, 1.0)
    return out_lo + t * (out_hi - out_lo)


def map_clamp_inv(v: float, in_lo: float, in_hi: float, out_hi: float, out_lo: float) -> float:
    t = clamp(safe_div(v - in_lo, in_hi - in_lo, 0.0), 0.0, 1.0)
    return out_hi - t * (out_hi - out_lo)


# ══════════════════════════════════════════════════
# 滾動統計工具
# ══════════════════════════════════════════════════
class RollingBuffer:
    """固定長度滾動數組，對應 Pine Script ta.* 函數"""

    def __init__(self, maxlen: int):
        self._buf: deque[float] = deque(maxlen=maxlen)

    def push(self, v: float):
        self._buf.append(v)

    def __len__(self):
        return len(self._buf)

    def __getitem__(self, idx: int) -> float:
        """idx=0 最新，idx=1 前一根，以此類推"""
        buf = self._buf
        n = len(buf)
        if idx >= n:
            return float("nan")
        return buf[n - 1 - idx]

    def sma(self, period: int) -> float:
        buf = list(self._buf)
        if len(buf) < period:
            return float("nan")
        return sum(buf[-period:]) / period

    def stdev(self, period: int) -> float:
        buf = list(self._buf)
        if len(buf) < period:
            return float("nan")
        sl = buf[-period:]
        m = sum(sl) / period
        return math.sqrt(sum((x - m) ** 2 for x in sl) / period)

    def highest(self, period: int) -> float:
        buf = list(self._buf)
        if not buf:
            return float("nan")
        return max(buf[-period:])

    def lowest(self, period: int) -> float:
        buf = list(self._buf)
        if not buf:
            return float("nan")
        return min(buf[-period:])

    def sum(self, period: int) -> float:
        buf = list(self._buf)
        if len(buf) < period:
            return float("nan")
        return sum(buf[-period:])


# ══════════════════════════════════════════════════
# Preset 解析
# ══════════════════════════════════════════════════
INTERVAL_MINUTES = {
    "1m": 1, "3m": 3, "5m": 5, "15m": 15, "30m": 30,
    "1h": 60, "2h": 120, "4h": 240, "6h": 360, "12h": 720, "1d": 1440,
}

PRESETS = {
    "Scalping":    dict(atr_len=10, base_mult=1.5, er_len=14, rsi_len=9,  sl_mult=1.0),
    "Default":     dict(atr_len=14, base_mult=2.0, er_len=20, rsi_len=14, sl_mult=1.5),
    "Swing":       dict(atr_len=21, base_mult=2.5, er_len=30, rsi_len=21, sl_mult=2.0),
    "Crypto 24/7": dict(atr_len=14, base_mult=2.8, er_len=20, rsi_len=14, sl_mult=2.5),
}


def resolve_preset(preset: str, interval: str, cfg: dict) -> dict:
    minutes = INTERVAL_MINUTES.get(interval, 60)
    if preset == "Auto":
        preset = "Scalping" if minutes <= 5 else ("Default" if minutes <= 240 else "Swing")

    if preset == "Custom":
        return dict(
            atr_len=cfg["main"]["atr_len"],
            base_mult=cfg["main"]["base_mult"],
            er_len=cfg["adaptive"]["er_length"],
            rsi_len=cfg["filters"]["rsi_len"],
            sl_mult=cfg["risk"]["sl_atr_mult"],
            resolved_preset=preset,
        )

    p = PRESETS.get(preset, PRESETS["Default"])
    return dict(**p, resolved_preset=preset)


# ══════════════════════════════════════════════════
# 訊號結果
# ══════════════════════════════════════════════════
@dataclass
class SignalResult:
    direction: str          # "BUY" | "SELL" | "NONE"
    symbol: str
    interval: str
    price: float
    sl: float
    tp1: float
    tp2: float
    tp3: float
    tp1_r: float
    tp2_r: float
    tp3_r: float
    score: float
    tqi: float
    er: float
    rsi: float
    vol_z: float
    preset: str
    tp_mode: str
    dyn_scale: float
    bar_index: int


# ══════════════════════════════════════════════════
# ATR 計算
# ══════════════════════════════════════════════════
class ATRCalc:
    def __init__(self, period: int):
        self.period = period
        self._tr_buf = RollingBuffer(period + 1)
        self._atr: float = float("nan")
        self._prev_close: float = float("nan")

    def update(self, high: float, low: float, close: float) -> float:
        if not math.isnan(self._prev_close):
            tr = max(high - low,
                     abs(high - self._prev_close),
                     abs(low - self._prev_close))
        else:
            tr = high - low
        self._tr_buf.push(tr)
        self._prev_close = close
        self._atr = self._tr_buf.sma(self.period)
        return self._atr

    @property
    def value(self) -> float:
        return self._atr


# ══════════════════════════════════════════════════
# RSI
# ══════════════════════════════════════════════════
class RSICalc:
    def __init__(self, period: int):
        self.period = period
        self._gains: RollingBuffer = RollingBuffer(period + 1)
        self._losses: RollingBuffer = RollingBuffer(period + 1)
        self._prev: float = float("nan")
        self._value: float = 50.0

    def update(self, close: float) -> float:
        if not math.isnan(self._prev):
            delta = close - self._prev
            self._gains.push(max(delta, 0.0))
            self._losses.push(max(-delta, 0.0))
            ag = self._gains.sma(self.period)
            al = self._losses.sma(self.period)
            if not math.isnan(ag) and not math.isnan(al):
                self._value = 100.0 - (100.0 / (1.0 + safe_div(ag, al, 1e-10)))
        self._prev = close
        return self._value

    @property
    def value(self) -> float:
        return self._value


# ══════════════════════════════════════════════════
# Efficiency Ratio
# ══════════════════════════════════════════════════
class ERCalc:
    def __init__(self, period: int):
        self.period = period
        self._closes = RollingBuffer(period + 1)
        self._value: float = 0.0

    def update(self, close: float) -> float:
        self._closes.push(close)
        n = len(self._closes)
        if n < self.period + 1:
            self._value = 0.0
            return self._value
        change = abs(self._closes[0] - self._closes[self.period])
        vol = sum(
            abs(self._closes[i] - self._closes[i + 1])
            for i in range(self.period)
        )
        self._value = safe_div(change, vol, 0.0)
        return self._value

    @property
    def value(self) -> float:
        return self._value


# ══════════════════════════════════════════════════
# Volume Z-score
# ══════════════════════════════════════════════════
class VolumeZCalc:
    def __init__(self, period: int):
        self._buf = RollingBuffer(period)
        self._value: float = 0.0

    def update(self, volume: float) -> float:
        self._buf.push(volume)
        n = len(self._buf)
        if n < 2:
            self._value = 0.0
            return self._value
        mean = self._buf.sma(n)
        std = self._buf.stdev(n)
        self._value = safe_div(volume - mean, std, 0.0)
        return self._value

    @property
    def value(self) -> float:
        return self._value


# ══════════════════════════════════════════════════
# Pivot 追蹤
# ══════════════════════════════════════════════════
class PivotTracker:
    """簡化 pivot，取滾動最高最低"""

    def __init__(self, period: int):
        self._highs = RollingBuffer(period * 2 + 1)
        self._lows = RollingBuffer(period * 2 + 1)
        self.last_pivot_high: float = float("nan")
        self.last_pivot_low: float = float("nan")
        self._period = period

    def update(self, high: float, low: float):
        self._highs.push(high)
        self._lows.push(low)
        n = len(self._highs)
        p = self._period
        if n >= p * 2 + 1:
            mid_h = self._highs[p]
            if mid_h == self._highs.highest(p * 2 + 1):
                self.last_pivot_high = mid_h
            mid_l = self._lows[p]
            if mid_l == self._lows.lowest(p * 2 + 1):
                self.last_pivot_low = mid_l


# ══════════════════════════════════════════════════
# 主引擎
# ══════════════════════════════════════════════════
class SATSEngine:
    """
    完整移植 Self-Aware Trend System v1.9.0
    每根 K 棒呼叫 update()，當有 BUY/SELL 訊號時回傳 SignalResult
    """

    def __init__(self, symbol: str, interval: str, cfg: dict):
        self.symbol = symbol
        self.interval = interval
        self.cfg = cfg

        # 解析 preset
        p = resolve_preset(cfg["main"]["use_preset"], interval, cfg)
        self.atr_len = p["atr_len"]
        self.base_mult = p["base_mult"]
        self.er_len = p["er_len"]
        self.rsi_len = p["rsi_len"]
        self.sl_mult = p["sl_mult"]
        self.resolved_preset = p["resolved_preset"]

        fc = cfg["filters"]
        tc = cfg["tqi"]
        rc = cfg["risk"]
        ac = cfg["adaptive"]

        # 計算器
        self._atr = ATRCalc(self.atr_len)
        self._atr_baseline = RollingBuffer(ac["atr_baseline_len"])
        self._er = ERCalc(self.er_len)
        self._rsi = RSICalc(self.rsi_len)
        self._vol_z = VolumeZCalc(fc["vol_len"])
        self._pivot = PivotTracker(fc["pivot_len"])

        # TQI 用緩衝
        struct_len = tc["struct_len"]
        mom_len = tc["mom_len"]
        self._highs = RollingBuffer(max(struct_len, 200))
        self._lows = RollingBuffer(max(struct_len, 200))
        self._closes = RollingBuffer(max(mom_len, self.er_len, fc["rsi_lookback"], 200) + 5)
        self._rsi_buf = RollingBuffer(fc["rsi_lookback"] + 5)

        # SuperTrend 狀態
        self._lower_band: float = float("nan")
        self._upper_band: float = float("nan")
        self._st_trend: int = 1
        self._trend_start_bar: int = 0

        # 自適應乘數平滑
        self._active_mult_sm: float = float("nan")
        self._passive_mult_sm: float = float("nan")

        # 訓練/學習
        self._signal_r_buffer: deque[float] = deque(maxlen=MAX_HISTORY_SIGS)

        # 活躍交易
        self._trade_dir: int = 0
        self._trade_entry_bar: int = 0
        self._trade_entry: float = float("nan")
        self._trade_sl: float = float("nan")
        self._trade_tp1: float = float("nan")
        self._trade_tp2: float = float("nan")
        self._trade_tp3: float = float("nan")
        self._trade_tp1r: float = float("nan")
        self._trade_tp2r: float = float("nan")
        self._trade_tp3r: float = float("nan")
        self._hit_tp1 = False
        self._hit_tp2 = False
        self._hit_tp3 = False
        self._trade_closed = False
        self._trade_events: list = []

        self._bar_index: int = 0
        self._tqi: float = 0.5
        self._vol_ratio: float = 1.0
        self._dyn_scale: float = 1.0

        # 預熱門檻
        warmup_floor = cfg["system"]["warmup_bars"]
        self._warmup = max(
            warmup_floor,
            self.atr_len, self.er_len, self.rsi_len,
            fc["vol_len"], fc["pivot_len"] * 2 + 1,
            tc["mom_len"], tc["struct_len"],
        ) + 10

    # ── 公開 ─────────────────────────────────────
    def update(
        self,
        open_: float, high: float, low: float, close: float, volume: float,
        is_closed: bool = True,
    ) -> Optional[SignalResult]:
        """
        傳入一根 K 棒資料。
        is_closed=True 代表這根 K 棒已確認（類似 barstate.isconfirmed）。
        回傳 SignalResult 或 None。
        """
        cfg = self.cfg
        tc = cfg["tqi"]
        fc = cfg["filters"]
        rc = cfg["risk"]
        ac = cfg["adaptive"]
        dtc = cfg["dynamic_tp"]

        bi = self._bar_index
        is_warmed = bi >= self._warmup

        # ── 基礎計算 ──────────────────────────────
        raw_atr = self._atr.update(high, low, close)
        self._atr_baseline.push(raw_atr if not math.isnan(raw_atr) else 0)
        atr_baseline = self._atr_baseline.sma(ac["atr_baseline_len"])
        vol_ratio = safe_div(raw_atr, atr_baseline if not math.isnan(atr_baseline) else raw_atr, 1.0)
        self._vol_ratio = vol_ratio

        er = self._er.update(close)
        rsi = self._rsi.update(close)
        vol_z = self._vol_z.update(volume) if volume > 0 else 0.0

        self._closes.push(close)
        self._highs.push(high)
        self._lows.push(low)
        self._rsi_buf.push(rsi)
        self._pivot.update(high, low)

        if math.isnan(raw_atr):
            self._bar_index += 1
            return None

        # ── TQI ───────────────────────────────────
        tqi = self._calc_tqi(close, er, vol_ratio, volume > 0, vol_z, tc)
        self._tqi = tqi

        # ── 自適應乘數 ────────────────────────────
        use_adapt = ac["enabled"]
        adapt_str = ac["adapt_strength"]
        legacy = (1.0 + adapt_str * (0.5 - er)) if use_adapt else 1.0

        eff_q_str = cfg.get("_eff_quality_str", cfg["tqi"]["quality_strength"])
        use_tqi = tc["enabled"]
        q_dev = math.pow(1.0 - tqi, tc["quality_curve"]) if use_tqi else 0.5
        tqi_mult = 1.0 - eff_q_str + eff_q_str * (0.6 + 0.8 * q_dev)
        sym_mult = self.base_mult * legacy * tqi_mult

        # 不對稱
        active_raw = sym_mult
        passive_raw = sym_mult
        if use_tqi and tc["asymmetric_bands"]:
            asym = tc["asym_strength"]
            active_raw = sym_mult * (1.0 - asym * tqi * 0.3)
            passive_raw = sym_mult * (1.0 + asym * tqi * 0.4)

        smooth = tc["smooth_mult"]
        alpha = MULT_SMOOTH_ALPHA
        if math.isnan(self._active_mult_sm):
            self._active_mult_sm = active_raw
            self._passive_mult_sm = passive_raw
        else:
            self._active_mult_sm  = self._active_mult_sm  * (1 - alpha) + active_raw  * alpha if smooth else active_raw
            self._passive_mult_sm = self._passive_mult_sm * (1 - alpha) + passive_raw * alpha if smooth else passive_raw

        # ── ATR (eff) ─────────────────────────────
        eff_atr = raw_atr * (0.5 + 0.5 * er) if tc["efficiency_weighted_atr"] else raw_atr

        # ── SuperTrend ────────────────────────────
        prev_trend = self._st_trend
        prev_lower = self._lower_band
        prev_upper = self._upper_band

        lo_mult = self._active_mult_sm if prev_trend == 1 else self._passive_mult_sm
        up_mult = self._passive_mult_sm if prev_trend == 1 else self._active_mult_sm

        lo_raw = close - lo_mult * eff_atr
        up_raw = close + up_mult * eff_atr

        prev_close1 = self._closes[1] if len(self._closes) > 1 else close

        if math.isnan(prev_lower):
            lower = lo_raw
            upper = up_raw
        else:
            lower = max(lo_raw, prev_lower) if prev_close1 > prev_lower else lo_raw
            upper = min(up_raw, prev_upper) if prev_close1 < prev_upper else up_raw

        self._lower_band = lower
        self._upper_band = upper

        trend_age = bi - self._trend_start_bar
        prev_tqi = float(self._tqi)   # same bar for simplicity

        # char-flip
        char_flip_down = False
        char_flip_up   = False
        if tc["char_flip"] and use_tqi and trend_age >= tc["char_flip_min_age"]:
            tqi_prev = self._get_prev_tqi()
            if tqi_prev > tc["char_flip_high"] and tqi < tc["char_flip_low"]:
                if prev_trend == 1 and close < close:  # simplified: use price vs band
                    char_flip_down = True
                elif prev_trend == -1 and close > close:
                    char_flip_up = True

        price_flip_up   = prev_trend == -1 and not math.isnan(prev_upper) and close > prev_upper
        price_flip_down = prev_trend ==  1 and not math.isnan(prev_lower) and close < prev_lower

        flip_up   = price_flip_up   or char_flip_up
        flip_down = price_flip_down or char_flip_down

        if flip_up:
            new_trend = 1
        elif flip_down:
            new_trend = -1
        else:
            new_trend = prev_trend

        if new_trend != prev_trend:
            self._trend_start_bar = bi

        self._st_trend = new_trend
        self._prev_tqi = tqi

        st_line = lower if new_trend == 1 else upper

        confirmed_flip_up   = flip_up   and is_closed and is_warmed
        confirmed_flip_down = flip_down and is_closed and is_warmed

        # ── Dynamic TP ────────────────────────────
        use_dyn = rc["tp_mode"] == "Dynamic"
        dyn_scale = self._calc_dyn_tp_scale(tqi, vol_ratio, dtc) if use_dyn else 1.0
        self._dyn_scale = dyn_scale

        tp1_r_base = rc["tp1_r"]
        tp2_r_base = rc["tp2_r"]
        tp3_r_base = rc["tp3_r"]
        # 保證順序
        tp1_r_base, tp2_r_base, tp3_r_base = sorted([tp1_r_base, tp2_r_base, tp3_r_base])

        if use_dyn:
            floor1 = dtc["floor_r1"]
            floor2 = floor1 * (tp2_r_base / max(tp1_r_base, 0.01))
            floor3 = floor1 * (tp3_r_base / max(tp1_r_base, 0.01))
            ceil_  = dtc["ceil_r3"]
            e1 = clamp(tp1_r_base * dyn_scale, floor1, ceil_)
            e2 = clamp(tp2_r_base * dyn_scale, floor2, ceil_)
            e3 = clamp(tp3_r_base * dyn_scale, floor3, ceil_)
            live_tp1r, live_tp2r, live_tp3r = sorted([e1, e2, e3])
        else:
            live_tp1r, live_tp2r, live_tp3r = tp1_r_base, tp2_r_base, tp3_r_base

        # ── 計算訊號分數 ──────────────────────────
        signal: Optional[SignalResult] = None

        # ── 命中檢測 ──────────────────────────────
        # 修正：移除每根 K 棒強行清空事件列表的邏輯，改為在處理完後由外部或特定時機清空。
        # 並且允許在即時更新時也進行命中檢測，以提升反應速度。
        if self._trade_dir != 0 and bi > self._trade_entry_bar:
            self._check_hits(high, low, bi)

        if confirmed_flip_up or confirmed_flip_down:
            is_buy = confirmed_flip_up
            score = self._calc_score(is_buy, close, er, vol_z, rsi, fc)

            entry = close
            if is_buy:
                sl_base = self._pivot.last_pivot_low if not math.isnan(self._pivot.last_pivot_low) else low
                raw_sl = sl_base - self.sl_mult * raw_atr
                min_sl = entry - self.sl_mult * raw_atr
                trade_sl = min(raw_sl, min_sl)
                risk = entry - trade_sl
                tp1 = entry + risk * live_tp1r
                tp2 = entry + risk * live_tp2r
                tp3 = entry + risk * live_tp3r
            else:
                sl_base = self._pivot.last_pivot_high if not math.isnan(self._pivot.last_pivot_high) else high
                raw_sl = sl_base + self.sl_mult * raw_atr
                min_sl = entry + self.sl_mult * raw_atr
                trade_sl = max(raw_sl, min_sl)
                risk = trade_sl - entry
                tp1 = entry - risk * live_tp1r
                tp2 = entry - risk * live_tp2r
                tp3 = entry - risk * live_tp3r

            # 更新活躍交易 (僅當分數達到最低標準，才在引擎內部視為開倉)
            min_score = self.cfg["filters"].get("min_score", 0)
            if score >= min_score:
                self._trade_dir       = 1 if is_buy else -1
                self._trade_entry_bar = bi
                self._trade_entry     = entry
                self._trade_sl        = trade_sl
                self._trade_tp1       = tp1
                self._trade_tp2       = tp2
                self._trade_tp3       = tp3
                self._trade_tp1r      = live_tp1r
                self._trade_tp2r      = live_tp2r
                self._trade_tp3r      = live_tp3r
                self._hit_tp1 = self._hit_tp2 = self._hit_tp3 = False
                self._trade_closed = False
                self._trade_events = []
                from datetime import datetime, timezone as _tz
                self._trade_entry_timestamp = datetime.now(_tz.utc).isoformat()
                self._trade_initial_risk    = abs(entry - trade_sl)

            signal = SignalResult(
                direction = "BUY" if is_buy else "SELL",
                symbol    = self.symbol,
                interval  = self.interval,
                price     = entry,
                sl        = trade_sl,
                tp1=tp1, tp2=tp2, tp3=tp3,
                tp1_r=live_tp1r, tp2_r=live_tp2r, tp3_r=live_tp3r,
                score     = score,
                tqi       = tqi,
                er        = er,
                rsi       = rsi,
                vol_z     = vol_z,
                preset    = self.resolved_preset,
                tp_mode   = rc["tp_mode"],
                dyn_scale = dyn_scale,
                bar_index = bi,
            )

        self.last_close = close   # 即時價格，供報告/指令讀取

        if is_closed:
            self._bar_index += 1
        return signal

    # ── 內部計算 ─────────────────────────────────
    def _calc_tqi(self, close, er, vol_ratio, has_vol, vol_z, tc) -> float:
        use_tqi = tc["enabled"]
        if not use_tqi:
            return 0.5

        tqi_er = clamp(er, 0.0, 1.0)

        if has_vol:
            tqi_vol = map_clamp(vol_z, -1.0, 2.0, 0.0, 1.0)
        else:
            tqi_vol = map_clamp(vol_ratio, 0.6, 1.8, 0.0, 1.0)

        struct_len = tc["struct_len"]
        hi = self._highs.highest(min(struct_len, len(self._highs))) if len(self._highs) >= 1 else close
        lo = self._lows.lowest(min(struct_len, len(self._lows))) if len(self._lows) >= 1 else close
        struct_range = hi - lo
        price_pos = safe_div(close - lo, struct_range, 0.5)
        tqi_struct = clamp(abs(price_pos - 0.5) * 2.0, 0.0, 1.0)

        mom_len = tc["mom_len"]
        if len(self._closes) > mom_len:
            window_change = self._closes[0] - self._closes[mom_len]
            aligned = 0
            for i in range(mom_len):
                bar_change = self._closes[i] - self._closes[i + 1]
                if (window_change > 0 and bar_change > 0) or (window_change < 0 and bar_change < 0):
                    aligned += 1
            tqi_mom = aligned / mom_len
        else:
            tqi_mom = 0.5

        w_er   = tc["weight_er"]
        w_vol  = tc["weight_vol"]
        w_str  = tc["weight_struct"]
        w_mom  = tc["weight_mom"]
        total_w = w_er + w_vol + w_str + w_mom
        total_w = total_w if total_w > 0 else 1.0

        tqi_raw = (tqi_er * w_er + tqi_vol * w_vol + tqi_struct * w_str + tqi_mom * w_mom) / total_w
        return clamp(tqi_raw, 0.0, 1.0)

    def _get_prev_tqi(self) -> float:
        return getattr(self, "_prev_tqi", 0.5)

    def _calc_dyn_tp_scale(self, tqi, vol_ratio, dtc) -> float:
        tqi_w = dtc["tqi_weight"]
        vol_w = dtc["vol_weight"]
        mn = dtc["min_scale"]
        mx = dtc["max_scale"]
        tqi_c = clamp(tqi, 0.0, 1.0)
        vol_c = clamp(map_clamp(vol_ratio, 0.5, 2.0, 0.0, 1.0), 0.0, 1.0)
        w_sum = tqi_w + vol_w
        w_sum = w_sum if w_sum > 0 else 1.0
        raw = (tqi_c * tqi_w + vol_c * vol_w) / w_sum
        return mn + raw * (mx - mn)

    def _calc_score(self, is_buy, close, er, vol_z, rsi, fc) -> float:
        atr = self._atr.value
        if math.isnan(atr) or atr == 0:
            return 0.0

        # Momentum
        close3 = self._closes[3] if len(self._closes) > 3 else close
        dir_move = (close3 - close) if is_buy else (close - close3)
        mom_score = map_clamp(safe_div(dir_move, atr, 0.0), 0.3, 2.0, 0.0, 17.0)

        # ER
        er_score = map_clamp(er, 0.15, 0.7, 0.0, 17.0)

        # Volume
        v_score = map_clamp(vol_z, 0.0, 3.0, 0.0, 17.0) if fc["use_volume"] else BYPASS_SCORE

        # RSI
        if fc["use_rsi"]:
            lookback = fc["rsi_lookback"]
            rsi_ob = fc["rsi_ob"]
            rsi_os = fc["rsi_os"]
            if is_buy:
                rsi_low = self._rsi_buf.lowest(min(lookback, len(self._rsi_buf)))
                rsi_depth = max(0.0, rsi_os - rsi_low) if not math.isnan(rsi_low) else 0.0
            else:
                rsi_high = self._rsi_buf.highest(min(lookback, len(self._rsi_buf)))
                rsi_depth = max(0.0, rsi_high - rsi_ob) if not math.isnan(rsi_high) else 0.0
            rsi_score = map_clamp(rsi_depth, 0.0, 15.0, 0.0, 17.0)
        else:
            rsi_score = BYPASS_SCORE

        # Structure
        if fc["use_structure"]:
            if is_buy and not math.isnan(self._pivot.last_pivot_low):
                piv_dist = abs(close - self._pivot.last_pivot_low)
            elif not is_buy and not math.isnan(self._pivot.last_pivot_high):
                piv_dist = abs(self._pivot.last_pivot_high - close)
            else:
                piv_dist = 0.0
            struct_score = map_clamp_inv(safe_div(piv_dist, atr, 0.0), 0.0, 1.5, 16.0, 6.0)
        else:
            struct_score = BYPASS_SCORE

        # Break depth
        if is_buy:
            break_depth = max(0.0, (self._upper_band if not math.isnan(self._upper_band) else close) - (self._closes[1] if len(self._closes) > 1 else close))
        else:
            break_depth = max(0.0, (self._closes[1] if len(self._closes) > 1 else close) - (self._lower_band if not math.isnan(self._lower_band) else close))
        break_score = map_clamp(safe_div(break_depth, atr, 0.0), 0.0, 1.0, 0.0, 16.0)

        return mom_score + er_score + v_score + rsi_score + struct_score + break_score

    def _check_hits(self, high, low, bi):
        """
        偵測 TP/SL/Timeout 命中，並將事件附加到 self._trade_events。

        事件格式（dict）：
          type      : "tp1_hit" | "tp2_hit" | "tp3_hit" | "sl_hit" | "timeout"
          direction : "BUY" | "SELL"
          entry     : 進場價
          exit_price: 命中時的 TP/SL 價格
          sl        : 止損價
          tp1/2/3   : 各 TP 價格
          tp1r/2r/3r: 各 R 倍數
          hit_tp1/2/3 : 截至本事件的命中狀態
          bars_open : 開倉根數
        """
        td = self._trade_dir
        tp1_reached = (high >= self._trade_tp1) if td == 1 else (low <= self._trade_tp1)
        tp2_reached = (high >= self._trade_tp2) if td == 1 else (low <= self._trade_tp2)
        tp3_reached = (high >= self._trade_tp3) if td == 1 else (low <= self._trade_tp3)
        sl_hit      = (low  <= self._trade_sl)  if td == 1 else (high >= self._trade_sl)

        # 修正：計算 bars_open（進場後經過的 K 棒數）。
        # 開倉當根為 0。
        bars_open = bi - self._trade_entry_bar
        timeout   = bars_open >= self.cfg["risk"].get("trade_timeout", 100)

        direction = "BUY" if td == 1 else "SELL"

        # 計算 R 倍數（固定使用開倉時的原始 risk，breakeven 移 SL 後不影響）
        import math as _math
        _initial_risk = getattr(self, "_trade_initial_risk", None) or abs(self._trade_entry - self._trade_sl)
        _initial_risk = _initial_risk if _initial_risk > 0 else float("nan")
        def _calc_r(exit_price):
            if _math.isnan(_initial_risk): return float("nan")
            raw = (exit_price - self._trade_entry) if td == 1 else (self._trade_entry - exit_price)
            return round(raw / _initial_risk, 4)

        base = {
            "direction"        : direction,
            "entry"            : self._trade_entry,
            "entry_timestamp"  : getattr(self, "_trade_entry_timestamp", ""),
            "sl"               : self._trade_sl,
            "tp1"              : self._trade_tp1,
            "tp2"              : self._trade_tp2,
            "tp3"              : self._trade_tp3,
            "tp1r"             : self._trade_tp1r,
            "tp2r"             : self._trade_tp2r,
            "tp3r"             : self._trade_tp3r,
            "bars_open"        : bars_open,
        }

        # TP 命中里程碑（只在首次命中時發事件）
        if tp1_reached and not self._hit_tp1:
            self._hit_tp1 = True
            
            # ── Breakeven 邏輯 ─────────────────────
            is_be = self.cfg["risk"].get("breakeven", False)
            if is_be:
                self._trade_sl = self._trade_entry
            # ──────────────────────────────────────

            base["sl"]      = self._trade_sl
            base["hit_tp1"] = True  # 同步，讓後續 tp2 事件看到正確狀態

            evt = {**base, "type": "tp1_hit", "exit_price": self._trade_tp1,
                   "hit_r": _calc_r(self._trade_tp1),
                   "hit_tp1": True, "hit_tp2": self._hit_tp2, "hit_tp3": self._hit_tp3,
                   "is_breakeven": is_be}
            self._trade_events.append(evt)

        if tp2_reached and not self._hit_tp2:
            self._hit_tp2 = True

            # ── TP2 鎖利邏輯 (Move SL to TP1) ──────
            is_be = self.cfg["risk"].get("breakeven", False)
            if is_be:
                self._trade_sl = self._trade_tp1
            # ──────────────────────────────────────

            base["hit_tp2"] = True
            base["sl"]      = self._trade_sl

            evt = {**base, "type": "tp2_hit", "exit_price": self._trade_tp2,
                   "hit_r": _calc_r(self._trade_tp2),
                   "hit_tp1": self._hit_tp1, "hit_tp2": True, "hit_tp3": self._hit_tp3,
                   "is_breakeven": is_be}
            self._trade_events.append(evt)
            
            # 重新檢查止損位，因為 SL 剛剛被移到了 TP1
            sl_hit = (low <= self._trade_sl) if td == 1 else (high >= self._trade_sl)

        # 關倉事件（TP3 / SL / Timeout，只取第一個發生的）
        if not getattr(self, "_trade_closed", False):
            base["hit_tp1"] = self._hit_tp1
            base["hit_tp2"] = self._hit_tp2
            base["sl"]      = self._trade_sl

            if tp3_reached:
                self._hit_tp3 = True
                evt = {**base, "type": "tp3_hit", "exit_price": self._trade_tp3,
                       "hit_r": _calc_r(self._trade_tp3),
                       "hit_tp1": self._hit_tp1, "hit_tp2": self._hit_tp2, "hit_tp3": True}
                self._trade_events.append(evt)
                self._trade_dir    = 0
                self._trade_closed = True
            elif sl_hit:
                evt = {**base, "type": "sl_hit", "exit_price": self._trade_sl,
                       "hit_r": _calc_r(self._trade_sl),
                       "hit_tp1": self._hit_tp1, "hit_tp2": self._hit_tp2, "hit_tp3": False}
                self._trade_events.append(evt)
                self._trade_dir    = 0
                self._trade_closed = True
            elif timeout:
                exit_price = high if td == 1 else low
                evt = {**base, "type": "timeout", "exit_price": exit_price,
                       "hit_r": _calc_r(exit_price),
                       "hit_tp1": self._hit_tp1, "hit_tp2": self._hit_tp2, "hit_tp3": self._hit_tp3}
                self._trade_events.append(evt)
                self._trade_dir    = 0
                self._trade_closed = True

    # ── 公開：取得本根 K 棒產生的交易事件 ────────
    @property
    def trade_events(self) -> list:
        """回傳本根 K 棒的交易事件（list[dict]），取完後應自行清空或忽略舊值。"""
        return self._trade_events

    # ── 預熱後重置交易狀態 ────────────────────────
    def reset_trade_state(self):
        """
        預熱完成後呼叫。清除持倉、TP/SL 狀態與事件佇列。
        技術指標緩衝區（ATR/RSI/ER/SuperTrend 等）保持不變。
        """
        self._trade_dir             = 0
        self._trade_entry_bar       = 0
        self._trade_entry           = float("nan")
        self._trade_sl              = float("nan")
        self._trade_tp1             = float("nan")
        self._trade_tp2             = float("nan")
        self._trade_tp3             = float("nan")
        self._trade_tp1r            = float("nan")
        self._trade_tp2r            = float("nan")
        self._trade_tp3r            = float("nan")
        self._hit_tp1               = False
        self._hit_tp2               = False
        self._hit_tp3               = False
        self._trade_closed          = False
        self._trade_events          = []
        self._trade_entry_timestamp = ""
        self._trade_initial_risk    = 0.0
        self._signal_r_buffer.clear()

    # ── 狀態查詢 ─────────────────────────────────
    @property
    def tqi(self) -> float:
        return self._tqi

    @property
    def trend(self) -> str:
        return "Bullish" if self._st_trend == 1 else "Bearish"

    @property
    def bar_index(self) -> int:
        return self._bar_index

    @property
    def position(self) -> Optional[dict]:
        """
        回傳目前活躍持倉資訊；無持倉（trade_dir == 0）時回傳 None。
        欄位：
          direction  : 'BUY' | 'SELL'
          entry      : 進場價格
          sl         : 止損價格
          tp1/2/3    : 三個止盈價格
          tp1r/2r/3r : 對應 Risk-Reward 倍數
          hit_tp1/2/3: 是否已命中各 TP
          bars_open  : 已開根數
        """
        if self._trade_dir == 0:
            return None
        return {
            "direction" : "BUY" if self._trade_dir == 1 else "SELL",
            "entry"     : self._trade_entry,
            "sl"        : self._trade_sl,
            "tp1"       : self._trade_tp1,
            "tp2"       : self._trade_tp2,
            "tp3"       : self._trade_tp3,
            "tp1r"      : self._trade_tp1r,
            "tp2r"      : self._trade_tp2r,
            "tp3r"      : self._trade_tp3r,
            "hit_tp1"   : self._hit_tp1,
            "hit_tp2"   : self._hit_tp2,
            "hit_tp3"   : self._hit_tp3,
            "bars_open" : self._bar_index - self._trade_entry_bar,
        }