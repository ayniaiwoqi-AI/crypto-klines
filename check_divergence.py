#!/usr/bin/env python3
"""
背离信号检测脚本
- 读取 /root/crypto_klines/{SYMBOL}.json（987根1H K线）
- 检测 CVD 背离 + RSI + K线形态
- 信号输出到观察池
- 支持增量检测（只输出新信号）
"""
import json
import os
import time
import math
import numpy as np
import requests
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from scipy.signal import find_peaks

# ========== 配置 ==========
KLINES_DIR = "/root/crypto_klines"
BATCH_SIZE = 10
INFLUX_URL = "http://localhost:8086"
INFLUX_TOKEN = "influx-token-crypto-2026"
INFLUX_ORG = "crypto-lab"
INFLUX_DB = "klines"
TG_BOT_TOKEN = "8603937566:AAECrJdrN3cRQeGZKU8_tgV_sMkaDKpx_iw"
TG_CHAT_ID = "390688348"
CACHE_FILE = "/root/crypto_klines/.oi_fr_cache.json"

# 背离检测参数
TARGET_K = 987
DIVERGENCE_WINDOW = 3   # 价格峰谷与CVD峰谷配对窗口（±3根K线）
RSI_PERIOD = 14
MIN_CVD_PEAKS = 5       # 最少需要多少个 CVD 峰谷才做判断

# 信号池 TTL（小时）
SIGNAL_TTL_HOURS = 24

# ========== 工具函数 ==========
def send_telegram(msg: str):
    """发送到 Telegram"""
    try:
        url = f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendMessage"
        resp = requests.post(url, json={
            "chat_id": TG_CHAT_ID,
            "text": msg,
            "parse_mode": "Markdown",
            "disable_web_page_preview": True
        }, timeout=10)
        return resp.json().get("ok", False)
    except Exception as e:
        print(f"[TG ERROR] {e}")
        return False

def calc_rsi(closes: np.array, period: int = 14) -> float:
    """计算 RSI（Wilder 平滑，与 TradingView/Binance 一致）"""
    if len(closes) < period + 1:
        return 50.0
    deltas = np.diff(closes[-period-1:])
    gains = np.where(deltas > 0, deltas, 0.0)
    losses = np.where(deltas < 0, -deltas, 0.0)

    # Wilder 指数平滑
    avg_gain = gains[0]
    avg_loss = losses[0]
    for i in range(1, len(deltas)):
        avg_gain = avg_gain * (period - 1) / period + gains[i] / period
        avg_loss = avg_loss * (period - 1) / period + losses[i] / period

    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))

CACHE_TTL_HOURS = 2  # 缓存超过2小时视为过期

def load_oi_fr_cache() -> dict:
    """加载本地 OI/FR 缓存（FR 用，OI 方向改查 InfluxDB），超时标记fresh=False"""
    if not os.path.exists(CACHE_FILE):
        return {}
    try:
        with open(CACHE_FILE) as f:
            raw = json.load(f)
    except Exception:
        return {}

    now = datetime.now()
    stale_count = 0
    for sym, data in raw.items():
        updated_str = data.get("updated", "")
        try:
            updated = datetime.fromisoformat(updated_str)
            age_h = (now - updated).total_seconds() / 3600
            data["fresh"] = age_h <= CACHE_TTL_HOURS
            if not data["fresh"]:
                stale_count += 1
        except Exception:
            data["fresh"] = False
            stale_count += 1

    if stale_count > 0:
        print(f"  [WARN] OI/FR缓存 {stale_count} 个币数据超过{CACHE_TTL_HOURS}h")
    return raw

def fetch_oi_direction_from_influx(symbols: list[str], oi_thresh: float = 0.005) -> dict:
    """从 InfluxDB 批量查最近 3h OI，分批返回 {symbol: '↑OI'/'↓OI'/'→OI'}
    oi_thresh: OI 变化百分比阈值（默认 0.5%），波动大时用 ATR 归一化阈值替代硬编码"""
    import requests as _req
    from collections import defaultdict
    token = "influx-token-crypto-2026"
    url = "http://localhost:8086/api/v2/query?org=crypto-lab"
    headers = {"Authorization": f"Token {token}", "Content-Type": "application/vnd.flux"}

    oi_dir = {}
    batch_size = 50
    for i in range(0, len(symbols), batch_size):
        batch = symbols[i:i+batch_size]
        sym_filter = " or ".join([f'r.symbol == "{s}"' for s in batch])
        body = f"""from(bucket: "oi-volume")
  |> range(start: -3h)
  |> filter(fn: (r) => {sym_filter})
  |> sort(columns: ["_time"], desc: false)
  |> group(columns: ["symbol"])
  |> limit(n: 2)"""

        try:
            resp = _req.post(url, headers=headers, data=body, timeout=20)
            if resp.status_code != 200:
                print(f"[OI batch {i//batch_size}] status {resp.status_code}: {resp.text[:100]}")
                continue
            lines = resp.text.strip().splitlines()
            bucket = defaultdict(list)
            for line in lines[1:]:
                parts = line.split(",")
                if len(parts) < 10:
                    continue
                try:
                    sym = parts[9].strip()
                    val = float(parts[6].strip())
                except (ValueError, IndexError):
                    continue
                bucket[sym].append(val)

            for sym, vals in bucket.items():
                vals.sort()
                old_val, curr_val = vals[0], vals[-1]
                if old_val <= 0:
                    oi_dir[sym] = "→OI"
                    continue
                pct = (curr_val - old_val) / old_val * 100
                thresh = oi_thresh.get(sym, 0.005) if isinstance(oi_thresh, dict) else oi_thresh
                if pct > thresh:
                    oi_dir[sym] = "↑OI"
                elif pct < -thresh:
                    oi_dir[sym] = "↓OI"
                else:
                    oi_dir[sym] = "→OI"
        except Exception as e:
            print(f"[OI batch {i//batch_size}] error: {e}")
            continue

    return oi_dir

def calc_cvd(klines: list) -> np.array:
    """计算 CVD 序列"""
    deltas = []
    for k in klines:
        h, l, c, v = k["h"], k["l"], k["c"], k["v"]
        if h == l:
            delta = 0.0
        else:
            delta = v * ((c - l) - (h - c)) / (h - l)
        deltas.append(delta)
    return np.cumsum(np.array(deltas))

def find_peaks_valleys(data: np.array, distance: int = 15, prominence_pct: float = 0.025):
    """找波峰波谷，distance大=波峰间隔远，prominence高=只有明显高低才识别"""
    if len(data) < 10:
        return np.array([]), np.array([])
    rng = data.max() - data.min()
    prom = rng * prominence_pct
    peaks, _ = find_peaks(data, distance=distance, prominence=prom)
    valleys, _ = find_peaks(-data, distance=distance, prominence=prom)
    return peaks, valleys

# ========== 本地 CVD 缓存（避免每轮全量重算） ==========
def load_local_cvd_cache(symbol: str) -> tuple:
    """返回 (peaks数组, valleys数组)，不存在则返回空数组"""
    import json as _json
    peaks_file = f"{KLINES_DIR}/.cvd_peaks_{symbol}.json"
    valleys_file = f"{KLINES_DIR}/.cvd_valleys_{symbol}.json"
    peaks, valleys = np.array([]), np.array([])
    if os.path.exists(peaks_file):
        try:
            with open(peaks_file) as f:
                raw = _json.load(f)
            if isinstance(raw, list):
                peaks = np.array(raw)
        except Exception:
            pass
    if os.path.exists(valleys_file):
        try:
            with open(valleys_file) as f:
                raw = _json.load(f)
            if isinstance(raw, list):
                valleys = np.array(raw)
        except Exception:
            pass
    return peaks, valleys

def calc_atr_threshold(closes: np.array, period: int = 20) -> float:
    """计算 ATR 阈值（用于 OI 方向判断的波动率归一化）"""
    if len(closes) < period + 1:
        return 0.005  # 兜底 ±0.5%
    trs = []
    for i in range(1, min(period + 1, len(closes))):
        tr = max(
            closes[i] - closes[i - 1],
            abs(closes[i] - closes[i - 1]),
            abs(closes[i] - closes[i - 1])
        )
        trs.append(tr)
    atr = sum(trs) / len(trs)
    return atr / closes[-1]  # 相对波动率

# ========== P0: CVD 动量 + 领先时长过滤 ==========
def calc_cvd_roc(cvd: np.array, period: int = 20) -> float:
    """
    CVD N周期动量变化率（Rate of Change）
    返回百分比，衡量 CVD 最近 N 根的净移动幅度
    """
    if len(cvd) < period + 1:
        return 0.0
    prev_val = cvd[-(period + 1)]
    if prev_val == 0:
        return 0.0
    return (cvd[-1] - prev_val) / abs(prev_val) * 100

def calc_cvd_lead_bars(cvd: np.array, cvd_peaks_or_valleys: np.array,
                       direction: str) -> int:
    """
    CVD 峰值到对应价格峰值之间隔了多少根K线
    direction: 'top'（CVD顶→价格顶）或 'bottom'（CVD谷→价格谷）
    返回：间隔K线数，>20 = 伪领先（噪音），3-15 = 有效领先
    """
    if len(cvd_peaks_or_valleys) < 2:
        return 999
    # 用最后1个CVD峰值/谷，找对应价格峰值/谷（已在调用前通过窗口配对）
    # 此函数由调用者传入已配对的索引，这里只计算距离
    return 0  # 占位，由调用处传入实际差值

def cvd_momentum_filter(cvd: np.array, direction: str,
                        min_roc_pct: float = 5.0) -> tuple:
    """
    P0 过滤：CVD 动量不足则跳过
    direction: 'short'(做空) 或 'long'(做多)
    做空：需要 CVD 最近20周期上涨 >5%（看空动能充足）
    做多：需要 CVD 最近20周期下跌 >5%（买入动能充足）
    返回 (通过: bool, roc: float, reason: str)
    """
    roc = calc_cvd_roc(cvd, period=20)
    if direction == "short":
        if roc < min_roc_pct:
            return False, roc, f"CVD动量不足(ROC={roc:.1f}%<{min_roc_pct}%)"
    elif direction == "long":
        if roc > -min_roc_pct:
            return False, roc, f"CVD动量不足(ROC={roc:.1f}%>-{min_roc_pct}%)"
    return True, roc, ""

# ========== P1: CVD 趋势结构过滤 ==========
def check_cvd_trend_structure(cvd: np.array, cvd_peaks: np.array,
                              cvd_valleys: np.array) -> str:
    """
    CVD 趋势结构：判断 CVD 当前处于上升/下降/震荡结构
    返回: 'up' / 'down' / 'neutral'

    上升结构：峰值不断抬高（cvd_peak[-1] > cvd_peak[-3] > cvd_peak[-5]）
    下降结构：谷值不断降低（cvd_valley[-1] < cvd_valley[-3] < cvd_valley[-5]）
    否则为震荡
    """
    if len(cvd_peaks) >= 5 and len(cvd) > 0:
        p0, p2, p4 = cvd[cvd_peaks[-1]], cvd[cvd_peaks[-3]], cvd[cvd_peaks[-5]]
        if p0 > p2 and p2 > p4:
            return "up"
    if len(cvd_valleys) >= 5 and len(cvd) > 0:
        v0, v2, v4 = cvd[cvd_valleys[-1]], cvd[cvd_valleys[-3]], cvd[cvd_valleys[-5]]
        if v0 < v2 and v2 < v4:
            return "down"
    return "neutral"

def cvd_trend_filter(trend_structure: str, signal_direction: str) -> int:
    """
    P1 过滤：根据 CVD 趋势结构对信号降权
    顶背离（做空）：CVD 处于上升结构中 → strength 3（顺势有效）
                  CVD 处于下降/震荡结构 → strength -1（逆势降权）
    底背离（做多）：CVD 处于下降结构中 → strength 3（顺势有效）
                  CVD 处于上升/震荡结构 → strength -1（逆势降权）
    返回 strength 调整值（0 = 无调整，-1 = 降权）
    """
    if signal_direction == "short":
        return 0 if trend_structure == "up" else -1
    elif signal_direction == "long":
        return 0 if trend_structure == "down" else -1
    return 0

def match_peaks_in_window(price_idx: int, target_indices: np.array, window: int = 3) -> tuple:
    """在 price_idx 附近找最近的 target 峰谷"""
    diffs = np.abs(target_indices - price_idx)
    if len(diffs) == 0:
        return None, None
    min_idx = np.argmin(diffs)
    if diffs[min_idx] <= window:
        return target_indices[min_idx], diffs[min_idx]
    return None, None

# ========== K线形态检测 ==========
def detect_engulfing(klines: list, pattern_idx: int) -> str:
    """
    检测吞没形态（需要回溯前1根确认）
    pattern_idx: 形态所在的K线索引（从0开始，0是最老的）
    返回: 'bullish_engulfing', 'bearish_engulfing', None
    """
    n = len(klines)
    # 需要前一前一后两根K线
    if pattern_idx < 1 or pattern_idx >= n - 1:
        return None
    
    prev = klines[pattern_idx - 1]
    curr = klines[pattern_idx]
    next_k = klines[pattern_idx + 1]  # 下一根确认用，但形态确认用curr
    
    # 前一根
    prev_is_bearish = prev["c"] < prev["o"]
    prev_is_bullish = prev["c"] > prev["o"]
    # 当前根
    curr_is_bearish = curr["c"] < curr["o"]
    curr_is_bullish = curr["c"] > curr["o"]
    
    # 阳吞阴（做多）
    if prev_is_bearish and curr_is_bullish:
        # 实体部分吞没
        if curr["c"] > prev["o"] and curr["o"] < prev["c"]:
            return "bullish_engulfing"
    
    # 阴吞阳（做空）
    if prev_is_bullish and curr_is_bearish:
        if curr["c"] < prev["o"] and curr["o"] > prev["c"]:
            return "bearish_engulfing"
    
    return None

def detect_hammer(klines: list, pattern_idx: int) -> str:
    """
    检测锤子线 / 射击之星
    """
    if pattern_idx < 1 or pattern_idx >= len(klines) - 1:
        return None
    
    k = klines[pattern_idx]
    body = abs(k["c"] - k["o"])
    upper_shadow = k["h"] - max(k["o"], k["c"])
    lower_shadow = min(k["o"], k["c"]) - k["l"]
    
    if body == 0:
        return None
    
    # 锤子线（做多）
    if (lower_shadow >= 2 * body and
        upper_shadow <= 0.5 * body and
        lower_shadow > 0):
        return "hammer"
    
    # 射击之星（做空）
    if (upper_shadow >= 2 * body and
        lower_shadow <= 0.5 * body and
        upper_shadow > 0):
        return "shooting_star"
    
    return None

def detect_pattern(klines: list, pattern_idx: int) -> str:
    """综合形态检测"""
    eng = detect_engulfing(klines, pattern_idx)
    if eng:
        return eng
    hammer = detect_hammer(klines, pattern_idx)
    if hammer:
        return hammer
    return None

# ========== 背离检测 ==========
def detect_divergence(symbol: str, klines: list):
    """
    检测背离
    返回: {
        'direction': 'long'/'short'/None,
        'strength': 1-3,
        'reason': str,
        'entry_price': float,
        'stop_loss': float,
        'target1': float,
        'target2': float,
        'pattern': str,
        'rsi': float,
        'cvd_strength': float,  # 背离幅度百分比
        'kline_idx': int,       # 形态所在K线索引
    }
    """
    if len(klines) < 100:
        return None

    closes = np.array([k["c"] for k in klines])
    cvd = calc_cvd(klines)

    # 尝试读本地 CVD 缓存（CVD波峰波谷位置，单位：数组索引）
    cached_peaks, cached_valleys = load_local_cvd_cache(symbol)
    if len(cached_peaks) >= 5 and len(cached_valleys) >= 5:
        price_peaks, price_valleys = find_peaks_valleys(closes, distance=15, prominence_pct=0.03)
        cvd_peaks = cached_peaks
        cvd_valleys = cached_valleys
    else:
        # 无缓存时重新检测价格和CVD峰谷
        price_peaks, price_valleys = find_peaks_valleys(closes, distance=15, prominence_pct=0.03)
        cvd_peaks, cvd_valleys = find_peaks_valleys(cvd, distance=15, prominence_pct=0.02)

    if len(price_peaks) < 2 or len(cvd_peaks) < 2 or len(price_valleys) < 2 or len(cvd_valleys) < 2:
        return None

    result = {
        "symbol": symbol,
        "direction": None,
        "strength": 0,
        "reason": "",
        "entry_price": 0,
        "stop_loss": 0,
        "target1": 0,
        "target2": 0,
        "pattern": None,
        "rsi": 50.0,
        "cvd_strength": 0,
        "kline_idx": None,
        "oi_direction": None,    # OI方向: 'rise'(OI增)/'fall'(OI减)/'stable'
        "funding_rate": None,    # 年化资金费率（小数形式）
    }

    # 计算 RSI
    rsi = calc_rsi(closes, RSI_PERIOD)
    result["rsi"] = round(rsi, 2)

    # ─── 顶背离（做空）：RSI>=65 才有效（避免在超买区域追空）────────
    # ─── 底背离（做多）：RSI<=35 才有效（避免在超卖区域追多）────────
    RSI_SHORT_MIN = 65  # 做空最低RSI门槛
    RSI_LONG_MAX = 35   # 做多最高RSI门槛

    # ATR 波动率归一化 OI 方向阈值（替代硬编码 ±0.5%）
    atr_pct = calc_atr_threshold(closes)
    oi_rise_thresh = max(atr_pct, 0.005)   # OI 上涨阈值（取 ATR 相对波动和 0.5% 的较大值）
    oi_fall_thresh = -max(atr_pct, 0.005)  # OI 下跌阈值

    # P1: 预计算 CVD 趋势结构（整个函数只算一次，供两个分支复用）
    trend_structure = check_cvd_trend_structure(cvd, cvd_peaks, cvd_valleys)

    # ─── CVD 领先型背离（价格滞后）──────────────
    # 做多：CVD创新低，但价格没创新低 + 看涨形态
    # 做空：CVD创新高，但价格没创新高 + 看跌形态

    # CVD历史新低（领先型底背离）
    if len(cvd_valleys) >= 2:
        all_cv = sorted(cvd_valleys)
        for ci in all_cv[-3:]:  # 最近3个CVD谷
            if ci == all_cv[-1]:
                continue
            curr_cvd_val = cvd[ci]
            prev_cvd_val = cvd[all_cv[-1]]
            if curr_cvd_val >= prev_cvd_val:
                continue
            nearby_pv = [pv for pv in price_valleys if abs(pv - ci) <= 10]
            if not nearby_pv:
                continue
            pv_nearest = min(nearby_pv, key=lambda x: abs(x - ci))
            curr_price = closes[pv_nearest]
            pv_sorted = sorted(price_valleys)
            if pv_nearest == pv_sorted[-1]:
                continue
            prev_pv = pv_sorted[-1]
            prev_price_val = closes[prev_pv]
            if curr_price >= prev_price_val:
                pattern = detect_pattern(klines, pv_nearest)
                if pattern in ["bullish_engulfing", "hammer"]:
                    # RSI过滤：超卖才做多
                    if rsi > RSI_LONG_MAX:
                        pass  # RSI不在超卖区，跳过但继续找下一个
                    else:
                        # P0: CVD 动量过滤
                        passed_roc, roc_val, roc_reason = cvd_momentum_filter(cvd, "long")
                        if not passed_roc:
                            # 动量不足，降权继续（strength-1 不跳过）
                            pass
                        # P1: 趋势结构降权
                        trend_adj = cvd_trend_filter(trend_structure, "long")
                        entry_price = curr_price
                        # 止损在前低下方，目标在entry上方（底背离做多）
                        sl_exact = prev_price_val * 0.98
                        tp1_exact = entry_price * 1.02
                        oi_dir = _oi_dir_map.get(symbol, "→OI")
                        共振标识 = "📈" if oi_dir == "↑OI" else ""
                        cvd_drop_pct = (curr_cvd_val - prev_cvd_val) / abs(prev_cvd_val) * 100
                        base_strength = 3
                        final_strength = max(1, base_strength + trend_adj)
                        result["direction"] = "long"
                        result["divergence_type"] = "cvd_bottom"
                        result["共振"] = 共振标识
                        result["reason"] = (f"CVD底背离(领先)：CVD新低 | 价格未新低 | 形态={pattern}{共振标识}")
                        result["divergence_desc"] = result["reason"]
                        result["strength"] = final_strength
                        result["entry_price"] = round(entry_price, 4)
                        result["stop_loss"] = round(sl_exact, 4)
                        result["target1"] = round(tp1_exact, 4)
                        result["target2"] = round(tp1_exact * 1.02, 4)
                        result["_entry_exact"] = entry_price
                        result["_sl_exact"] = sl_exact
                        result["_tp1_exact"] = tp1_exact
                        result["pattern"] = pattern
                        result["cvd_strength"] = round(abs(cvd_drop_pct), 2)
                        result["kline_idx"] = pv_nearest
                        result["price_idx"] = pv_nearest
                        # 底背离 8个波峰波谷字段（与顶背离对称）
                        result["_curr_cvd_peak_val"] = round(curr_cvd_val, 2)
                        result["_prev_cvd_peak_val"] = round(prev_cvd_val, 2)
                        result["_curr_price_peak_val"] = round(curr_price, 6)
                        result["_prev_price_peak_val"] = round(prev_price_val, 6)
                        result["_curr_cvd_peak_idx"] = int(ci)
                        result["_prev_cvd_peak_idx"] = int(all_cv[-1])
                        result["_curr_price_peak_idx"] = int(pv_nearest)
                        result["_prev_price_peak_idx"] = int(prev_pv)
                        # P0/P1 新增字段
                        result["_cvd_roc"] = round(roc_val, 2)
                        result["_cvd_trend"] = trend_structure
                        return result

    # CVD历史新高（领先型顶背离）
    if len(cvd_peaks) >= 2:
        all_cp = sorted(cvd_peaks)
        for ci in all_cp[-3:]:
            if ci == all_cp[-1]:
                continue
            curr_cvd_peak = cvd[ci]
            prev_cvd_peak = cvd[all_cp[-1]]
            if curr_cvd_peak <= prev_cvd_peak:
                continue
            nearby_pp = [pp for pp in price_peaks if abs(pp - ci) <= 10]
            if not nearby_pp:
                continue
            pp_nearest = min(nearby_pp, key=lambda x: abs(x - ci))
            curr_price = closes[pp_nearest]
            pp_sorted = sorted(price_peaks)
            if pp_nearest == pp_sorted[-1]:
                continue
            prev_pp = pp_sorted[-1]
            prev_price_val = closes[prev_pp]
            if curr_price <= prev_price_val:
                pattern = detect_pattern(klines, pp_nearest)
                if pattern in ["bearish_engulfing", "shooting_star"]:
                    # RSI过滤：超买才做空
                    if rsi < RSI_SHORT_MIN:
                        pass  # RSI不在超买区，跳过但继续找下一个
                    else:
                        # P0: CVD 动量过滤
                        passed_roc, roc_val, roc_reason = cvd_momentum_filter(cvd, "short")
                        if not passed_roc:
                            pass
                        # P1: 趋势结构降权
                        trend_adj = cvd_trend_filter(trend_structure, "short")
                        entry_price = curr_price
                        # 止损在前高上方，目标在前高下方（顶背离做空）
                        sl_exact = prev_price_val * 1.02
                        tp1_exact = prev_price_val * 0.98
                        oi_dir = _oi_dir_map.get(symbol, "→OI")
                        共振标识 = "📉" if oi_dir == "↓OI" else ""
                        cvd_rise_pct = (curr_cvd_peak - prev_cvd_peak) / abs(prev_cvd_peak) * 100
                        base_strength = 3
                        final_strength = max(1, base_strength + trend_adj)
                        result["direction"] = "short"
                        result["divergence_type"] = "cvd_top"
                        result["共振"] = 共振标识
                        result["reason"] = (f"CVD顶背离(领先)：CVD新高 | 价格未新高 | 形态={pattern}{共振标识}")
                        result["divergence_desc"] = result["reason"]
                        result["strength"] = final_strength
                        result["entry_price"] = round(entry_price, 4)
                        result["stop_loss"] = round(sl_exact, 4)
                        result["target1"] = round(tp1_exact, 4)
                        result["target2"] = round(tp1_exact * 0.98, 4)
                        result["_entry_exact"] = entry_price
                        result["_sl_exact"] = sl_exact
                        result["_tp1_exact"] = tp1_exact
                        result["pattern"] = pattern
                        result["cvd_strength"] = round(abs(cvd_rise_pct), 2)
                        result["kline_idx"] = pp_nearest
                        result["price_idx"] = pp_nearest
                        result["_curr_cvd_peak_val"] = round(curr_cvd_peak, 2)
                        result["_prev_cvd_peak_val"] = round(prev_cvd_peak, 2)
                        result["_curr_price_peak_val"] = round(curr_price, 6)
                        result["_prev_price_peak_val"] = round(prev_price_val, 6)
                        result["_curr_cvd_peak_idx"] = int(ci)
                        result["_prev_cvd_peak_idx"] = int(all_cp[-1])
                        result["_curr_price_peak_idx"] = int(pp_nearest)
                        result["_prev_price_peak_idx"] = int(prev_pp)
                        # P0/P1 新增字段
                        result["_cvd_roc"] = round(roc_val, 2)
                        result["_cvd_trend"] = trend_structure
                        return result

    return None

# ========== 信号池管理 ==========
def load_watch_pool() -> dict:
    """加载现有信号池"""
    pool_file = f"{KLINES_DIR}/.watch_pool.json"
    if os.path.exists(pool_file):
        with open(pool_file) as f:
            content = f.read().strip()
            if content:
                return json.loads(content)
    return {}

def save_watch_pool(pool: dict):
    """保存信号池"""
    pool_file = f"{KLINES_DIR}/.watch_pool.json"
    with open(pool_file, "w") as f:
        json.dump(pool, f, indent=2, default=str)

def clean_pool(pool: dict) -> tuple:
    """清理过期信号（超过TTL），返回 (有效池, 过期池)"""
    now = datetime.now()
    cleaned, expired = {}, {}
    for sym, sig in pool.items():
        try:
            created = datetime.fromisoformat(sig.get("created_at", "2020-01-01"))
            age_hours = (now - created).total_seconds() / 3600
            if age_hours < SIGNAL_TTL_HOURS:
                cleaned[sym] = sig
            else:
                expired[sym] = sig
        except:
            pass
    return cleaned, expired

# ========== 单币检测 ==========
_oi_fr_cache = {}
_oi_dir_map = {}

def check_symbol(symbol: str):
    """检测单个币"""
    filepath = f"{KLINES_DIR}/{symbol}.json"
    if not os.path.exists(filepath):
        return symbol, None
    
    try:
        with open(filepath) as f:
            data = json.load(f)
        klines = data.get("klines", [])
        if len(klines) < 100:
            return symbol, None
        
        signal = detect_divergence(symbol, klines)
        
        # 补充 OI/FR 信息
        if signal and (_oi_fr_cache or _oi_dir_map):
            # OI 方向：直接取 InfluxDB 查出的 map
            oi_dir = _oi_dir_map.get(symbol, "→OI")
            
            # FR：取缓存
            fr = None
            if _oi_fr_cache:
                fr = _oi_fr_cache.get(symbol, {}).get("fr")
            
            signal["oi_direction"] = oi_dir
            signal["funding_rate"] = fr
        
        return symbol, signal
    except Exception as e:
        return symbol, None

# 推送统一由 push_pool.py 负责，本文件只负责检测和写入信号池

# ========== 主流程 ==========
def main():
    print(f"[{datetime.now()}] 开始背离检测")

    # 加载 OI/FR 缓存（FR 用）
    global _oi_fr_cache
    _oi_fr_cache = load_oi_fr_cache()
    print(f"  OI/FR 缓存: {len(_oi_fr_cache)} 个币")

    # 所有币（提前定义，InfluxDB OI 查询需要）
    files = [f[:-5] for f in os.listdir(KLINES_DIR)
             if f.endswith(".json") and not f.startswith(".")]
    print(f"  检测 {len(files)} 个交易对")

    # 预计算所有币的 ATR 波动率阈值（用于 OI 方向判断）
    atr_thresh = {}
    for sym in files:
        fp = f"{KLINES_DIR}/{sym}.json"
        if os.path.exists(fp):
            try:
                with open(fp) as f:
                    data = json.load(f)
                klines = data.get("klines", [])
                if len(klines) >= 20:
                    closes = np.array([k["c"] for k in klines])
                    atr_thresh[sym] = calc_atr_threshold(closes)
            except Exception:
                pass
    print(f"  ATR 阈值: {len(atr_thresh)} 个币")

    # 查 InfluxDB OI 方向（最近 3h）
    global _oi_dir_map
    _oi_dir_map = fetch_oi_direction_from_influx(files, atr_thresh)
    print(f"  OI 方向: {len(_oi_dir_map)} 个币有数据")

    # 加载现有信号池（并追踪过期信号的结果）
    pool = load_watch_pool()
    pool, expired = clean_pool(pool)

    # 加载 outcomes 历史
    outcomes_file = f"{KLINES_DIR}/.outcomes.json"
    outcomes = {}
    if os.path.exists(outcomes_file):
        try:
            with open(outcomes_file) as f:
                outcomes = json.load(f)
        except Exception:
            pass

    # 追踪过期信号的命中结果
    for sym, sig in expired.items():
        if sym in outcomes:
            continue  # 已记录过
        try:
            entry = sig.get("_entry_exact", sig.get("entry_price"))
            sl = sig.get("_sl_exact", sig.get("stop_loss"))
            direction = sig.get("direction")
            with open(f"{KLINES_DIR}/{sym}.json") as f:
                klines = json.load(f).get("klines", [])
            if not klines:
                continue
            latest_price = klines[-1]["c"]
            hit = None
            if direction == "long":
                if latest_price <= sl:
                    hit = "sl"   # 止损
                elif latest_price >= sig.get("_tp1_exact", sig.get("target1")):
                    hit = "tp"   # 止盈
                else:
                    hit = "open"
            elif direction == "short":
                if latest_price >= sl:
                    hit = "sl"
                elif latest_price <= sig.get("_tp1_exact", sig.get("target1")):
                    hit = "tp"
                else:
                    hit = "open"
            outcomes[sym] = {"hit": hit, "entry": entry, "exit": latest_price,
                             "direction": direction, "expired_at": datetime.now().isoformat()}
        except Exception:
            pass

    with open(outcomes_file, "w") as f:
        json.dump(outcomes, f, indent=2, default=str)

    new_signals = []
    start = time.time()

    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {executor.submit(check_symbol, sym): sym for sym in files}

        for i, future in enumerate(as_completed(futures), 1):
            sym, signal = future.result()
            if signal and signal.get("direction"):
                signal["created_at"] = datetime.now().isoformat()
                pool[sym] = signal
                new_signals.append(signal)

            if i % 100 == 0 or i == len(files):
                print(f"  进度 {i}/{len(files)}")

    # 保存信号池
    save_watch_pool(pool)

    elapsed = time.time() - start
    print(f"\n[{datetime.now()}] 完成，耗时 {elapsed:.1f}s")
    print(f"当前池信号数: {len(pool)} | 新增: {len(new_signals)}")

    # 自动写入 .status.json
    status = {
        "generated_at": datetime.now().isoformat(),
        "kline_files": len(files),
        "pool_signals": len(pool),
        "pool_long": sum(1 for s in pool.values() if s.get("direction") == "long"),
        "pool_short": sum(1 for s in pool.values() if s.get("direction") == "short"),
        "new_signals_this_run": len(new_signals),
        "cvd_peaks_cache": len([f for f in os.listdir(KLINES_DIR) if f.startswith(".cvd_peaks_")]),
        "cvd_valleys_cache": len([f for f in os.listdir(KLINES_DIR) if f.startswith(".cvd_valleys_")]),
        "latest_fetch": datetime.now().isoformat(),
        "cron_jobs": [
            "klines_fetch :00",
            "cvd_calc :06",
            "oi_fr_fetch :12",
            "kline_peaks_calc :18",
            "divergence_check :20"
        ]
    }
    with open(f"{KLINES_DIR}/.status.json", "w") as f:
        json.dump(status, f, indent=2)
    print(f"  .status.json 已更新")

    # 完整扫描后推送一次（不管有没有新信号）
    from subprocess import run
    run(["python3", f"{KLINES_DIR}/push_pool.py", "--force"], check=False)

    # 也输出当前池所有信号摘要
    if pool:
        print(f"\n=== 当前观察池 ({len(pool)} 个信号) ===")
        for sym, sig in list(pool.items())[:10]:
            print(f"  {sym}: {sig['direction']} | 入场 {sig['entry_price']} | RSI {sig['rsi']} | {sig['pattern']}")
        if len(pool) > 10:
            print(f"  ... 还有 {len(pool)-10} 个")

if __name__ == "__main__":
    main()
