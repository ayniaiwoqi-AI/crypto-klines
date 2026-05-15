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

def load_oi_fr_cache() -> dict:
    """加载本地 OI/FR 缓存（FR 用，OI 方向改查 InfluxDB）"""
    if os.path.exists(CACHE_FILE):
        try:
            with open(CACHE_FILE) as f:
                return json.load(f)
        except Exception:
            pass
    return {}

def fetch_oi_direction_from_influx(symbols: list[str]) -> dict:
    """从 InfluxDB 批量查最近 3h OI，分批返回 {symbol: '↑OI'/'↓OI'/'→OI'}"""
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
                if pct > 0.5:
                    oi_dir[sym] = "↑OI"
                elif pct < -0.5:
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
    if len(klines) < 50:
        return None
    
    closes = np.array([k["c"] for k in klines])
    cvd = calc_cvd(klines)
    
    # 检测价格峰谷
    price_peaks, price_valleys = find_peaks_valleys(closes, distance=15, prominence_pct=0.03)
    # 检测CVD峰谷
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
    
    # 转换索引：price_peaks/valleys 是数组索引（从0开始）
    # 需要配对最新几个峰谷做判断
    # 取最近的3个价格峰谷
    
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
                    entry_price = curr_price
                    # 止损在前低下方，目标在entry上方（底背离做多）
                    sl_exact = prev_price_val * 0.98
                    tp1_exact = entry_price * 1.02
                    oi_dir = _oi_dir_map.get(symbol, "→OI")
                    共振标识 = "📈" if oi_dir == "↑OI" else ""
                    cvd_drop_pct = (curr_cvd_val - prev_cvd_val) / abs(prev_cvd_val) * 100
                    result["direction"] = "long"
                    result["divergence_type"] = "cvd_bottom"
                    result["共振"] = 共振标识
                    result["reason"] = (f"CVD底背离(领先)：CVD新低 | 价格未新低 | 形态={pattern}{共振标识}")
                    result["divergence_desc"] = result["reason"]
                    result["strength"] = 3
                    result["entry_price"] = round(entry_price, 4)
                    result["stop_loss"] = round(sl_exact, 4)
                    result["target1"] = round(tp1_exact, 4)
                    result["target2"] = round(tp1_exact * 1.02, 4)
                    # 存精确值用于显示百分比（避免小数值四舍五入失真）
                    result["_entry_exact"] = entry_price
                    result["_sl_exact"] = sl_exact
                    result["_tp1_exact"] = tp1_exact
                    result["pattern"] = pattern
                    result["cvd_strength"] = round(abs(cvd_drop_pct), 2)
                    result["kline_idx"] = pv_nearest
                    result["price_idx"] = pv_nearest
                    # 波峰波谷详情（用于展示）
                    result["_curr_cvd_peak_val"] = round(curr_cvd_val, 2)
                    result["_prev_cvd_peak_val"] = round(prev_cvd_val, 2)
                    result["_curr_price_peak_val"] = round(curr_price, 6)
                    result["_prev_price_peak_val"] = round(prev_price_val, 6)
                    result["_curr_cvd_peak_idx"] = int(ci)
                    result["_prev_cvd_peak_idx"] = int(all_cv[-1])
                    result["_curr_price_peak_idx"] = int(pv_nearest)
                    result["_prev_price_peak_idx"] = int(prev_pv)
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
                    entry_price = curr_price
                    # 止损在前高上方，目标在前高下方（顶背离做空）
                    sl_exact = prev_price_val * 1.02
                    tp1_exact = prev_price_val * 0.98
                    oi_dir = _oi_dir_map.get(symbol, "→OI")
                    共振标识 = "📉" if oi_dir == "↓OI" else ""
                    cvd_rise_pct = (curr_cvd_peak - prev_cvd_peak) / abs(prev_cvd_peak) * 100
                    result["direction"] = "short"
                    result["divergence_type"] = "cvd_top"
                    result["共振"] = 共振标识
                    result["reason"] = (f"CVD顶背离(领先)：CVD新高 | 价格未新高 | 形态={pattern}{共振标识}")
                    result["divergence_desc"] = result["reason"]
                    result["strength"] = 3
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
                    # 波峰波谷详情（用于展示）
                    result["_curr_cvd_peak_val"] = round(curr_cvd_peak, 2)
                    result["_prev_cvd_peak_val"] = round(prev_cvd_peak, 2)
                    result["_curr_price_peak_val"] = round(curr_price, 6)
                    result["_prev_price_peak_val"] = round(prev_price_val, 6)
                    result["_curr_cvd_peak_idx"] = int(ci)
                    result["_prev_cvd_peak_idx"] = int(all_cp[-1])
                    result["_curr_price_peak_idx"] = int(pp_nearest)
                    result["_prev_price_peak_idx"] = int(prev_pp)
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

def clean_pool(pool: dict) -> dict:
    """清理过期信号（超过TTL）"""
    now = datetime.now()
    cleaned = {}
    for sym, sig in pool.items():
        try:
            created = datetime.fromisoformat(sig.get("created_at", "2020-01-01"))
            age_hours = (now - created).total_seconds() / 3600
            if age_hours < SIGNAL_TTL_HOURS:
                cleaned[sym] = sig
        except:
            pass
    return cleaned

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
        if len(klines) < 50:
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

# ========== 格式化信号消息 ==========
def format_signal_message(sig: dict) -> str:
    """格式化 Telegram 推送消息"""
    direction_emoji = "🟢" if sig["direction"] == "long" else "🔴"
    direction_text = "看涨" if sig["direction"] == "long" else "看跌"
    
    tp1_pct = (sig["target1"] - sig["entry_price"]) / sig["entry_price"] * 100
    tp2_pct = (sig["target2"] - sig["entry_price"]) / sig["entry_price"] * 100
    sl_pct = (sig["entry_price"] - sig["stop_loss"]) / sig["entry_price"] * 100
    
    if sig["direction"] == "short":
        tp1_pct = (sig["entry_price"] - sig["target1"]) / sig["entry_price"] * 100
        tp2_pct = (sig["entry_price"] - sig["target2"]) / sig["entry_price"] * 100
        sl_pct = (sig["stop_loss"] - sig["entry_price"]) / sig["entry_price"] * 100
    
    # OI 方向 + Funding Rate
    oi_display = sig.get("oi_direction", "→OI")
    fr_val = sig.get("funding_rate")
    fr_display = f"{fr_val*100:.4f}%" if fr_val is not None else "N/A"
    
    msg = (
        f"{direction_emoji} *{sig['symbol']}* {direction_text}\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"📊 背离：CVD{'底背离' if sig['direction']=='long' else '顶背离'}\n"
        f"📈 RSI：{sig['rsi']}\n"
        f"📋 形态：{sig['pattern']}\n"
        f"💰 OI：{oi_display}（↑持仓增 ↓持仓减 →稳定）\n"
        f"💸 资金费：{fr_display}\n"
        f"\n"
        f"入场 {sig['entry_price']} | 止损 {sig['stop_loss']} (-{sl_pct:.1f}%)\n"
        f"目标1 {sig['target1']} (+{tp1_pct:.1f}%) | 目标2 {sig['target2']} (+{tp2_pct:.1f}%)\n"
        f"\n"
        f"📝 {sig['reason']}\n"
        f"🕐 {sig.get('created_at', datetime.now().isoformat())}"
    )
    return msg

# ========== 批量推送 ==========
def fmt_signal(sig: dict) -> str:
    emoji = "🟢" if sig["direction"] == "long" else "🔴"
    sym = sig["symbol"]
    rsi = sig["rsi"]
    div = "顶📉CVD" if sig.get("divergence_type") == "cvd_top" else "底📈CVD" if sig.get("divergence_type") == "cvd_bottom" else "背离"
    div_tag = ""
    共振 = sig.get("共振", "")  # OI共振标识：📈或📉
    oi_arrow = sig.get("oi_direction", "→OI")
    oi_display = oi_arrow[0] if oi_arrow else "→"
    tp1_pct = (sig["target1"] - sig["entry_price"]) / sig["entry_price"] * 100
    sl_pct = (sig["entry_price"] - sig["stop_loss"]) / sig["entry_price"] * 100
    if sig["direction"] == "short":
        tp1_pct = (sig["entry_price"] - sig["target1"]) / sig["entry_price"] * 100
        sl_pct = (sig["stop_loss"] - sig["entry_price"]) / sig["entry_price"] * 100
    共振 = sig.get("共振", "")
    div_desc = sig.get("divergence_desc", "")
    extra_lines = f"\n📊 {div_desc}" if div_desc else ""
    tp_str = f"目+{abs(tp1_pct):.1f}%({sig['target1']})"
    return (f"{emoji} *{sym}* | {div}{div_tag} | {共振} | OI{oi_display} | "
            f"入{sig['entry_price']} {sl_str} {tp_str}{extra_lines}")

def batch_send(signals: list, label: str = ""):
    if not signals:
        return
    total = len(signals)
    with ThreadPoolExecutor(max_workers=5) as executor:
        for i in range(0, total, BATCH_SIZE):
            batch = signals[i:i+BATCH_SIZE]
            lines = [fmt_signal(s) for s in batch]
            header = (f"📊 *观察池{label}* | {datetime.now().strftime('%m-%d %H:%M')} | "
                     f"{total}信号" + (f" ({i+1}-{i+len(batch)})" if total > BATCH_SIZE else ""))
            msg = header + "\n" + "\n".join(lines)
            future = executor.submit(send_telegram, msg)
            ok = future.result()
            print(f"  批次 {i//BATCH_SIZE+1}: {len(batch)}信号 {'✅' if ok else '❌'}")
            time.sleep(0.5)

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

    # 查 InfluxDB OI 方向（最近 3h）
    global _oi_dir_map
    _oi_dir_map = fetch_oi_direction_from_influx(files)
    print(f"  OI 方向: {len(_oi_dir_map)} 个币有数据")

    # 加载现有信号池
    pool = load_watch_pool()
    pool = clean_pool(pool)

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
