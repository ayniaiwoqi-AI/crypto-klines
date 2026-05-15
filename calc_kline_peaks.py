#!/usr/bin/env python3
"""
K线价格波峰波谷计算
- 读取 /root/crypto_klines/{SYMBOL}.json（987根1H K线）
- 检测价格局部波峰和波谷
- 写入 InfluxDB (klines bucket)
- 增量：本地缓存已记录时间戳，后续只写入新出现的峰谷
"""
import json
import os
import time
import numpy as np
import requests
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from scipy.signal import find_peaks

# ========== 配置 ==========
KLINES_DIR = "/root/crypto_klines"
INFLUX_URL = "http://localhost:8086"
INFLUX_TOKEN = "influx-token-crypto-2026"
INFLUX_DB = "klines"
TARGET_K = 987
PEAK_DISTANCE = 8  # 最小峰谷间隔（根K线）
PEAK_PROMINENCE_PCT = 0.02  # 波峰显著性（相对波动幅度）

# ========== InfluxDB ==========
def write_influx(lines: list[str]) -> bool:
    if not lines:
        return True
    headers = {
        "Authorization": f"Token {INFLUX_TOKEN}",
        "Content-Type": "text/plain"
    }
    resp = requests.post(
        f"{INFLUX_URL}/write?db={INFLUX_DB}&org=crypto-lab&rp=autogen&precision=ns",
        headers=headers,
        data="\n".join(lines),
        timeout=30
    )
    if resp.status_code != 204:
        print(f"    [WARN] 写入失败: {resp.status_code} {resp.text[:100]}")
        return False
    return True

# ========== 价格波峰波谷检测 ==========
def find_price_peaks_valleys(closes: np.array, distance: int = PEAK_DISTANCE, prominence_pct: float = PEAK_PROMINENCE_PCT):
    """检测价格波峰和波谷"""
    price_range = closes.max() - closes.min()
    prominence = price_range * prominence_pct
    
    peaks, _ = find_peaks(closes, distance=distance, prominence=prominence)
    valleys, _ = find_peaks(-closes, distance=distance, prominence=prominence)
    return peaks, valleys

# ========== 本地事件缓存（用于增量） ==========
def load_local_events(symbol: str, event_type: str):
    cache_file = f"{KLINES_DIR}/.kline_{event_type}_{symbol}.json"
    if os.path.exists(cache_file):
        with open(cache_file) as f:
            raw = json.load(f)
            if isinstance(raw, list):
                return set(raw)
    return set()

def save_local_events(symbol: str, event_type: str, times: set):
    """保存时排序，保证增量检测正确"""
    cache_file = f"{KLINES_DIR}/.kline_{event_type}_{symbol}.json"
    with open(cache_file, "w") as f:
        json.dump(sorted(list(times)), f)

# ========== 单币处理 ==========
def process_symbol(symbol: str):
    filepath = f"{KLINES_DIR}/{symbol}.json"
    if not os.path.exists(filepath):
        return symbol, 0, 0, None
    
    with open(filepath) as f:
        data = json.load(f)
    
    klines = data.get("klines", [])
    if len(klines) < 20:
        return symbol, 0, 0, "数据不足"
    
    closes = np.array([k["c"] for k in klines])
    highs = np.array([k["h"] for k in klines])
    lows = np.array([k["l"] for k in klines])
    volumes = np.array([k["v"] for k in klines])
    
    peaks, valleys = find_price_peaks_valleys(closes)
    
    local_peaks = load_local_events(symbol, "peak")
    local_valleys = load_local_events(symbol, "valley")
    
    lines = []
    new_peak = 0
    new_valley = 0
    
    for idx in peaks:
        k = klines[idx]
        ts_ns = k["t"] * 1_000_000
        if ts_ns not in local_peaks:
            lines.append(
                f"kline_peaks,symbol={symbol} "
                f"price={k['c']},high={k['h']},low={k['l']},volume={k['v']},cvd_delta=0 {ts_ns}"
            )
            local_peaks.add(ts_ns)
            new_peak += 1
    
    for idx in valleys:
        k = klines[idx]
        ts_ns = k["t"] * 1_000_000
        if ts_ns not in local_valleys:
            lines.append(
                f"kline_valleys,symbol={symbol} "
                f"price={k['c']},high={k['h']},low={k['l']},volume={k['v']},cvd_delta=0 {ts_ns}"
            )
            local_valleys.add(ts_ns)
            new_valley += 1
    
    if lines:
        ok = write_influx(lines)
        if ok:
            return symbol, new_peak, new_valley, None
        # InfluxDB失败：缓存已保存，防止重复检测
        return symbol, new_peak, new_valley, "InfluxDB写入失败"

    # 无新数据：确保缓存文件存在（避免每次都重新全量计算）
    if not os.path.exists(f"{KLINES_DIR}/.kline_peak_{symbol}.json"):
        save_local_events(symbol, "peak", local_peaks)
    if not os.path.exists(f"{KLINES_DIR}/.kline_valley_{symbol}.json"):
        save_local_events(symbol, "valley", local_valleys)

    return symbol, 0, 0, None

# ========== 主流程 ==========
def _is_kline_file(fname: str) -> bool:
    if fname.endswith(".json"):
        for prefix in (".cvd_peaks_", ".cvd_valleys_", ".kline_peak_", ".kline_valley_",
                       ".watch_pool", ".push_state", ".oi_fr_cache", ".status"):
            if fname.startswith(prefix):
                return False
        return True
    return False

def main():
    files = [f[:-5] for f in os.listdir(KLINES_DIR) if _is_kline_file(f)]
    print(f"[{datetime.now()}] K线波峰波谷计算，共 {len(files)} 个交易对")
    
    results = {"ok": 0, "skip": 0, "fail": 0, "failed": []}
    start = time.time()
    
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {executor.submit(process_symbol, sym): sym for sym in files}
        
        for i, future in enumerate(as_completed(futures), 1):
            sym, n_peaks, n_valleys, err = future.result()
            if err:
                results["fail"] += 1
                results["failed"].append(f"{sym}: {err}")
            elif n_peaks + n_valleys == 0:
                results["skip"] += 1
            else:
                results["ok"] += 1
            
            if i % 100 == 0 or i == len(files):
                elapsed = time.time() - start
                print(f"进度 {i}/{len(files)} | 新增 {results['ok']} | 无更新 {results['skip']} | 失败 {results['fail']} | {elapsed:.1f}s")
    
    elapsed = time.time() - start
    print(f"\n[{datetime.now()}] 完成，耗时 {elapsed:.1f}s")
    print(f"新增事件: {results['ok']} | 无更新: {results['skip']} | 失败: {results['fail']}")
    if results["failed"]:
        print(f"失败: {results['failed'][:5]}")

if __name__ == "__main__":
    main()
