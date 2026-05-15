#!/usr/bin/env python3
"""
CVD + 波峰波谷 计算并写入 InfluxDB
- 读取 /root/crypto_klines/{SYMBOL}.json
- 计算 CVD 序列、检测波峰波谷
- 写入 InfluxDB (klines bucket)
- 支持增量（只写入新的波峰波谷）
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
INFLUX_ORG = "crypto-lab"
INFLUX_DB = "klines"

# CVD 参数
PROMINENCE_PCT = 0.005  # 波峰/谷的显著性（相对于CVD总量）

# ========== InfluxDB 写入 ==========
def write_influx(lines: list[str]):
    """批量写入 InfluxDB（line protocol）"""
    if not lines:
        return
    headers = {
        "Authorization": f"Token {INFLUX_TOKEN}",
        "Content-Type": "text/plain"
    }
    resp = requests.post(
        f"{INFLUX_URL}/write?db={INFLUX_DB}&org={INFLUX_ORG}&rp=autogen&precision=ns",
        headers=headers,
        data="\n".join(lines),
        timeout=30
    )
    if resp.status_code != 204:
        print(f"    [WARN] 写入失败: {resp.status_code} {resp.text[:100]}")
    return resp.status_code == 204

# ========== CVD 计算 ==========
def calc_cvd(klines: list) -> tuple:
    """
    返回 (deltas, cvd, klines)
    delta = volume * ((close - low) - (high - close)) / (high - low)
    """
    deltas = []
    for k in klines:
        high, low, close, volume = k["h"], k["l"], k["c"], k["v"]
        if high == low:
            delta = 0.0
        else:
            delta = volume * ((close - low) - (high - close)) / (high - low)
        deltas.append(delta)
    return np.array(deltas), np.cumsum(deltas)

def find_peaks_valleys(cvd: np.array, distance: int = 10, prominence_pct: float = 0.005):
    """检测CVD波峰和波谷"""
    # 自动计算 prominence
    cvd_range = cvd.max() - cvd.min()
    prominence = cvd_range * prominence_pct
    
    peaks, _ = find_peaks(cvd, distance=distance, prominence=prominence)
    valleys, _ = find_peaks(-cvd, distance=distance, prominence=prominence)
    return peaks, valleys

# ========== 增量读取本地已有波峰/谷 ==========
def load_local_events(symbol: str) -> tuple:
    """返回 (已存的peak_times, 已存的valley_times) 集合"""
    peaks_file = f"/root/crypto_klines/.cvd_peaks_{symbol}.json"
    valleys_file = f"/root/crypto_klines/.cvd_valleys_{symbol}.json"
    
    peaks = set()
    valleys = set()
    
    if os.path.exists(peaks_file):
        with open(peaks_file) as f:
            peaks = set(json.load(f))
    if os.path.exists(valleys_file):
        with open(valleys_file) as f:
            valleys = set(json.load(f))
    
    return peaks, valleys

def save_local_events(symbol: str, peaks: list, valleys: list):
    peaks_file = f"/root/crypto_klines/.cvd_peaks_{symbol}.json"
    valleys_file = f"/root/crypto_klines/.cvd_valleys_{symbol}.json"
    with open(peaks_file, "w") as f:
        json.dump(peaks, f)
    with open(valleys_file, "w") as f:
        json.dump(valleys, f)

# ========== 单币处理 ==========
def process_symbol(symbol: str):
    """处理单个币：计算CVD+波峰波谷，写入InfluxDB"""
    filepath = f"{KLINES_DIR}/{symbol}.json"
    if not os.path.exists(filepath):
        return symbol, 0, 0, 0, None

    try:
        with open(filepath) as f:
            data = json.load(f)
    except Exception as e:
        return symbol, 0, 0, 0, f"文件读取失败: {e}"

    # 兼容两种格式：{"klines":[...]} 或直接 [...]
    if isinstance(data, dict):
        klines = data.get("klines", [])
    else:
        klines = data

    if not isinstance(klines, list) or len(klines) < 10:
        return symbol, 0, 0, 0, "数据不足"
    if not isinstance(klines[0], dict):
        return symbol, 0, 0, 0, f"格式错误: {type(klines[0])}"
    
    # 计算CVD
    deltas, cvd = calc_cvd(klines)
    
    # 检测波峰波谷
    peaks, valleys = find_peaks_valleys(cvd, distance=10, prominence_pct=PROMINENCE_PCT)
    
    # 读取本地已有事件（用于增量）
    local_peaks, local_valleys = load_local_events(symbol)
    
    lines = []
    new_peak_count = 0
    new_valley_count = 0
    
    # 波峰
    for idx in peaks:
        k = klines[idx]
        ts_ns = k["t"] * 1_000_000  # ms -> ns
        if ts_ns not in local_peaks:
            lines.append(f"cvd_peaks,symbol={symbol} cvd={cvd[idx]},price={k['c']},cvd_delta={deltas[idx] if idx > 0 else 0} {ts_ns}")
            local_peaks.add(ts_ns)
            new_peak_count += 1
    
    # 波谷
    for idx in valleys:
        k = klines[idx]
        ts_ns = k["t"] * 1_000_000
        if ts_ns not in local_valleys:
            lines.append(f"cvd_valleys,symbol={symbol} cvd={cvd[idx]},price={k['c']},cvd_delta={deltas[idx] if idx > 0 else 0} {ts_ns}")
            local_valleys.add(ts_ns)
            new_valley_count += 1
    
    # 写入InfluxDB
    if lines:
        write_influx(lines)
        # 保存本地事件记录
        save_local_events(symbol, list(local_peaks), list(local_valleys))
    
    return symbol, new_peak_count, new_valley_count, len(lines), None

# ========== 主流程 ==========
def main():
    files = [f[:-5] for f in os.listdir(KLINES_DIR) if f.endswith(".json")]
    print(f"[{datetime.now()}] 开始CVD计算，共 {len(files)} 个交易对")
    
    results = {"ok": 0, "skip": 0, "fail": 0, "failed": []}
    start = time.time()
    
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {executor.submit(process_symbol, sym): sym for sym in files}
        
        for i, future in enumerate(as_completed(futures), 1):
            sym, n_peaks, n_valleys, n_lines, err = future.result()
            if err:
                results["fail"] += 1
                results["failed"].append(f"{sym}: {err}")
            elif n_lines == 0:
                results["skip"] += 1
            else:
                results["ok"] += 1
            
            if i % 100 == 0 or i == len(files):
                elapsed = time.time() - start
                print(f"进度 {i}/{len(files)} | 新增事件 {results['ok']} | 无更新 {results['skip']} | 失败 {results['fail']} | {elapsed:.1f}s")
    
    elapsed = time.time() - start
    print(f"\n[{datetime.now()}] 完成，耗时 {elapsed:.1f}s")
    print(f"新增事件: {results['ok']} | 无更新: {results['skip']} | 失败: {results['fail']}")
    if results["failed"]:
        print(f"失败: {results['failed'][:5]}")

if __name__ == "__main__":
    main()
