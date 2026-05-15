#!/usr/bin/env python3
"""
CVD + 波峰波谷 计算并写入 InfluxDB
- 读取 /root/crypto_klines/{SYMBOL}.json（全量K线）
- 计算 CVD 序列、检测波峰波谷
- 写入 InfluxDB (klines bucket)
- timestamps 本地缓存排序后保存，保证增量检测正确
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

# CVD 参数（与 check_divergence.py 保持一致）
CVD_DISTANCE = 15
CVD_PROMINENCE_PCT = 0.02

# ========== InfluxDB 写入 ==========
def write_influx(lines: list[str]):
    """批量写入 InfluxDB（line protocol）"""
    if not lines:
        return True
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
        return False
    return True

# ========== CVD 计算 ==========
def calc_cvd(klines: list) -> tuple:
    """
    返回 (deltas, cvd)
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

def find_peaks_valleys(cvd: np.array, distance: int = CVD_DISTANCE, prominence_pct: float = CVD_PROMINENCE_PCT):
    """检测CVD波峰和波谷（distance/prominence与check_divergence.py一致）"""
    cvd_range = cvd.max() - cvd.min()
    prominence = cvd_range * prominence_pct
    peaks, _ = find_peaks(cvd, distance=distance, prominence=prominence)
    valleys, _ = find_peaks(-cvd, distance=distance, prominence=prominence)
    return peaks, valleys

# ========== 本地事件缓存 ==========
def load_local_events(symbol: str) -> tuple:
    """返回 (已存peak_timestamps集合, 已存valley_timestamps集合)"""
    peaks_file = f"/root/crypto_klines/.cvd_peaks_{symbol}.json"
    valleys_file = f"/root/crypto_klines/.cvd_valleys_{symbol}.json"
    peaks, valleys = set(), set()
    if os.path.exists(peaks_file):
        with open(peaks_file) as f:
            raw = json.load(f)
            if isinstance(raw, list):
                peaks = set(raw)
    if os.path.exists(valleys_file):
        with open(valleys_file) as f:
            raw = json.load(f)
            if isinstance(raw, list):
                valleys = set(raw)
    return peaks, valleys

def save_local_events(symbol: str, peaks: set, valleys: set):
    """保存事件时间戳（排序后保存，保证增量检测正确）"""
    peaks_file = f"/root/crypto_klines/.cvd_peaks_{symbol}.json"
    valleys_file = f"/root/crypto_klines/.cvd_valleys_{symbol}.json"
    with open(peaks_file, "w") as f:
        json.dump(sorted(list(peaks)), f)
    with open(valleys_file, "w") as f:
        json.dump(sorted(list(valleys)), f)

# ========== 单币处理 ==========
def process_symbol(symbol: str):
    """处理单个币：计算CVD+波峰波谷，增量写入InfluxDB"""
    filepath = f"{KLINES_DIR}/{symbol}.json"
    if not os.path.exists(filepath):
        return symbol, 0, 0, 0, "文件不存在"

    try:
        with open(filepath) as f:
            data = json.load(f)
    except Exception as e:
        return symbol, 0, 0, 0, f"读取失败: {e}"

    if isinstance(data, dict):
        klines = data.get("klines", [])
    else:
        klines = data

    if not isinstance(klines, list) or len(klines) < 10:
        return symbol, 0, 0, 0, "数据不足"
    if not isinstance(klines[0], dict):
        return symbol, 0, 0, 0, f"格式错误: {type(klines[0])}"

    # 读取本地已有事件（用于增量判断）
    local_peaks, local_valleys = load_local_events(symbol)

    # 计算CVD（全量K线）
    deltas, cvd = calc_cvd(klines)

    # 检测波峰波谷（与check_divergence.py一致）
    peaks, valleys = find_peaks_valleys(cvd, distance=CVD_DISTANCE, prominence_pct=CVD_PROMINENCE_PCT)

    lines = []
    new_peak_count = 0
    new_valley_count = 0

    for idx in peaks:
        k = klines[idx]
        ts_ns = k["t"] * 1_000_000
        if ts_ns not in local_peaks:
            lines.append(f"cvd_peaks,symbol={symbol} cvd={cvd[idx]},price={k['c']},cvd_delta={deltas[idx] if idx > 0 else 0} {ts_ns}")
            local_peaks.add(ts_ns)
            new_peak_count += 1

    for idx in valleys:
        k = klines[idx]
        ts_ns = k["t"] * 1_000_000
        if ts_ns not in local_valleys:
            lines.append(f"cvd_valleys,symbol={symbol} cvd={cvd[idx]},price={k['c']},cvd_delta={deltas[idx] if idx > 0 else 0} {ts_ns}")
            local_valleys.add(ts_ns)
            new_valley_count += 1

    # 无论写入成功与否，都更新本地缓存（防止重复检测）
    save_local_events(symbol, local_peaks, local_valleys)

    if lines:
        ok = write_influx(lines)
        if not ok:
            return symbol, new_peak_count, new_valley_count, len(lines), "InfluxDB写入失败"
        return symbol, new_peak_count, new_valley_count, len(lines), None

    return symbol, 0, 0, 0, None

# ========== 主流程 ==========
def _is_kline_file(fname: str) -> bool:
    """判断是否为K线数据文件（非缓存、非配置）"""
    if fname.endswith(".json"):
        # 排除所有缓存文件
        for prefix in (".cvd_peaks_", ".cvd_valleys_", ".kline_peak_", ".kline_valley_",
                       ".watch_pool", ".push_state", ".oi_fr_cache", ".status"):
            if fname.startswith(prefix):
                return False
        return True
    return False

def main():
    files = [f[:-5] for f in os.listdir(KLINES_DIR) if _is_kline_file(f)]
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
                print(f"进度 {i}/{len(files)} | 新增 {results['ok']} | 无更新 {results['skip']} | 失败 {results['fail']} | {elapsed:.1f}s")

    elapsed = time.time() - start
    print(f"\n[{datetime.now()}] 完成，耗时 {elapsed:.1f}s")
    print(f"新增事件: {results['ok']} | 无更新: {results['skip']} | 失败: {results['fail']}")
    if results["failed"]:
        print(f"失败: {results['failed'][:5]}")

if __name__ == "__main__":
    main()
