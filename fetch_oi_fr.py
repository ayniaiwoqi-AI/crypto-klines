#!/usr/bin/env python3
"""
OI + Funding Rate 采集脚本
- 来源: Binance fapi 原生 API（premiumIndex + openInterest）
- 写入: InfluxDB (oi-volume bucket + funding-rate bucket)
- 每小时轮询一次，静默运行
"""
import os
import httpx
import json
import time
import requests
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

PROXY = "http://ayniaiwoqi:PfvqzYsKC2@192.147.179.236:51523"

# InfluxDB
INFLUX_URL = "http://localhost:8086"
INFLUX_TOKEN = "influx-token-crypto-2026"
OI_DB = "oi-volume"
FR_DB = "funding-rate"
ORG = "crypto-lab"

# ========== InfluxDB 写入 ==========
def write_influx(lines: list[str], db: str) -> bool:
    if not lines:
        return True
    headers = {
        "Authorization": f"Token {INFLUX_TOKEN}",
        "Content-Type": "text/plain"
    }
    resp = requests.post(
        f"{INFLUX_URL}/write?db={db}&org={ORG}&rp=autogen&precision=ns",
        headers=headers,
        data="\n".join(lines),
        timeout=30
    )
    if resp.status_code != 204:
        print(f"  [WARN] {db} 写入失败: {resp.status_code} {resp.text[:100]}")
        return False
    return True

# ========== 获取交易对列表 ==========
def get_trading_symbols() -> list[str]:
    url = "https://fapi.binance.com/fapi/v1/exchangeInfo"
    proxy = PROXY if PROXY else None
    try:
        with httpx.Client(proxy=proxy, timeout=15.0) as client:
            resp = client.get(url)
            data = resp.json()
        return [
            s["symbol"] for s in data.get("symbols", [])
            if (s["contractType"] == "PERPETUAL"
                and s["quoteAsset"] == "USDT"
                and s["status"] == "TRADING")
        ]
    except Exception as e:
        print(f"  [ERROR] 获取交易对列表失败: {e}")
        return []

# ========== 获取 OI ==========
def fetch_oi(symbol: str):
    try:
        proxy = PROXY if PROXY else None
        with httpx.Client(proxy=proxy, timeout=8.0) as client:
            resp = client.get(
                "https://fapi.binance.com/fapi/v1/openInterest",
                params={"symbol": symbol}
            )
            if resp.status_code == 200:
                d = resp.json()
                return symbol, float(d["openInterest"])
    except Exception:
        pass
    return symbol, None

# ========== 获取 Funding Rate ==========
def fetch_fr(symbol: str):
    try:
        proxy = PROXY if PROXY else None
        with httpx.Client(proxy=proxy, timeout=8.0) as client:
            resp = client.get(
                "https://fapi.binance.com/fapi/v1/premiumIndex",
                params={"symbol": symbol}
            )
            if resp.status_code == 200:
                d = resp.json()
                return symbol, float(d.get("lastFundingRate") or 0)
    except Exception:
        pass
    return symbol, None

CACHE_FILE = "/root/crypto_klines/.oi_fr_cache.json"

def save_cache(oi_results: dict, fr_results: dict):
    # 加载旧缓存，取每个币的旧 current OI 作为 prev_oi
    prev_cache = {}
    if os.path.exists(CACHE_FILE):
        try:
            with open(CACHE_FILE) as f:
                prev_cache = json.load(f)
        except Exception:
            pass
    
    cache = {}
    for sym in set(list(oi_results.keys()) + list(fr_results.keys())):
        old_oi = prev_cache.get(sym, {}).get("oi")
        new_oi = oi_results.get(sym)
        cache[sym] = {
            "oi": new_oi,
            "fr": fr_results.get(sym),
            "prev_oi": old_oi,           # 上个小时的 OI（用于判断方向）
            "updated": datetime.now().isoformat()
        }
    with open(CACHE_FILE, "w") as f:
        json.dump(cache, f)
    print(f"  本地缓存已更新: {CACHE_FILE}")

# ========== 主流程 ==========
def main():
    print(f"[{datetime.now()}] 开始采集 OI + Funding Rate")
    
    symbols = get_trading_symbols()
    print(f"  交易对数量: {len(symbols)}")
    
    if not symbols:
        print("  [ERROR] 无交易对")
        return
    
    now_ns = int(time.time() * 1e9)
    
    # 并发获取 OI（30并发）
    print("  获取 OI...")
    start = time.time()
    oi_results = {}
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = [executor.submit(fetch_oi, sym) for sym in symbols]
        for future in as_completed(futures):
            sym, oi = future.result()
            if oi is not None:
                oi_results[sym] = oi
    print(f"  OI 成功: {len(oi_results)}/{len(symbols)}, 耗时 {time.time()-start:.1f}s")
    
    # 并发获取 Funding Rate（5并发，避免触发Binance限速）
    print("  获取 Funding Rate...")
    start = time.time()
    fr_results = {}
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = [executor.submit(fetch_fr, sym) for sym in symbols]
        for future in as_completed(futures):
            sym, fr = future.result()
            if fr is not None:
                fr_results[sym] = fr
    print(f"  FR 成功: {len(fr_results)}/{len(symbols)}, 耗时 {time.time()-start:.1f}s")
    
    # 写入 InfluxDB
    print("  写入 InfluxDB...")
    oi_lines = [
        f"open_interest,symbol={sym} oi={oi} {now_ns}"
        for sym, oi in oi_results.items()
    ]
    fr_lines = [
        f"funding_rate,symbol={sym} rate={fr} {now_ns}"
        for sym, fr in fr_results.items()
    ]
    
    oi_ok = write_influx(oi_lines, OI_DB)
    fr_ok = write_influx(fr_lines, FR_DB)
    
    # 更新本地缓存
    save_cache(oi_results, fr_results)
    
    print(f"[{datetime.now()}] 完成 | OI: {'OK' if oi_ok else 'FAIL'} ({len(oi_lines)}条) | FR: {'OK' if fr_ok else 'FAIL'} ({len(fr_lines)}条)")
    
    # 打印几个关键币
    for sym in ["BTCUSDT", "ETHUSDT", "SOLUSDT"]:
        oi = oi_results.get(sym)
        fr = fr_results.get(sym)
        fr_pct = f"{fr*100:.4f}%" if fr is not None else "N/A"
        print(f"  {sym}: OI={oi}, FR={fr_pct}")

if __name__ == "__main__":
    main()
