#!/usr/bin/env python3
"""
币安 USDT 永续合约 K线拉取脚本
- 并发20拉取所有 TRADING 状态交易对
- 每交易对独立文件 /root/crypto_klines/{SYMBOL}.json
- 支持增量更新（只补新K线）
"""
import httpx
import json
import time
import os
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

PROXY = "http://ayniaiwoqi:PfvqzYsKC2@192.147.179.236:51523"
STABLECOINS = {"usdt","usdc","busd","dai","usdd","usdp","tusd","frax","husd","ousd","mim","ust","ustc","lusd"}
LIMIT = 1000
TARGET_K = 987  # 固定987根K线
KLINES_DIR = "/root/crypto_klines"
MAX_WORKERS = 2

os.makedirs(KLINES_DIR, exist_ok=True)

def get_trading_symbols():
    url = "https://fapi.binance.com/fapi/v1/exchangeInfo"
    with httpx.Client(proxy=PROXY, timeout=20.0) as client:
        resp = client.get(url)
        data = resp.json()
    symbols = []
    for s in data.get("symbols", []):
        if (s["contractType"] == "PERPETUAL"
            and s["quoteAsset"] == "USDT"
            and s["status"] == "TRADING"
            and s["baseAsset"].lower() not in STABLECOINS):
            symbols.append(s["symbol"])
    return symbols

def fetch_klines(symbol):
    url = "https://fapi.binance.com/fapi/v1/klines"
    params = {"symbol": symbol, "interval": "1h", "limit": LIMIT}
    try:
        with httpx.Client(proxy=PROXY, timeout=15.0) as client:
            resp = client.get(url, params=params)
            if resp.status_code == 200:
                data = resp.json()
                klines = [{
                    "t": k[0],
                    "o": float(k[1]),
                    "h": float(k[2]),
                    "l": float(k[3]),
                    "c": float(k[4]),
                    "v": float(k[5]),
                } for k in data[-TARGET_K:]]  # 只保留最新987根，不足则返回全部
                return symbol, klines, None
            else:
                return symbol, None, f"HTTP {resp.status_code}"
    except Exception as e:
        return symbol, None, str(e)

def update_symbol(symbol):
    """增量更新：对比本地最新K线时间，只补新K线"""
    filepath = f"{KLINES_DIR}/{symbol}.json"
    
    if os.path.exists(filepath):
        with open(filepath) as f:
            local = json.load(f)
        local_klines = local.get("klines", [])
        local_latest_t = local_klines[-1]["t"] if local_klines else 0
        
        # 拉最新K线
        url = "https://fapi.binance.com/fapi/v1/klines"
        params = {"symbol": symbol, "interval": "1h", "limit": LIMIT}
        with httpx.Client(proxy=PROXY, timeout=15.0) as client:
            resp = client.get(url, params=params)
            if resp.status_code != 200:
                return symbol, 0, f"HTTP {resp.status_code}"
            remote_data = resp.json()
        
        # 只取本地没有的新K线（remote_data 格式是原始列表，取最后987根）
        new_klines = [k for k in remote_data if k[0] > local_latest_t][-TARGET_K:]
        if new_klines:
            merged = (local_klines + [{
                "t": k[0],
                "o": float(k[1]),
                "h": float(k[2]),
                "l": float(k[3]),
                "c": float(k[4]),
                "v": float(k[5]),
            } for k in new_klines])[-TARGET_K:]  # 合并后截断到987根
            # 新币上线不足987根时提示
            if len(merged) < TARGET_K:
                print(f"  [WARN] {symbol} 只有 {len(merged)} 根K线（上线不足 {TARGET_K} 根）")
            with open(filepath, "w") as f:
                json.dump({
                    "symbol": symbol,
                    "interval": "1h",
                    "klines": merged,
                    "count": len(merged),
                    "fetched_at": datetime.now().isoformat()
                }, f)
            return symbol, len(new_klines), None
        else:
            return symbol, 0, None  # 无新K线
    else:
        # 首次全量拉取
        symbol, klines, err = fetch_klines(symbol)
        if klines:
            # 新币上线不足987根时提示
            if len(klines) < TARGET_K:
                print(f"  [WARN] {symbol} 只有 {len(klines)} 根K线（上线不足 {TARGET_K} 根）")
            with open(filepath, "w") as f:
                json.dump({
                    "symbol": symbol,
                    "interval": "1h",
                    "klines": klines,
                    "count": len(klines),
                    "fetched_at": datetime.now().isoformat()
                }, f)
            return symbol, len(klines), None
        else:
            return symbol, 0, err

def main():
    symbols = get_trading_symbols()
    print(f"[{datetime.now()}] 开始拉取 {len(symbols)} 个交易对")
    
    results = {"success": 0, "fail": 0, "no_update": 0, "failed": []}
    start = time.time()
    
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(update_symbol, sym): sym for sym in symbols}
        
        for i, future in enumerate(as_completed(futures), 1):
            symbol, updated, err = future.result()
            if err:
                results["fail"] += 1
                results["failed"].append(f"{symbol}: {err}")
            elif updated > 0:
                results["success"] += 1
            else:
                results["no_update"] += 1
            
            if i % 100 == 0 or i == len(symbols):
                elapsed = time.time() - start
                print(f"进度 {i}/{len(symbols)} | 新增 {results['success']} | 无更新 {results['no_update']} | 失败 {results['fail']} | {elapsed:.1f}s")
    
    elapsed = time.time() - start
    print(f"\n[{datetime.now()}] 完成，耗时 {elapsed:.1f}s")
    print(f"新增K线: {results['success']} | 无更新: {results['no_update']} | 失败: {results['fail']}")
    if results["failed"]:
        print(f"失败: {results['failed'][:5]}")

if __name__ == "__main__":
    main()
