#!/usr/bin/env python3
"""
币安 USDT 永续合约 K线拉取脚本
- 并发20拉取所有 TRADING 状态交易对
- 每交易对独立文件 /root/crypto_klines/{SYMBOL}.json
- 支持增量更新（只补新K线）
- 防封策略: 随机打乱顺序 + 随机间隔 + 429指数退避
"""
import httpx
import json
import time
import random
import os
from datetime import datetime, timezone, timedelta


BJ_TZ = timezone(timedelta(hours=8))

def now_bj():
    """返回北京时间（UTC+8）"""
    return datetime.now(BJ_TZ)
from concurrent.futures import ThreadPoolExecutor, as_completed

PROXY = "http://ayniaiwoqi:***@192.147.179.236:51523"
STABLECOINS = {"usdt","usdc","busd","dai","usdd","usdp","tusd","frax","husd","ousd","mim","ust","ustc","lusd"}
LIMIT = 1000
TARGET_K = 987  # 固定987根K线
KLINES_DIR = "/root/crypto_klines"
MAX_WORKERS = 20

# API防封配置
MIN_DELAY_MS = 30     # 最小请求间隔（K线请求较轻，可更快）
MAX_DELAY_MS = 100    # 最大请求间隔
MAX_RETRIES = 3       # 429最大重试次数
INITIAL_BACKOFF_MS = 200  # 初次退避

os.makedirs(KLINES_DIR, exist_ok=True)

def random_delay():
    """请求间随机延迟"""
    delay_s = (random.randint(MIN_DELAY_MS, MAX_DELAY_MS) + random.random()) / 1000
    time.sleep(delay_s)

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
    # 随机打乱，避免每次以相同顺序请求
    random.shuffle(symbols)
    return symbols

def _fetch_with_backoff(url: str, params: dict, symbol: str, attempt: int = 0) -> tuple:
    """带429退避的请求"""
    proxy = PROXY if PROXY else None
    backoff_ms = INITIAL_BACKOFF_MS * (2 ** attempt)

    try:
        with httpx.Client(proxy=proxy, timeout=15.0) as client:
            resp = client.get(url, params=params)
            if resp.status_code == 200:
                return symbol, resp.json(), None
            elif resp.status_code == 429:
                if attempt < MAX_RETRIES:
                    jitter = random.randint(0, backoff_ms // 2)
                    sleep_s = (backoff_ms + jitter) / 1000
                    print(f"  [RATE] {symbol} 429, 退避 {sleep_s*1000:.0f}ms (尝试 {attempt+1}/{MAX_RETRIES})")
                    time.sleep(sleep_s)
                    return _fetch_with_backoff(url, params, symbol, attempt + 1)
                else:
                    print(f"  [FAIL] {symbol} 429 超限")
                    return symbol, None, f"HTTP 429"
            else:
                return symbol, None, f"HTTP {resp.status_code}"
    except Exception as e:
        return symbol, None, str(e)

def fetch_klines(symbol):
    url = "https://fapi.binance.com/fapi/v1/klines"
    params = {"symbol": symbol, "interval": "1h", "limit": LIMIT}
    sym, data, err = _fetch_with_backoff(url, params, symbol)
    if data is not None:
        klines = [{
            "t": k[0],
            "o": float(k[1]),
            "h": float(k[2]),
            "l": float(k[3]),
            "c": float(k[4]),
            "v": float(k[5]),
        } for k in data[-TARGET_K:]]
        return symbol, klines, None
    return symbol, None, err

def update_symbol(symbol):
    """增量更新：对比本地最新K线时间，只补新K线"""
    filepath = f"{KLINES_DIR}/{symbol}.json"

    if os.path.exists(filepath):
        with open(filepath) as f:
            local = json.load(f)
        local_klines = local.get("klines", [])
        local_latest_t = local_klines[-1]["t"] if local_klines else 0

        # 拉最新K线
        sym, remote_data, err = fetch_klines(symbol)
        if err:
            return symbol, 0, err

        # 只取本地没有的新K线
        new_klines = [k for k in remote_data if k["t"] > local_latest_t][-TARGET_K:]
        if new_klines:
            merged = (local_klines + [{
                "t": k["t"],
                "o": k["o"],
                "h": k["h"],
                "l": k["l"],
                "c": k["c"],
                "v": k["v"],
            } for k in new_klines])[-TARGET_K:]
            if len(merged) < TARGET_K:
                print(f"  [WARN] {symbol} 只有 {len(merged)} 根K线")
            with open(filepath, "w") as f:
                json.dump({
                    "symbol": symbol,
                    "interval": "1h",
                    "klines": merged,
                    "count": len(merged),
                    "fetched_at": now_bj().isoformat()
                }, f)
            return symbol, len(new_klines), None
        else:
            return symbol, 0, None  # 无新K线
    else:
        # 首次全量拉取
        symbol, klines, err = fetch_klines(symbol)
        if klines:
            if len(klines) < TARGET_K:
                print(f"  [WARN] {symbol} 只有 {len(klines)} 根K线")
            with open(filepath, "w") as f:
                json.dump({
                    "symbol": symbol,
                    "interval": "1h",
                    "klines": klines,
                    "count": len(klines),
                    "fetched_at": now_bj().isoformat()
                }, f)
            return symbol, len(klines), None
        else:
            return symbol, 0, err

def main():
    # 每小时以分钟为种子，同一小时内顺序固定
    random.seed(int(time.time() // 60))
    symbols = get_trading_symbols()
    print(f"[{now_bj()}] 开始拉取 {len(symbols)} 个交易对")

    results = {"success": 0, "fail": 0, "no_update": 0, "failed": []}
    rate_limited = 0
    start = time.time()

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(update_symbol, sym): sym for sym in symbols}

        for i, future in enumerate(as_completed(futures), 1):
            symbol, updated, err = future.result()
            if err:
                if "429" in str(err):
                    rate_limited += 1
                results["fail"] += 1
                results["failed"].append(f"{symbol}: {err}")
            elif updated > 0:
                results["success"] += 1
            else:
                results["no_update"] += 1

            # 每请求间隔随机，防止burst触发限速
            random_delay()

            if i % 100 == 0 or i == len(symbols):
                elapsed = time.time() - start
                print(f"进度 {i}/{len(symbols)} | 新增 {results['success']} | 无更新 {results['no_update']} | 失败 {results['fail']} | {elapsed:.1f}s")

    elapsed = time.time() - start
    print(f"\n[{now_bj()}] 完成，耗时 {elapsed:.1f}s")
    print(f"新增K线: {results['success']} | 无更新: {results['no_update']} | 失败: {results['fail']} | 限流429: {rate_limited}")
    if results["failed"]:
        print(f"失败: {results['failed'][:5]}")

if __name__ == "__main__":
    main()
