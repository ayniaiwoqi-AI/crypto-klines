#!/usr/bin/env python3
"""
OI + Funding Rate 采集脚本
- 来源: Binance fapi 原生 API（premiumIndex + openInterest）
- 写入: InfluxDB (oi-volume bucket + funding-rate bucket)
- 每小时轮询一次，静默运行
- 防封策略: 随机打乱顺序 + 随机间隔 + 429指数退避
"""
import os
import httpx
import json
import time
import random
import requests
from datetime import datetime, timezone, timedelta


BJ_TZ = timezone(timedelta(hours=8))

def now_bj():
    """返回北京时间（UTC+8）"""
    return datetime.now(BJ_TZ)
from concurrent.futures import ThreadPoolExecutor, as_completed

# 代理从环境变量读取，不硬编码
_proxy_user = os.getenv("PROXY_USER", "ayniaiwoqi")
_proxy_pass = os.getenv("PROXY_PASS", "")
_proxy_host = os.getenv("PROXY_HOST", "192.147.179.236")
_proxy_port = os.getenv("PROXY_PORT", "51523")
PROXY = f"http://{_proxy_user}:{_proxy_pass}@{_proxy_host}:{_proxy_port}"

# InfluxDB
INFLUX_URL = "http://localhost:8086"
INFLUX_TOKEN = "influx-token-crypto-2026"
OI_DB = "oi-volume"
FR_DB = "funding-rate"
ORG = "crypto-lab"

# API防封配置
MIN_DELAY_MS = 50    # 最小请求间隔
MAX_DELAY_MS = 150    # 最大请求间隔
MAX_RETRIES = 3       # 429最大重试次数
INITIAL_BACKOFF_MS = 200  # 初次退避

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

# ========== 随机延迟 ==========
def random_delay():
    """请求间随机延迟，防止burst触发限速"""
    delay_s = (random.randint(MIN_DELAY_MS, MAX_DELAY_MS) + random.random()) / 1000
    time.sleep(delay_s)

# ========== 获取交易对列表（只调用一次） ==========
def get_trading_symbols() -> list[str]:
    url = "https://fapi.binance.com/fapi/v1/exchangeInfo"
    proxy = PROXY if PROXY else None
    try:
        with httpx.Client(proxy=proxy, timeout=15.0) as client:
            resp = client.get(url)
            data = resp.json()
        symbols = [
            s["symbol"] for s in data.get("symbols", [])
            if (s["contractType"] == "PERPETUAL"
                and s["quoteAsset"] == "USDT"
                and s["status"] == "TRADING")
        ]
        # 随机打乱顺序，避免同一worker每次都以相同顺序请求
        random.shuffle(symbols)
        return symbols
    except Exception as e:
        print(f"  [ERROR] 获取交易对列表失败: {e}")
        return []

# ========== 带退避的请求 ==========
def _fetch_with_backoff(url: str, params: dict, symbol: str, attempt: int = 0) -> tuple:
    """
    发送请求，429时指数退避重试
    返回: (symbol, result_value_or_None)
    """
    proxy = PROXY if PROXY else None
    backoff_ms = INITIAL_BACKOFF_MS * (2 ** attempt)

    try:
        with httpx.Client(proxy=proxy, timeout=8.0) as client:
            resp = client.get(url, params=params)

            if resp.status_code == 200:
                return symbol, resp.json()
            elif resp.status_code == 429:
                if attempt < MAX_RETRIES:
                    jitter = random.randint(0, backoff_ms // 2)
                    sleep_s = (backoff_ms + jitter) / 1000
                    print(f"  [RATE] {symbol} 429, 退避 {sleep_s*1000:.0f}ms (尝试 {attempt+1}/{MAX_RETRIES})")
                    time.sleep(sleep_s)
                    return _fetch_with_backoff(url, params, symbol, attempt + 1)
                else:
                    print(f"  [FAIL] {symbol} 429 超限，放弃")
                    return symbol, None
            else:
                return symbol, None
    except Exception:
        return symbol, None

# ========== 获取 OI（并发30） ==========
def fetch_oi(symbol: str):
    result = _fetch_with_backoff(
        "https://fapi.binance.com/fapi/v1/openInterest",
        {"symbol": symbol},
        symbol
    )
    if result[1] is not None:
        return result[0], float(result[1].get("openInterest", 0))
    return symbol, None

# ========== 获取 Funding Rate（并发15，避免过度burst） ==========
def fetch_fr(symbol: str):
    result = _fetch_with_backoff(
        "https://fapi.binance.com/fapi/v1/premiumIndex",
        {"symbol": symbol},
        symbol
    )
    if result[1] is not None:
        return result[0], float(result[1].get("lastFundingRate") or 0)
    return symbol, None

CACHE_FILE = "/root/crypto_klines/.oi_fr_cache.json"
CACHE_TTL_HOURS = 2  # 缓存超过2小时视为过期

def save_cache(oi_results: dict, fr_results: dict):
    prev_cache = {}
    if os.path.exists(CACHE_FILE):
        try:
            with open(CACHE_FILE) as f:
                prev_cache = json.load(f)
        except Exception:
            pass

    now = now_bj()
    cache = {}
    for sym in set(list(oi_results.keys()) + list(fr_results.keys())):
        old_oi = prev_cache.get(sym, {}).get("oi")
        new_oi = oi_results.get(sym)
        cache[sym] = {
            "oi": new_oi,
            "fr": fr_results.get(sym),
            "prev_oi": old_oi,
            "updated": now.isoformat(),
            "fresh": True,
        }
    with open(CACHE_FILE, "w") as f:
        json.dump(cache, f)
    print(f"  本地缓存已更新: {CACHE_FILE}")

def load_cache_with_ttl():
    """加载缓存，超过TTL的标记为过期"""
    if not os.path.exists(CACHE_FILE):
        return {}
    try:
        with open(CACHE_FILE) as f:
            raw = json.load(f)
    except Exception:
        return {}

    now = now_bj()
    fresh_cache = {}
    stale_count = 0
    for sym, data in raw.items():
        updated_str = data.get("updated", "")
        try:
            updated = datetime.fromisoformat(updated_str)
            age_h = (now - updated).total_seconds() / 3600
            if age_h > CACHE_TTL_HOURS:
                data["fresh"] = False
                stale_count += 1
            else:
                data["fresh"] = True
        except Exception:
            data["fresh"] = False
            stale_count += 1
        fresh_cache[sym] = data

    if stale_count > 0:
        print(f"  [WARN] OI/FR缓存中有 {stale_count} 个币数据超过{CACHE_TTL_HOURS}h，视为过期")
    return fresh_cache

# ========== 并发执行（带随机间隔） ==========
def fetch_all(symbols: list, fetch_fn, max_workers: int, label: str):
    """并发执行 + 每请求随机延迟"""
    results = {}
    start = time.time()
    success = 0
    rate_limited = 0

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(fetch_fn, sym): sym for sym in symbols}
        for future in as_completed(futures):
            sym, val = future.result()
            if val is not None:
                results[sym] = val
                success += 1
            else:
                rate_limited += 1
            # 每请求间隔随机，防止burst触发限速
            random_delay()

    elapsed = time.time() - start
    print(f"  {label} 成功: {success}/{len(symbols)}, 限速跳过: {rate_limited}, 耗时 {elapsed:.1f}s")
    return results

# ========== 主流程 ==========
def main():
    # 用时间种子打乱，每次运行顺序不同
    random.seed(int(time.time() // 60))  # 每分钟种子相同，小时内顺序固定但小时间不同
    print(f"[{now_bj()}] 开始采集 OI + Funding Rate")

    symbols = get_trading_symbols()
    if not symbols:
        print("  [ERROR] 无交易对")
        return

    print(f"  交易对数量: {len(symbols)}")

    now_ns = int(time.time() * 1e9)

    # OI: 并发30
    oi_results = fetch_all(symbols, fetch_oi, max_workers=30, label="OI")

    # FR: 并发15（比之前5提升3倍，仍低于总限额1200/min）
    fr_results = fetch_all(symbols, fetch_fr, max_workers=15, label="FR")

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

    save_cache(oi_results, fr_results)

    print(f"[{now_bj()}] 完成 | OI: {'OK' if oi_ok else 'FAIL'} ({len(oi_lines)}条) | FR: {'OK' if fr_ok else 'FAIL'} ({len(fr_lines)}条)")

    for sym in ["BTCUSDT", "ETHUSDT", "SOLUSDT"]:
        oi = oi_results.get(sym)
        fr = fr_results.get(sym)
        fr_pct = f"{fr*100:.4f}%" if fr is not None else "N/A"
        print(f"  {sym}: OI={oi}, FR={fr_pct}")

if __name__ == "__main__":
    main()
