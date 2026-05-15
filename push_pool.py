#!/usr/bin/env python3
"""
观察池推送脚本（事件驱动）
- check_divergence.py 检测到新信号 → 立即调用本脚本推送
- 每15分钟 cron 触发时：只推池有变化的，全量推或静默跳过
- 推送格式：每条消息最多10个信号
"""
import json
import os
import time
import hashlib
import requests
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

KLINES_DIR = "/root/crypto_klines"
POOL_FILE = f"{KLINES_DIR}/.watch_pool.json"
STATE_FILE = f"{KLINES_DIR}/.push_state.json"  # 记录上次池哈希
TG_BOT_TOKEN = "8603937566:AAECrJdrN3cRQeGZKU8_tgV_sMkaDKpx_iw"
TG_CHAT_ID = "390688348"
BATCH_SIZE = 10

def send_telegram(msg: str):
    try:
        url = f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendMessage"
        resp = requests.post(url, json={
            "chat_id": TG_CHAT_ID,
            "text": msg,
            "parse_mode": "Markdown",
            "disable_web_page_preview": True
        }, timeout=15)
        data = resp.json()
        ok = data.get("ok", False)
        if not ok:
            print(f"[TG FAIL] {resp.status_code}: {data}")
        return ok
    except Exception as e:
        print(f"[TG ERROR] {e}")
        return False

def load_pool():
    if os.path.exists(POOL_FILE):
        with open(POOL_FILE) as f:
            content = f.read().strip()
            if content:
                return json.loads(content)
    return {}

def clean_pool(pool):
    now = datetime.now()
    cleaned = {}
    for sym, sig in pool.items():
        try:
            created = datetime.fromisoformat(sig.get("created_at", "2020-01-01"))
            age_hours = (now - created).total_seconds() / 3600
            if age_hours < 24:
                cleaned[sym] = sig
        except:
            pass
    return cleaned

def pool_hash(pool):
    """池内容哈希，用于检测变化"""
    # 用 symbol + direction + entry_price + created_at 算哈希
    items = []
    for sym, sig in sorted(pool.items()):
        items.append(f"{sym}:{sig.get('direction')}:{sig.get('entry_price')}:{sig.get('created_at')}")
    return hashlib.md5("|".join(items).encode()).hexdigest()

def load_state():
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE) as f:
            return json.load(f)
    return {"hash": "", "last_push": ""}

def save_state(state):
    with open(STATE_FILE, "w") as f:
        json.dump(state, f)

def fmt_signal(sig: dict) -> str:
    emoji = "🟢" if sig["direction"] == "long" else "🔴"
    sym = sig["symbol"]

    div = ("顶背离" if sig.get("divergence_type") == "cvd_top"
           else "底背离" if sig.get("divergence_type") == "cvd_bottom"
           else "背离")
    pattern = sig.get("pattern", "").replace("shooting_star", "流星").replace("bearish_engulfing", "吞没").replace("bullish_engulfing", "吞噬").replace("hammer", "锤子")

    # OI共振标识：有OI方向时才标
    oi_dir = sig.get("oi_direction", "")
    共振标识 = "📉" if oi_dir == "↓OI" else "📈" if oi_dir == "↑OI" else ""

    # 自适应小数位
    def fmt_price(p):
        if p is None: return "N/A"
        if p >= 1: return f"{p:.4f}"
        if p >= 0.01: return f"{p:.4f}"
        if p >= 0.0001: return f"{p:.5f}"
        return f"{p:.8f}"

    entry = sig.get("_entry_exact", sig["entry_price"])
    sl = sig.get("_sl_exact", sig["stop_loss"])
    tp1 = sig.get("_tp1_exact", sig["target1"])

    if sig["direction"] == "long":
        sl_pct = (entry - sl) / entry * 100  # 正数，但表示亏损
        tp_pct = (tp1 - entry) / entry * 100
    else:
        sl_pct = (sl - entry) / entry * 100
        tp_pct = (entry - tp1) / entry * 100

    # OI + FR + RSI
    fr = sig.get("funding_rate")
    fr_str = f"{fr*100:.4f}%" if fr is not None else "N/A"
    oi_arrow = oi_dir[0] if oi_dir else "→"
    oi_line = f"OI{oi_arrow} {fr_str} RSI={sig['rsi']}"

    # 波峰波谷
    curr_cvd = sig.get("_curr_cvd_peak_val")
    prev_cvd = sig.get("_prev_cvd_peak_val")
    curr_price = sig.get("_curr_price_peak_val")
    prev_price = sig.get("_prev_price_peak_val")
    if sig["divergence_type"] == "cvd_bottom":
        # 底背离：价格未新低
        if curr_cvd and prev_cvd:
            cvd_str = f"CVD {curr_cvd/1e6:.1f}M <前低 {prev_cvd/1e6:.1f}M"
        else:
            cvd_str = "CVD创新低"
        price_str = f"价格{fmt_price(curr_price)}>=前低{fmt_price(prev_price)}" if (curr_price and prev_price) else "价格未新低"
    else:
        # 顶背离：价格未新高
        if curr_cvd and prev_cvd:
            cvd_str = f"CVD {curr_cvd/1e6:.1f}M >前高 {prev_cvd/1e6:.1f}M"
        else:
            cvd_str = "CVD创新高"
        price_str = f"价格{fmt_price(curr_price)}<=前高{fmt_price(prev_price)}" if (curr_price and prev_price) else "价格未新高"

    line1 = f"{emoji}{sym} | {div} | {pattern}{共振标识} | {oi_line}"
    # 做多止损是亏损，显示为负
    sl_display_pct = sl_pct if sig["direction"] == "short" else -sl_pct
    line2 = f"  入{fmt_price(entry)} | 损{fmt_price(sl)}({sl_display_pct:+.1f}%) | 目{fmt_price(tp1)}({tp_pct:+.1f}%)"
    line3 = f"  {cvd_str} | {price_str}"
    return "\n".join([line1, line2, line3])

def batch_send(signals: list, label: str = ""):
    if not signals:
        return True
    total = len(signals)
    all_ok = True
    with ThreadPoolExecutor(max_workers=5) as executor:
        for i in range(0, total, BATCH_SIZE):
            batch = signals[i:i+BATCH_SIZE]
            lines = []
            for j, s in enumerate(batch):
                if j > 0:
                    lines.append("━━━━━━━━")
                lines.append(fmt_signal(s))
            header = (f"📊 *观察池{label}* | {datetime.now().strftime('%m-%d %H:%M')} | "
                     f"{total}信号" + (f" ({i+1}-{i+len(batch)})" if total > BATCH_SIZE else ""))
            msg = header + "\n" + "\n".join(lines)
            future = executor.submit(send_telegram, msg)
            ok = future.result()
            if not ok:
                all_ok = False
            print(f"  批次 {i//BATCH_SIZE+1}: {len(batch)}信号 {'✅' if ok else '❌'}")
            time.sleep(0.5)
    return all_ok

def push_pool(force: bool = False, label: str = ""):
    """
    推池。force=True 则无视变化直接推。
    返回 True=有推送, False=无变化跳过
    """
    pool = load_pool()
    pool = clean_pool(pool)

    if not pool:
        print(f"[{datetime.now()}] 观察池为空，跳过")
        return False

    state = load_state()
    current_hash = pool_hash(pool)

    if not force and current_hash == state.get("hash"):
        print(f"[{datetime.now()}] 池无变化，跳过推送 (hash={current_hash})")
        return False

    long_signals = [s for s in pool.values() if s.get("direction") == "long"]
    short_signals = [s for s in pool.values() if s.get("direction") == "short"]
    label = label or "全量"
    print(f"[{datetime.now()}] 推送 {len(pool)} 信号 | 🟢{len(long_signals)} 🔴{len(short_signals)}")

    all_signals = sorted(pool.values(), key=lambda x: x["symbol"])
    ok = batch_send(all_signals, label)

    if ok:
        save_state({"hash": current_hash, "last_push": datetime.now().isoformat()})

    return ok

if __name__ == "__main__":
    import sys
    force = "--force" in sys.argv
    label = ""
    for a in sys.argv[1:]:
        if not a.startswith("--"):
            label = a
    push_pool(force=force, label=label)
