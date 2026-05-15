#!/usr/bin/env python3
"""
观测池可视化 Web 页面
Flask 单文件，读取 .watch_pool.json 和 .status.json
启动: python3 watch_pool_web.py [port]
访问: http://<本机IP>:端口
"""
import json, os, time
from datetime import datetime, timezone, timedelta


BJ_TZ = timezone(timedelta(hours=8))

def now_bj():
    """返回北京时间（UTC+8）"""
    return datetime.now(BJ_TZ)
from flask import Flask, jsonify, render_template_string

app = Flask(__name__)

KLINES_DIR = "/root/crypto_klines"
WATCH_FILE = f"{KLINES_DIR}/.watch_pool.json"
STATUS_FILE = f"{KLINES_DIR}/.status.json"
OUTCOMES_FILE = f"{KLINES_DIR}/.outcomes.json"
REFRESH_INTERVAL = 30  # JS 轮询间隔（秒）

# ─── 数据读取 ────────────────────────────────────────────
def read_json(path, default=None):
    try:
        with open(path) as f:
            return json.load(f)
    except Exception:
        return default or {}

def load_pool():
    return read_json(WATCH_FILE, {})

def load_status():
    return read_json(STATUS_FILE, {})

def load_outcomes():
    return read_json(OUTCOMES_FILE, {})

def calc_pool_stats(pool):
    longs = [s for s in pool.values() if s.get("direction") == "long"]
    shorts = [s for s in pool.values() if s.get("direction") == "short"]
    return len(pool), len(longs), len(shorts)

# ─── HTML 模板 ────────────────────────────────────────────
HTML = """
<!DOCTYPE html>
<html lang="zh">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>观测池</title>
<style>
* { box-sizing: border-box; margin: 0; padding: 0; }

body {
  background: #0d1117;
  color: #e6edf3;
  font-family: 'SF Mono', 'Cascadia Code', 'Fira Code', monospace;
  font-size: 13px;
  min-height: 100vh;
}

header {
  background: #161b22;
  border-bottom: 1px solid #30363d;
  padding: 12px 20px;
  display: flex;
  align-items: center;
  gap: 24px;
  flex-wrap: wrap;
  position: sticky;
  top: 0;
  z-index: 100;
}

header h1 { font-size: 15px; color: #58a6ff; }

.stat {
  display: inline-flex;
  align-items: center;
  gap: 6px;
  padding: 4px 10px;
  border-radius: 6px;
  font-size: 12px;
}
.stat-total { background: #1f2937; }
.stat-long  { background: #0d3320; color: #3fb950; }
.stat-short { background: #2d1010; color: #f85149; }

.meta { font-size: 11px; color: #8b949e; margin-left: auto; }

#pools { padding: 16px 20px; }

.pool-grid {
  display: grid;
  grid-template-columns: repeat(auto-fill, minmax(340px, 1fr));
  gap: 12px;
}

.card {
  background: #161b22;
  border: 1px solid #30363d;
  border-radius: 8px;
  overflow: hidden;
  transition: border-color 0.2s;
}
.card:hover { border-color: #58a6ff; }

.card-header {
  padding: 10px 12px;
  display: flex;
  align-items: center;
  gap: 8px;
  border-bottom: 1px solid #21262d;
}

.direction {
  font-size: 11px;
  font-weight: bold;
  padding: 2px 8px;
  border-radius: 4px;
  text-transform: uppercase;
}
.direction.long  { background: #0d3320; color: #3fb950; }
.direction.short { background: #2d1010; color: #f85149; }

.symbol { font-weight: bold; font-size: 14px; }

.pattern {
  margin-left: auto;
  font-size: 11px;
  color: #8b949e;
  background: #21262d;
  padding: 2px 6px;
  border-radius: 4px;
}

.card-body { padding: 10px 12px; font-size: 12px; }

.row {
  display: flex;
  justify-content: space-between;
  padding: 3px 0;
  border-bottom: 1px solid #21262d;
}
.row:last-child { border-bottom: none; }
.label { color: #8b949e; }
.value { font-weight: bold; }

.value.positive { color: #3fb950; }
.value.negative { color: #f85149; }
.value.neutral   { color: #e6edf3; }

.signal-flags {
  margin-top: 6px;
  display: flex;
  gap: 4px;
  flex-wrap: wrap;
}
.flag {
  font-size: 10px;
  padding: 1px 5px;
  border-radius: 3px;
}
.flag-roc-up    { background: #1a3a1a; color: #56d364; }
.flag-roc-down  { background: #2d1010; color: #f85149; }
.flag-trend-up   { background: #1a3a1a; color: #56d364; }
.flag-trend-down { background: #2d1010; color: #f85149; }
.flag-neutral    { background: #21262d; color: #8b949e; }

.cvd-compare {
  font-size: 11px;
  color: #8b949e;
  margin-top: 4px;
}

.escalation { font-size: 11px; margin-top: 4px; }
.escalation .共振 { color: #f0883e; }

/* outcome 颜色 */
.outcome-tp { color: #3fb950; }
.outcome-sl { color: #f85149; }
.outcome-open { color: #e6edf3; }

.empty { text-align: center; color: #8b949e; padding: 40px; }

/* section titles */
.section-title {
  font-size: 12px;
  color: #8b949e;
  padding: 8px 0 4px;
  border-bottom: 1px solid #21262d;
  margin-bottom: 8px;
  text-transform: uppercase;
  letter-spacing: 0.5px;
}
</style>
</head>
<body>

<header>
  <h1>📊 观测池</h1>
  <span class="stat stat-total" id="stat-total">—</span>
  <span class="stat stat-long"  id="stat-long">—</span>
  <span class="stat stat-short" id="stat-short">—</span>
  <span class="stat" style="background:#21262d; color:#8b949e;" id="stat-new">—</span>
  <div class="meta" id="meta">加载中…</div>
</header>

<div id="pools">
  <div class="empty">加载中…</div>
</div>

<script>
const REFRESH = {{ refresh_interval }};

function timeAgo(isoStr) {
  if (!isoStr) return '—';
  // 统一按北京时间(+08:00)计算，不受浏览器本地时区影响
  const BJ_OFFSET = 8 * 60; // 北京UTC+8，分钟
  const ms = new Date(isoStr).getTime();
  const nowMs = Date.now();
  const diff = (nowMs - ms) / 1000;
  if (diff < 60)   return Math.floor(diff) + 's前';
  if (diff < 3600) return Math.floor(diff/60) + 'm前';
  return Math.floor(diff/3600) + 'h前';
}

function fmtPrice(p, default_val) {
  if (!p && p !== 0) return default_val || '—';
  p = parseFloat(p);
  if (p >= 1)    return p.toFixed(4);
  if (p >= 0.01) return p.toFixed(5);
  if (p >= 0.0001) return p.toFixed(6);
  return p.toFixed(8);
}

function fmtPct(v) {
  if (!v && v !== 0) return '—';
  v = parseFloat(v);
  const sign = v >= 0 ? '+' : '';
  return sign + v.toFixed(2) + '%';
}

function rocFlag(roc) {
  if (roc === undefined || roc === null) return '';
  const v = parseFloat(roc);
  if (v > 5)  return '<span class="flag flag-roc-up">ROC↑' + v.toFixed(1) + '%</span>';
  if (v < -5) return '<span class="flag flag-roc-down">ROC↓' + v.toFixed(1) + '%</span>';
  return '<span class="flag flag-neutral">ROC' + v.toFixed(1) + '%</span>';
}

function trendFlag(trend) {
  if (!trend) return '';
  if (trend === 'up')   return '<span class="flag flag-trend-up">↑趋势</span>';
  if (trend === 'down') return '<span class="flag flag-trend-down">↓趋势</span>';
  return '<span class="flag flag-neutral">↔震荡</span>';
}

function directionLabel(dir) {
  if (dir === 'long')  return '<span class="direction long">做多</span>';
  if (dir === 'short') return '<span class="direction short">做空</span>';
  return '<span class="direction" style="background:#21262d;color:#8b949e;">—</span>';
}

async function fetchData() {
  try {
    const [poolRes, statusRes] = await Promise.all([
      fetch('/api/pool'),
      fetch('/api/status')
    ]);
    const pool = await poolRes.json();
    const status = await statusRes.json();
    render(pool, status);
  } catch(e) {
    console.error(e);
  }
  setTimeout(fetchData, REFRESH * 1000);
}

function render(pool, status) {
  const symbols = Object.keys(pool);
  const longs  = symbols.filter(s => pool[s].direction === 'long').length;
  const shorts = symbols.filter(s => pool[s].direction === 'short').length;

  document.getElementById('stat-total').textContent = '总量 ' + symbols.length;
  document.getElementById('stat-long').textContent  = '做多 ' + longs;
  document.getElementById('stat-short').textContent = '做空 ' + shorts;
  document.getElementById('stat-new').textContent = '本轮 +' + (status.new_signals_this_run || 0);

  // 北京时间格式化（显式加8小时，不受浏览器本地时区干扰）
  function fmtBJTime(isoStr) {
    if (!isoStr) return '—';
    const d = new Date(isoStr);
    const bj = new Date(d.getTime() + 8 * 3600 * 1000);
    const pad = n => String(n).padStart(2, '0');
    return `${pad(bj.getUTCHours())}:${pad(bj.getUTCMinutes())}:${pad(bj.getUTCSeconds())}`;
  }

  const updated = status.generated_at ? fmtBJTime(status.generated_at) : '—';
  document.getElementById('meta').textContent =
    `K线 ${status.kline_files || 0} 个 | 池 ${symbols.length} 个 | 更新 ${updated}`;

  const container = document.getElementById('pools');

  if (symbols.length === 0) {
    container.innerHTML = '<div class="empty">观测池为空，等待新信号…</div>';
    return;
  }

  // 排序：做空在前（看空优先），再按 strength 降序
  const sorted = symbols.sort((a, b) => {
    const sa = pool[a], sb = pool[b];
    if (sb.strength !== sa.strength) return (sb.strength || 0) - (sa.strength || 0);
    if (sb.direction !== sa.direction) return sb.direction === 'short' ? 1 : -1;
    return 0;
  });

  let html = '<div class="pool-grid">';

  for (const sym of sorted) {
    const s = pool[sym];
    const dir = s.direction || '—';
    const dirClass = dir === 'long' ? 'positive' : dir === 'short' ? 'negative' : 'neutral';
    const entry = fmtPrice(s.entry_price);
    const sl    = fmtPrice(s.stop_loss);
    const tp1   = fmtPrice(s.target1);

    // 止损盈亏%
    let slPct = '—';
    if (s._entry_exact && s._sl_exact) {
      const diff = (s._sl_exact - s._entry_exact) / s._entry_exact * 100;
      slPct = (diff >= 0 ? '+' : '') + diff.toFixed(1) + '%';
    }

    const rocFlagHtml = rocFlag(s._cvd_roc);
    const trendFlagHtml = trendFlag(s._cvd_trend);
    const age = timeAgo(s.created_at);

    // CVD 对比行
    let cvdLine = '—';
    if (s._curr_cvd_peak_val !== undefined && s._prev_cvd_peak_val !== undefined) {
      const cvdCurr = parseFloat(s._curr_cvd_peak_val);
      const cvdPrev = parseFloat(s._prev_cvd_peak_val);
      const abs = Math.abs;
      if (cvdCurr > 0) {
        cvdLine = `CVD ${abs(cvdCurr/1e6).toFixed(1)}M ${cvdCurr>cvdPrev?'>':'<'}前高 ${abs(cvdPrev/1e6).toFixed(1)}M`;
      } else {
        cvdLine = `CVD ${abs(cvdCurr/1e6).toFixed(1)}M ${cvdCurr<cvdPrev?'<':'>'}前低 ${abs(cvdPrev/1e6).toFixed(1)}M`;
      }
    }

    html += `
    <div class="card">
      <div class="card-header">
        ${directionLabel(dir)}
        <span class="symbol">${sym}</span>
        <span class="pattern">${s.pattern || '—'}</span>
      </div>
      <div class="card-body">
        <div class="row">
          <span class="label">入场</span>
          <span class="value ${dirClass}">${entry}</span>
        </div>
        <div class="row">
          <span class="label">止损</span>
          <span class="value negative">${sl} (${slPct})</span>
        </div>
        <div class="row">
          <span class="label">目标1</span>
          <span class="value positive">${tp1}</span>
        </div>
        <div class="row">
          <span class="label">RSI</span>
          <span class="value neutral">${s.rsi ?? '—'}</span>
        </div>
        <div class="row">
          <span class="label">CVD强度</span>
          <span class="value neutral">${s.cvd_strength ?? '—'}</span>
        </div>
        <div class="row">
          <span class="label">资金费率</span>
          <span class="value neutral">${s.funding_rate != null ? (parseFloat(s.funding_rate)*100).toFixed(4)+'%' : '—'}</span>
        </div>
        <div class="row">
          <span class="label">强度</span>
          <span class="value neutral">${s.strength ?? '—'}</span>
        </div>
        <div class="row">
          <span class="label">存活</span>
          <span class="value neutral">${age}</span>
        </div>
        <div class="row escalation">
          <span class="label">共振</span>
          <span class="value">${s['共振'] || ''} ${s.oi_direction || ''}</span>
        </div>
        <div class="signal-flags">
          ${rocFlagHtml}
          ${trendFlagHtml}
          <span class="flag flag-neutral">${s.divergence_type || ''}</span>
        </div>
        <div class="cvd-compare">${cvdLine}</div>
      </div>
    </div>`;
  }

  html += '</div>';
  container.innerHTML = html;
}

fetchData();
</script>
</body>
</html>
"""

@app.route("/")
def index():
    return render_template_string(HTML, refresh_interval=REFRESH_INTERVAL)

@app.route("/api/pool")
def api_pool():
    pool = load_pool()
    # 附加 outcomes
    outcomes = load_outcomes()
    for sym, outcome in outcomes.items():
        if sym in pool:
            pool[sym]["_outcome"] = outcome
    return jsonify(pool)

@app.route("/api/status")
def api_status():
    return jsonify(load_status())

@app.route("/api/outcomes")
def api_outcomes():
    return jsonify(load_outcomes())

if __name__ == "__main__":
    import sys
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 5050
    print(f"启动 http://0.0.0.0:{port}")
    app.run(host="0.0.0.0", port=port, debug=False)
