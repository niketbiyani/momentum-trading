'use strict';

// ── State ─────────────────────────────────────────────────────────────────────
let ws = null;
let state = { stocks: {}, signals: [], active_tf: '1m', status: 'Connecting…' };
let activeTf = '1m';
let searchQuery = '';
let reconnectDelay = 1000;

// ── WebSocket ─────────────────────────────────────────────────────────────────
function connect() {
  const proto = location.protocol === 'https:' ? 'wss' : 'ws';
  ws = new WebSocket(`${proto}://${location.host}/ws`);

  ws.onopen = () => {
    reconnectDelay = 1000;
    setConnected(true);
  };

  ws.onmessage = (evt) => {
    try {
      state = JSON.parse(evt.data);
      activeTf = state.active_tf || activeTf;
      render();
    } catch (e) {
      console.error('Parse error', e);
    }
  };

  ws.onclose = () => {
    setConnected(false);
    setTimeout(connect, reconnectDelay);
    reconnectDelay = Math.min(reconnectDelay * 2, 16000);
  };

  ws.onerror = () => ws.close();
}

function setConnected(connected) {
  const dot  = document.getElementById('status-dot');
  const overlay = document.getElementById('connecting');
  dot.className = connected ? 'live' : 'error';
  overlay.classList.toggle('visible', !connected);
}

function sendTf(tf) {
  activeTf = tf;
  if (ws && ws.readyState === WebSocket.OPEN) {
    ws.send(JSON.stringify({ set_tf: tf }));
  }
  updateTfButtons();
  render();
}

// ── Clock ─────────────────────────────────────────────────────────────────────
function updateClock() {
  const now = new Date();
  document.getElementById('clock').textContent =
    now.toLocaleTimeString('en-IN', { hour12: false }) + ' IST';
}
setInterval(updateClock, 1000);
updateClock();

// ── TF buttons ────────────────────────────────────────────────────────────────
function updateTfButtons() {
  document.querySelectorAll('.tf-btn').forEach(btn => {
    btn.classList.toggle('active', btn.dataset.tf === activeTf);
  });
}

// ── Search ────────────────────────────────────────────────────────────────────
document.getElementById('search').addEventListener('input', e => {
  searchQuery = e.target.value.trim().toUpperCase();
  render();
});

// ── Helpers ───────────────────────────────────────────────────────────────────
const fmt = (v, dec=2) => v != null ? Number(v).toFixed(dec) : '---';
const fmtPct = (v) => v != null ? (v >= 0 ? '+' : '') + Number(v).toFixed(2) + '%' : '---';
const fmtRsi = (v) => v != null ? Number(v).toFixed(1) : '---';

function rsiClass(v) {
  if (v == null) return '';
  if (v >= 70) return 'rsi-ob';
  if (v <= 30) return 'rsi-os';
  return '';
}

function deltaClass(v) {
  if (v == null || v === 0) return 'neu';
  return v > 0 ? 'up' : 'down';
}

function sigBadge(opt) {
  if (!opt || !opt.signal_status) return '';
  const s = opt.signal_status;
  const dir = opt.signal_direction === 'UP' ? '▲' : opt.signal_direction === 'DOWN' ? '▼' : '';
  return `<span class="sig-badge sig-${s}">${dir} ${s}</span>`;
}

function optRowClass(ceOpt, peOpt) {
  // Highlight row if either leg has WATCH or ENTRY
  for (const opt of [ceOpt, peOpt]) {
    if (!opt) continue;
    if (opt.signal_status === 'ENTRY') return 'has-signal-ENTRY';
    if (opt.signal_status === 'WATCH') return 'has-signal-WATCH';
  }
  return '';
}

function optCell(opt, tf) {
  if (!opt) return '<td>—</td><td>—</td><td>—</td><td>—</td><td>—</td>';
  const ind = (opt.indicators || {})[tf] || opt;  // flat structure
  const ltp  = opt.ltp > 0 ? fmt(opt.ltp) : '—';
  const dPct = opt.ltp_change_pct;
  const rsi  = ind.rsi != null ? ind.rsi : opt.rsi;
  const mh   = ind.macd_hist != null ? ind.macd_hist : opt.macd_hist;
  const bars = opt.bars || 0;

  return `
    <td>${ltp}</td>
    <td class="${deltaClass(dPct)}">${fmtPct(dPct)}</td>
    <td class="${rsiClass(rsi)}">${fmtRsi(rsi)}</td>
    <td class="${deltaClass(mh)}">${mh != null ? (mh >= 0 ? '+' : '') + fmt(mh, 3) : '—'}</td>
    <td>${sigBadge(opt)}</td>`;
}

// ── Main render ───────────────────────────────────────────────────────────────
function render() {
  renderStatus();
  renderTable();
  renderSignals();
}

function renderStatus() {
  document.getElementById('status-text').textContent = state.status || '';
  updateTfButtons();
}

function renderTable() {
  const tbody = document.getElementById('table-body');
  const stocks = state.stocks || {};
  const keys = Object.keys(stocks)
    .filter(sym => !searchQuery || sym.includes(searchQuery))
    .sort();

  if (!keys.length) {
    tbody.innerHTML = `<tr><td colspan="13" class="empty-msg">No data yet — connecting to feed…</td></tr>`;
    return;
  }

  const rows = keys.map(sym => {
    const s = stocks[sym];
    const ce = s.options && s.options.CE;
    const pe = s.options && s.options.PE;
    const rowClass = optRowClass(ce, pe);
    const spot = s.spot > 0 ? `₹${Number(s.spot).toLocaleString('en-IN', {minimumFractionDigits:2, maximumFractionDigits:2})}` : '—';

    return `<tr class="${rowClass}">
      <td class="col-stock">${sym}</td>
      <td class="col-spot">${spot}</td>
      <td class="col-sep"></td>
      ${optCell(ce, activeTf)}
      <td class="col-sep"></td>
      ${optCell(pe, activeTf)}
    </tr>`;
  });

  tbody.innerHTML = rows.join('');
}

function renderSignals() {
  const list = document.getElementById('signals-list');
  const signals = (state.signals || []).slice(0, 50);

  if (!signals.length) {
    list.innerHTML = `<div class="empty-msg">No signals yet — monitoring for spikes…</div>`;
    return;
  }

  list.innerHTML = signals.map(sig => {
    const dir = sig.direction === 'UP' ? '▲' : '▼';
    const dirClass = sig.direction === 'UP' ? 'up' : 'down';
    return `<div class="signal-item">
      <div>
        <span class="sig-ts">${sig.timestamp}</span>
        <span class="sig-stock"> ${sig.underlying || sig.stock || ''}</span>
        <span class="sig-dir ${dirClass}"> ${dir}</span>
        <span class="sig-badge sig-${sig.status}" style="margin-left:6px">${sig.status}</span>
      </div>
      <div class="sig-meta">${sig.timeframe} · ${sig.message || ''}</div>
    </div>`;
  }).join('');
}

// ── Boot ──────────────────────────────────────────────────────────────────────
document.querySelectorAll('.tf-btn').forEach(btn => {
  btn.addEventListener('click', () => sendTf(btn.dataset.tf));
});

connect();
