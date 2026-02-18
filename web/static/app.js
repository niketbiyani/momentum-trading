'use strict';

// ── State ─────────────────────────────────────────────────────────────────────
let ws = null;
let state = {
  stocks: {}, signals: [], active_tf: '1m', status: 'Connecting…',
  nifty: { spot: 0, options: {} },
};
let activeTf  = '1m';
let activeTab = 'nifty';   // 'nifty' | 'stocks'
let lbTf      = '5s';      // timeframe used for lookback heatmap
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
      state   = JSON.parse(evt.data);
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
  const dot     = document.getElementById('status-dot');
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
  document.getElementById('clock').textContent =
    new Date().toLocaleTimeString('en-IN', { hour12: false }) + ' IST';
}
setInterval(updateClock, 1000);
updateClock();

// ── Tab switching ─────────────────────────────────────────────────────────────
function switchTab(tab) {
  activeTab = tab;

  // Update tab buttons
  document.querySelectorAll('.tab-btn').forEach(btn => {
    btn.classList.toggle('active', btn.dataset.tab === tab);
  });

  // Show/hide tab content
  document.querySelectorAll('.tab-content').forEach(el => {
    el.classList.toggle('active', el.id === 'tab-' + tab);
  });

  // Show stocks controls only on stocks tab
  document.getElementById('stocks-controls').classList.toggle('hidden', tab !== 'stocks');

  render();
}

document.querySelectorAll('.tab-btn').forEach(btn => {
  btn.addEventListener('click', () => switchTab(btn.dataset.tab));
});

// Initialise: hide stocks controls on startup (Nifty is the default tab)
document.getElementById('stocks-controls').classList.add('hidden');

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
const fmt    = (v, dec = 2) => v != null ? Number(v).toFixed(dec) : '—';
const fmtPct = (v) => v != null ? (v >= 0 ? '+' : '') + Number(v).toFixed(2) + '%' : '—';
const fmtRsi = (v) => v != null ? Number(v).toFixed(1) : '—';

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
  const s   = opt.signal_status;
  const dir = opt.signal_direction === 'UP' ? '▲' : opt.signal_direction === 'DOWN' ? '▼' : '';
  return `<span class="sig-badge sig-${s}">${dir} ${s}</span>`;
}

function optRowClass(ceOpt, peOpt) {
  for (const opt of [ceOpt, peOpt]) {
    if (!opt) continue;
    if (opt.signal_status === 'ENTRY') return 'has-signal-ENTRY';
    if (opt.signal_status === 'WATCH') return 'has-signal-WATCH';
  }
  return '';
}

function optCell(opt, tf) {
  if (!opt) return '<td>—</td><td>—</td><td>—</td><td>—</td><td>—</td><td>—</td>';
  const ind  = (opt.indicators || {})[tf] || {};
  const ltp  = opt.ltp > 0 ? fmt(opt.ltp) : '—';
  const dPct = opt.ltp_change_pct;
  const rsi  = ind.rsi  != null ? ind.rsi  : opt.rsi;
  const mh   = ind.macd_hist != null ? ind.macd_hist : opt.macd_hist;
  const spk  = ind.spk10 != null ? ind.spk10 : null;
  const spkStr = spk != null
    ? (spk >= 0 ? '+' : '') + spk.toFixed(1) + '%'
    : '—';
  return `
    <td>${ltp}</td>
    <td class="${deltaClass(dPct)}">${fmtPct(dPct)}</td>
    <td class="${rsiClass(rsi)}">${fmtRsi(rsi)}</td>
    <td class="${deltaClass(mh)}">${mh != null ? (mh >= 0 ? '+' : '') + fmt(mh, 3) : '—'}</td>
    <td class="${spkClass(spk)}" style="${spk != null ? heatBg(spk) ? 'background:' + heatBg(spk) + ';' : '' : ''}">${spkStr}</td>
    <td>${sigBadge(opt)}</td>`;
}

// ── Nifty multi-TF cell (uses per-TF indicator + signal from indicators dict) ──
function niftyTfCells(opt) {
  const TFS = ['5s', '15s', '1m'];
  let cells = '';
  for (const tf of TFS) {
    const ind = (opt && opt.indicators) ? (opt.indicators[tf] || {}) : {};
    const rsi = ind.rsi  != null ? ind.rsi  : null;
    const mh  = ind.macd_hist != null ? ind.macd_hist : null;
    const sigObj = ind.signal_status
      ? { signal_status: ind.signal_status, signal_direction: ind.signal_direction }
      : null;
    cells += `
      <td class="${rsiClass(rsi)}">${fmtRsi(rsi)}</td>
      <td class="${deltaClass(mh)}">${mh != null ? (mh >= 0 ? '+' : '') + fmt(mh, 3) : '—'}</td>
      <td>${sigBadge(sigObj)}</td>`;
  }
  return cells;
}

// ── Render dispatcher ─────────────────────────────────────────────────────────
function render() {
  renderStatus();
  if (activeTab === 'nifty') {
    renderNiftyTab();
    renderLookbackHeatmap();
  } else {
    renderTable();
    renderSignals();
    updateTfButtons();
  }
}

function renderStatus() {
  document.getElementById('status-text').textContent = state.status || '';
}

// ── Nifty tab ─────────────────────────────────────────────────────────────────
function renderNiftyTab() {
  const nifty = state.nifty || {};
  const spot  = nifty.spot || 0;
  const opts  = nifty.options || {};

  // Update spot price
  document.getElementById('nifty-spot-val').textContent = spot > 0
    ? '₹' + Number(spot).toLocaleString('en-IN', { minimumFractionDigits: 2, maximumFractionDigits: 2 })
    : '—';

  // Render the 4 option rows: ATM CE, ITM CE, ATM PE, ITM PE
  const ORDER = [
    { key: 'ATM_CE', label: 'ATM CE', cls: 'opt-ce row-atm' },
    { key: 'ITM_CE', label: 'ITM CE', cls: 'opt-ce row-itm' },
    { key: 'ATM_PE', label: 'ATM PE', cls: 'opt-pe row-atm' },
    { key: 'ITM_PE', label: 'ITM PE', cls: 'opt-pe row-itm' },
  ];

  const tbody = document.getElementById('nifty-body');

  if (Object.keys(opts).length === 0) {
    tbody.innerHTML = `<tr><td colspan="13" class="empty-msg">Waiting for Nifty data…</td></tr>`;
    renderNiftySignals();
    return;
  }

  const rows = ORDER.map(({ key, label, cls }) => {
    const opt  = opts[key];
    const ltp  = opt && opt.ltp > 0 ? fmt(opt.ltp) : '—';
    const dPct = opt ? opt.ltp_change_pct : null;
    const strike = opt ? (opt.strike || '—') : '—';

    // Row highlight if any TF has ENTRY or WATCH signal
    let rowHi = '';
    if (opt && opt.indicators) {
      for (const tf of ['5s', '15s', '1m']) {
        const ind = opt.indicators[tf] || {};
        if (ind.signal_status === 'ENTRY') { rowHi = 'has-signal-ENTRY'; break; }
        if (ind.signal_status === 'WATCH') { rowHi = 'has-signal-WATCH'; }
      }
    }

    return `<tr class="${rowHi}">
      <td class="${cls}">${label}</td>
      <td>${strike}</td>
      <td>${ltp}</td>
      <td class="${deltaClass(dPct)}">${fmtPct(dPct)}</td>
      ${niftyTfCells(opt)}
    </tr>`;
  });

  tbody.innerHTML = rows.join('');
  renderNiftySignals();
}

function renderNiftySignals() {
  const list    = document.getElementById('nifty-signals-list');
  const signals = (state.signals || []).filter(s => s.underlying === 'NIFTY').slice(0, 20);

  if (!signals.length) {
    list.innerHTML = `<div class="empty-msg">No Nifty signals yet…</div>`;
    return;
  }

  list.innerHTML = signals.map(sig => {
    const dir      = sig.direction === 'UP' ? '▲' : '▼';
    const dirClass = sig.direction === 'UP' ? 'up' : 'down';
    return `<div class="signal-item">
      <div>
        <span class="sig-ts">${sig.timestamp}</span>
        <span class="sig-stock"> ${sig.label || sig.underlying}</span>
        <span class="sig-dir ${dirClass}"> ${dir}</span>
        <span class="sig-badge sig-${sig.status}" style="margin-left:6px">${sig.status}</span>
      </div>
      <div class="sig-meta">${sig.timeframe} · ${sig.message || ''}</div>
    </div>`;
  }).join('');
}

// ── Stocks tab ────────────────────────────────────────────────────────────────
function renderTable() {
  const tbody  = document.getElementById('table-body');
  const stocks = state.stocks || {};
  const keys   = Object.keys(stocks)
    .filter(sym => !searchQuery || sym.includes(searchQuery))
    .sort();

  if (!keys.length) {
    tbody.innerHTML = `<tr><td colspan="13" class="empty-msg">No data yet — connecting to feed…</td></tr>`;
    return;
  }

  const rows = keys.map(sym => {
    const s  = stocks[sym];
    const ce = s.options && s.options.CE;
    const pe = s.options && s.options.PE;
    const rowClass = optRowClass(ce, pe);
    const spot     = s.spot > 0
      ? `₹${Number(s.spot).toLocaleString('en-IN', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`
      : '—';

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
  const list    = document.getElementById('signals-list');
  const signals = (state.signals || []).slice(0, 50);

  if (!signals.length) {
    list.innerHTML = `<div class="empty-msg">No signals yet — monitoring for spikes…</div>`;
    return;
  }

  list.innerHTML = signals.map(sig => {
    const dir      = sig.direction === 'UP' ? '▲' : '▼';
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

// ── Lookback heatmap ──────────────────────────────────────────────────────────
const LB_PERIODS = [10, 20, 30, 40, 50, 60, 70, 80, 90, 100];

// Compute background color for a delta % value
// Large positive = bright green; large negative = bright red; near 0 = transparent
function heatBg(v) {
  if (v == null || Math.abs(v) < 0.3) return '';
  const intensity = Math.min(Math.abs(v) / 6, 1);   // saturates at ±6%
  const alpha = 0.15 + intensity * 0.70;
  return v > 0
    ? `rgba(63,185,80,${alpha.toFixed(2)})`
    : `rgba(248,81,73,${alpha.toFixed(2)})`;
}

function spkClass(v) {
  if (v == null || Math.abs(v) < 0.5) return 'spk-muted';
  return v > 0 ? 'spk-pos' : 'spk-neg';
}

function renderLookbackHeatmap() {
  const tbody = document.getElementById('lookback-body');
  const nifty = state.nifty || {};
  const opts  = nifty.options || {};

  const ORDER = [
    { key: 'ATM_CE', label: 'ATM CE', cls: 'opt-ce' },
    { key: 'ITM_CE', label: 'ITM CE', cls: 'opt-ce' },
    { key: 'ATM_PE', label: 'ATM PE', cls: 'opt-pe' },
    { key: 'ITM_PE', label: 'ITM PE', cls: 'opt-pe' },
  ];

  const rows = ORDER.map(({ key, label, cls }) => {
    const opt = opts[key];
    const ind = (opt && opt.indicators && opt.indicators[lbTf]) || {};
    const ld  = ind.lb_delta || {};

    const cells = LB_PERIODS.map(p => {
      const v = ld[String(p)];
      if (v == null) return `<td class="hc-empty">—</td>`;
      const bg      = heatBg(v);
      const valCls  = v > 0 ? 'hc-pos' : v < 0 ? 'hc-neg' : 'hc-neu';
      const bgStyle = bg ? `background:${bg};` : '';
      const sign    = v > 0 ? '+' : '';
      const tip     = `${p}b delta: ${sign}${v.toFixed(2)}%`;
      return `<td class="${valCls}" style="${bgStyle}" title="${tip}">${sign}${v.toFixed(1)}%</td>`;
    }).join('');

    return `<tr><td class="${cls}">${label}</td>${cells}</tr>`;
  });

  tbody.innerHTML = rows.length ? rows.join('') :
    `<tr><td colspan="11" class="empty-msg">Waiting for Nifty data…</td></tr>`;
}

// ── Lookback TF buttons ───────────────────────────────────────────────────────
function updateLbTfButtons() {
  document.querySelectorAll('.lb-tf-btn').forEach(btn => {
    btn.classList.toggle('active', btn.dataset.lbtf === lbTf);
  });
}

document.querySelectorAll('.lb-tf-btn').forEach(btn => {
  btn.addEventListener('click', () => {
    lbTf = btn.dataset.lbtf;
    updateLbTfButtons();
    renderLookbackHeatmap();
  });
});

// ── TF button wiring ──────────────────────────────────────────────────────────
document.querySelectorAll('.tf-btn').forEach(btn => {
  btn.addEventListener('click', () => sendTf(btn.dataset.tf));
});

// ── Boot ──────────────────────────────────────────────────────────────────────
connect();
