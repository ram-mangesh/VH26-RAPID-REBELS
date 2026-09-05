const HISTORY_LEN = 60;
const COLORS = {
  critical: '#ef4444',
  high: '#f59e0b',
  low: '#3b82f6',
  grid: '#1e293b',
  text: '#64748b',
};

let ws = null;
let pipelineRunning = true;
let queueHistory = { critical: [], high: [], low: [] };
let throughputHistory = { critical: [], high: [], low: [] };
let shedLog = [];
let lastShedCount = 0;

function connect() {
  const proto = location.protocol === 'https:' ? 'wss:' : 'ws:';
  ws = new WebSocket(`${proto}//${location.host}/ws`);

  ws.onopen = () => {
    const badge = document.getElementById('statusBadge');
    badge.className = 'status-badge connected';
    document.getElementById('statusText').textContent = 'Connected';
    addLog('info', 'WebSocket connected — streaming live metrics');
  };

  ws.onclose = () => {
    const badge = document.getElementById('statusBadge');
    badge.className = 'status-badge disconnected';
    document.getElementById('statusText').textContent = 'Disconnected — reconnecting...';
    setTimeout(connect, 2000);
  };

  ws.onerror = () => {
    ws.close();
  };

  ws.onmessage = (evt) => {
    try {
      const state = JSON.parse(evt.data);
      updateDashboard(state);
    } catch (e) {
      console.error('Parse error:', e);
    }
  };
}

function updateDashboard(state) {
  const m = state.metrics || {};
  const q = state.queues || {};
  const s = state.simulator || {};
  const w = state.workers || {};
  const r = state.routing || {};

  document.getElementById('currentRate').textContent = (s.rate || 0).toFixed(1);
  document.getElementById('kpiThroughput').textContent = (m.throughput_eps || 0).toFixed(1);
  document.getElementById('kpiProcessed').textContent = formatNum(m.total_processed || 0);
  document.getElementById('kpiShed').textContent = formatNum(m.total_shed || 0);
  document.getElementById('kpiWorkersBusy').textContent = w.busy || 0;
  document.getElementById('kpiWorkersTotal').textContent = w.total || 0;

  const modeBadge = document.getElementById('modeBadge');
  if (r.mode === 'spike') {
    modeBadge.textContent = 'SPIKE';
    modeBadge.className = 'mode-badge spike';
  } else {
    modeBadge.textContent = 'NORMAL';
    modeBadge.className = 'mode-badge';
  }

  document.getElementById('routeMode').textContent = r.mode?.toUpperCase() || 'NORMAL';
  document.getElementById('routeAdmitted').textContent = formatNum(r.total_admitted || 0);
  document.getElementById('routeShed').textContent = formatNum(r.total_shed || 0);

  const tiers = m.tiers || {};
  updateLatency('C', tiers.CRITICAL || {});
  updateLatency('H', tiers.HIGH || {});
  updateLatency('L', tiers.LOW || {});

  document.getElementById('qCritical').textContent = q.critical || 0;
  document.getElementById('qHigh').textContent = q.high || 0;
  document.getElementById('qLow').textContent = q.low || 0;

  document.getElementById('tCritical').textContent = (tiers.CRITICAL?.throughput_eps || 0).toFixed(1);
  document.getElementById('tHigh').textContent = (tiers.HIGH?.throughput_eps || 0).toFixed(1);
  document.getElementById('tLow').textContent = (tiers.LOW?.throughput_eps || 0).toFixed(1);

  updateStrategy(r.mode, s.rate);

  pushHistory(queueHistory, {
    critical: q.critical || 0,
    high: q.high || 0,
    low: q.low || 0,
  });
  pushHistory(throughputHistory, {
    critical: tiers.CRITICAL?.throughput_eps || 0,
    high: tiers.HIGH?.throughput_eps || 0,
    low: tiers.LOW?.throughput_eps || 0,
  });

  drawChart('queueChart', queueHistory, true);
  drawChart('throughputChart', throughputHistory, false);

  updateOverloadIndicator(state, r.mode);
  updateBreakdown(m.event_type_breakdown || {});
  updateReliability(state);
  updateDecisionTrace(state.traces || []);
  updateFaultPanel(state.fault || {}, state.persistence || {});

  detectShedEvents(m);
}

function updateOverloadIndicator(state, mode) {
  const ind = document.getElementById('overloadIndicator2');
  const text = document.getElementById('overloadText2');
  const q = state.queues || {};
  const totalLoad = (q.total || 0) / 75000;
  const isSpike = mode === 'spike';

  if (isSpike && totalLoad > 0.4) {
    ind.className = 'overload-indicator critical';
    text.textContent = 'LOAD: CRITICAL (' + (totalLoad * 100).toFixed(0) + '%)';
  } else if (isSpike) {
    ind.className = 'overload-indicator warn';
    text.textContent = 'LOAD: SPIKED — shedding x batching';
  } else {
    ind.className = 'overload-indicator';
    text.textContent = 'LOAD: NORMAL (' + (totalLoad * 100).toFixed(0) + '%)';
  }
}

function updateBreakdown(breakdown) {
  const canvas = document.getElementById('breakdownChart');
  if (!canvas) return;
  const ctx = canvas.getContext('2d');
  const dpr = window.devicePixelRatio || 1;
  const rect = canvas.getBoundingClientRect();
  canvas.width = rect.width * dpr;
  canvas.height = rect.height * dpr;
  ctx.scale(dpr, dpr);
  const W = rect.width;
  const H = rect.height;
  ctx.clearRect(0, 0, W, H);

  const types = ['payment', 'order', 'inventory_update', 'user_click', 'app_log'];
  const total = types.reduce((sum, t) => sum + (breakdown[t]?.processed || 0), 0) || 1;
  let x = 0;
  const barH = 60;
  const gap = 4;

  types.forEach((t, i) => {
    const data = breakdown[t] || { processed: 0, shed: 0, pct_processed: 0 };
    const colors = ['#ef4444', '#dc2626', '#f59e0b', '#3b82f6', '#60a5fa'];
    const w = (W - gap * (types.length - 1)) / types.length;
    const h = Math.max(3, (data.processed / total) * barH);

    ctx.fillStyle = colors[i];
    ctx.fillRect(x, H - h, w, h);

    if (data.shed > 0) {
      ctx.fillStyle = '#7f1d1d';
      const shedH = Math.max(2, (data.shed / (data.processed + data.shed || 1)) * (barH - h));
      ctx.fillRect(x, H - h - shedH, w, shedH);
    }

    x += w + gap;
  });

  const grid = document.getElementById('breakdownGrid');
  grid.innerHTML = '';
  types.forEach((t) => {
    const data = breakdown[t] || { processed: 0, shed: 0, latency_avg_ms: 0 };
    const el = document.createElement('div');
    el.className = 'bd-item';
    const label = t === 'inventory_update' ? 'inventory' : t === 'user_click' ? 'click' : t === 'app_log' ? 'log' : t;
    el.innerHTML = '<div class="bd-type">' + label + '</div><div class="bd-processed">' + formatNum(data.processed) + '</div><div class="bd-shed">' + (data.shed ? 'shed ' + formatNum(data.shed) : 'never shed') + '</div><div class="bd-lat">' + (data.latency_avg_ms ? data.latency_avg_ms.toFixed(0) + 'ms' : '') + '</div>';
    grid.appendChild(el);
  });
}

function updateReliability(state) {
  const m = state.metrics || {};
  const cost = state.cost || {};

  document.getElementById('relDedup').textContent = formatNum(m.total_duplicates_detected || 0);
  document.getElementById('relRetried').textContent = formatNum(m.total_retried || 0);

  const adaptive = cost.adaptive?.total || 0;
  const naive = cost.naive_baseline?.total || 0;
  document.getElementById('relCostAdaptive').textContent = '$' + adaptive.toFixed(4);
  document.getElementById('relCostNaive').textContent = '$' + naive.toFixed(4);

  if (naive > 0) {
    const saved = ((naive - adaptive) / naive) * 100;
    document.getElementById('relCostSaved').textContent = saved.toFixed(1) + '%';
  }
}

function updateLatency(prefix, tierData) {
  document.getElementById('lat' + prefix + 'Avg').textContent = tierData.latency_avg_ms?.toFixed(1) || '0';
  document.getElementById('lat' + prefix + 'P50').textContent = tierData.latency_p50_ms?.toFixed(1) || '0';
  document.getElementById('lat' + prefix + 'P95').textContent = tierData.latency_p95_ms?.toFixed(1) || '0';
  document.getElementById('lat' + prefix + 'P99').textContent = tierData.latency_p99_ms?.toFixed(1) || '0';
}

function updateStrategy(mode, rate) {
  const isSpike = String(mode).toLowerCase() === 'spike';
  document.getElementById('stratCritical').textContent = 'Stream — process immediately, 1 at a time (NEVER shed)';
  document.getElementById('stratHigh').textContent = isSpike ? 'Batch (size=5) — grouped under spike load' : 'Stream — process immediately';
  document.getElementById('stratLow').textContent = isSpike ? 'Batch (size=50, timeout=500ms) — aggressive batching + shedding active' : 'Batch (size=25, timeout=200ms) — micro-batched';
}

function detectShedEvents(metrics) {
  const currentShed = metrics.total_shed || 0;
  if (currentShed > lastShedCount && lastShedCount > 0) {
    const diff = currentShed - lastShedCount;
    addLog('shed', 'SHED: ' + diff + ' non-critical event(s) dropped (total: ' + currentShed + ')');
  }
  lastShedCount = currentShed;
}

let lastTraceKey = '';
function updateDecisionTrace(traces) {
  const list = document.getElementById('traceList');
  if (!list) return;
  if (traces.length === 0) return;
  const key = traces.map(t => t.event_id).join('|');
  if (key === lastTraceKey) return;
  lastTraceKey = key;

  let html = '';
  for (const t of traces.slice(0, 24)) {
    const c = t.components || {};
    const se = t.shed_reason ? ' · ' + t.shed_reason : '';
    const cls = t.admitted ? 'trace-line-admit' : 'trace-line-shed';
    const tag = t.admitted ? 'ADMIT' : 'SHED';
    html += '<div class="' + cls + '">[' + tag + '] ' + t.event_type + ' <b>' + t.priority + '</b> · score=' + t.score + ' urgenc=' + t.urgency + ' ' + t.strategy + '(n=' + t.batch_size + ') · prio=' + c.priority + ' lat=' + c.latency + ' load=' + c.load + ' sat=' + c.saturation + ' size=' + c.size + se + '</div>';
  }
  list.innerHTML = html;
}

function updateFaultPanel(f, persistence) {
  document.getElementById('faultInjected').textContent = formatNum(f.faults_injected || 0);
  document.getElementById('faultRetried').textContent = formatNum(f.retries_performed || 0);
  document.getElementById('dupBlocked').textContent = formatNum(persistence.duplicates_blocked || 0);
  document.getElementById('persisted').textContent = formatNum(persistence.total_persisted || 0);
}

async function killWorker() {
  await fetch('/api/kill-worker', { method: 'POST' });
  addLog('shed', 'Worker crash injected — event retried idempotently (persisted once)');
}

async function enableFaults() {
  await fetch('/api/fault/enable', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ failure_rate: 0.5, max_retries: 2 }) });
  addLog('info', 'Faults ENABLED (50% transient failure, 2 retries)');
}

async function disableFaults() {
  await fetch('/api/fault/disable', { method: 'POST' });
  addLog('info', 'Faults disabled');
}

async function runAB() {
  const status = document.getElementById('abStatus');
  status.textContent = 'Running adaptive vs naive under 40K/min overload (~25s)...';
  status.className = 'ab-status';
  document.getElementById('abGrid').style.display = 'none';
  await fetch('/api/ab', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ rate: 40000 }) });
  setTimeout(() => pollABResult(), 6000);
}

async function pollABResult() {
  const status = document.getElementById('abStatus');
  try {
    const r = await (await fetch('/api/ab/result')).json();
    if (r.done && r.result) {
      const sum = r.result.summary;
      status.textContent = 'Complete. Same 40K/min spike, same 8 workers.';
      status.className = 'ab-status';
      document.getElementById('abGrid').style.display = 'grid';
      document.getElementById('abAdaptiveLat').textContent = sum.critical_p95_adaptive_ms + ' ms';
      document.getElementById('abNaiveLat').textContent = sum.critical_p95_naive_ms + ' ms';
      document.getElementById('abAdaptiveShed').textContent = formatNum(sum.shed_adaptive);
      document.getElementById('abNaiveShed').textContent = formatNum(sum.shed_naive);
      document.getElementById('abSpeedup').textContent = sum.speedup_x + 'x';
      addLog('info', 'A/B done: adaptive ' + sum.critical_p95_adaptive_ms + 'ms vs naive ' + sum.critical_p95_naive_ms + 'ms (' + sum.speedup_x + 'x faster)');
    } else {
      setTimeout(() => pollABResult(), 3000);
    }
  } catch (e) {
    setTimeout(() => pollABResult(), 3000);
  }
}

function pushHistory(hist, data) {
  for (const key of Object.keys(hist)) {
    hist[key].push(data[key]);
    if (hist[key].length > HISTORY_LEN) hist[key].shift();
  }
}

function drawChart(canvasId, history, isStacked) {
  const canvas = document.getElementById(canvasId);
  if (!canvas) return;
  const ctx = canvas.getContext('2d');
  const dpr = window.devicePixelRatio || 1;
  const rect = canvas.getBoundingClientRect();
  canvas.width = rect.width * dpr;
  canvas.height = rect.height * dpr;
  ctx.scale(dpr, dpr);
  const W = rect.width;
  const H = rect.height;

  ctx.clearRect(0, 0, W, H);

  const allVals = [...history.critical, ...history.high, ...history.low];
  const maxVal = Math.max(...allVals, 1) * 1.2;
  const stepX = W / Math.max(HISTORY_LEN - 1, 1);

  ctx.strokeStyle = COLORS.grid;
  ctx.lineWidth = 0.5;
  for (let i = 0; i <= 4; i++) {
    const y = (H / 4) * i;
    ctx.beginPath();
    ctx.moveTo(0, y);
    ctx.lineTo(W, y);
    ctx.stroke();
  }

  const tiers = [{ key: 'low', color: COLORS.low }, { key: 'high', color: COLORS.high }, { key: 'critical', color: COLORS.critical }];

  for (const tier of tiers) {
    const data = history[tier.key];
    if (data.length < 2) continue;

    ctx.beginPath();
    ctx.strokeStyle = tier.color;
    ctx.lineWidth = 1.5;
    ctx.lineJoin = 'round';

    for (let i = 0; i < data.length; i++) {
      const x = i * stepX;
      const y = H - (data[i] / maxVal) * H;
      if (i === 0) ctx.moveTo(x, y);
      else ctx.lineTo(x, y);
    }
    ctx.stroke();

    const grad = ctx.createLinearGradient(0, 0, 0, H);
    grad.addColorStop(0, tier.color + '30');
    grad.addColorStop(1, tier.color + '05');

    ctx.lineTo((data.length - 1) * stepX, H);
    ctx.lineTo(0, H);
    ctx.closePath();
    ctx.fillStyle = grad;
    ctx.fill();
  }
}

function addLog(type, msg) {
  const log = document.getElementById('shedLog');
  const time = new Date().toLocaleTimeString();
  const entry = document.createElement('div');
  entry.className = 'log-entry log-' + type;
  entry.textContent = '[' + time + '] ' + msg;
  log.insertBefore(entry, log.firstChild);
  while (log.children.length > 50) {
    log.removeChild(log.lastChild);
  }
}

function formatNum(n) {
  if (n >= 1000000) return (n / 1000000).toFixed(1) + 'M';
  if (n >= 1000) return (n / 1000).toFixed(1) + 'K';
  return String(n);
}

async function setRate(rate) {
  document.getElementById('btnBaseline').classList.toggle('active', rate === 1000);
  document.getElementById('btnSpike').classList.toggle('active', rate === 20000);
  addLog('info', 'Rate set to ' + rate.toLocaleString() + ' events/min');
  await fetch('/api/rate', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ rate }) });
}

async function togglePipeline() {
  const btn = document.getElementById('btnStop');
  if (pipelineRunning) {
    await fetch('/api/stop', { method: 'POST' });
    btn.textContent = 'Start';
    btn.classList.remove('btn-gray');
    btn.classList.add('btn-green');
    addLog('info', 'Pipeline stopped — graphs will decrease as queue clears');
    pipelineRunning = false;
  } else {
    await fetch('/api/start', { method: 'POST' });
    btn.textContent = 'Stop';
    btn.classList.remove('btn-green');
    btn.classList.add('btn-gray');
    addLog('info', 'Pipeline started — graphs DECREASING = healthy processing');
    pipelineRunning = true;
  }
}

async function resetPipeline() {
  await fetch('/api/reset', { method: 'POST' });
  lastShedCount = 0;
  queueHistory = { critical: [], high: [], low: [] };
  throughputHistory = { critical: [], high: [], low: [] };
  document.getElementById('shedLog').innerHTML = '';
  addLog('info', 'Pipeline reset — all metrics cleared');
}

connect();

function applyTheme(theme) {
  if (theme === 'light') {
    document.documentElement.classList.add('light');
  } else {
    document.documentElement.classList.remove('light');
  }
  COLORS.grid = getComputedStyle(document.documentElement).getPropertyValue('--chart-grid').trim();
}

function toggleTheme() {
  const isLight = document.documentElement.classList.contains('light');
  const next = isLight ? 'dark' : 'light';
  localStorage.setItem('theme', next);
  applyTheme(next);
}

applyTheme(localStorage.getItem('theme') || 'dark');
