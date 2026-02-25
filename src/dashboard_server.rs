use std::collections::VecDeque;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::State;
use axum::response::IntoResponse;
use axum::routing::get;
use axum::Router;
use tokio::sync::broadcast;
use tracing::{info, warn};

const SNAP_CAP: usize = 20_000;
const OPENED_CAP: usize = 200;
const CLOSED_CAP: usize = 200;
const PENDING_CAP: usize = 100;
const BROADCAST_CAP: usize = 512;

// ---------------------------------------------------------------------------
// Shared state
// ---------------------------------------------------------------------------

pub struct DashboardState {
    snaps: Mutex<VecDeque<String>>,
    opened: Mutex<VecDeque<String>>,
    closed: Mutex<VecDeque<String>>,
    pending: Mutex<VecDeque<String>>,
    tx: broadcast::Sender<String>,
}

pub type SharedState = Arc<DashboardState>;

fn ring_push(queue: &Mutex<VecDeque<String>>, item: String, cap: usize) {
    let mut guard = queue.lock().expect("dashboard ring buffer mutex poisoned");
    if guard.len() >= cap {
        guard.pop_front();
    }
    guard.push_back(item);
}

impl DashboardState {
    pub fn push_snap(&self, s: String) {
        ring_push(&self.snaps, s.clone(), SNAP_CAP);
        let _ = self.tx.send(s);
    }

    pub fn push_opened(&self, s: String) {
        ring_push(&self.opened, s.clone(), OPENED_CAP);
        let _ = self.tx.send(s);
    }

    pub fn push_closed(&self, s: String) {
        ring_push(&self.closed, s.clone(), CLOSED_CAP);
        let _ = self.tx.send(s);
    }

    pub fn push_pending(&self, s: String) {
        ring_push(&self.pending, s.clone(), PENDING_CAP);
        let _ = self.tx.send(s);
    }

    /// Remove a pending order by condition_id and broadcast a removal event.
    pub fn remove_pending(&self, condition_id: &str, reason: &str) {
        {
            let mut guard = self.pending.lock().expect("pending mutex poisoned");
            guard.retain(|entry| {
                // Parse the JSON to check condition_id; keep entries that don't match
                if let Ok(v) = serde_json::from_str::<serde_json::Value>(entry) {
                    v.get("condition_id").and_then(|c| c.as_str()) != Some(condition_id)
                } else {
                    true
                }
            });
        }
        let msg = serde_json::json!({
            "t": "pending_remove",
            "condition_id": condition_id,
            "reason": reason,
        });
        let _ = self.tx.send(msg.to_string());
    }
}

/// Create the shared dashboard state.
pub fn make_dashboard_state() -> SharedState {
    let (tx, _) = broadcast::channel(BROADCAST_CAP);
    Arc::new(DashboardState {
        snaps: Mutex::new(VecDeque::with_capacity(SNAP_CAP)),
        opened: Mutex::new(VecDeque::with_capacity(OPENED_CAP)),
        closed: Mutex::new(VecDeque::with_capacity(CLOSED_CAP)),
        pending: Mutex::new(VecDeque::with_capacity(PENDING_CAP)),
        tx,
    })
}

// ---------------------------------------------------------------------------
// Axum server
// ---------------------------------------------------------------------------

pub async fn run_dashboard_server(state: SharedState, port: u16) {
    let app = Router::new()
        .route("/", get(root_handler))
        .route("/ws", get(ws_handler))
        .with_state(state);

    let addr = SocketAddr::from(([127, 0, 0, 1], port));
    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .expect("failed to bind dashboard server");

    info!("dashboard available at http://localhost:{}", port);
    axum::serve(listener, app)
        .await
        .expect("dashboard server error");
}

// ---------------------------------------------------------------------------
// Route handlers
// ---------------------------------------------------------------------------

async fn root_handler() -> impl IntoResponse {
    axum::response::Html(DASHBOARD_HTML)
}

async fn ws_handler(
    ws: WebSocketUpgrade,
    State(state): State<SharedState>,
) -> impl IntoResponse {
    ws.on_upgrade(|socket| handle_socket(socket, state))
}

async fn handle_socket(mut socket: WebSocket, state: SharedState) {
    // Subscribe to live updates before reading history to avoid a race.
    let mut rx = state.tx.subscribe();

    // Build init message with all ring buffers.
    let init_msg = {
        let snaps = state
            .snaps
            .lock()
            .expect("snaps mutex poisoned")
            .iter()
            .cloned()
            .collect::<Vec<_>>();
        let opened = state
            .opened
            .lock()
            .expect("opened mutex poisoned")
            .iter()
            .cloned()
            .collect::<Vec<_>>();
        let closed = state
            .closed
            .lock()
            .expect("closed mutex poisoned")
            .iter()
            .cloned()
            .collect::<Vec<_>>();
        let pending = state
            .pending
            .lock()
            .expect("pending mutex poisoned")
            .iter()
            .cloned()
            .collect::<Vec<_>>();
        format!(
            r#"{{"t":"init","snaps":[{}],"opened":[{}],"closed":[{}],"pending":[{}]}}"#,
            snaps.join(","),
            opened.join(","),
            closed.join(","),
            pending.join(",")
        )
    };
    if socket.send(Message::Text(init_msg)).await.is_err() {
        return;
    }

    // Stream live updates.
    loop {
        match rx.recv().await {
            Ok(msg) => {
                if socket.send(Message::Text(msg)).await.is_err() {
                    break;
                }
            }
            Err(broadcast::error::RecvError::Lagged(n)) => {
                warn!(skipped = n, "dashboard ws receiver lagged");
            }
            Err(broadcast::error::RecvError::Closed) => break,
        }
    }
}

// ---------------------------------------------------------------------------
// Embedded HTML dashboard
// ---------------------------------------------------------------------------

const DASHBOARD_HTML: &str = r#"<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Bot Dashboard</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.4/dist/chart.umd.min.js"></script>
<style>
  * { box-sizing: border-box; margin: 0; padding: 0; }
  body { background: #0d1117; color: #c9d1d9; font-family: 'Segoe UI', system-ui, monospace; font-size: 14px; }
  #app { max-width: 1400px; margin: 0 auto; padding: 16px; }
  #status { font-size: 12px; color: #8b949e; margin-bottom: 8px; }
  #status.ok { color: #3fb950; }
  #status.err { color: #f85149; }

  /* Stats strip */
  #stats { display: flex; flex-wrap: wrap; gap: 12px; background: #161b22; border: 1px solid #30363d; border-radius: 8px; padding: 12px 16px; margin-bottom: 16px; }
  .stat { display: flex; flex-direction: column; min-width: 90px; }
  .stat-label { font-size: 11px; color: #8b949e; text-transform: uppercase; letter-spacing: .05em; }
  .stat-value { font-size: 16px; font-weight: 700; margin-top: 2px; }
  .pos { color: #3fb950; }
  .neg { color: #f85149; }
  .neu { color: #c9d1d9; }

  /* Timeframe bar */
  .tf-bar { display: flex; gap: 4px; margin-bottom: 12px; }
  .tf-btn { background: #21262d; border: 1px solid #30363d; color: #8b949e; border-radius: 4px; padding: 4px 10px; font-size: 11px; cursor: pointer; }
  .tf-btn.active { background: rgba(56,139,253,.13); border-color: #388bfd; color: #58a6ff; }
  .tf-btn:hover:not(.active) { background: #30363d; }

  /* Chart grid */
  #charts { display: grid; grid-template-columns: 1fr 1fr; grid-template-rows: auto auto auto; gap: 16px; margin-bottom: 16px; }
  .chart-box { background: #161b22; border: 1px solid #30363d; border-radius: 8px; padding: 12px; }
  .chart-box h3 { font-size: 12px; color: #8b949e; text-transform: uppercase; letter-spacing: .05em; margin-bottom: 8px; }
  .chart-box canvas { display: block; width: 100% !important; }
  #win-rate-box { grid-column: 1 / -1; }

  /* Trades section */
  #trades { background: #161b22; border: 1px solid #30363d; border-radius: 8px; padding: 16px; }
  .tab-bar { display: flex; gap: 4px; margin-bottom: 16px; border-bottom: 1px solid #30363d; padding-bottom: 8px; }
  .tab-btn { background: none; border: none; color: #8b949e; font-size: 13px; cursor: pointer; padding: 4px 12px; border-radius: 4px; }
  .tab-btn.active { background: #21262d; color: #c9d1d9; }
  .tab-btn:hover:not(.active) { color: #c9d1d9; }
  .cards { display: grid; grid-template-columns: repeat(auto-fill, minmax(260px, 1fr)); gap: 12px; }
  .card { background: #0d1117; border: 1px solid #30363d; border-radius: 8px; padding: 14px; cursor: pointer; transition: border-color 0.15s; }
  .card:hover { border-color: #58a6ff; }
  .card.green { border-left: 3px solid #3fb950; }
  .card.yellow { border-left: 3px solid #e3b341; }
  .card.red { border-left: 3px solid #f85149; }
  .card-header { display: flex; align-items: center; gap: 6px; margin-bottom: 6px; font-size: 12px; font-weight: 700; }
  .card-title { font-size: 12px; color: #8b949e; margin-bottom: 8px; line-height: 1.4; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
  .card-row { display: flex; justify-content: space-between; font-size: 11px; color: #8b949e; margin-bottom: 3px; }
  .card-row span:last-child { color: #c9d1d9; }
  .card-pnl { margin-top: 8px; font-size: 14px; font-weight: 700; }
  .no-trades { color: #8b949e; font-size: 13px; padding: 16px 0; text-align: center; }

  /* Modal */
  #modal-overlay { display: none; position: fixed; inset: 0; background: rgba(0,0,0,.7); z-index: 1000; align-items: center; justify-content: center; }
  #modal-overlay.open { display: flex; }
  #modal { background: #161b22; border: 1px solid #30363d; border-radius: 10px; padding: 24px; max-width: 520px; width: 90%; max-height: 80vh; overflow-y: auto; position: relative; }
  #modal-close { position: absolute; top: 12px; right: 16px; background: none; border: none; color: #8b949e; font-size: 18px; cursor: pointer; }
  #modal-close:hover { color: #c9d1d9; }
  #modal h2 { font-size: 15px; margin-bottom: 16px; color: #c9d1d9; }
  .modal-section { margin-bottom: 14px; }
  .modal-section h4 { font-size: 11px; color: #8b949e; text-transform: uppercase; letter-spacing: .05em; margin-bottom: 6px; border-bottom: 1px solid #21262d; padding-bottom: 4px; }
  .modal-row { display: flex; justify-content: space-between; font-size: 12px; padding: 2px 0; }
  .modal-row .label { color: #8b949e; }
  .modal-row .val { color: #c9d1d9; font-weight: 600; }
  .modal-row .val.pos { color: #3fb950; }
  .modal-row .val.neg { color: #f85149; }

  /* Page tabs */
  #page-tabs { display: flex; gap: 4px; margin-bottom: 12px; padding-bottom: 8px; border-bottom: 1px solid #30363d; }
  .page-tab { background: none; border: none; color: #8b949e; font-size: 14px; cursor: pointer; padding: 6px 18px; border-radius: 6px; font-weight: 500; }
  .page-tab.active { background: #21262d; color: #c9d1d9; }
  .page-tab:hover:not(.active) { background: #161b22; color: #c9d1d9; }

  /* Trades Sheet */
  #sheet-page { margin-top: 4px; }
  .sheet-container { overflow-x: auto; overflow-y: auto; max-height: calc(100vh - 140px); border: 1px solid #30363d; border-radius: 8px; }
  #trades-table { width: 100%; border-collapse: collapse; font-size: 13px; }
  #trades-table thead th { position: sticky; top: 0; z-index: 10; background: #161b22; color: #8b949e; font-size: 11px; text-transform: uppercase; letter-spacing: .05em; padding: 10px 12px; text-align: left; border-bottom: 1px solid #30363d; white-space: nowrap; }
  #trades-table tbody tr:nth-child(odd)  { background: #0d1117; }
  #trades-table tbody tr:nth-child(even) { background: #161b22; }
  #trades-table tbody tr:hover { background: #1c2128; }
  #trades-table td { padding: 8px 12px; color: #c9d1d9; white-space: nowrap; }
  .badge { display: inline-block; padding: 2px 8px; border-radius: 10px; font-size: 11px; font-weight: 600; }
  .badge-open   { background: rgba(56,139,253,.15); color: #58a6ff; border: 1px solid rgba(56,139,253,.3); }
  .badge-pending { background: rgba(227,179,65,.15); color: #e3b341; border: 1px solid rgba(227,179,65,.3); }
  .badge-closed { background: rgba(139,148,158,.12); color: #8b949e; border: 1px solid rgba(139,148,158,.25); }
</style>
</head>
<body>
<div id="app">
  <div id="page-tabs">
    <button class="page-tab active" id="ptab-dashboard" onclick="showPage('dashboard')">Dashboard</button>
    <button class="page-tab" id="ptab-sheet" onclick="showPage('sheet')">Trades Sheet</button>
  </div>
  <div id="status">Connecting...</div>
  <div id="dashboard-page">

  <div id="stats">
    <div class="stat"><span class="stat-label">Bankroll</span><span class="stat-value neu" id="s-capital">—</span></div>
    <div class="stat"><span class="stat-label">Equity</span><span class="stat-value neu" id="s-equity">—</span></div>
    <div class="stat"><span class="stat-label">ROI</span><span class="stat-value neu" id="s-roi">—</span></div>
    <div class="stat"><span class="stat-label">Realized</span><span class="stat-value neu" id="s-realized">—</span></div>
    <div class="stat"><span class="stat-label">Unrealized</span><span class="stat-value neu" id="s-unrealized">—</span></div>
    <div class="stat"><span class="stat-label">Fees</span><span class="stat-value neu" id="s-fees">—</span></div>
    <div class="stat"><span class="stat-label">Open Pos</span><span class="stat-value neu" id="s-open">—</span></div>
    <div class="stat"><span class="stat-label">Trades</span><span class="stat-value neu" id="s-trades">—</span></div>
    <div class="stat"><span class="stat-label">Wins</span><span class="stat-value pos" id="s-wins">—</span></div>
    <div class="stat"><span class="stat-label">Losses</span><span class="stat-value neg" id="s-losses">—</span></div>
    <div class="stat"><span class="stat-label">Win Rate</span><span class="stat-value neu" id="s-wr">—</span></div>
  </div>

  <div class="tf-bar">
    <button class="tf-btn active" data-tf="live">Live</button>
    <button class="tf-btn" data-tf="5m">5m</button>
    <button class="tf-btn" data-tf="15m">15m</button>
    <button class="tf-btn" data-tf="1h">1h</button>
    <button class="tf-btn" data-tf="6h">6h</button>
    <button class="tf-btn" data-tf="1d">1d</button>
  </div>

  <div id="charts">
    <div class="chart-box"><h3>Equity</h3><canvas id="c-equity" height="180"></canvas></div>
    <div class="chart-box"><h3>Realized &amp; Unrealized P&amp;L</h3><canvas id="c-pnl" height="180"></canvas></div>
    <div class="chart-box"><h3>ROI %</h3><canvas id="c-roi" height="180"></canvas></div>
    <div class="chart-box"><h3>Per-Asset Realized P&amp;L</h3><canvas id="c-asset" height="180"></canvas></div>
    <div class="chart-box" id="win-rate-box"><h3>Win Rate %</h3><canvas id="c-wr" height="120"></canvas></div>
  </div>

  <div id="trades">
    <div class="tab-bar">
      <button class="tab-btn" id="tab-pending" onclick="showTab('pending')">Pending</button>
      <button class="tab-btn active" id="tab-open" onclick="showTab('open')">Open Trades</button>
      <button class="tab-btn" id="tab-closed" onclick="showTab('closed')">Closed Trades</button>
    </div>
    <div id="panel-pending" class="cards" style="display:none"></div>
    <div id="panel-open" class="cards"></div>
    <div id="panel-closed" class="cards" style="display:none"></div>
  </div>

  </div><!-- #dashboard-page -->

  <div id="sheet-page" style="display:none">
    <div class="sheet-container">
      <table id="trades-table">
        <thead><tr>
          <th>#</th><th>Coin</th><th>Date &amp; Time</th><th>Timeframe</th>
          <th>Status</th><th>Decision</th><th>Result</th><th>W/L</th>
          <th>Stake</th><th>P&amp;L</th><th>Fee</th>
        </tr></thead>
        <tbody id="trades-tbody"></tbody>
      </table>
    </div>
  </div>

</div>

<!-- Modal -->
<div id="modal-overlay">
  <div id="modal">
    <button id="modal-close">&#x2715;</button>
    <h2 id="modal-title">Trade Details</h2>
    <div id="modal-body"></div>
  </div>
</div>

<script>
// ---- helpers ---------------------------------------------------------------
const $ = id => document.getElementById(id);
const fmt = (v, d=2) => v == null ? '—' : (v >= 0 ? '+' : '') + v.toFixed(d);
const fmtMoney = v => v == null ? '—' : '$' + v.toFixed(2);
const fmtPct = (v, d=1) => v == null ? '—' : (v >= 0 ? '+' : '') + v.toFixed(d) + '%';
const ASSETS = ['BTC','ETH','XRP','SOL'];

function colorFor(v) { return v >= 0 ? '#3fb950' : '#f85149'; }
function colorClass(v) { return v >= 0 ? 'pos' : 'neg'; }

function fmtTime(iso) {
  if (!iso) return '—';
  return new Date(iso).toLocaleTimeString([], {hour:'2-digit', minute:'2-digit'});
}
function fmtDateTime(iso) {
  if (!iso) return '—';
  return new Date(iso).toLocaleString();
}
function fmtDuration(secs) {
  if (secs == null) return '—';
  const m = Math.floor(Math.abs(secs) / 60), s = Math.abs(secs) % 60;
  return m > 0 ? m + 'm ' + s + 's' : s + 's';
}
function fmtPctRaw(v, d=2) {
  if (v == null) return '—';
  return (v * 100).toFixed(d) + '%';
}

// ---- State -----------------------------------------------------------------
const allSnaps = [];          // [{ts, snap}] — full history
let tfMs = 60_000;            // current timeframe window in ms
let activeTab = 'open';
let activePage = 'dashboard'; // 'dashboard' | 'sheet'
const openTrades = {};        // token_id -> opened event
const closedTrades = [];      // array of closed records
let latestPositions = [];     // positions array from latest snap
const allOpenedEvents = {};   // token_id -> opened event (persists after close)
const pendingTrades = {};     // condition_id -> pending order event

// ---- Timeframe filter ------------------------------------------------------
const TF_MAP = {
  live: 60_000,
  '5m':  5  * 60_000,
  '15m': 15 * 60_000,
  '1h':  60 * 60_000,
  '6h':  6  * 60 * 60_000,
  '1d':  24 * 60 * 60_000,
};

document.querySelectorAll('.tf-btn').forEach(btn => {
  btn.addEventListener('click', () => {
    document.querySelectorAll('.tf-btn').forEach(b => b.classList.remove('active'));
    btn.classList.add('active');
    tfMs = TF_MAP[btn.dataset.tf] || 60_000;
    redrawCharts();
  });
});

function filteredSnaps() {
  const cutoff = Date.now() - tfMs;
  const raw = allSnaps.filter(p => p.ts >= cutoff);
  // Downsample to ~500 points max so Chart.js stays fast on long timeframes
  const MAX_PTS = 500;
  if (raw.length <= MAX_PTS) return raw;
  const step = raw.length / MAX_PTS;
  const out = [];
  for (let i = 0; i < MAX_PTS; i++) out.push(raw[Math.floor(i * step)]);
  if (out[out.length - 1] !== raw[raw.length - 1]) out.push(raw[raw.length - 1]);
  return out;
}

// ---- Chart defaults --------------------------------------------------------
Chart.defaults.color = '#8b949e';
Chart.defaults.borderColor = '#21262d';
Chart.defaults.font.family = "'Segoe UI', system-ui, monospace";
Chart.defaults.font.size = 11;

function makeLineChart(id, datasets) {
  return new Chart($(id), {
    type: 'line',
    data: { labels: [], datasets },
    options: {
      animation: false,
      responsive: true,
      maintainAspectRatio: true,
      interaction: { mode: 'index', intersect: false },
      plugins: {
        legend: { display: datasets.length > 1 },
        tooltip: {
          mode: 'index',
          intersect: false,
          callbacks: {
            title: (items) => {
              if (items.length > 0) {
                const idx = items[0].dataIndex;
                const pts = filteredSnaps();
                if (pts[idx]) return new Date(pts[idx].ts).toLocaleString();
              }
              return '';
            }
          }
        }
      },
      scales: {
        x: { display: false },
        y: { grid: { color: '#21262d' }, ticks: { maxTicksLimit: 5 } }
      },
      elements: { point: { radius: 0, hitRadius: 8, hoverRadius: 3 }, line: { tension: 0.2 } }
    }
  });
}

const equityChart = makeLineChart('c-equity', [{
  label: 'Equity', borderColor: '#58a6ff', backgroundColor: 'rgba(88,166,255,.08)', fill: true, data: []
}]);
const pnlChart = makeLineChart('c-pnl', [
  { label: 'Realized', borderColor: '#3fb950', backgroundColor: 'transparent', fill: false, data: [] },
  { label: 'Unrealized', borderColor: '#d29922', backgroundColor: 'transparent', fill: false, data: [] }
]);
const roiChart = makeLineChart('c-roi', [{
  label: 'ROI %', borderColor: '#bc8cff', backgroundColor: 'rgba(188,140,255,.08)', fill: true, data: []
}]);
const wrChart = makeLineChart('c-wr', [{
  label: 'Win Rate %', borderColor: '#39d353', backgroundColor: 'rgba(57,211,83,.08)', fill: true, data: []
}]);
const assetChart = new Chart($('c-asset'), {
  type: 'bar',
  data: {
    labels: ASSETS,
    datasets: [{ label: 'Realized P&L', data: [0,0,0,0], backgroundColor: ASSETS.map(() => '#58a6ff'), borderRadius: 4 }]
  },
  options: {
    animation: false, responsive: true, maintainAspectRatio: true, indexAxis: 'y',
    plugins: { legend: { display: false } },
    scales: { x: { grid: { color: '#21262d' }, ticks: { maxTicksLimit: 5 } }, y: { grid: { color: '#21262d' } } }
  }
});

// ---- Chart rendering -------------------------------------------------------
function redrawCharts() {
  const pts = filteredSnaps();
  const labels = pts.map(p => new Date(p.ts).toLocaleTimeString());

  equityChart.data.labels = labels;
  equityChart.data.datasets[0].data = pts.map(p => p.snap.equity);
  equityChart.update('none');

  pnlChart.data.labels = labels;
  pnlChart.data.datasets[0].data = pts.map(p => p.snap.realized);
  pnlChart.data.datasets[1].data = pts.map(p => p.snap.unrealized);
  pnlChart.update('none');

  roiChart.data.labels = labels;
  roiChart.data.datasets[0].data = pts.map(p => p.snap.roi_pct);
  roiChart.update('none');

  wrChart.data.labels = labels;
  wrChart.data.datasets[0].data = pts.map(p => p.snap.win_rate);
  wrChart.update('none');

  if (pts.length > 0) {
    const last = pts[pts.length - 1].snap;
    if (Array.isArray(last.per_asset)) {
      const map = {};
      last.per_asset.forEach(r => { map[r.asset] = r.realized; });
      const vals = ASSETS.map(a => map[a] ?? 0);
      assetChart.data.datasets[0].data = vals;
      assetChart.data.datasets[0].backgroundColor = vals.map(v => colorFor(v));
      assetChart.update('none');
    }
  }
}

// ---- Stats strip -----------------------------------------------------------
function applyStats(snap) {
  $('s-capital').textContent   = fmtMoney(snap.capital);
  $('s-equity').textContent    = fmtMoney(snap.equity);
  const roi = snap.roi_pct;
  $('s-roi').textContent       = roi == null ? '—' : fmt(roi) + '%';
  $('s-roi').className         = 'stat-value ' + (roi >= 0 ? 'pos' : 'neg');
  $('s-realized').textContent  = fmtMoney(snap.realized);
  $('s-realized').className    = 'stat-value ' + ((snap.realized ?? 0) >= 0 ? 'pos' : 'neg');
  $('s-unrealized').textContent = fmtMoney(snap.unrealized);
  $('s-fees').textContent      = fmtMoney(snap.fees);
  $('s-open').textContent      = snap.open_positions ?? '—';
  $('s-trades').textContent    = snap.trades ?? '—';
  $('s-wins').textContent      = snap.wins ?? '—';
  $('s-losses').textContent    = snap.losses ?? '—';
  const wr = snap.win_rate;
  $('s-wr').textContent        = wr == null ? '—' : wr.toFixed(1) + '%';
}

// ---- Tab switching ---------------------------------------------------------
function showTab(tab) {
  activeTab = tab;
  $('tab-pending').classList.toggle('active', tab === 'pending');
  $('tab-open').classList.toggle('active', tab === 'open');
  $('tab-closed').classList.toggle('active', tab === 'closed');
  $('panel-pending').style.display = tab === 'pending' ? '' : 'none';
  $('panel-open').style.display    = tab === 'open'    ? '' : 'none';
  $('panel-closed').style.display  = tab === 'closed'  ? '' : 'none';
}

// ---- Page switching --------------------------------------------------------
function showPage(page) {
  activePage = page;
  $('ptab-dashboard').classList.toggle('active', page === 'dashboard');
  $('ptab-sheet').classList.toggle('active', page === 'sheet');
  $('dashboard-page').style.display = page === 'dashboard' ? '' : 'none';
  $('sheet-page').style.display     = page === 'sheet'     ? '' : 'none';
  if (page === 'sheet') renderTradesTable();
}

// ---- Trades Sheet helpers --------------------------------------------------
function fmtSheetDateTime(iso) {
  if (!iso) return '—';
  const d = new Date(iso);
  const mo  = String(d.getMonth()+1).padStart(2,'0');
  const day = String(d.getDate()).padStart(2,'0');
  const hh  = String(d.getHours()).padStart(2,'0');
  const mm  = String(d.getMinutes()).padStart(2,'0');
  const ss  = String(d.getSeconds()).padStart(2,'0');
  return mo + '/' + day + ' ' + hh + ':' + mm + ':' + ss;
}

function computeTimeframe(ev) {
  if (!ev || !ev.end_time || !ev.window_start) return (ev && ev.timeframe) || '?';
  const secs = (new Date(ev.end_time) - new Date(ev.window_start)) / 1000;
  if (Math.abs(secs - 300)  <= 30) return '5min';
  if (Math.abs(secs - 900)  <= 30) return '15min';
  if (Math.abs(secs - 3600) <= 60) return '1h';
  return Math.round(secs / 60) + 'min';
}

function marginOfError(rec) {
  if (rec.predicted_yes_prob == null || rec.actual_yes_outcome == null) return null;
  const actual = rec.actual_yes_outcome ? 1.0 : 0.0;
  return Math.abs(rec.predicted_yes_prob - actual);
}

function computeClosedTimeframe(rec) {
  if (rec.window_start && rec.end_time) {
    const secs = (new Date(rec.end_time) - new Date(rec.window_start)) / 1000;
    if (Math.abs(secs - 300) <= 30) return '5min';
    if (Math.abs(secs - 900) <= 30) return '15min';
    if (Math.abs(secs - 3600) <= 60) return '1h';
    return Math.round(secs / 60) + 'min';
  }
  const ev = allOpenedEvents[rec.token_id];
  if (ev) return computeTimeframe(ev);
  return '?';
}

function buildTradeRows() {
  const rows = [];
  for (const tok of Object.keys(openTrades)) {
    const opened = allOpenedEvents[tok] || openTrades[tok];
    const pos = latestPositions.find(p => p.token_id === tok);
    rows.push({ status: 'open', tok, opened, pos });
  }
  for (const rec of closedTrades) {
    const opened = allOpenedEvents[rec.token_id] || {};
    rows.push({ status: 'closed', rec, opened });
  }
  rows.sort((a, b) => {
    const ta = a.status === 'open' ? (a.opened && a.opened.opened_at) : (a.rec && a.rec.opened_at);
    const tb = b.status === 'open' ? (b.opened && b.opened.opened_at) : (b.rec && b.rec.opened_at);
    return new Date(tb || 0) - new Date(ta || 0);
  });
  return rows;
}

function renderTradesTable() {
  const tbody = $('trades-tbody');
  if (!tbody) return;
  const rows = buildTradeRows();
  if (rows.length === 0) {
    tbody.innerHTML = '<tr><td colspan="11" style="text-align:center;color:#8b949e;padding:24px">No trades yet</td></tr>';
    return;
  }
  let html = '';
  rows.forEach((row, i) => {
    const n = i + 1;
    if (row.status === 'open') {
      const ev  = row.opened || {};
      const pos = row.pos;
      const pnl = pos ? pos.unrealized_pnl : null;
      const coin     = ev.asset || '?';
      const dt       = fmtSheetDateTime(ev.opened_at);
      const tf       = computeTimeframe(ev);
      const decision = ev.side === 'YES' ? '↑ UP' : (ev.side === 'NO' ? '↓ DOWN' : '—');
      const isPending = pos && pos.pending_settlement;
      const wlText   = pnl == null ? '—' : (pnl >= 0 ? 'WINNING' : 'LOSING');
      const wlCls    = pnl == null ? '' : colorClass(pnl);
      const stake    = ev.kelly_stake_usdc != null ? '$' + Number(ev.kelly_stake_usdc).toFixed(2) : '—';
      const pnlText  = pnl != null ? fmt(pnl, 2) : '—';
      const pnlCls   = pnl != null ? colorClass(pnl) : '';
      const feeVal   = (ev.cost_fee != null && ev.kelly_stake_usdc != null) ? ev.cost_fee * ev.kelly_stake_usdc : null;
      const feeText  = feeVal != null ? '~$' + feeVal.toFixed(2) : '—';
      const badgeHtml = isPending
        ? '<span class="badge badge-pending">PENDING</span>'
        : '<span class="badge badge-open">OPEN</span>';
      html += '<tr>' +
        '<td>' + n + '</td>' +
        '<td><b>' + coin + '</b></td>' +
        '<td>' + dt + '</td>' +
        '<td>' + tf + '</td>' +
        '<td>' + badgeHtml + '</td>' +
        '<td>' + decision + '</td>' +
        '<td>—</td>' +
        '<td class="' + wlCls + '">' + wlText + '</td>' +
        '<td>' + stake + '</td>' +
        '<td class="' + pnlCls + '">' + pnlText + '</td>' +
        '<td>' + feeText + '</td>' +
        '</tr>';
    } else {
      const rec  = row.rec;
      const ev   = row.opened || {};
      const pnl  = rec.realized_pnl;
      const coin = rec.asset || ev.asset || '?';
      const dt   = fmtSheetDateTime(rec.opened_at);
      const tf   = computeClosedTimeframe(rec);
      const side = rec.side || ev.side;
      const decision   = side === 'YES' ? '↑ UP' : (side === 'NO' ? '↓ DOWN' : '—');
      const resultText = rec.actual_yes_outcome != null ? (rec.actual_yes_outcome ? '↑ UP' : '↓ DOWN') : '—';
      const wlText = rec.won ? 'WIN' : 'LOSS';
      const wlCls  = rec.won ? 'pos' : 'neg';
      const stake  = ev.kelly_stake_usdc != null ? '$' + Number(ev.kelly_stake_usdc).toFixed(2) : '—';
      const pnlText = pnl != null ? fmt(pnl, 2) : '—';
      const pnlCls  = pnl != null ? colorClass(pnl) : '';
      const feeVal  = (ev.cost_fee != null && ev.kelly_stake_usdc != null) ? ev.cost_fee * ev.kelly_stake_usdc : null;
      const feeText = feeVal != null ? '~$' + feeVal.toFixed(2) : '—';
      html += '<tr>' +
        '<td>' + n + '</td>' +
        '<td><b>' + coin + '</b></td>' +
        '<td>' + dt + '</td>' +
        '<td>' + tf + '</td>' +
        '<td><span class="badge badge-closed">CLOSED</span></td>' +
        '<td>' + decision + '</td>' +
        '<td>' + resultText + '</td>' +
        '<td class="' + wlCls + '">' + wlText + '</td>' +
        '<td>' + stake + '</td>' +
        '<td class="' + pnlCls + '">' + pnlText + '</td>' +
        '<td>' + feeText + '</td>' +
        '</tr>';
    }
  });
  tbody.innerHTML = html;
}

// ---- Card rendering --------------------------------------------------------
function renderOpenCard(tok, opened, positions) {
  const pos = positions && positions.find(p => p.token_id === tok);
  const pnl = pos ? pos.unrealized_pnl : null;
  const roi = pos ? pos.unrealized_roi_pct : null;
  const isPending = pos && pos.pending_settlement;
  const isGreen = pnl == null || pnl >= 0;
  const card = document.createElement('div');
  card.className = 'card ' + (isPending ? 'yellow' : (isGreen ? 'green' : 'red'));
  card.onclick = () => openModal('open', tok);
  const asset = opened.asset || (tok ? tok.slice(0,8) : '?');
  const tf = pos ? pos.timeframe : (opened.timeframe || '?');
  const entryPrice = opened.entry_price ?? pos?.entry_price ?? 0;
  const currentPrice = pos ? pos.current_price : 0;
  const ptb = pos ? pos.price_to_beat : null;
  const statusLabel = isPending ? '&#x1F7E1; PENDING' : '&#x1F535; OPEN';
  card.innerHTML =
    '<div class="card-header">' +
      '<span>' + statusLabel + '</span>' +
      '<span>' + asset + '</span>' +
      '<span style="margin-left:auto;color:#8b949e">' + tf + '</span>' +
    '</div>' +
    '<div class="card-title">' + (opened.question || '—') + '</div>' +
    '<div class="card-row"><span>Side</span><span>' + (opened.side || '?') + '</span></div>' +
    '<div class="card-row"><span>Entry</span><span>$' + Number(entryPrice).toFixed(3) + '</span></div>' +
    '<div class="card-row"><span>Current</span><span>$' + Number(currentPrice).toFixed(3) + '</span></div>' +
    (ptb != null ? '<div class="card-row"><span>Price to Beat</span><span>$' + Number(ptb).toLocaleString() + '</span></div>' : '') +
    '<div class="card-pnl ' + (isGreen ? 'pos' : 'neg') + '">' +
      (pnl != null ? fmt(pnl, 2) + ' USDC' : '—') +
      (roi != null ? ' <small style="font-size:11px">(' + fmt(roi, 1) + '%)</small>' : '') +
    '</div>' +
    '<div class="card-row" style="margin-top:4px"><span>Opened</span><span>' + fmtTime(opened.opened_at) + '</span></div>' +
    (pos && pos.end_time ? '<div class="card-row"><span>Expires</span><span>' + fmtTime(pos.end_time) + '</span></div>' : '');
  return card;
}

function renderClosedCard(rec) {
  const won = rec.won;
  const pnl = rec.realized_pnl;
  const tf = computeClosedTimeframe(rec);
  const entryPrice = rec.avg_entry_price;
  const qty = rec.quantity;
  const roiPct = (entryPrice != null && qty != null && entryPrice > 0 && qty > 0)
    ? (pnl / (entryPrice * qty)) * 100 : null;
  const moe = marginOfError(rec);
  const ptb = rec.official_price_to_beat;
  const card = document.createElement('div');
  card.className = 'card ' + (won ? 'green' : 'red');
  card.onclick = () => openModal('closed', null, rec);
  card.innerHTML =
    '<div class="card-header">' +
      '<span>' + (won ? '&#x2705; WIN' : '&#x274C; LOSS') + '</span>' +
      '<span>' + (rec.asset || '?') + '</span>' +
      '<span style="margin-left:auto;color:#8b949e">' + tf + '</span>' +
    '</div>' +
    '<div class="card-title">' + (rec.market_question || '—') + '</div>' +
    '<div class="card-row"><span>Side</span><span>' + (rec.side || '?') + ' &middot; ' + fmtDuration(rec.hold_seconds) + '</span></div>' +
    (entryPrice != null ? '<div class="card-row"><span>Entry</span><span>$' + Number(entryPrice).toFixed(3) + '</span></div>' : '') +
    '<div class="card-row"><span>Payout</span><span>$' + Number(rec.payout_price ?? 0).toFixed(2) + '</span></div>' +
    (ptb != null ? '<div class="card-row"><span>Price to Beat</span><span>$' + Number(ptb).toLocaleString() + '</span></div>' : '') +
    '<div class="card-pnl ' + (won ? 'pos' : 'neg') + '">' +
      (pnl != null ? fmt(pnl, 2) + ' USDC' : '—') +
      (roiPct != null ? ' <small style="font-size:11px">(' + fmt(roiPct, 1) + '%)</small>' : '') +
    '</div>' +
    (moe != null ? '<div class="card-row" style="margin-top:4px"><span>Margin of Error</span><span style="color:#d29922">&plusmn;' + (moe * 100).toFixed(1) + '%</span></div>' : '') +
    '<div class="card-row" style="margin-top:' + (moe != null ? '0' : '4') + 'px"><span>Opened</span><span>' + fmtTime(rec.opened_at) + '</span></div>' +
    '<div class="card-row"><span>Closed</span><span>' + fmtTime(rec.closed_at) + '</span></div>';
  return card;
}

function renderOpenCards(positions) {
  const panel = $('panel-open');
  panel.innerHTML = '';
  const keys = Object.keys(openTrades);
  if (keys.length === 0) {
    panel.innerHTML = '<div class="no-trades">No open positions</div>';
    return;
  }
  keys.forEach(tok => panel.appendChild(renderOpenCard(tok, openTrades[tok], positions)));
}

function renderClosedCards() {
  const panel = $('panel-closed');
  panel.innerHTML = '';
  if (closedTrades.length === 0) {
    panel.innerHTML = '<div class="no-trades">No closed trades yet</div>';
    return;
  }
  [...closedTrades].reverse().forEach(rec => panel.appendChild(renderClosedCard(rec)));
}

function computePendingTimeframe(msg) {
  if (!msg.end_time || !msg.window_start) return '?';
  const secs = (new Date(msg.end_time) - new Date(msg.window_start)) / 1000;
  if (Math.abs(secs - 300)  <= 30) return '5min';
  if (Math.abs(secs - 900)  <= 30) return '15min';
  if (Math.abs(secs - 3600) <= 60) return '1h';
  return Math.round(secs / 60) + 'min';
}

function renderPendingCard(msg) {
  const card = document.createElement('div');
  card.className = 'card yellow';
  card.onclick = () => openModal('pending_detail', null, null, msg);
  const asset = msg.asset || '?';
  const tf = computePendingTimeframe(msg);
  const side = msg.side || '?';
  const d = msg.diagnostics || {};
  card.innerHTML =
    '<div class="card-header">' +
      '<span>&#x23F3; PENDING</span>' +
      '<span>' + asset + '</span>' +
      '<span style="margin-left:auto;color:#8b949e">' + tf + '</span>' +
    '</div>' +
    '<div class="card-title">' + (msg.question || '—') + '</div>' +
    '<div class="card-row"><span>Side</span><span>' + side + '</span></div>' +
    '<div class="card-row"><span>Limit Price</span><span>$' + Number(msg.limit_price || 0).toFixed(3) + '</span></div>' +
    '<div class="card-row"><span>Amount (USDC)</span><span>$' + Number(msg.amount || 0).toFixed(2) + '</span></div>' +
    '<div class="card-row"><span>Placed At</span><span>' + fmtTime(msg.placed_at) + '</span></div>' +
    '<div class="card-row"><span>Expires</span><span>' + fmtTime(msg.end_time) + '</span></div>' +
    (d.net_edge != null ? '<div class="card-row" style="margin-top:6px"><span>Net Edge</span><span>' + (d.net_edge * 100).toFixed(1) + '%</span></div>' : '') +
    (d.posterior != null ? '<div class="card-row"><span>Posterior</span><span>' + (d.posterior * 100).toFixed(1) + '%</span></div>' : '') +
    (d.kelly_final != null ? '<div class="card-row"><span>Kelly Final</span><span>' + d.kelly_final.toFixed(4) + '</span></div>' : '');
  return card;
}

function renderPendingCards() {
  const panel = $('panel-pending');
  panel.innerHTML = '';
  const keys = Object.keys(pendingTrades);
  if (keys.length === 0) {
    panel.innerHTML = '<div class="no-trades">No pending orders</div>';
    return;
  }
  keys.forEach(cid => panel.appendChild(renderPendingCard(pendingTrades[cid])));
}

// ---- Modal -----------------------------------------------------------------
function mRow(label, val, cls) {
  return '<div class="modal-row"><span class="label">' + label + '</span><span class="val' + (cls ? ' ' + cls : '') + '">' + (val ?? '—') + '</span></div>';
}
function mSection(title, rows) {
  return '<div class="modal-section"><h4>' + title + '</h4>' + rows + '</div>';
}

function openModal(type, tokenId, closedRec, pendingMsg) {
  if (type === 'pending_detail') {
    const p = pendingMsg;
    const d = p.diagnostics || {};
    $('modal-title').textContent = p.question || 'Pending Order';
    $('modal-body').innerHTML =
      mSection('Order Info',
        mRow('Asset', p.asset) +
        mRow('Side', p.side) +
        mRow('Limit Price', p.limit_price != null ? '$' + Number(p.limit_price).toFixed(3) : null) +
        mRow('Amount', p.amount != null ? '$' + Number(p.amount).toFixed(2) : null) +
        mRow('Placed At', fmtDateTime(p.placed_at)) +
        mRow('Expires', fmtDateTime(p.end_time))
      ) +
      mSection('Model &amp; Probability',
        mRow('P(model)', fmtPctRaw(d.p_model)) +
        mRow('Market P', fmtPctRaw(d.market_implied_p)) +
        mRow('Posterior', fmtPctRaw(d.posterior))
      ) +
      mSection('Edge &amp; Costs',
        mRow('Gross Edge', d.gross_edge != null ? fmt(d.gross_edge * 100, 1) + '%' : null) +
        mRow('Net Edge', d.net_edge != null ? fmt(d.net_edge * 100, 1) + '%' : null) +
        mRow('Fee Cost', d.cost_fee != null ? (d.cost_fee * 100).toFixed(2) + '%' : null) +
        mRow('Slippage', d.cost_slippage != null ? (d.cost_slippage * 100).toFixed(2) + '%' : null) +
        mRow('Total Cost', d.cost_total != null ? (d.cost_total * 100).toFixed(2) + '%' : null)
      ) +
      mSection('Kelly Sizing',
        mRow('Kelly Raw', d.kelly_raw != null ? d.kelly_raw.toFixed(4) : null) +
        mRow('Kelly Adjusted', d.kelly_adjusted != null ? d.kelly_adjusted.toFixed(4) : null) +
        mRow('Kelly Final', d.kelly_final != null ? d.kelly_final.toFixed(4) : null) +
        mRow('Stake (USDC)', d.kelly_stake_usdc != null ? '$' + Number(d.kelly_stake_usdc).toFixed(2) : null)
      );
    $('modal-overlay').classList.add('open');
    return;
  }
  if (type === 'open') {
    const ev = openTrades[tokenId];
    if (!ev) return;
    $('modal-title').textContent = ev.question || 'Open Trade';
    $('modal-body').innerHTML =
      mSection('Trade Info',
        mRow('Asset', ev.asset) +
        mRow('Side', ev.side) +
        mRow('Model', ev.model_type) +
        mRow('Entry Price', ev.entry_price != null ? '$' + Number(ev.entry_price).toFixed(3) : null) +
        mRow('Opened', fmtDateTime(ev.opened_at))
      ) +
      mSection('Model &amp; Probability',
        mRow('P(model)', fmtPctRaw(ev.p_model)) +
        mRow('Market P', fmtPctRaw(ev.market_implied_p)) +
        mRow('Posterior', fmtPctRaw(ev.posterior))
      ) +
      mSection('Edge &amp; Costs',
        mRow('Gross Edge', ev.gross_edge != null ? fmt(ev.gross_edge * 100, 1) + '%' : null) +
        mRow('Net Edge', ev.net_edge != null ? fmt(ev.net_edge * 100, 1) + '%' : null) +
        mRow('Fee Cost', ev.cost_fee != null ? (ev.cost_fee * 100).toFixed(2) + '%' : null) +
        mRow('Slippage', ev.cost_slippage != null ? (ev.cost_slippage * 100).toFixed(2) + '%' : null) +
        mRow('Total Cost', ev.cost_total != null ? (ev.cost_total * 100).toFixed(2) + '%' : null)
      ) +
      mSection('Kelly Sizing',
        mRow('Kelly Raw', ev.kelly_raw != null ? ev.kelly_raw.toFixed(4) : null) +
        mRow('Kelly Adjusted', ev.kelly_adjusted != null ? ev.kelly_adjusted.toFixed(4) : null) +
        mRow('Kelly Final', ev.kelly_final != null ? ev.kelly_final.toFixed(4) : null) +
        mRow('Stake (USDC)', ev.kelly_stake_usdc != null ? '$' + Number(ev.kelly_stake_usdc).toFixed(2) : null)
      );
  } else {
    const rec = closedRec;
    $('modal-title').textContent = rec.market_question || 'Closed Trade';
    const won = rec.won;
    const mEntryPrice = rec.avg_entry_price;
    const mQty = rec.quantity;
    const mRoiPct = (mEntryPrice != null && mQty != null && mEntryPrice > 0 && mQty > 0)
      ? ((rec.realized_pnl ?? 0) / (mEntryPrice * mQty)) * 100 : null;
    const mMoe = marginOfError(rec);
    const mTf = computeClosedTimeframe(rec);
    $('modal-body').innerHTML =
      mSection('Trade Info',
        mRow('Asset', rec.asset) +
        mRow('Side', rec.side) +
        mRow('Model', rec.model_type) +
        mRow('Outcome', won ? '&#x2705; WIN' : '&#x274C; LOSS', won ? 'pos' : 'neg') +
        mRow('Entry Price', mEntryPrice != null ? '$' + Number(mEntryPrice).toFixed(3) : null) +
        mRow('Quantity', mQty != null ? Number(mQty).toFixed(2) : null) +
        mRow('Hold Time', fmtDuration(rec.hold_seconds)) +
        mRow('Predicted P(Yes)', rec.predicted_yes_prob != null ? (rec.predicted_yes_prob * 100).toFixed(1) + '%' : null)
      ) +
      mSection('Financials',
        mRow('Realized P&amp;L', rec.realized_pnl != null ? fmt(rec.realized_pnl, 2) + ' USDC' : null, (rec.realized_pnl ?? 0) >= 0 ? 'pos' : 'neg') +
        mRow('ROI', mRoiPct != null ? fmt(mRoiPct, 1) + '%' : null, mRoiPct != null ? (mRoiPct >= 0 ? 'pos' : 'neg') : '') +
        mRow('Payout Price', rec.payout_price != null ? '$' + Number(rec.payout_price).toFixed(2) : null) +
        mRow('Official Price to Beat', rec.official_price_to_beat != null ? '$' + Number(rec.official_price_to_beat).toLocaleString() : null) +
        mRow('Official Final Price', rec.official_final_price != null ? '$' + Number(rec.official_final_price).toLocaleString() : null)
      ) +
      mSection('Model Accuracy',
        mRow('Margin of Error', mMoe != null ? '&plusmn;' + (mMoe * 100).toFixed(1) + '%' : null) +
        mRow('Predicted P(Yes)', rec.predicted_yes_prob != null ? (rec.predicted_yes_prob * 100).toFixed(1) + '%' : null) +
        mRow('Actual Outcome', rec.actual_yes_outcome != null ? (rec.actual_yes_outcome ? 'YES (1.0)' : 'NO (0.0)') : null)
      ) +
      mSection('Timing',
        mRow('Timeframe', mTf) +
        mRow('Opened', fmtDateTime(rec.opened_at)) +
        mRow('Closed', fmtDateTime(rec.closed_at)) +
        mRow('Settlement Source', rec.settlement_source)
      );
  }
  $('modal-overlay').classList.add('open');
}

$('modal-close').addEventListener('click', () => $('modal-overlay').classList.remove('open'));
$('modal-overlay').addEventListener('click', e => {
  if (e.target === $('modal-overlay')) $('modal-overlay').classList.remove('open');
});

// ---- Message handlers ------------------------------------------------------
function handleSnap(msg) {
  const ts = msg.ts || Date.now();
  allSnaps.push({ ts, snap: msg });
  if (allSnaps.length > 20000) allSnaps.shift();
  if (Array.isArray(msg.positions)) {
    latestPositions = msg.positions;
    if (activeTab === 'open') renderOpenCards(latestPositions);
    if (activePage === 'sheet') renderTradesTable();
  }
  applyStats(msg);
  redrawCharts();
}

function handleOpened(msg) {
  allOpenedEvents[msg.token_id] = msg;
  openTrades[msg.token_id] = msg;
  if (activeTab === 'open') renderOpenCards(latestPositions);
  if (activePage === 'sheet') renderTradesTable();
}

function handleClosed(msg) {
  delete openTrades[msg.token_id];
  closedTrades.push(msg);
  if (activeTab === 'open') renderOpenCards(latestPositions);
  if (activeTab === 'closed') renderClosedCards();
  if (activePage === 'sheet') renderTradesTable();
}

function handlePending(msg) {
  pendingTrades[msg.condition_id] = msg;
  if (activeTab === 'pending') renderPendingCards();
}

function handlePendingRemove(msg) {
  delete pendingTrades[msg.condition_id];
  if (activeTab === 'pending') renderPendingCards();
}

function handleInit(msg) {
  (msg.snaps || []).forEach(s => {
    const ts = s.ts || Date.now();
    allSnaps.push({ ts, snap: s });
    if (Array.isArray(s.positions)) latestPositions = s.positions;
  });
  (msg.opened || []).forEach(o => {
    allOpenedEvents[o.token_id] = o;
    openTrades[o.token_id] = o;
  });
  (msg.closed || []).forEach(c => {
    if (!allOpenedEvents[c.token_id]) allOpenedEvents[c.token_id] = {};
    delete openTrades[c.token_id];
    closedTrades.push(c);
  });
  (msg.pending || []).forEach(p => {
    pendingTrades[p.condition_id] = p;
  });
  if (allSnaps.length > 0) applyStats(allSnaps[allSnaps.length - 1].snap);
  redrawCharts();
  renderOpenCards(latestPositions);
  renderClosedCards();
  renderPendingCards();
  if (activePage === 'sheet') renderTradesTable();
}

// ---- WebSocket -------------------------------------------------------------
function connect() {
  const proto = location.protocol === 'https:' ? 'wss:' : 'ws:';
  const ws = new WebSocket(proto + '//' + location.host + '/ws');
  const statusEl = $('status');

  ws.onopen = () => { statusEl.textContent = 'Connected'; statusEl.className = 'ok'; };
  ws.onclose = () => { statusEl.textContent = 'Disconnected — reconnecting in 2s\u2026'; statusEl.className = 'err'; setTimeout(connect, 2000); };
  ws.onerror = () => { ws.close(); };

  ws.onmessage = evt => {
    let data;
    try { data = JSON.parse(evt.data); } catch { return; }
    if      (data.t === 'init')           handleInit(data);
    else if (data.t === 'snap')           handleSnap(data);
    else if (data.t === 'opened')         handleOpened(data);
    else if (data.t === 'closed')         handleClosed(data);
    else if (data.t === 'pending')        handlePending(data);
    else if (data.t === 'pending_remove') handlePendingRemove(data);
  };
}

connect();
</script>
</body>
</html>"#;
