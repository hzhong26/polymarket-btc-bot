# Polymarket Crypto Trading Bot

Algorithmic trading bot for [Polymarket](https://polymarket.com) crypto prediction markets. Uses Bayesian probability estimation, Kelly criterion position sizing, and multi-source price aggregation to identify and trade binary outcome markets (BTC, ETH, SOL, XRP) with edges in the 5-minute and 15-minute windows.

Built in Rust for low-latency execution (~2-6s tick-to-order).

---

## Table of Contents

- [Architecture](#architecture)
- [Modules](#modules)
- [How It Works](#how-it-works)
- [Getting Started](#getting-started)
- [Configuration](#configuration)
- [CLI Usage](#cli-usage)
- [Dashboard](#dashboard)
- [Versioning](#versioning)
- [Roadmap](#roadmap)

---

## Architecture

```
                          +------------------+
                          |   Gamma Scanner  |  (30s poll + boundary scan)
                          | market discovery |
                          +--------+---------+
                                   |
+-------------------+     +--------v---------+     +------------------+
|   Price Feeds     | --> |  Trading Loop    | --> |  Venue Client    |
| Chainlink,Binance | --> | (select! loop)   | --> | (CLOB + WS)     |
| Coinbase,CoinGecko|     +--------+---------+     +------------------+
+-------------------+              |
                          +--------v---------+
                          | Bayesian Model   |
                          | Edge Calculator  |
                          | Kelly Sizer      |
                          | Risk Manager     |
                          +------------------+
                                   |
                    +--------------+--------------+
                    |              |              |
              +-----v----+  +-----v----+  +-----v----+
              | Dashboard |  |  JSONL   |  | Trade    |
              | (browser) |  |  Logger  |  | Sheet    |
              +----------+  +----------+  +----------+
```

**Single binary crate** (`src/main.rs`) with library (`src/lib.rs`). Async runtime via Tokio. All monetary arithmetic uses `rust_decimal` for precision.

---

## Modules

| Module | Path | Purpose |
|--------|------|---------|
| **trading_loop** | `src/trading_loop/` | Core orchestrator: scan, evaluate, execute, settle |
| **bayesian** | `src/bayesian/` | Probability models (GBM, directional), time-aware EWMA volatility, calibration |
| **kelly** | `src/kelly/` | Kelly criterion position sizing with uncertainty haircut |
| **edge** | `src/edge/` | Net edge calculation with dynamic fee model & fee asymmetry detection |
| **price_feeds** | `src/price_feeds/` | Multi-source aggregation: Chainlink, Binance, Coinbase, CoinGecko, CryptoCompare |
| **market_scanner** | `src/market_scanner/` | Gamma API scanner for tradeable 5m/15m crypto markets |
| **venue** | `src/venue/` | Polymarket client (International: EIP-712 on Polygon; US: EdDSA) |
| **executor** | `src/executor/` | Order management & WebSocket fill monitoring |
| **risk** | `src/risk/` | Position limits, drawdown halt, cooldown, kill switch |
| **lmsr** | `src/lmsr/` | LMSR pricing engine (theoretical cross-check) |
| **dashboard_server** | `src/dashboard_server.rs` | Real-time browser dashboard (axum HTTP/WS + embedded Chart.js) |
| **config** | `src/config/` | Configuration types & TOML loading |
| **analyze** | `src/analyze.rs` | Post-session analysis: calibration curves, Brier decomposition, tuning recommendations |
| **replay** | `src/replay.rs` | Vol fix impact analysis: replays pipeline evals with new vol estimator |
| **backtest** | `src/backtest.rs` | Parameter sweep backtester replaying logged pipeline evaluations |
| **timezone** | `src/timezone.rs` | Eastern Time conversion (Polymarket market windows use ET) |

---

## How It Works

### 1. Market Discovery
The **Gamma Scanner** polls Polymarket's Gamma API every 30 seconds for binary crypto markets (BTC/ETH/SOL/XRP) expiring within 5-15 minutes. An immediate scan is also dispatched at every 5-minute boundary crossing to discover new markets without waiting for the timer. Markets are filtered by liquidity, spread, and tag.

### 2. Price Aggregation
Five independent price feeds publish to an **aggregator** that:
- Requires a quorum of 2+ fresh, non-anomalous sources
- Rejects outliers deviating >2% from consensus
- Computes a median spot price with a confidence score (0-1)

### 3. Bayesian Probability Estimation
For each active market, the bot computes a **posterior probability**:
- **Strike markets** (e.g., "BTC above $97,500?"): Black-Scholes GBM binary call — `N(d2)`
- **Up/Down markets** (e.g., "BTC Up or Down?"): Directional model with annualized drift from EWMA
- **Confidence shrinkage**: Blends model output with market-implied price weighted by feed confidence

**Volatility** is estimated per-asset using a time-aware EWMA that:
- Tracks actual inter-observation timestamps (not assumed uniform spacing)
- Uses time-weighted decay: `λ_eff = exp(-dt / half_life)` with variance stored per-second
- Prefers raw Binance observations over aggregated median for highest-fidelity signal
- Guards against too-fast ticks (<50ms) and reconnect gaps (>30s)

### 4. Edge Calculation
**Gross edge** = posterior probability - market price, computed for both sides (YES and NO).

**Net edge** = gross - dynamic fees - latency penalty. The bot picks whichever side has higher net edge, detecting **fee asymmetry flips** where the cheaper side wins despite lower gross edge (Polymarket's quadratic fee formula makes extreme prices 3-8x cheaper than mid-prices).

### 5. Kelly Criterion Sizing
Position size follows fractional Kelly:
```
f* = (b*p - q) / b                   (full Kelly)
f_adj = f* * (1 - brier_score)       (uncertainty haircut)
f_final = kelly_fraction * f_adj      (fractional multiplier)
stake = bankroll * f_final
```
The **Brier score** (mean squared error of past predictions) auto-calibrates from rolling settlement data — worse calibration means more conservative sizing.

### 6. Pipeline Filtering
Before any order, the pipeline checks:
- Confidence >= 80%
- Spread ratio <= 1%
- Liquidity >= $1,000
- Time to expiry >= 60s
- Dynamic threshold (base + confidence/spread/calibration penalties)
- Side multipliers (YES default 1.0x, NO default 1.2x — configurable)
- Max net edge cap (rejects suspiciously large edges)
- Risk limits (position size, total exposure, drawdown, cooldown)

### 7. Execution & Settlement
**Paper mode**: Orders go through a pending order lifecycle with simulated fills based on orderbook state. **Live mode**: Orders placed via the CLOB API with fills arriving over WebSocket. At market expiry, the bot polls Gamma API for official resolution (with backoff). P&L is locked in and the calibration tracker records the prediction vs. actual outcome.

### 8. Reference Price Capture
At 5-minute boundary crossings:
1. Chainlink one-shot captures reference prices for all assets (3s grace period)
2. Immediate market scan dispatches to discover new markets
3. Reference cache is keyed by (asset, window_start) to deduplicate across markets

---

## Getting Started

### Prerequisites
- Rust 1.75.0+ (`rustup update stable`)
- A Polymarket account with API credentials
- An RPC endpoint for Polygon (Chainlink feeds)

### Build
```bash
cargo build --release
```

### Environment
Create a `.env` file:
```env
POLYMARKET_PRIVATE_KEY=0x...    # International venue (EIP-712)
POLYMARKET_API_KEY=...          # US venue (EdDSA)
```

### Run (Paper Mode)
```bash
./target/release/polymarket-btc-bot config.toml --paper
```

### Run (Live)
```bash
./target/release/polymarket-btc-bot config.toml
```

---

## Configuration

All configuration is in `config.toml`. Key sections:

### `[venue]`
Venue type (`international` or `us`) and endpoint URLs.

### `[price_feeds]`
Chainlink contract addresses (BTC/ETH/SOL/XRP on Polygon), Binance WS URL, Coinbase/CoinGecko/CryptoCompare endpoints. Controls staleness windows, anomaly detection threshold, and source quorum.

### `[trading]`
| Parameter | Default | Description |
|-----------|---------|-------------|
| `edge_threshold` | `0.11` | Minimum net edge (11%) to trade |
| `kelly_fraction` | `0.20` | Fractional Kelly multiplier |
| `uncertainty_haircut_max` | `0.90` | Reject if model uncertainty > 90% |
| `latency_penalty` | `0.005` | 0.5% execution slippage hedge |
| `market_tags` | BTC, ETH, XRP, SOL | Asset universe |
| `market_duration_max_secs` | `900` | Max market window (15 min) |
| `fee_rate` / `fee_exponent` | `0.25` / `2` | Polymarket dynamic fee formula |
| `max_net_edge` | `0.20` | Reject edges above this cap |
| `yes_edge_threshold_mult` | `1.0` | YES-side edge multiplier |
| `no_edge_threshold_mult` | `1.2` | NO trades need 20% more edge |
| `ewma_lambda` | `0.94` | EWMA decay factor (global default) |
| `vol_window_secs` | `300` | Volatility estimation window |
| `paper_bankroll_usdc` | `2000` | Simulated bankroll for paper mode |
| `obi_weight` | `0.0` | Orderbook imbalance signal weight (0 = disabled) |
| `obi_depth_levels` | `5` | Orderbook levels for OBI calculation |

### `[trading.asset_params.*]`
Per-asset overrides for edge threshold multiplier, uncertainty multiplier, and EWMA lambda.

### `[risk]`
| Parameter | Default | Description |
|-----------|---------|-------------|
| `max_position_usdc` | `50` | Max single position (paper) |
| `max_total_exposure` | `200` | Max total deployed capital (paper) |
| `max_drawdown_pct` | `0.50` | Halt at 50% drawdown |
| `cooldown_after_loss_ms` | `60000` | 60s pause after consecutive losses |
| `max_consecutive_losses` | `3` | Trigger cooldown after 3 losses |
| `max_open_orders` | `10` | Max concurrent orders |

### `[risk.live]`
Live-mode overrides for risk limits (separate from paper-mode defaults).

### `[logging]`
Console log level/format, Prometheus metrics port, dashboard port (paper: 8080, live: 8081), and JSONL file logging settings (directory, flush interval, no-trade sampling rate, gzip compression).

---

## CLI Usage

```bash
# Live trading
polymarket-btc-bot [config.toml]

# Paper trading (simulated, no real orders)
polymarket-btc-bot [config.toml] --paper

# Disable JSONL logging
polymarket-btc-bot [config.toml] --no-log

# Post-session analysis (calibration curves, Brier decomposition, tuning recs)
polymarket-btc-bot --analyze [log-dirs...]

# Filter analysis by run number
polymarket-btc-bot --analyze logs/2026-02-25 --run 3

# List run summaries
polymarket-btc-bot --runs [log-dirs...]

# Parameter sweep backtester (replays logged pipeline evaluations)
polymarket-btc-bot --backtest [log-dir] [--run N]

# Replay with new vol estimator (compares old vs new vol, Brier, P&L)
polymarket-btc-bot --replay [log-dirs...] [--run N]
```

---

## Dashboard

A real-time browser dashboard is served on `http://localhost:8080` (paper) or `http://localhost:8081` (live).

**Features**:
- Live P&L chart (Chart.js) with timeframe filters (Live / 5m / 15m / 1h / 6h / 1d)
- Open positions with mark-to-market updates
- Trade history with three tabs: Pending / Open / Closed
- Pending orders with yellow flashcards (hourglass badge, diagnostics on click)
- Click-to-expand diagnostics modal for any trade
- WebSocket protocol for instant updates

**Protocol**:
- On connect: full state snapshot (`init` message with snaps, opened, closed, pending)
- Live: `snap` messages with P&L fields + position cards on every evaluation tick
- Trade events: `pending`, `pending_remove`, `opened`, `closed` messages with full diagnostics

---

## Versioning

### v1.8.2 — Replay Mode & Conviction Filter
*Tag: `v1.8.2`*

- **`--replay` mode**: Replays logged pipeline evaluations through the new vol estimator, comparing old vs new volatility, posteriors, Brier scores, decision changes, and hypothetical P&L using actual settlement outcomes.
- **Vol floor** (`MIN_ANNUALIZED_VOL=0.15`): Prevents unrealistically low vol estimates from producing overconfident posteriors.
- **Conviction filter** (`MIN_CONVICTION=0.08`): Rejects trades where the posterior is too close to 0.50 (low directional conviction).
- **Brier score comparison**: Per-asset old vs new Brier scores in replay report.

### v1.8.0 — Time-Aware Volatility & Raw Price Signal
*Tag: `v1.8.0`*

Two critical vol estimation fixes:
- **Time-aware EWMA**: Decay is now proportional to actual elapsed time between observations (`λ_eff = exp(-dt/τ)`) instead of treating irregular ticks as uniform. Variance stored per-second eliminates assumed tick interval.
- **Raw Binance signal**: Vol estimator now extracts the undampened Binance observation instead of the aggregated median (which was smoothed by slow sources like CoinGecko at 45s update rate).
- **Boundary scan dispatch**: Immediate market scan at 5-minute boundaries so new markets are discovered without waiting for the 30s timer.
- **Live-mode dashboard**: Separate port (8081) and log directory for live trading.
- **Live-mode risk overrides**: `[risk.live]` config section for separate paper/live risk limits.

### v1.5.0 — Simplified Baseline + OBI Infrastructure
*Tag: `v1.5.0`*

Config simplification back to Run 8 baseline (best performing):
- Reverted over-tuned per-asset multipliers, edge threshold, and EWMA overrides
- **OBI infrastructure** (P6): Orderbook imbalance signal plumbed through but disabled (`obi_weight=0.0`)
- Concurrent orderbook fetches to fix select loop starvation
- Scan timeout and Chainlink RPC timeout (3s each)
- Pending order lifecycle for paper mode

### v1.4.0 — Backtest & Analysis Improvements
*Tag: `v1.4.0`*

- **`--backtest` parameter sweep**: Replays logged pipeline evaluations with different configs
- **Brier decomposition** (P14): Reliability, resolution, uncertainty breakdown per asset
- **XLSX trade sheets**: Per-run analysis blocks with open/closed trade tracking
- **Pending order fill analysis**: Fill rate breakdown by asset and side in `--analyze`

### v1.1.0 — Post-Run Tuning
*Tag: `v1.1.0`*

- **Max net edge cap** (`max_net_edge=0.20`): Rejects suspiciously large edges (>20% had 33% WR)
- **NO-side edge multiplier** (`no_edge_threshold_mult=1.2`): NO side needs 20% more edge
- **Chainlink feeds for all 4 assets** (previously BTC-only)
- **ET date directories** for logs matching Polymarket market windows
- **Per-asset EWMA lambda** and edge threshold overrides
- **`--analyze` mode** with calibration curves, edge buckets, side/TTE breakdown

### v1.0.0 — Functionally Complete Baseline
*Tag: `v1.0.0` | Commit: `fe6b513`*

Initial release with all core systems:
- Trading loop: scan, evaluate, execute, settle pipeline
- Bayesian models: GBM (strike) and directional (up/down) probability estimation
- EWMA volatility with regime detection
- Kelly criterion with Brier-score uncertainty haircut
- Dynamic fee model with fee asymmetry detection
- 5 price feeds with median aggregation, quorum, anomaly rejection
- Dual venue support (International EIP-712, US EdDSA)
- Risk management: position limits, drawdown halt, cooldown, kill switch
- Paper mode with simulated fills
- Real-time browser dashboard (axum + Chart.js)
- JSONL structured logging

---

## Roadmap

| # | Feature | Status |
|---|---------|--------|
| P0 | Dynamic Fee Model | Done |
| P1a | Per-Asset Volatility | Done |
| P1b | EWMA Volatility | Done |
| P5 | Fee-Asymmetric Edge | Done |
| P3a | TTE Sweet Spot Filtering | Done |
| P6 | Orderbook Imbalance Signal | Done (infra, disabled) |
| P14 | Brier Decomposition | Done |
| P3b | TTE Vol Scaling | Planned |
| P4 | Cross-Asset Lead-Lag (BTC -> alts) | Planned |
| P2 | Implied Vol Calibration (IV-RV) | Planned |
| P7 | Regime-Aware Kelly Sizing | Planned |
| P8 | Ensemble Model Blending | Backlog |
| P9 | Probability Shrinkage | Rejected |
| P10 | Side Bias Adjustment | Backlog |
| P11 | Drawdown-Adaptive Sizing | Backlog |
| P12 | Slippage Model | Backlog |
| P13 | Correlation-Based Sizing | Backlog |

---

## Dependencies

**Core**: tokio, reqwest, axum, serde, serde_json, toml, rust_decimal
**Crypto/Signing**: alloy-signer, alloy-signer-local, ed25519-dalek, hmac, sha2, base64
**WebSocket**: tokio-tungstenite, futures-util
**Time**: chrono | **Regex**: regex | **Env**: dotenvy
**Observability**: tracing, tracing-subscriber, metrics, metrics-exporter-prometheus
**TLS**: rustls (ring provider) | **Error handling**: anyhow, thiserror
**Spreadsheet**: rust_xlsxwriter

---

## Testing

```bash
cargo test          # 279 unit tests
cargo bench         # LMSR + Bayesian benchmarks
```

## License

Private. All rights reserved.
