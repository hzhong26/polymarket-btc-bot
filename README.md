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
                          |   Gamma Scanner  |  (30s poll)
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
              | (browser) |  |  Logger  |  | Sheet CSV|
              +----------+  +----------+  +----------+
```

**Single binary crate** (`src/main.rs`) with library (`src/lib.rs`). Async runtime via Tokio. All monetary arithmetic uses `rust_decimal` for precision.

---

## Modules

| Module | Path | Purpose |
|--------|------|---------|
| **trading_loop** | `src/trading_loop/` | Core orchestrator: scan, evaluate, execute, settle |
| **bayesian** | `src/bayesian/` | Probability models (GBM, directional), EWMA volatility, calibration |
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
| **analyze** | `src/analyze.rs` | Post-session analysis: calibration curves, tuning recommendations |
| **timezone** | `src/timezone.rs` | Eastern Time conversion (Polymarket market windows use ET) |

---

## How It Works

### 1. Market Discovery
The **Gamma Scanner** polls Polymarket's Gamma API every 30 seconds for binary crypto markets (BTC/ETH/SOL/XRP) expiring within 5-15 minutes. Markets are filtered by liquidity, spread, and tag.

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

**Volatility** is estimated per-asset using EWMA (lambda=0.94 default; XRP/SOL override to 0.90 for faster reaction). Regime detection flags high-vol environments.

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
- YES-side multiplier (configurable, default 1.2x)
- Max net edge cap (configurable, rejects suspiciously large edges)
- Risk limits (position size, total exposure, drawdown, cooldown)

### 7. Execution & Settlement
Orders are placed via the CLOB API. Fills arrive over WebSocket. At market expiry, Polymarket resolves the outcome and P&L is locked in. The calibration tracker records the prediction vs. actual outcome for future uncertainty estimation.

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
| `kelly_fraction` | `0.12` | Fractional Kelly multiplier |
| `uncertainty_haircut_max` | `0.90` | Reject if model uncertainty > 90% |
| `latency_penalty` | `0.005` | 0.5% execution slippage hedge |
| `market_tags` | BTC, ETH, XRP, SOL | Asset universe |
| `market_duration_max_secs` | `900` | Max market window (15 min) |
| `fee_rate` / `fee_exponent` | `0.25` / `2` | Polymarket dynamic fee formula |
| `max_net_edge` | `0.20` | Reject edges above this cap |
| `yes_edge_threshold_mult` | `1.2` | YES trades need 20% more edge |
| `ewma_lambda` | `0.94` | EWMA decay factor (global default) |
| `vol_window_secs` | `300` | Volatility estimation window |
| `paper_bankroll_usdc` | `10000` | Simulated bankroll for paper mode |

### `[trading.asset_params.*]`
Per-asset overrides for edge threshold multiplier, uncertainty multiplier, and EWMA lambda.

### `[risk]`
| Parameter | Default | Description |
|-----------|---------|-------------|
| `max_position_usdc` | `500` | Max single position |
| `max_total_exposure` | `2000` | Max total deployed capital |
| `max_drawdown_pct` | `0.50` | Halt at 50% drawdown |
| `cooldown_after_loss_ms` | `60000` | 60s pause after consecutive losses |
| `max_consecutive_losses` | `3` | Trigger cooldown after 3 losses |
| `max_open_orders` | `10` | Max concurrent orders |

### `[logging]`
Console log level/format, Prometheus metrics port, dashboard port, and JSONL file logging settings (directory, flush interval, no-trade sampling rate, gzip compression).

---

## CLI Usage

```bash
# Live trading
polymarket-btc-bot [config.toml]

# Paper trading (simulated, no real orders)
polymarket-btc-bot [config.toml] --paper

# Disable JSONL logging
polymarket-btc-bot [config.toml] --no-log

# Post-session analysis (calibration curves, tuning recommendations)
polymarket-btc-bot --analyze [log-dirs...]

# List run summaries
polymarket-btc-bot --runs [log-dirs...]

# Filter analysis by condition or tag
polymarket-btc-bot --analyze logs/ --run bitcoin
```

---

## Dashboard

A real-time browser dashboard is served on `http://localhost:8080` (configurable).

**Features**:
- Live P&L chart (Chart.js) with timeframe filters (5m / 15m / 1h / 6h / 1d)
- Open positions with mark-to-market updates
- Trade history (opened/closed) with click-to-expand diagnostics
- WebSocket protocol for instant updates

**Protocol**:
- On connect: full state snapshot (`init` message with all snaps, opened, closed)
- Live: `snap` messages with P&L fields + position cards on every evaluation tick
- Trade events: `opened` and `closed` messages with full diagnostics

---

## Versioning

### v1.0.0 — Functionally Complete Baseline
*Tag: `v1.0.0` | Commit: `fe6b513`*

The initial release with all core systems operational:

- **Trading Loop**: Full scan → evaluate → execute → settle pipeline
- **Bayesian Models**: GBM (strike markets) and directional (up/down markets) probability estimation
- **EWMA Volatility**: Per-asset exponentially weighted moving average with regime detection
- **Kelly Criterion**: Fractional Kelly sizing with Brier-score uncertainty haircut
- **Dynamic Fee Model**: Polymarket's quadratic fee formula (`fee_rate * (p*(1-p))^exponent`)
- **Fee Asymmetry Detection**: Picks the side with better net edge after fees, not just best gross edge
- **5 Price Feeds**: Chainlink (on-chain), Binance (WebSocket), Coinbase, CoinGecko, CryptoCompare
- **Price Aggregator**: Median with quorum, anomaly rejection, confidence scoring
- **Dual Venue Support**: International (EIP-712 on Polygon) and US (EdDSA)
- **Risk Management**: Position limits, exposure caps, drawdown halt, consecutive-loss cooldown, kill switch
- **Paper Mode**: Full simulation with in-memory fills and self-resolution
- **Dashboard**: Real-time browser dashboard (axum + Chart.js) with WebSocket updates
- **JSONL Logging**: Structured event logs with sampled no-trade events, gzip on roll
- **Trade Sheet**: CSV trade log with ET date directories
- **Post-Session Analysis**: `--analyze` mode with calibration curves, Brier scores, and tuning recommendations
- **Auto-Calibration**: Rolling Brier score per asset feeds into Kelly uncertainty haircut at runtime
- **Per-Asset Tuning**: Configurable edge threshold multipliers, uncertainty multipliers, EWMA lambda overrides per asset (BTC, ETH, SOL, XRP)
- **Confidence Shrinkage**: Model predictions blended toward market-implied price when feed confidence is low

### v1.1.0 — Post-Run Tuning & Multi-Asset Improvements
*Tag: `v1.1.0`*

Updates driven by analysis of the v1.0.0 run (131 settled trades, Feb 24-25):

**Tuning knobs from run analysis:**
- **Max net edge cap** (`max_net_edge = 0.20`): Rejects trades with suspiciously large edges. Data showed edge >0.20 had only 33% win rate and lost money — likely model overconfidence or stale data rather than real opportunity.
- **YES-side edge multiplier** (`yes_edge_threshold_mult = 1.2`): YES trades must clear 20% higher threshold. Run data: YES = 40.5% win rate / -$16 avg P&L vs NO = 63.8% win rate / +$82 avg P&L.

**Multi-asset & infrastructure:**
- **Chainlink feeds for all 4 assets**: Previously BTC-only; now BTC, ETH, SOL, XRP all have on-chain oracle feeds on Polygon
- **ET date directories for CSV logs**: Log directories now use Eastern Time dates (matching Polymarket market windows) instead of UTC, preventing split directories around midnight
- **Paper-mode self-resolution**: Markets resolve immediately at expiry in paper mode without waiting for Gamma API polling (eliminates 10+ min settlement delays)
- **Dashboard snap buffer increase**: Buffer increased to 20K+ snapshots with client-side downsampling for smoother charting
- **Dashboard snap throttle in paper mode**: Reduces WebSocket message frequency during high-speed paper simulation
- **Per-asset EWMA lambda overrides**: XRP and SOL use lambda=0.90 (faster reaction) vs global default 0.94
- **Per-asset edge threshold multipliers**: XRP at 1.5x, SOL at 2.5x base threshold (noisier assets need more edge)
- **Trade sheet CSV logging**: Run-level headers, open/closed trade tracking, condition-level deduplication
- **Analyze module** (`--analyze`): Post-session analysis with edge buckets, side breakdown, TTE analysis, and tuning recommendations

---

## Roadmap

| # | Feature | Status |
|---|---------|--------|
| P0 | Dynamic Fee Model | Done |
| P1a | Per-Asset Volatility | Done |
| P1b | EWMA Volatility | Done |
| P5 | Fee-Asymmetric Edge | Done |
| P2 | Implied Vol Calibration (IV-RV) | Planned |
| P3a | TTE Sweet Spot Filtering | Planned |
| P3b | TTE Vol Scaling | Planned |
| P4 | Cross-Asset Lead-Lag (BTC -> alts) | Planned |
| P6 | Orderbook Imbalance Signal | Planned |
| P7 | Regime-Aware Kelly Sizing | Planned |
| P8 | Ensemble Model Blending | Planned |

---

## Dependencies

**Core**: tokio, reqwest, axum, serde, serde_json, toml, rust_decimal
**Crypto/Signing**: alloy-signer, alloy-signer-local, ed25519-dalek, hmac, sha2, base64
**WebSocket**: tokio-tungstenite, futures-util
**Time**: chrono | **Regex**: regex | **Env**: dotenvy
**Observability**: tracing, tracing-subscriber, metrics, metrics-exporter-prometheus
**TLS**: rustls (ring provider) | **Error handling**: anyhow, thiserror

---

## Testing

```bash
cargo test          # 265 unit tests
cargo bench         # LMSR + Bayesian benchmarks
```

## License

Private. All rights reserved.
