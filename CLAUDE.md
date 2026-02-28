# Project Memory — Polymarket BTC Bot

## Project Overview
Rust algorithmic trading bot for Polymarket crypto prediction markets (BTC, ETH, SOL, XRP). Uses Bayesian GBM probability estimation, Kelly criterion sizing, and multi-source price aggregation to trade 5-minute and 15-minute binary outcome markets.

**Repo**: `hzhong26/polymarket-btc-bot` on GitHub
**Language**: Rust (tokio async, rust_decimal for money)
**Current version**: v1.9.0

## Current Config State (v1.9.0)
All parameters are in `config.toml`. Key trading parameters:

| Parameter | Value | Notes |
|-----------|-------|-------|
| `ewma_lambda` | 0.97 | Global + per-asset (BTC, XRP, SOL). Smoother vol. |
| `min_annualized_vol` | 0.10 | Vol floor for GBM posteriors |
| `min_conviction` | 0.16 | Min |posterior - 0.50| to trade |
| `yes_edge_threshold_mult` | 999.0 | NO-only trading (YES disabled) |
| `no_edge_threshold_mult` | 1.20 | NO trades need 20% more edge than base |
| `edge_threshold` | 0.11 | Base 11% minimum net edge |
| `reversion_weight` | 0.05 | Mean reversion signal enabled |
| `reversion_lookback_secs` | 120.0 | 2-minute lookback window |
| `reversion_negative_mult` | 1.5 | Negative moves revert faster |
| `obi_weight` | 0.10 | Orderbook imbalance in log-odds space |
| `kelly_fraction` | 0.20 | Fractional Kelly sizing |
| `max_net_edge` | 0.20 | Cap on suspiciously large edges |

## Key Architecture Decisions

### NO-Only Trading
YES tokens are systematically overpriced due to retail optimism premium. YES had 34% WR and -$1,716 P&L across all data. Every top sweep config disabled YES. This is structural, not a parameter tuning artifact.

### Vol Regime Detection
Hardcoded in `src/trading_loop/pipeline.rs` (lines ~383-395):
- `ratio > 1.5` → expanding vol → threshold × 1.5
- `ratio < 0.7` → contracting vol → threshold × 0.9
Sweep confirmed these defaults are optimal.

### Mean Reversion Signal
`src/bayesian/mod.rs` applies mean reversion adjustment after GBM posterior:
- Negative z-score (price dropped) → increase P(above strike)
- Asymmetric: negative moves revert 1.5× faster
- Scaled by sqrt(time_remaining / lookback)

### OBI Signal
Applied in log-odds space in both live pipeline (`src/trading_loop/pipeline.rs` lines ~334-347) and replay (`src/replay.rs`).

## CLI Modes
- `--paper` — Paper trading with simulated fills
- `--replay logs/YYYY-MM-DD` — Replay with new vol estimator, compare old vs new P&L
- `--sweep logs/day1 logs/day2 ...` — Parameter grid search across multiple days
- `--analyze logs/YYYY-MM-DD` — Post-session analysis (calibration, Brier decomposition)
- `--backtest logs/YYYY-MM-DD` — Parameter sweep on single day
- `--runs logs/YYYY-MM-DD` — List run summaries

## Sweep Infrastructure
`src/replay.rs` contains the full sweep system:
- `ReplayConfig` struct with all tunable parameters
- `build_param_grid()` generates combinations (last run: 9,000 configs)
- `compute_sweep_metrics()` computes P&L, WR, Brier per config per day
- `print_sweep_report()` ranks configs by combined P&L, requires consistency (profitable on ALL days)
- Fixed params from earlier sweeps: lambda=0.97, min_vol=0.10, yes_mult=999, window=100

## Parameter Tuning History
See README.md "Parameter Tuning History" section for complete before/after tables with feedback for each version (v1.0.0 → v1.1.0 → v1.5.0 → v1.8.2 → v1.9.0).

## Important Files
| File | Purpose |
|------|---------|
| `config.toml` | All runtime configuration |
| `src/replay.rs` | Replay + sweep system (ReplayConfig, grid search) |
| `src/bayesian/mod.rs` | BayesianUpdater (GBM + mean reversion + vol) |
| `src/bayesian/volatility.rs` | EWMA vol estimator + regime ratio |
| `src/bayesian/momentum.rs` | MomentumTracker (z-score for mean reversion) |
| `src/trading_loop/pipeline.rs` | Core evaluate_market() with vol regime + OBI |
| `src/trading_loop/mod.rs` | Main trading loop orchestrator |
| `src/config/types.rs` | Config struct definitions + defaults |
| `src/main.rs` | CLI parsing, --sweep/--replay dispatch |

## Performance Baseline (v1.9.0 backtest)
- Feb 26: P&L +$821, WR 59.6%, 57 trades (all NO)
- Feb 27: P&L +$1,061, WR 68.6%, 70 trades (all NO)
- Combined: +$1,882 (vs old -$1,341)
- Caveat: only 2 days / 424 settlements

## Build & Test
```bash
cargo build --release
cargo test            # 297 tests (291 lib + 6 bin)
./target/release/polymarket-btc-bot --replay logs/2026-02-27
./target/release/polymarket-btc-bot --sweep logs/2026-02-26 logs/2026-02-27
```

## Log Structure
Logs are in `logs/YYYY-MM-DD/` (ET dates). JSONL format with pipeline evaluations, settlements, price observations. Replay and sweep read these logs to simulate different parameter configs.
