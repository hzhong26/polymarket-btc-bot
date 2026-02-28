use std::collections::HashMap;
use std::path::PathBuf;

use anyhow::{bail, Context};
use chrono::{DateTime, Utc};

use crate::analyze::{self, SettledTrade};
use crate::bayesian::{models, momentum::MomentumTracker, volatility::VolatilityEstimator};

// ── Replay config ────────────────────────────────────────────────

/// All tunable parameters for the replay pipeline.
#[derive(Debug, Clone)]
pub struct ReplayConfig {
    // Volatility estimator
    pub ewma_lambda: f64,
    pub reference_interval_secs: f64,
    pub vol_window_obs: usize,
    pub default_volatility: f64,
    pub high_vol_threshold: f64,
    pub min_annualized_vol: f64,
    // Mean reversion
    pub reversion_lookback_secs: f64,
    pub reversion_weight: f64,
    pub reversion_negative_mult: f64,
    // Trade selection
    pub min_conviction: f64,
    pub yes_edge_threshold_mult: f64,
    pub no_edge_threshold_mult: f64,
    /// Scales the logged effective_threshold (simulates different base edge_threshold).
    /// 1.0 = use as logged, 0.8 = 20% lower threshold, 1.2 = 20% higher.
    pub threshold_scale: f64,
    // Vol regime
    pub vol_regime_short_window_secs: f64,
    pub vol_regime_expand_threshold: f64,
    pub vol_regime_expand_mult: f64,
    pub vol_regime_contract_threshold: f64,
    pub vol_regime_contract_mult: f64,
    // OBI signal
    pub obi_weight: f64,
}

impl Default for ReplayConfig {
    fn default() -> Self {
        Self {
            // v1.9.0 sweep-optimized defaults (match config.toml)
            ewma_lambda: 0.97,
            reference_interval_secs: 3.0,
            vol_window_obs: 100,
            default_volatility: 0.80,
            high_vol_threshold: 1.0,
            min_annualized_vol: 0.10,
            reversion_lookback_secs: 120.0,
            reversion_weight: 0.05,
            reversion_negative_mult: 1.5,
            min_conviction: 0.16,
            yes_edge_threshold_mult: 999.0,
            no_edge_threshold_mult: 1.20,
            threshold_scale: 1.0,
            vol_regime_short_window_secs: 300.0,
            vol_regime_expand_threshold: 1.5,
            vol_regime_expand_mult: 1.5,
            vol_regime_contract_threshold: 0.7,
            vol_regime_contract_mult: 0.9,
            obi_weight: 0.10,
        }
    }
}

/// Summary metrics from a single replay run.
#[derive(Debug, Clone)]
pub struct SweepMetrics {
    pub total_trades: usize,
    pub yes_trades: usize,
    pub no_trades: usize,
    pub wins: usize,
    pub yes_wins: usize,
    pub no_wins: usize,
    pub total_pnl: f64,
    pub yes_pnl: f64,
    pub no_pnl: f64,
    pub brier_same_set: f64,
    pub brier_n: usize,
}

// ── Data structures ──────────────────────────────────────────────

/// Price observation extracted from a logged event's observations array.
#[derive(Debug, Clone)]
struct PriceObs {
    asset: String,
    source: String,
    price: f64,
    timestamp_secs: f64,
}

/// A pipeline evaluation with all fields needed for replay.
#[derive(Debug, Clone)]
struct ReplayEval {
    condition_id: String,
    asset: String,
    timestamp: DateTime<Utc>,
    // Model inputs
    model_type: String,
    spot: f64,
    strike: f64,
    reference_price: Option<f64>,
    time_to_expiry_secs: f64,
    confidence: f64,
    market_implied_p: f64,
    // Orderbook
    best_bid: f64,
    best_ask: f64,
    // OBI
    obi_raw: Option<f64>,
    // Costs (reuse from logs)
    cost_total: f64,
    // Thresholds
    effective_threshold: f64,
    // Original results (for comparison)
    original_vol: f64,
    original_posterior: f64,
    original_net_edge: Option<f64>,
    original_action: String,
    original_side: Option<String>,
    original_stake_usdc: Option<f64>,
    // Price observations from this event (for vol feeding)
    observations: Vec<PriceObs>,
}

/// Result of replaying a single evaluation with new vol.
#[derive(Debug)]
struct ReplayResult {
    condition_id: String,
    asset: String,
    // Old
    old_vol: f64,
    old_posterior: f64,
    old_action: String,
    old_side: Option<String>,
    old_net_edge: Option<f64>,
    old_stake: Option<f64>,
    // New
    new_vol: f64,
    new_model_posterior: f64,
    new_adjusted_posterior: f64,
    new_side: String,
    new_gross_edge: f64,
    new_net_edge: f64,
    new_action: String,
    // For P&L
    best_bid: f64,
    best_ask: f64,
}

// ── Entry point ──────────────────────────────────────────────────

pub fn run(dirs: &[String], run_filter: Option<&str>) -> anyhow::Result<()> {
    let cfg = ReplayConfig::default();
    let (evals, settled) = load_replay_data(dirs, run_filter)?;

    println!("=== REPLAY MODE: Vol Fix Impact Analysis ===\n");
    println!(
        "Config: λ={}, ref_dt={}s, window={}, default_vol={}",
        cfg.ewma_lambda, cfg.reference_interval_secs, cfg.vol_window_obs, cfg.default_volatility
    );
    println!("Parsed {} evaluations, {} settlements\n", evals.len(), settled.len());

    let results = replay_pipeline(&evals, &cfg);
    print_replay_report(&results, &settled);

    Ok(())
}

/// Run a parameter sweep across all combinations on the given log dirs.
pub fn sweep(all_dirs: &[Vec<String>]) -> anyhow::Result<()> {
    // Load data for each day
    let mut datasets: Vec<(String, Vec<ReplayEval>, HashMap<String, SettledTrade>)> = Vec::new();
    for dirs in all_dirs {
        let label = dirs.last().cloned().unwrap_or_else(|| "unknown".into());
        let (evals, settled) = load_replay_data(dirs, None)?;
        println!("  {}: {} evals, {} settlements", label, evals.len(), settled.len());
        if evals.is_empty() {
            bail!("No evaluations found in {label}");
        }
        datasets.push((label, evals, settled));
    }

    // Build parameter grid
    let grid = build_param_grid();
    println!("\n=== PARAMETER SWEEP: {} configs × {} days ===\n", grid.len(), datasets.len());

    // Run all combinations
    let mut all_results: Vec<(ReplayConfig, Vec<SweepMetrics>)> = Vec::new();
    for cfg in &grid {
        let mut day_metrics = Vec::new();
        for (_label, evals, settled) in &datasets {
            let results = replay_pipeline(evals, cfg);
            let metrics = compute_sweep_metrics(&results, settled);
            day_metrics.push(metrics);
        }
        all_results.push((cfg.clone(), day_metrics));
    }

    // Print results
    print_sweep_report(&all_results, &datasets);

    Ok(())
}

/// Load and parse replay data from log directories.
fn load_replay_data(
    dirs: &[String],
    run_filter: Option<&str>,
) -> anyhow::Result<(Vec<ReplayEval>, HashMap<String, SettledTrade>)> {
    let log_dirs = analyze::resolve_log_dirs(dirs)?;
    let files = analyze::find_event_files(&log_dirs)?;
    if files.is_empty() {
        bail!("No event files found in the specified directories");
    }

    let resolved_filter = if let Some(selector) = run_filter {
        let runs = analyze::discover_runs(&files)?;
        let resolved = analyze::resolve_run_id(&runs, selector)?;
        Some(resolved)
    } else {
        None
    };

    let (evals, settled) = parse_replay_data(&files, resolved_filter.as_deref())?;
    if evals.is_empty() {
        bail!("No pipeline evaluations found");
    }
    Ok((evals, settled))
}

// ── Parsing ──────────────────────────────────────────────────────

fn parse_replay_data(
    files: &[PathBuf],
    run_id_filter: Option<&str>,
) -> anyhow::Result<(Vec<ReplayEval>, HashMap<String, SettledTrade>)> {
    let mut evals: Vec<ReplayEval> = Vec::new();
    let mut settled: HashMap<String, SettledTrade> = HashMap::new();
    // Dedup: keep last trade_candidate per condition_id, or last no_trade
    let mut seen_trades: HashMap<String, usize> = HashMap::new();

    for file in files {
        let content = std::fs::read_to_string(file)
            .with_context(|| format!("Failed to read {}", file.display()))?;

        for line in content.lines() {
            if line.is_empty() {
                continue;
            }

            // Fast run_id filter
            if let Some(filter) = run_id_filter {
                match analyze::extract_json_str(line, "run_id") {
                    Some(ref id) if id == filter => {}
                    _ => continue,
                }
            }

            if !line.contains("position_settled") && !line.contains("pipeline_evaluation") {
                continue;
            }

            let Ok(val) = serde_json::from_str::<serde_json::Value>(line) else {
                continue;
            };
            let Some(event_type) = val.get("event_type").and_then(|v| v.as_str()) else {
                continue;
            };

            match event_type {
                "position_settled" => {
                    if let Some(trade) = analyze::parse_settled(&val) {
                        settled.insert(trade.condition_id.clone(), trade);
                    }
                }
                "pipeline_evaluation" => {
                    if let Some(eval) = parse_replay_eval(&val) {
                        let cid = eval.condition_id.clone();
                        let is_trade = eval.original_action == "trade_candidate";

                        if let Some(&idx) = seen_trades.get(&cid) {
                            if is_trade {
                                evals[idx] = eval;
                            }
                            continue;
                        }

                        if is_trade {
                            seen_trades.insert(cid, evals.len());
                        }
                        evals.push(eval);
                    }
                }
                _ => {}
            }
        }
    }

    // Sort by timestamp for chronological vol feeding
    evals.sort_by_key(|e| e.timestamp);

    Ok((evals, settled))
}

fn parse_replay_eval(val: &serde_json::Value) -> Option<ReplayEval> {
    let p = val.get("payload")?;
    let market = p.get("market")?;
    let mc = p.get("model_context")?;
    let ec = p.get("execution_context")?;
    let cc = p.get("cost_context")?;
    let src = p.get("sizing_risk_context")?;
    let action = p.get("action")?;

    let condition_id = market.get("condition_id")?.as_str()?.to_string();
    let asset = market.get("asset")?.as_str()?.to_string();

    // Parse timestamp from the event (use price observation timestamps or event timestamp)
    let timestamp = val
        .get("timestamp")
        .and_then(|t| t.as_str())
        .and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok())
        .map(|dt| dt.with_timezone(&Utc))
        .unwrap_or_else(Utc::now);

    let model_type = mc.get("model_type")?.as_str()?.to_string();
    let spot = p
        .get("price_context")
        .and_then(|pc| pc.get("aggregated_spot"))
        .and_then(|v| v.as_f64())?;
    let strike = mc.get("strike_price").and_then(|v| v.as_f64()).unwrap_or(0.0);
    let reference_price = mc.get("reference_price").and_then(|v| v.as_f64());
    let time_to_expiry_secs = mc.get("time_to_expiry_secs").and_then(|v| v.as_f64())?;
    let confidence = p
        .get("price_context")
        .and_then(|pc| pc.get("confidence"))
        .and_then(|v| v.as_f64())
        .unwrap_or(0.5);
    let market_implied_p = mc.get("market_implied_p").and_then(|v| v.as_f64())?;

    let book = ec.get("orderbook")?;
    let best_bid = book.get("best_bid").and_then(|v| v.as_f64()).unwrap_or(0.0);
    let best_ask = book.get("best_ask").and_then(|v| v.as_f64()).unwrap_or(1.0);

    let obi_raw = mc.get("obi_raw").and_then(|v| v.as_f64());
    let cost_total = cc.get("total_cost").and_then(|v| v.as_f64()).unwrap_or(0.02);
    let effective_threshold = cc.get("effective_threshold").and_then(|v| v.as_f64())?;

    let original_vol = mc
        .get("model_volatility")
        .and_then(|v| v.as_f64())
        .unwrap_or(0.80);
    let original_posterior = mc.get("posterior").and_then(|v| v.as_f64())?;
    let original_net_edge = cc.get("net_edge").and_then(|v| v.as_f64());
    let original_action = action.get("kind")?.as_str()?.to_string();
    let original_side = action.get("side").and_then(|v| v.as_str()).map(String::from);
    let original_stake_usdc = src.get("stake_usdc").and_then(|v| v.as_f64());

    // Extract price observations from this event
    let observations = parse_price_obs(p, &asset);

    Some(ReplayEval {
        condition_id,
        asset,
        timestamp,
        model_type,
        spot,
        strike,
        reference_price,
        time_to_expiry_secs,
        confidence,
        market_implied_p,
        best_bid,
        best_ask,
        obi_raw,
        cost_total,
        effective_threshold,
        original_vol,
        original_posterior,
        original_net_edge,
        original_action,
        original_side,
        original_stake_usdc,
        observations,
    })
}

/// Extract price observations from a pipeline evaluation payload.
/// Prefers Binance source (matching live behavior), but includes all.
fn parse_price_obs(payload: &serde_json::Value, asset: &str) -> Vec<PriceObs> {
    let Some(obs_arr) = payload
        .get("price_context")
        .and_then(|pc| pc.get("observations"))
        .and_then(|v| v.as_array())
    else {
        return Vec::new();
    };

    obs_arr
        .iter()
        .filter_map(|obs| {
            let source = obs.get("source")?.as_str()?.to_string();
            let price = obs.get("price")?.as_f64()?;
            let ts_str = obs.get("timestamp")?.as_str()?;
            let ts = chrono::DateTime::parse_from_rfc3339(ts_str)
                .ok()?
                .with_timezone(&Utc);
            let timestamp_secs =
                ts.timestamp() as f64 + ts.timestamp_subsec_millis() as f64 / 1000.0;

            if price <= 0.0 {
                return None;
            }

            Some(PriceObs {
                asset: asset.to_string(),
                source,
                price,
                timestamp_secs,
            })
        })
        .collect()
}

// ── Core replay logic ────────────────────────────────────────────

fn replay_pipeline(evals: &[ReplayEval], cfg: &ReplayConfig) -> Vec<ReplayResult> {
    let mut vol_estimators: HashMap<String, VolatilityEstimator> = HashMap::new();
    let mut momentum_trackers: HashMap<String, MomentumTracker> = HashMap::new();
    let mut results = Vec::with_capacity(evals.len());

    for eval in evals {
        // Get or create vol estimator for this asset
        let estimator = vol_estimators
            .entry(eval.asset.clone())
            .or_insert_with(|| {
                VolatilityEstimator::new(
                    cfg.vol_window_obs,
                    cfg.high_vol_threshold,
                    cfg.ewma_lambda,
                    cfg.reference_interval_secs,
                )
            });
        let momentum = momentum_trackers
            .entry(eval.asset.clone())
            .or_insert_with(MomentumTracker::new);

        // Feed observations (prefer Binance, fallback to any source)
        let binance_obs = eval.observations.iter().find(|o| o.source == "binance");
        let obs_to_feed = binance_obs.or_else(|| {
            eval.observations
                .iter()
                .max_by(|a, b| a.timestamp_secs.partial_cmp(&b.timestamp_secs).unwrap())
        });

        if let Some(obs) = obs_to_feed {
            estimator.add_price(obs.price, obs.timestamp_secs);
            momentum.add_price(obs.price, obs.timestamp_secs);
        }

        // Snapshot new vol (with floor)
        let new_vol = estimator
            .annualized_volatility()
            .unwrap_or(cfg.default_volatility)
            .max(cfg.min_annualized_vol);

        // Recompute model probability
        let mut new_model_p = if eval.model_type == "updown_directional" {
            if let Some(ref_price) = eval.reference_price {
                models::directional_up_probability(
                    eval.spot,
                    ref_price,
                    eval.time_to_expiry_secs,
                    new_vol,
                    0.0, // no drift in replay
                )
            } else {
                // Fall back to original if no reference price
                eval.original_posterior
            }
        } else {
            // strike_gbm
            models::binary_call_probability(
                eval.spot,
                eval.strike,
                eval.time_to_expiry_secs,
                new_vol,
            )
        };

        // Mean reversion adjustment (same logic as BayesianUpdater::update_with_details)
        if cfg.reversion_lookback_secs > 0.0 && cfg.reversion_weight > 0.0 {
            if let Some(z) = momentum.z_score(cfg.reversion_lookback_secs) {
                let time_scale = (eval.time_to_expiry_secs / cfg.reversion_lookback_secs)
                    .sqrt()
                    .min(1.0);
                let mut adjustment = -z * cfg.reversion_weight * time_scale;
                if z < 0.0 {
                    adjustment *= cfg.reversion_negative_mult;
                }
                new_model_p = (new_model_p + adjustment).clamp(0.001, 0.999);
            }
        }

        // OBI adjustment (log-odds space)
        if cfg.obi_weight.abs() > 1e-9 {
            if let Some(obi) = eval.obi_raw {
                // Convert to log-odds, adjust, convert back
                let clamped = new_model_p.clamp(0.001, 0.999);
                let log_odds = (clamped / (1.0 - clamped)).ln();
                let adjusted_log_odds = log_odds + cfg.obi_weight * obi;
                new_model_p = (1.0 / (1.0 + (-adjusted_log_odds).exp())).clamp(0.001, 0.999);
            }
        }

        // Confidence weighting (same as live pipeline)
        let new_adjusted = (eval.confidence * new_model_p
            + (1.0 - eval.confidence) * eval.market_implied_p)
            .clamp(0.001, 0.999);

        // Compute edge for both sides
        let yes_gross = new_adjusted - eval.best_ask;
        let no_gross = eval.best_bid - new_adjusted;

        let (new_side, new_gross_edge) = if yes_gross >= no_gross {
            ("BUY YES".to_string(), yes_gross)
        } else {
            ("BUY NO".to_string(), no_gross)
        };

        let new_net_edge = new_gross_edge - eval.cost_total;

        // Base threshold with scale factor
        let mut effective_threshold = eval.effective_threshold * cfg.threshold_scale;

        // Vol regime adjustment
        if let Some(ratio) = estimator.vol_regime_ratio(cfg.vol_regime_short_window_secs) {
            if ratio > cfg.vol_regime_expand_threshold {
                effective_threshold *= cfg.vol_regime_expand_mult;
            } else if ratio < cfg.vol_regime_contract_threshold {
                effective_threshold *= cfg.vol_regime_contract_mult;
            }
        }

        // Side-specific multipliers
        if new_side == "BUY YES" {
            effective_threshold *= cfg.yes_edge_threshold_mult;
        } else {
            effective_threshold *= cfg.no_edge_threshold_mult;
        }

        // Conviction floor + threshold gate
        let conviction = (new_adjusted - 0.5).abs();
        let new_action = if cfg.min_conviction > 0.0 && conviction < cfg.min_conviction {
            "no_trade".to_string()
        } else if new_net_edge >= effective_threshold {
            "trade_candidate".to_string()
        } else {
            "no_trade".to_string()
        };

        results.push(ReplayResult {
            condition_id: eval.condition_id.clone(),
            asset: eval.asset.clone(),
            old_vol: eval.original_vol,
            old_posterior: eval.original_posterior,
            old_action: eval.original_action.clone(),
            old_side: eval.original_side.clone(),
            old_net_edge: eval.original_net_edge,
            old_stake: eval.original_stake_usdc,
            new_vol,
            new_model_posterior: new_model_p,
            new_adjusted_posterior: new_adjusted,
            new_side,
            new_gross_edge,
            new_net_edge,
            new_action,
            best_bid: eval.best_bid,
            best_ask: eval.best_ask,
        });
    }

    results
}

// ── Report ───────────────────────────────────────────────────────

fn print_replay_report(results: &[ReplayResult], settled: &HashMap<String, SettledTrade>) {
    if results.is_empty() {
        println!("No results to report.");
        return;
    }

    // ── Vol comparison ───────────────────────────────────────────
    println!("=== Volatility Comparison ===\n");
    println!(
        "  {:<6} {:>10} {:>10} {:>8}",
        "Asset", "OLD mean", "NEW mean", "Ratio"
    );
    println!("  {}", "-".repeat(38));

    let assets = ["BTC", "ETH", "SOL", "XRP"];
    for asset in &assets {
        let asset_results: Vec<_> = results.iter().filter(|r| r.asset == *asset).collect();
        if asset_results.is_empty() {
            continue;
        }
        let n = asset_results.len() as f64;
        let old_mean: f64 = asset_results.iter().map(|r| r.old_vol).sum::<f64>() / n;
        let new_mean: f64 = asset_results.iter().map(|r| r.new_vol).sum::<f64>() / n;
        let ratio = if old_mean > 0.0 {
            new_mean / old_mean
        } else {
            f64::NAN
        };
        println!(
            "  {:<6} {:>9.1}% {:>9.1}% {:>7.1}x",
            asset,
            old_mean * 100.0,
            new_mean * 100.0,
            ratio
        );
    }

    // ── Posterior comparison ──────────────────────────────────────
    println!("\n=== Posterior Comparison ===\n");
    println!(
        "  {:<6} {:>10} {:>10} {:>12}",
        "Asset", "OLD mean", "NEW mean", "Δ to 0.50"
    );
    println!("  {}", "-".repeat(42));

    for asset in &assets {
        let asset_results: Vec<_> = results.iter().filter(|r| r.asset == *asset).collect();
        if asset_results.is_empty() {
            continue;
        }
        let n = asset_results.len() as f64;
        let old_mean: f64 = asset_results.iter().map(|r| r.old_posterior).sum::<f64>() / n;
        let new_mean: f64 = asset_results
            .iter()
            .map(|r| r.new_adjusted_posterior)
            .sum::<f64>()
            / n;
        let old_dist = (old_mean - 0.5).abs();
        let new_dist = (new_mean - 0.5).abs();
        let delta = old_dist - new_dist; // positive = closer to 0.50
        let arrow = if delta > 0.005 { "✓" } else { "" };
        println!(
            "  {:<6} {:>9.3} {:>9.3} {:>+10.3} {}",
            asset, old_mean, new_mean, delta, arrow
        );
    }

    // ── Decision changes ─────────────────────────────────────────
    println!("\n=== Decision Changes ===\n");

    let total = results.len();
    let mut unchanged = 0usize;
    let mut trade_to_notrade = 0usize;
    let mut notrade_to_trade = 0usize;
    let mut side_flipped = 0usize;

    for r in results {
        if r.old_action == r.new_action {
            if r.old_action == "trade_candidate" {
                let old_side = r.old_side.as_deref().unwrap_or("");
                if old_side != r.new_side {
                    side_flipped += 1;
                } else {
                    unchanged += 1;
                }
            } else {
                unchanged += 1;
            }
        } else if r.old_action == "trade_candidate" && r.new_action == "no_trade" {
            trade_to_notrade += 1;
        } else if r.old_action == "no_trade" && r.new_action == "trade_candidate" {
            notrade_to_trade += 1;
        } else {
            unchanged += 1;
        }
    }

    println!("  Total evaluations:   {:>6}", total);
    println!(
        "  Unchanged:           {:>6}  ({:.1}%)",
        unchanged,
        unchanged as f64 / total as f64 * 100.0
    );
    println!(
        "  trade→no_trade:      {:>6}  ({:.1}%)  ← higher vol, less confident",
        trade_to_notrade,
        trade_to_notrade as f64 / total as f64 * 100.0
    );
    println!(
        "  no_trade→trade:      {:>6}  ({:.1}%)",
        notrade_to_trade,
        notrade_to_trade as f64 / total as f64 * 100.0
    );
    println!(
        "  Side flipped:        {:>6}  ({:.1}%)",
        side_flipped,
        side_flipped as f64 / total as f64 * 100.0
    );

    // ── Brier Score Comparison ─────────────────────────────────
    println!("\n=== Brier Score Comparison ===\n");

    let calc_brier = |pairs: &[(f64, bool)]| -> f64 {
        if pairs.is_empty() {
            return 0.0;
        }
        let sum: f64 = pairs
            .iter()
            .map(|(p, actual)| {
                let outcome = if *actual { 1.0 } else { 0.0 };
                (p - outcome).powi(2)
            })
            .sum();
        sum / pairs.len() as f64
    };

    // Dedup: keep last evaluation per condition_id (regardless of trade/no_trade decision).
    // This gives us the old and new posteriors for every market, on the same set.
    let mut last_by_cid: HashMap<&str, &ReplayResult> = HashMap::new();
    for r in results {
        last_by_cid.insert(&r.condition_id, r);
    }

    // Build trade-decision maps for P&L section (trade_candidates only)
    let mut old_tc_by_cid: HashMap<&str, &ReplayResult> = HashMap::new();
    let mut new_tc_by_cid: HashMap<&str, &ReplayResult> = HashMap::new();
    for r in results {
        if r.old_action == "trade_candidate" {
            old_tc_by_cid.insert(&r.condition_id, r);
        }
        if r.new_action == "trade_candidate" {
            new_tc_by_cid.insert(&r.condition_id, r);
        }
    }

    // 1) ALL settled markets — apples-to-apples on the same set of markets.
    let mut all_old_pairs: Vec<(f64, bool)> = Vec::new();
    let mut all_new_pairs: Vec<(f64, bool)> = Vec::new();
    let mut all_old_by_asset: HashMap<String, Vec<(f64, bool)>> = HashMap::new();
    let mut all_new_by_asset: HashMap<String, Vec<(f64, bool)>> = HashMap::new();

    for (cid, r) in &last_by_cid {
        if let Some(st) = settled.get(*cid) {
            all_old_pairs.push((r.old_posterior, st.actual_yes_outcome));
            all_new_pairs.push((r.new_adjusted_posterior, st.actual_yes_outcome));
            all_old_by_asset
                .entry(r.asset.clone())
                .or_default()
                .push((r.old_posterior, st.actual_yes_outcome));
            all_new_by_asset
                .entry(r.asset.clone())
                .or_default()
                .push((r.new_adjusted_posterior, st.actual_yes_outcome));
        }
    }

    // 2) Matched markets — only markets BOTH pipelines would have traded.
    let mut matched_old_pairs: Vec<(f64, bool)> = Vec::new();
    let mut matched_new_pairs: Vec<(f64, bool)> = Vec::new();

    for (cid, old_r) in &old_tc_by_cid {
        if let Some(new_r) = new_tc_by_cid.get(cid) {
            if let Some(st) = settled.get(*cid) {
                matched_old_pairs.push((old_r.old_posterior, st.actual_yes_outcome));
                matched_new_pairs.push((new_r.new_adjusted_posterior, st.actual_yes_outcome));
            }
        }
    }

    let print_brier_row = |label: &str, old_b: f64, old_n: usize, new_b: f64, new_n: usize| {
        let delta = new_b - old_b;
        let arrow = if delta < -0.005 {
            " better"
        } else if delta > 0.005 {
            " worse"
        } else {
            ""
        };
        println!(
            "  {:<22} {:>6.4} (N={:<4}) {:>6.4} (N={:<4}) {:>+7.4}{}",
            label, old_b, old_n, new_b, new_n, delta, arrow
        );
    };

    println!(
        "  {:<22} {:>13} {:>13} {:>8}",
        "", "OLD", "NEW", "Δ"
    );
    println!("  {}", "-".repeat(60));

    let all_old_b = calc_brier(&all_old_pairs);
    let all_new_b = calc_brier(&all_new_pairs);
    print_brier_row(
        "All settled (same set)",
        all_old_b,
        all_old_pairs.len(),
        all_new_b,
        all_new_pairs.len(),
    );

    let matched_old_b = calc_brier(&matched_old_pairs);
    let matched_new_b = calc_brier(&matched_new_pairs);
    print_brier_row(
        "Matched trades only",
        matched_old_b,
        matched_old_pairs.len(),
        matched_new_b,
        matched_new_pairs.len(),
    );

    println!("  (0.25 = random, lower = better)");

    // Per-asset breakdown on all settled (same set)
    let assets = ["BTC", "ETH", "SOL", "XRP"];
    println!(
        "\n  Per-asset (all settled, same set):"
    );
    println!(
        "  {:<6} {:>10} {:>6} {:>10} {:>6} {:>8}",
        "Asset", "OLD Brier", "N", "NEW Brier", "N", "Δ"
    );
    println!("  {}", "-".repeat(52));
    for asset in &assets {
        let ob = all_old_by_asset
            .get(*asset)
            .map(|v| (calc_brier(v), v.len()))
            .unwrap_or((0.0, 0));
        let nb = all_new_by_asset
            .get(*asset)
            .map(|v| (calc_brier(v), v.len()))
            .unwrap_or((0.0, 0));
        if ob.1 > 0 || nb.1 > 0 {
            let d = nb.0 - ob.0;
            println!(
                "  {:<6} {:>10.4} {:>6} {:>10.4} {:>6} {:>+8.4}",
                asset, ob.0, ob.1, nb.0, nb.1, d
            );
        }
    }

    // ── Hypothetical P&L ─────────────────────────────────────────
    println!("\n=== Hypothetical P&L (settled markets) ===\n");
    println!(
        "  {:<6} {:>8} {:>8} {:>10} {:>10} {:>8}",
        "Asset", "OLD N", "NEW N", "OLD P&L", "NEW P&L", "Δ"
    );
    println!("  {}", "-".repeat(58));

    // Build per-condition_id best results (deduplicated)
    let mut old_by_cid: HashMap<String, &ReplayResult> = HashMap::new();
    let mut new_by_cid: HashMap<String, &ReplayResult> = HashMap::new();
    for r in results {
        if r.old_action == "trade_candidate" {
            old_by_cid
                .entry(r.condition_id.clone())
                .or_insert(r);
        }
        if r.new_action == "trade_candidate" {
            new_by_cid
                .entry(r.condition_id.clone())
                .or_insert(r);
        }
    }

    let mut total_old_pnl = 0.0f64;
    let mut total_new_pnl = 0.0f64;
    let mut total_old_trades = 0usize;
    let mut total_new_trades = 0usize;
    let mut total_old_wins = 0usize;
    let mut total_new_wins = 0usize;

    for asset in &assets {
        let mut old_pnl = 0.0f64;
        let mut new_pnl = 0.0f64;
        let mut old_n = 0usize;
        let mut new_n = 0usize;

        for (cid, st) in settled.iter() {
            if st.asset != *asset {
                continue;
            }

            // OLD P&L: use actual realized_pnl if the old code traded this
            if old_by_cid.contains_key(cid) {
                old_pnl += st.realized_pnl;
                old_n += 1;
                if st.won {
                    total_old_wins += 1;
                }
            }

            // NEW P&L: estimate from new decision
            if let Some(r) = new_by_cid.get(cid) {
                let exec_price = if r.new_side == "BUY YES" {
                    r.best_ask
                } else {
                    1.0 - r.best_bid
                };
                let stake = r.old_stake.unwrap_or(10.0); // fallback to $10
                let is_buy_yes = r.new_side == "BUY YES";
                let outcome_yes = st.actual_yes_outcome;

                let won = (is_buy_yes && outcome_yes) || (!is_buy_yes && !outcome_yes);
                let pnl = if won {
                    if exec_price > 0.0 {
                        stake * (1.0 / exec_price - 1.0)
                    } else {
                        0.0
                    }
                } else {
                    -stake
                };

                new_pnl += pnl;
                new_n += 1;
                if won {
                    total_new_wins += 1;
                }
            }
        }

        if old_n > 0 || new_n > 0 {
            let delta = new_pnl - old_pnl;
            println!(
                "  {:<6} {:>8} {:>8} {:>+10.2} {:>+10.2} {:>+8.2}",
                asset, old_n, new_n, old_pnl, new_pnl, delta
            );
        }
        total_old_pnl += old_pnl;
        total_new_pnl += new_pnl;
        total_old_trades += old_n;
        total_new_trades += new_n;
    }

    println!("  {}", "-".repeat(58));
    println!(
        "  {:<6} {:>8} {:>8} {:>+10.2} {:>+10.2} {:>+8.2}",
        "TOTAL",
        total_old_trades,
        total_new_trades,
        total_old_pnl,
        total_new_pnl,
        total_new_pnl - total_old_pnl
    );

    // Win rates
    println!();
    if total_old_trades > 0 {
        println!(
            "  OLD Win Rate: {:.1}% ({}/{})",
            total_old_wins as f64 / total_old_trades as f64 * 100.0,
            total_old_wins,
            total_old_trades
        );
    }
    if total_new_trades > 0 {
        println!(
            "  NEW Win Rate: {:.1}% ({}/{})",
            total_new_wins as f64 / total_new_trades as f64 * 100.0,
            total_new_wins,
            total_new_trades
        );
    }

    // ── Per-side breakdown ───────────────────────────────────────
    println!("\n=== Decision Breakdown by Side ===\n");
    println!(
        "  {:<10} {:>8} {:>8} {:>8} {:>10}   {:>8} {:>8} {:>8} {:>10}",
        "Side", "OLD N", "Wins", "Win%", "P&L", "NEW N", "Wins", "Win%", "P&L"
    );
    println!("  {}", "-".repeat(90));

    for side_label in &["BUY YES", "BUY NO"] {
        let mut old_trades = 0usize;
        let mut old_wins = 0usize;
        let mut old_pnl = 0.0f64;
        let mut new_trades = 0usize;
        let mut new_wins = 0usize;
        let mut new_pnl = 0.0f64;

        // OLD side
        for (cid, r) in &old_by_cid {
            let old_side = r.old_side.as_deref().unwrap_or("");
            if old_side != *side_label {
                continue;
            }
            if let Some(st) = settled.get(cid) {
                let is_buy_yes = *side_label == "BUY YES";
                let won =
                    (is_buy_yes && st.actual_yes_outcome) || (!is_buy_yes && !st.actual_yes_outcome);
                old_trades += 1;
                if won {
                    old_wins += 1;
                }
                old_pnl += st.realized_pnl;
            }
        }

        // NEW side
        for (cid, r) in &new_by_cid {
            if r.new_side != *side_label {
                continue;
            }
            if let Some(st) = settled.get(cid) {
                let is_buy_yes = *side_label == "BUY YES";
                let won =
                    (is_buy_yes && st.actual_yes_outcome) || (!is_buy_yes && !st.actual_yes_outcome);
                let exec_price = if is_buy_yes {
                    r.best_ask
                } else {
                    1.0 - r.best_bid
                };
                let stake = r.old_stake.unwrap_or(10.0);
                let trade_pnl = if won {
                    if exec_price > 0.0 {
                        stake * (1.0 / exec_price - 1.0)
                    } else {
                        0.0
                    }
                } else {
                    -stake
                };

                new_trades += 1;
                if won {
                    new_wins += 1;
                }
                new_pnl += trade_pnl;
            }
        }

        let old_wr = if old_trades > 0 {
            format!("{:.1}%", old_wins as f64 / old_trades as f64 * 100.0)
        } else {
            "-".to_string()
        };
        let new_wr = if new_trades > 0 {
            format!("{:.1}%", new_wins as f64 / new_trades as f64 * 100.0)
        } else {
            "-".to_string()
        };
        println!(
            "  {:<10} {:>8} {:>8} {:>8} {:>+10.2}   {:>8} {:>8} {:>8} {:>+10.02}",
            side_label,
            old_trades, old_wins, old_wr, old_pnl,
            new_trades, new_wins, new_wr, new_pnl
        );
    }
}

// ── Parameter sweep ─────────────────────────────────────────────

fn build_param_grid() -> Vec<ReplayConfig> {
    let base = ReplayConfig {
        // Fixed from previous sweep (strongly determined)
        ewma_lambda: 0.97,
        min_annualized_vol: 0.10,
        yes_edge_threshold_mult: 999.0, // NO-only
        vol_window_obs: 100,
        ..ReplayConfig::default()
    };

    // ── Trade selection ──
    let min_convictions = [0.08, 0.12, 0.16, 0.20, 0.24];
    let no_edge_mults = [0.7, 0.85, 1.0, 1.2, 1.5];
    let threshold_scales = [0.5, 0.7, 0.85, 1.0, 1.15, 1.3];

    // ── Vol regime ──
    let regime_configs: [(f64, f64, f64, f64); 3] = [
        (99.0, 1.0, 0.0, 1.0),    // OFF
        (1.5, 1.5, 0.7, 0.9),     // default
        (1.3, 1.5, 0.8, 0.9),     // sensitive
    ];

    // ── Mean reversion (focused from previous sweep: 120s lookback won) ──
    let reversion_configs: [(f64, f64, f64); 5] = [
        (0.0,  180.0, 1.5),  // OFF
        (0.03, 120.0, 1.0),  // medium, symmetric
        (0.03, 120.0, 1.5),  // medium, asymmetric
        (0.05, 120.0, 1.5),  // heavy, asymmetric
        (0.05, 180.0, 1.5),  // heavy, longer
    ];

    // ── OBI signal ──
    let obi_weights = [0.0, 0.02, 0.05, 0.10];

    let mut grid = Vec::new();
    for &mc in &min_convictions {
        for &nem in &no_edge_mults {
            for &ts in &threshold_scales {
                for &(et, em, ct, cm) in &regime_configs {
                    for &(rw, lb, nm) in &reversion_configs {
                        for &ow in &obi_weights {
                            grid.push(ReplayConfig {
                                min_conviction: mc,
                                no_edge_threshold_mult: nem,
                                threshold_scale: ts,
                                vol_regime_expand_threshold: et,
                                vol_regime_expand_mult: em,
                                vol_regime_contract_threshold: ct,
                                vol_regime_contract_mult: cm,
                                reversion_weight: rw,
                                reversion_lookback_secs: lb,
                                reversion_negative_mult: nm,
                                obi_weight: ow,
                                ..base.clone()
                            });
                        }
                    }
                }
            }
        }
    }
    grid
}

fn compute_sweep_metrics(
    results: &[ReplayResult],
    settled: &HashMap<String, SettledTrade>,
) -> SweepMetrics {
    // Dedup: last evaluation per condition_id
    let mut last_by_cid: HashMap<&str, &ReplayResult> = HashMap::new();
    for r in results {
        last_by_cid.insert(&r.condition_id, r);
    }

    // Trade candidates only (deduped)
    let mut new_by_cid: HashMap<&str, &ReplayResult> = HashMap::new();
    for r in results {
        if r.new_action == "trade_candidate" {
            new_by_cid.insert(&r.condition_id, r);
        }
    }

    // Brier on all settled (same set)
    let mut brier_pairs: Vec<(f64, bool)> = Vec::new();
    for (cid, r) in &last_by_cid {
        if let Some(st) = settled.get(*cid) {
            brier_pairs.push((r.new_adjusted_posterior, st.actual_yes_outcome));
        }
    }
    let brier_n = brier_pairs.len();
    let brier_same_set = if brier_n > 0 {
        brier_pairs
            .iter()
            .map(|(p, actual)| {
                let o = if *actual { 1.0 } else { 0.0 };
                (p - o).powi(2)
            })
            .sum::<f64>()
            / brier_n as f64
    } else {
        0.25
    };

    // P&L and win tracking per side
    let mut total_trades = 0usize;
    let mut yes_trades = 0usize;
    let mut no_trades = 0usize;
    let mut wins = 0usize;
    let mut yes_wins = 0usize;
    let mut no_wins = 0usize;
    let mut total_pnl = 0.0f64;
    let mut yes_pnl = 0.0f64;
    let mut no_pnl = 0.0f64;

    for (cid, r) in &new_by_cid {
        if let Some(st) = settled.get(*cid) {
            let is_buy_yes = r.new_side == "BUY YES";
            let won =
                (is_buy_yes && st.actual_yes_outcome) || (!is_buy_yes && !st.actual_yes_outcome);
            let exec_price = if is_buy_yes {
                r.best_ask
            } else {
                1.0 - r.best_bid
            };
            let stake = r.old_stake.unwrap_or(10.0);
            let trade_pnl = if won {
                if exec_price > 0.0 {
                    stake * (1.0 / exec_price - 1.0)
                } else {
                    0.0
                }
            } else {
                -stake
            };

            total_trades += 1;
            total_pnl += trade_pnl;
            if won {
                wins += 1;
            }
            if is_buy_yes {
                yes_trades += 1;
                yes_pnl += trade_pnl;
                if won {
                    yes_wins += 1;
                }
            } else {
                no_trades += 1;
                no_pnl += trade_pnl;
                if won {
                    no_wins += 1;
                }
            }
        }
    }

    SweepMetrics {
        total_trades,
        yes_trades,
        no_trades,
        wins,
        yes_wins,
        no_wins,
        total_pnl,
        yes_pnl,
        no_pnl,
        brier_same_set,
        brier_n,
    }
}

fn print_sweep_report(
    all_results: &[(ReplayConfig, Vec<SweepMetrics>)],
    datasets: &[(String, Vec<ReplayEval>, HashMap<String, SettledTrade>)],
) {
    let n_days = datasets.len();

    // Filter: must have ≥ 10 trades on each day, and be profitable on both days
    let mut ranked: Vec<(usize, f64, bool)> = Vec::new(); // (index, combined_pnl, consistent)
    for (i, (_cfg, day_metrics)) in all_results.iter().enumerate() {
        let combined_pnl: f64 = day_metrics.iter().map(|m| m.total_pnl).sum();
        let all_profitable = day_metrics.iter().all(|m| m.total_pnl > 0.0);
        let min_trades = day_metrics.iter().map(|m| m.total_trades).min().unwrap_or(0);
        if min_trades >= 10 {
            ranked.push((i, combined_pnl, all_profitable));
        }
    }

    // Sort: consistent configs first, then by combined P&L descending
    ranked.sort_by(|a, b| {
        b.2.cmp(&a.2) // consistent first
            .then(b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal))
    });

    // Print top 25
    let top_n = ranked.len().min(25);
    println!(
        "=== TOP {} CONFIGURATIONS (of {} with ≥10 trades/day) ===\n",
        top_n,
        ranked.len()
    );

    // Header
    let regime_label = |cfg: &ReplayConfig| -> &str {
        if cfg.vol_regime_expand_threshold > 50.0 {
            "OFF"
        } else if cfg.vol_regime_expand_mult > 1.7 {
            "aggr"
        } else if cfg.vol_regime_expand_threshold < 1.4 {
            "sens"
        } else {
            "dflt"
        }
    };

    print!("  {:>4} {:>5} {:>5} {:>5} {:>4} {:>4} {:>4} {:>4} {:>5}",
        "Rank", "Conv", "NoMx", "ThSc", "RevW", "Lb", "Rgme", "OBI", "Brier");
    for (label, _, _) in datasets {
        let short = label.chars().rev().take(5).collect::<String>().chars().rev().collect::<String>();
        print!("  {:>7} {:>5} {:>4}",
            format!("P&L_{short}"), "WR", "N");
    }
    println!("  {:>9} {:>4}", "TOTAL", "Con");
    print!("  {:>4} {:>5} {:>5} {:>5} {:>4} {:>4} {:>4} {:>4} {:>5}",
        "----", "-----", "-----", "-----", "----", "----", "----", "----", "-----");
    for _ in datasets {
        print!("  {:>7} {:>5} {:>4}", "-------", "-----", "----");
    }
    println!("  {:>9} {:>4}", "---------", "----");

    for (rank, &(idx, combined_pnl, consistent)) in ranked.iter().take(top_n).enumerate() {
        let (cfg, day_metrics) = &all_results[idx];
        let rev_w = if cfg.reversion_weight < 0.001 {
            "off".to_string()
        } else {
            format!("{:.2}", cfg.reversion_weight)
        };
        let lb = if cfg.reversion_weight < 0.001 {
            "-".to_string()
        } else {
            format!("{:.0}", cfg.reversion_lookback_secs)
        };
        let rgme = regime_label(cfg);
        let obi = if cfg.obi_weight < 0.001 {
            "off".to_string()
        } else {
            format!("{:.2}", cfg.obi_weight)
        };
        // Average Brier across days
        let avg_brier: f64 = day_metrics.iter().map(|m| m.brier_same_set).sum::<f64>()
            / day_metrics.len() as f64;

        print!(
            "  {:>4} {:>5.2} {:>5.2} {:>5.2} {:>4} {:>4} {:>4} {:>4} {:>.4}",
            rank + 1,
            cfg.min_conviction,
            cfg.no_edge_threshold_mult,
            cfg.threshold_scale,
            rev_w,
            lb,
            rgme,
            obi,
            avg_brier
        );

        for m in day_metrics {
            let wr = if m.total_trades > 0 {
                m.wins as f64 / m.total_trades as f64 * 100.0
            } else {
                0.0
            };
            print!(
                "  {:>+7.0} {:>4.1}% {:>4}",
                m.total_pnl, wr, m.total_trades
            );
        }

        let con_mark = if consistent { "Y" } else { "n" };
        println!("  {:>+9.0} {:>4}", combined_pnl, con_mark);
    }

    // Print the single best consistent config details
    if let Some(&(best_idx, best_pnl, true)) = ranked.iter().find(|(_, _, c)| *c) {
        let (cfg, day_metrics) = &all_results[best_idx];
        println!("\n=== BEST CONSISTENT CONFIG (profitable on ALL days) ===\n");
        println!("  Combined P&L: ${:.0}", best_pnl);
        println!();
        println!("  # Trade selection");
        println!("  min_conviction           = {:.2}", cfg.min_conviction);
        let ym = if cfg.yes_edge_threshold_mult > 100.0 {
            "999.0 (YES disabled)".to_string()
        } else {
            format!("{:.1}", cfg.yes_edge_threshold_mult)
        };
        println!("  yes_edge_threshold_mult  = {}", ym);
        println!("  no_edge_threshold_mult   = {:.2}", cfg.no_edge_threshold_mult);
        println!("  threshold_scale          = {:.2}", cfg.threshold_scale);
        let ow = if cfg.obi_weight < 0.001 {
            "0.0 (disabled)".to_string()
        } else {
            format!("{:.2}", cfg.obi_weight)
        };
        println!("  obi_weight               = {}", ow);
        if cfg.vol_regime_expand_threshold > 50.0 {
            println!("  vol_regime               = OFF");
        } else {
            println!(
                "  vol_regime               = expand>{:.1}→×{:.1}, contract<{:.1}→×{:.1}",
                cfg.vol_regime_expand_threshold,
                cfg.vol_regime_expand_mult,
                cfg.vol_regime_contract_threshold,
                cfg.vol_regime_contract_mult
            );
        }
        println!();
        println!("  # Volatility estimator");
        println!("  ewma_lambda              = {:.2}", cfg.ewma_lambda);
        println!("  vol_window_obs           = {}", cfg.vol_window_obs);
        println!("  min_annualized_vol       = {:.2}", cfg.min_annualized_vol);
        println!();
        println!("  # Mean reversion");
        let rw = if cfg.reversion_weight < 0.001 {
            "0.0 (disabled)".to_string()
        } else {
            format!("{:.2}", cfg.reversion_weight)
        };
        println!("  reversion_weight         = {}", rw);
        if cfg.reversion_weight >= 0.001 {
            println!("  reversion_lookback_secs  = {:.0}", cfg.reversion_lookback_secs);
            println!("  reversion_negative_mult  = {:.1}", cfg.reversion_negative_mult);
        }
        for (i, (label, _, _)) in datasets.iter().enumerate() {
            let m = &day_metrics[i];
            let wr = if m.total_trades > 0 {
                m.wins as f64 / m.total_trades as f64 * 100.0
            } else {
                0.0
            };
            let yes_wr = if m.yes_trades > 0 {
                format!("{:.1}%", m.yes_wins as f64 / m.yes_trades as f64 * 100.0)
            } else {
                "-".into()
            };
            let no_wr = if m.no_trades > 0 {
                format!("{:.1}%", m.no_wins as f64 / m.no_trades as f64 * 100.0)
            } else {
                "-".into()
            };
            println!(
                "\n  {}:",
                label
            );
            println!(
                "    Trades: {} (YES: {}, NO: {})  |  WR: {:.1}% (YES: {}, NO: {})",
                m.total_trades, m.yes_trades, m.no_trades, wr, yes_wr, no_wr
            );
            println!(
                "    P&L: ${:+.0} (YES: ${:+.0}, NO: ${:+.0})  |  Brier: {:.4}",
                m.total_pnl, m.yes_pnl, m.no_pnl, m.brier_same_set
            );
        }
    } else {
        println!("\n  No config was profitable on ALL days simultaneously.");
        if let Some(&(best_idx, best_pnl, _)) = ranked.first() {
            let (cfg, _) = &all_results[best_idx];
            println!(
                "  Best overall (${:+.0}): conv={:.2}, yes_mult={:.1}, rev={:.2}",
                best_pnl, cfg.min_conviction, cfg.yes_edge_threshold_mult, cfg.reversion_weight
            );
        }
    }

    // Summary stats
    let n_consistent = ranked.iter().filter(|(_, _, c)| *c).count();
    let n_profitable_combined = ranked.iter().filter(|(_, pnl, _)| *pnl > 0.0).count();
    println!("\n  --- Summary ---");
    println!("  Total configs tested: {}", all_results.len());
    println!("  With ≥10 trades/day: {}", ranked.len());
    println!(
        "  Profitable on ALL {} days: {}",
        n_days, n_consistent
    );
    println!("  Profitable combined: {}", n_profitable_combined);
}
