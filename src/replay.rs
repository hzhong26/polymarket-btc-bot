use std::collections::HashMap;
use std::path::PathBuf;

use anyhow::{bail, Context};
use chrono::{DateTime, Utc};

use crate::analyze::{self, SettledTrade};
use crate::bayesian::{models, volatility::VolatilityEstimator};

// ── Replay config (matching current config.toml defaults) ────────

const EWMA_LAMBDA: f64 = 0.94;
const REFERENCE_INTERVAL_SECS: f64 = 3.0;
const VOL_WINDOW_OBS: usize = 100;
const DEFAULT_VOLATILITY: f64 = 0.80;
const HIGH_VOL_THRESHOLD: f64 = 1.0;
const MIN_ANNUALIZED_VOL: f64 = 0.15;
const MIN_CONVICTION: f64 = 0.08;

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
    let log_dirs = analyze::resolve_log_dirs(dirs)?;
    let files = analyze::find_event_files(&log_dirs)?;
    if files.is_empty() {
        bail!("No event files found in the specified directories");
    }

    // Resolve --run shorthand (e.g. "2" → full run_id)
    let resolved_filter = if let Some(selector) = run_filter {
        let runs = analyze::discover_runs(&files)?;
        let resolved = analyze::resolve_run_id(&runs, selector)?;
        Some(resolved)
    } else {
        None
    };
    let effective_filter = resolved_filter.as_deref();

    println!("=== REPLAY MODE: Vol Fix Impact Analysis ===\n");
    println!("Config: λ={EWMA_LAMBDA}, ref_dt={REFERENCE_INTERVAL_SECS}s, window={VOL_WINDOW_OBS}, default_vol={DEFAULT_VOLATILITY}");
    println!(
        "Log dirs: {}",
        log_dirs
            .iter()
            .map(|d| d.display().to_string())
            .collect::<Vec<_>>()
            .join(", ")
    );
    if let Some(f) = effective_filter {
        println!("Run filter: {f}");
    }

    // Parse data
    let (evals, settled) = parse_replay_data(&files, effective_filter)?;
    println!(
        "Parsed {} evaluations, {} settlements\n",
        evals.len(),
        settled.len()
    );

    if evals.is_empty() {
        bail!("No pipeline evaluations found");
    }

    // Run replay
    let results = replay_pipeline(&evals);

    // Print report
    print_replay_report(&results, &settled);

    Ok(())
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
        .unwrap_or(DEFAULT_VOLATILITY);
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

fn replay_pipeline(evals: &[ReplayEval]) -> Vec<ReplayResult> {
    let mut vol_estimators: HashMap<String, VolatilityEstimator> = HashMap::new();
    let mut results = Vec::with_capacity(evals.len());

    for eval in evals {
        // Get or create vol estimator for this asset
        let estimator = vol_estimators
            .entry(eval.asset.clone())
            .or_insert_with(|| {
                VolatilityEstimator::new(
                    VOL_WINDOW_OBS,
                    HIGH_VOL_THRESHOLD,
                    EWMA_LAMBDA,
                    REFERENCE_INTERVAL_SECS,
                )
            });

        // Feed observations (prefer Binance, fallback to any source)
        let binance_obs = eval.observations.iter().find(|o| o.source == "binance");
        let obs_to_feed = binance_obs.or_else(|| {
            eval.observations
                .iter()
                .max_by(|a, b| a.timestamp_secs.partial_cmp(&b.timestamp_secs).unwrap())
        });

        if let Some(obs) = obs_to_feed {
            estimator.add_price(obs.price, obs.timestamp_secs);
        }

        // Snapshot new vol (with floor)
        let new_vol = estimator
            .annualized_volatility()
            .unwrap_or(DEFAULT_VOLATILITY)
            .max(MIN_ANNUALIZED_VOL);

        // Recompute model probability
        let new_model_p = if eval.model_type == "updown_directional" {
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

        // Conviction floor + threshold gate
        let conviction = (new_adjusted - 0.5).abs();
        let new_action = if MIN_CONVICTION > 0.0 && conviction < MIN_CONVICTION {
            "no_trade".to_string()
        } else if new_net_edge >= eval.effective_threshold {
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
    println!("\n=== Brier Score Comparison (settled trade_candidates) ===\n");

    // Match replay results to settled outcomes for Brier computation.
    // Use the last trade_candidate per condition_id (deduped).
    let mut old_brier_pairs: Vec<(f64, bool)> = Vec::new(); // (predicted_yes_prob, actual_yes)
    let mut new_brier_pairs: Vec<(f64, bool)> = Vec::new();
    let mut old_brier_by_asset: HashMap<String, Vec<(f64, bool)>> = HashMap::new();
    let mut new_brier_by_asset: HashMap<String, Vec<(f64, bool)>> = HashMap::new();

    // Dedup: keep last trade_candidate per condition_id for old and new
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

    for (cid, r) in &old_tc_by_cid {
        if let Some(st) = settled.get(*cid) {
            old_brier_pairs.push((r.old_posterior, st.actual_yes_outcome));
            old_brier_by_asset
                .entry(r.asset.clone())
                .or_default()
                .push((r.old_posterior, st.actual_yes_outcome));
        }
    }
    for (cid, r) in &new_tc_by_cid {
        if let Some(st) = settled.get(*cid) {
            new_brier_pairs.push((r.new_adjusted_posterior, st.actual_yes_outcome));
            new_brier_by_asset
                .entry(r.asset.clone())
                .or_default()
                .push((r.new_adjusted_posterior, st.actual_yes_outcome));
        }
    }

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

    let old_brier = calc_brier(&old_brier_pairs);
    let new_brier = calc_brier(&new_brier_pairs);
    let delta = new_brier - old_brier;
    let arrow = if delta < -0.005 {
        "  <- better"
    } else if delta > 0.005 {
        "  <- worse"
    } else {
        ""
    };

    println!(
        "  OLD Brier: {:.4}  (N={})    0.25 = random",
        old_brier,
        old_brier_pairs.len()
    );
    println!(
        "  NEW Brier: {:.4}  (N={})    lower = better{}",
        new_brier,
        new_brier_pairs.len(),
        arrow
    );
    println!("  Delta:     {:+.4}", delta);

    // Per-asset breakdown
    let assets = ["BTC", "ETH", "SOL", "XRP"];
    println!(
        "\n  {:<6} {:>10} {:>6} {:>10} {:>6} {:>8}",
        "Asset", "OLD Brier", "N", "NEW Brier", "N", "Δ"
    );
    println!("  {}", "-".repeat(52));
    for asset in &assets {
        let ob = old_brier_by_asset
            .get(*asset)
            .map(|v| (calc_brier(v), v.len()))
            .unwrap_or((0.0, 0));
        let nb = new_brier_by_asset
            .get(*asset)
            .map(|v| (calc_brier(v), v.len()))
            .unwrap_or((0.0, 0));
        if ob.1 > 0 || nb.1 > 0 {
            let d = if ob.1 > 0 && nb.1 > 0 {
                nb.0 - ob.0
            } else {
                0.0
            };
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
    println!("\n=== NEW Decision Breakdown by Side ===\n");
    println!(
        "  {:<10} {:>8} {:>8} {:>8} {:>10}",
        "Side", "Trades", "Wins", "Win%", "Est P&L"
    );
    println!("  {}", "-".repeat(48));

    for side in &["BUY YES", "BUY NO"] {
        let mut trades = 0usize;
        let mut wins = 0usize;
        let mut pnl = 0.0f64;

        for (cid, r) in &new_by_cid {
            if r.new_side != *side {
                continue;
            }
            if let Some(st) = settled.get(cid) {
                let is_buy_yes = *side == "BUY YES";
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

                trades += 1;
                if won {
                    wins += 1;
                }
                pnl += trade_pnl;
            }
        }

        if trades > 0 {
            println!(
                "  {:<10} {:>8} {:>8} {:>7.1}% {:>+10.2}",
                side,
                trades,
                wins,
                wins as f64 / trades as f64 * 100.0,
                pnl
            );
        }
    }
}
