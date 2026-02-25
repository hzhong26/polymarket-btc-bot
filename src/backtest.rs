use std::collections::HashMap;
use std::path::PathBuf;

use anyhow::{bail, Context};
use rust_decimal::prelude::ToPrimitive;

use crate::analyze::{
    self, SettledTrade,
};
use crate::config;

// ── Data structures ────────────────────────────────────────────────

/// A single pipeline evaluation extracted from JSONL logs, containing all
/// fields needed to re-apply parameterized decision gates.
#[derive(Debug, Clone)]
#[allow(dead_code)]
struct BacktestEval {
    condition_id: String,
    asset: String,
    time_to_expiry_secs: f64,
    // Model outputs
    posterior: Option<f64>,
    market_implied_p: Option<f64>,
    // Edge data (None if rejected before edge calc)
    gross_edge: Option<f64>,
    net_edge: Option<f64>,
    cost_total: Option<f64>,
    executable_price: Option<f64>,
    edge_side: Option<String>,
    effective_threshold: f64,
    base_threshold: f64,
    // Kelly (None if rejected before Kelly)
    kelly_final: Option<f64>,
    stake_usdc: Option<f64>,
    // Book snapshot for P&L estimation
    best_bid: Option<f64>,
    best_ask: Option<f64>,
    // Original action
    action_kind: String,
    was_sampled: bool,
    sample_rate: Option<f64>,
}

/// Parameters for a single sweep configuration.
#[derive(Debug, Clone)]
struct SweepParams {
    label: String,
    edge_threshold: f64,
    yes_edge_threshold_mult: f64,
    no_edge_threshold_mult: f64,
    prob_shrinkage_alpha: Option<f64>,
    min_time_to_expiry_secs: f64,
    max_net_edge: Option<f64>,
    asset_edge_mults: HashMap<String, f64>,
}

/// Results from running one sweep configuration.
#[derive(Debug, Clone)]
#[allow(dead_code)]
struct SweepResult {
    params: SweepParams,
    total_evals: usize,
    simulated_trades: usize,
    matched_outcomes: usize,
    wins: usize,
    losses: usize,
    total_pnl: f64,
    brier_score: Option<f64>,
    by_asset: HashMap<String, AssetStats>,
    by_side: HashMap<String, SideStats>,
}

#[derive(Debug, Clone, Default)]
struct AssetStats {
    trades: usize,
    wins: usize,
    pnl: f64,
}

#[derive(Debug, Clone, Default)]
struct SideStats {
    trades: usize,
    wins: usize,
    pnl: f64,
}

// ── JSONL parser ───────────────────────────────────────────────────

/// Parse all pipeline_evaluation and position_settled events from JSONL files.
/// Returns (all evals, settled outcomes keyed by condition_id).
fn parse_backtest_data(
    files: &[PathBuf],
    run_id_filter: Option<&str>,
) -> anyhow::Result<(Vec<BacktestEval>, HashMap<String, SettledTrade>)> {
    let mut evals: Vec<BacktestEval> = Vec::new();
    let mut settled: HashMap<String, SettledTrade> = HashMap::new();
    // Track which condition_ids we've seen as trade_candidate to dedup
    let mut seen_trade_candidates: HashMap<String, usize> = HashMap::new();

    for file in files {
        let content = std::fs::read_to_string(file)
            .with_context(|| format!("Failed to read {}", file.display()))?;
        for line in content.lines() {
            if line.is_empty() {
                continue;
            }
            // Fast run_id filter before JSON parsing
            if let Some(filter_id) = run_id_filter {
                match analyze::extract_json_str(line, "run_id") {
                    Some(ref id) if id == filter_id => {}
                    _ => continue,
                }
            }
            // Fast check: skip lines that don't contain either event type
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
                    if let Some(eval) = parse_backtest_eval(&val) {
                        let cid = eval.condition_id.clone();
                        let is_trade = eval.action_kind == "trade_candidate";

                        // Dedup: prefer trade_candidate over no_trade for same condition_id.
                        // If we've already seen a trade_candidate for this condition_id, skip.
                        if let Some(&existing_idx) = seen_trade_candidates.get(&cid) {
                            if is_trade {
                                // Replace with the trade_candidate version
                                evals[existing_idx] = eval;
                            }
                            // else: already have trade_candidate, skip no_trade
                            continue;
                        }

                        if is_trade {
                            seen_trade_candidates.insert(cid, evals.len());
                        }
                        evals.push(eval);
                    }
                }
                _ => {}
            }
        }
    }

    Ok((evals, settled))
}

/// Extract a BacktestEval from a pipeline_evaluation JSONL event.
fn parse_backtest_eval(val: &serde_json::Value) -> Option<BacktestEval> {
    let p = val.get("payload")?;

    let market = p.get("market")?;
    let condition_id = market.get("condition_id")?.as_str()?.to_string();
    let asset = market.get("asset")?.as_str()?.to_string();

    let mc = p.get("model_context")?;
    let time_to_expiry_secs = mc.get("time_to_expiry_secs")?.as_f64()?;
    let posterior = mc.get("posterior").and_then(|v| v.as_f64());
    let market_implied_p = mc.get("market_implied_p").and_then(|v| v.as_f64());

    let cc = p.get("cost_context")?;
    let gross_edge = cc.get("gross_edge").and_then(|v| v.as_f64());
    let net_edge = cc.get("net_edge").and_then(|v| v.as_f64());
    let cost_total = cc.get("total_cost").and_then(|v| v.as_f64());
    let effective_threshold = cc.get("effective_threshold")?.as_f64()?;
    let base_threshold = cc.get("base_threshold")?.as_f64()?;

    let ec = p.get("execution_context")?;
    let executable_price = ec.get("executable_price").and_then(|v| v.as_f64());
    let ob = ec.get("orderbook")?;
    let best_bid = ob.get("best_bid").and_then(|v| v.as_f64());
    let best_ask = ob.get("best_ask").and_then(|v| v.as_f64());

    let src = p.get("sizing_risk_context")?;
    let kelly_final = src.get("kelly_final").and_then(|v| v.as_f64());
    let stake_usdc = src.get("stake_usdc").and_then(|v| v.as_f64());

    let action = p.get("action")?;
    let action_kind = action.get("kind")?.as_str()?.to_string();
    let edge_side = action.get("side").and_then(|v| v.as_str()).map(String::from);

    let was_sampled = p
        .get("was_sampled")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    let sample_rate = p.get("sample_rate").and_then(|v| v.as_f64());

    Some(BacktestEval {
        condition_id,
        asset,
        time_to_expiry_secs,
        posterior,
        market_implied_p,
        gross_edge,
        net_edge,
        cost_total,
        executable_price,
        edge_side,
        effective_threshold,
        base_threshold,
        kelly_final,
        stake_usdc,
        best_bid,
        best_ask,
        action_kind,
        was_sampled,
        sample_rate,
    })
}

// ── Parameter sweep engine ─────────────────────────────────────────

/// Run a single sweep configuration against all evals.
fn run_sweep(
    params: &SweepParams,
    evals: &[BacktestEval],
    settled: &HashMap<String, SettledTrade>,
) -> SweepResult {
    let mut result = SweepResult {
        params: params.clone(),
        total_evals: evals.len(),
        simulated_trades: 0,
        matched_outcomes: 0,
        wins: 0,
        losses: 0,
        total_pnl: 0.0,
        brier_score: None,
        by_asset: HashMap::new(),
        by_side: HashMap::new(),
    };

    let mut brier_sum = 0.0f64;
    let mut brier_count = 0usize;

    for eval in evals {
        // 1. TTE filter
        if eval.time_to_expiry_secs < params.min_time_to_expiry_secs {
            continue;
        }

        // 2. Data check: need edge data to re-evaluate
        let Some(net_edge) = eval.net_edge else {
            continue;
        };
        let Some(_gross_edge) = eval.gross_edge else {
            continue;
        };
        let Some(cost_total) = eval.cost_total else {
            continue;
        };
        let Some(posterior) = eval.posterior else {
            continue;
        };
        let Some(executable_price) = eval.executable_price else {
            continue;
        };
        let Some(ref edge_side) = eval.edge_side else {
            continue;
        };

        // 3. Probability shrinkage
        let shrunk_posterior = if let Some(alpha) = params.prob_shrinkage_alpha {
            let market_p = eval.market_implied_p.unwrap_or(posterior);
            (alpha * posterior + (1.0 - alpha) * market_p).clamp(0.001, 0.999)
        } else {
            posterior
        };

        // 4. Recompute edge with shrunk posterior
        let new_net_edge = if params.prob_shrinkage_alpha.is_some() {
            let is_buy_yes = edge_side.contains("YES");
            let new_gross = if is_buy_yes {
                shrunk_posterior - executable_price
            } else {
                // BUY NO: (1 - posterior) is our NO prob, entry is (1 - best_bid)
                let no_entry = eval.best_bid.map(|b| 1.0 - b).unwrap_or(executable_price);
                (1.0 - shrunk_posterior) - no_entry
            };
            new_gross - cost_total
        } else {
            net_edge
        };

        // 5. Threshold gate with side/asset multipliers
        // The logged base_threshold already includes asset_edge_mult from config.
        // Compute a clean threshold from sweep params.
        // base_threshold_in_log = config_edge * asset_edge_mult_at_log_time
        // We derive the logged config_edge by dividing out the asset mult.
        let is_buy_yes = edge_side.contains("YES");

        let asset_mult = params
            .asset_edge_mults
            .get(&eval.asset)
            .copied()
            .unwrap_or(1.0);

        let side_mult = if is_buy_yes {
            params.yes_edge_threshold_mult
        } else {
            params.no_edge_threshold_mult
        };

        // Reconstruct: logged base = old_config_edge * old_asset_mult
        // We want: new_base = params.edge_threshold * new_asset_mult
        // The dynamic penalties (confidence/spread/calibration) are captured as:
        //   penalty_delta = effective_threshold_before_side_mult - base_threshold
        // Since we can't perfectly separate side_mult from effective_threshold in all
        // cases, use the simpler approach: compute threshold from params + add the
        // penalty portion from the logged data.
        // Approximate: penalty = effective_threshold - base_threshold (ignoring side_mult
        // that may be baked in for step-6 events; this is a small error).
        let logged_penalty = (eval.effective_threshold - eval.base_threshold).max(0.0);
        let new_base = params.edge_threshold * asset_mult;
        let final_threshold = (new_base + logged_penalty) * side_mult;

        if new_net_edge < final_threshold {
            continue;
        }

        // 6. Max edge cap
        if let Some(cap) = params.max_net_edge {
            if new_net_edge > cap {
                continue;
            }
        }

        // 7. This eval passes — it's a simulated trade
        result.simulated_trades += 1;

        let side_key = if is_buy_yes {
            "YES".to_string()
        } else {
            "NO".to_string()
        };

        // Match to settled outcome
        if let Some(trade) = settled.get(&eval.condition_id) {
            result.matched_outcomes += 1;

            // Use actual realized_pnl for trades that were taken in the original run
            let pnl = if eval.action_kind == "trade_candidate" {
                trade.realized_pnl
            } else {
                // Hypothetical trade: estimate P&L
                estimate_pnl(trade, executable_price, is_buy_yes)
            };

            let won = if is_buy_yes {
                trade.actual_yes_outcome
            } else {
                !trade.actual_yes_outcome
            };

            if won {
                result.wins += 1;
            } else {
                result.losses += 1;
            }
            result.total_pnl += pnl;

            // Brier
            let outcome = if trade.actual_yes_outcome { 1.0 } else { 0.0 };
            brier_sum += (shrunk_posterior - outcome).powi(2);
            brier_count += 1;

            // By asset
            let asset_stats = result.by_asset.entry(eval.asset.clone()).or_default();
            asset_stats.trades += 1;
            if won {
                asset_stats.wins += 1;
            }
            asset_stats.pnl += pnl;

            // By side
            let side_stats = result.by_side.entry(side_key).or_default();
            side_stats.trades += 1;
            if won {
                side_stats.wins += 1;
            }
            side_stats.pnl += pnl;
        }
    }

    if brier_count > 0 {
        result.brier_score = Some(brier_sum / brier_count as f64);
    }

    result
}

/// Estimate P&L for a hypothetical trade that wasn't actually taken.
fn estimate_pnl(trade: &SettledTrade, executable_price: f64, is_buy_yes: bool) -> f64 {
    // Use a reasonable default stake since we don't have the actual stake
    let default_stake = 50.0; // typical stake size
    let quantity = default_stake / executable_price;

    if is_buy_yes {
        let payout = if trade.actual_yes_outcome {
            1.0
        } else {
            0.0
        };
        quantity * (payout - executable_price)
    } else {
        // For BUY NO, entry cost is (1 - best_bid) or similar
        let no_entry = 1.0 - executable_price;
        let payout = if !trade.actual_yes_outcome {
            1.0
        } else {
            0.0
        };
        quantity * (payout - no_entry)
    }
}

// ── Default parameter grid ─────────────────────────────────────────

fn build_default_grid(cfg: &config::TradingConfig) -> Vec<SweepParams> {
    let base_edge = cfg.edge_threshold.to_f64().unwrap_or(0.11);
    let base_yes_mult = cfg.yes_edge_threshold_mult.to_f64().unwrap_or(1.0);
    let base_no_mult = cfg.no_edge_threshold_mult.to_f64().unwrap_or(1.0);
    let base_tte = cfg.min_time_to_expiry_secs as f64;
    let base_max_edge = cfg.max_net_edge.and_then(|v| v.to_f64());
    let base_shrinkage = cfg.prob_shrinkage_alpha;

    // Collect per-asset edge multipliers from config
    let asset_mults: HashMap<String, f64> = [
        ("BTC", &cfg.asset_params.btc),
        ("ETH", &cfg.asset_params.eth),
        ("XRP", &cfg.asset_params.xrp),
        ("SOL", &cfg.asset_params.sol),
    ]
    .iter()
    .map(|(name, tuning)| {
        (
            name.to_string(),
            tuning.edge_threshold_mult.to_f64().unwrap_or(1.0),
        )
    })
    .collect();

    let mut grid: Vec<SweepParams> = Vec::new();

    // Baseline: current config
    grid.push(SweepParams {
        label: "baseline".to_string(),
        edge_threshold: base_edge,
        yes_edge_threshold_mult: base_yes_mult,
        no_edge_threshold_mult: base_no_mult,
        prob_shrinkage_alpha: base_shrinkage,
        min_time_to_expiry_secs: base_tte,
        max_net_edge: base_max_edge,
        asset_edge_mults: asset_mults.clone(),
    });

    // Edge threshold sweep
    for &et in &[0.08, 0.09, 0.10, 0.12, 0.13, 0.14, 0.15] {
        if (et - base_edge).abs() < 0.001 {
            continue;
        }
        grid.push(SweepParams {
            label: format!("edge={:.2}", et),
            edge_threshold: et,
            yes_edge_threshold_mult: base_yes_mult,
            no_edge_threshold_mult: base_no_mult,
            prob_shrinkage_alpha: base_shrinkage,
            min_time_to_expiry_secs: base_tte,
            max_net_edge: base_max_edge,
            asset_edge_mults: asset_mults.clone(),
        });
    }

    // Probability shrinkage sweep
    for &alpha in &[0.6, 0.7, 0.8, 0.9] {
        grid.push(SweepParams {
            label: format!("shrink={:.1}", alpha),
            edge_threshold: base_edge,
            yes_edge_threshold_mult: base_yes_mult,
            no_edge_threshold_mult: base_no_mult,
            prob_shrinkage_alpha: Some(alpha),
            min_time_to_expiry_secs: base_tte,
            max_net_edge: base_max_edge,
            asset_edge_mults: asset_mults.clone(),
        });
    }

    // TTE sweep
    for &tte in &[60.0, 90.0, 180.0, 240.0] {
        if (tte - base_tte).abs() < 1.0 {
            continue;
        }
        grid.push(SweepParams {
            label: format!("tte={:.0}", tte),
            edge_threshold: base_edge,
            yes_edge_threshold_mult: base_yes_mult,
            no_edge_threshold_mult: base_no_mult,
            prob_shrinkage_alpha: base_shrinkage,
            min_time_to_expiry_secs: tte,
            max_net_edge: base_max_edge,
            asset_edge_mults: asset_mults.clone(),
        });
    }

    // Side multiplier sweep
    let side_combos: &[(&str, f64, f64)] = &[
        ("sides=1.0/1.0", 1.0, 1.0),
        ("sides=1.0/1.2", 1.0, 1.2),
        ("sides=1.2/1.0", 1.2, 1.0),
        ("sides=1.5/1.0", 1.5, 1.0),
        ("sides=1.0/1.5", 1.0, 1.5),
    ];
    for &(label, yes_m, no_m) in side_combos {
        if (yes_m - base_yes_mult).abs() < 0.001 && (no_m - base_no_mult).abs() < 0.001 {
            continue;
        }
        grid.push(SweepParams {
            label: label.to_string(),
            edge_threshold: base_edge,
            yes_edge_threshold_mult: yes_m,
            no_edge_threshold_mult: no_m,
            prob_shrinkage_alpha: base_shrinkage,
            min_time_to_expiry_secs: base_tte,
            max_net_edge: base_max_edge,
            asset_edge_mults: asset_mults.clone(),
        });
    }

    // Max net edge sweep
    for &cap in &[0.15, 0.25, 0.30] {
        let base_cap = base_max_edge.unwrap_or(f64::INFINITY);
        if (cap - base_cap).abs() < 0.001 {
            continue;
        }
        grid.push(SweepParams {
            label: format!("maxedge={:.2}", cap),
            edge_threshold: base_edge,
            yes_edge_threshold_mult: base_yes_mult,
            no_edge_threshold_mult: base_no_mult,
            prob_shrinkage_alpha: base_shrinkage,
            min_time_to_expiry_secs: base_tte,
            max_net_edge: Some(cap),
            asset_edge_mults: asset_mults.clone(),
        });
    }

    // Combo: best guesses from prior analysis
    grid.push(SweepParams {
        label: "combo:tight".to_string(),
        edge_threshold: 0.13,
        yes_edge_threshold_mult: 1.2,
        no_edge_threshold_mult: 1.2,
        prob_shrinkage_alpha: Some(0.8),
        min_time_to_expiry_secs: 120.0,
        max_net_edge: Some(0.20),
        asset_edge_mults: asset_mults.clone(),
    });

    grid.push(SweepParams {
        label: "combo:shrink".to_string(),
        edge_threshold: base_edge,
        yes_edge_threshold_mult: base_yes_mult,
        no_edge_threshold_mult: base_no_mult,
        prob_shrinkage_alpha: Some(0.7),
        min_time_to_expiry_secs: base_tte,
        max_net_edge: base_max_edge,
        asset_edge_mults: asset_mults.clone(),
    });

    grid
}

// ── Output formatting ──────────────────────────────────────────────

fn print_results(
    results: &[SweepResult],
    total_evals: usize,
    trade_candidate_count: usize,
    no_trade_count: usize,
    settled_count: usize,
    dir_labels: &[String],
    run_id: Option<&str>,
    no_trade_sample_rate: Option<f64>,
) {
    println!();
    println!("============ Parameter Sweep Backtest ============");
    println!("  Logs: {}", dir_labels.join(", "));
    if let Some(rid) = run_id {
        println!("  Run:  {}", rid);
    }
    println!(
        "  Evals: {} ({} trade_candidate, {} no_trade)",
        total_evals, trade_candidate_count, no_trade_count,
    );
    println!("  Settled outcomes: {}", settled_count);

    if let Some(rate) = no_trade_sample_rate {
        if rate < 1.0 {
            println!();
            println!(
                "  WARNING: no_trade_sample_rate={:.2} — only {:.0}% of no_trade evals are logged.",
                rate,
                rate * 100.0,
            );
            println!(
                "  Loosening parameters (lower thresholds) underestimates new trade candidates."
            );
            println!("  Increase sample rate for future runs to get accurate looser-param estimates.");
        }
    }

    // Find baseline P&L for delta column
    let baseline_pnl = results
        .first()
        .map(|r| r.total_pnl)
        .unwrap_or(0.0);

    println!();
    println!(
        "  {:<20} {:>6} {:>7} {:>6} {:>10} {:>7} {:>8}",
        "Label", "Trades", "Matched", "Win%", "P&L", "Brier", "vs Base"
    );
    println!("  {}", "\u{2500}".repeat(68));

    for r in results {
        let win_pct = if r.matched_outcomes > 0 {
            r.wins as f64 / r.matched_outcomes as f64 * 100.0
        } else {
            0.0
        };
        let brier_str = r
            .brier_score
            .map(|b| format!("{:.3}", b))
            .unwrap_or_else(|| "---".to_string());
        let delta_str = if r.params.label == "baseline" {
            "---".to_string()
        } else {
            let delta = r.total_pnl - baseline_pnl;
            format!("{:+.0}", delta)
        };
        println!(
            "  {:<20} {:>6} {:>7} {:>5.1}% {:>+10.0} {:>7} {:>8}",
            r.params.label,
            r.simulated_trades,
            r.matched_outcomes,
            win_pct,
            r.total_pnl,
            brier_str,
            delta_str,
        );
    }

    // Best by P&L and Brier (excluding baseline)
    let non_baseline: Vec<&SweepResult> = results
        .iter()
        .filter(|r| r.params.label != "baseline")
        .collect();

    if !non_baseline.is_empty() {
        println!();
        if let Some(best_pnl) = non_baseline
            .iter()
            .filter(|r| r.matched_outcomes >= 5)
            .max_by(|a, b| a.total_pnl.partial_cmp(&b.total_pnl).unwrap())
        {
            let wr = if best_pnl.matched_outcomes > 0 {
                best_pnl.wins as f64 / best_pnl.matched_outcomes as f64 * 100.0
            } else {
                0.0
            };
            println!(
                "  Best by P&L:   {}  ({:+.0}, {:.1}% WR)",
                best_pnl.params.label, best_pnl.total_pnl, wr
            );
        }
        if let Some(best_brier) = non_baseline
            .iter()
            .filter(|r| r.brier_score.is_some() && r.matched_outcomes >= 5)
            .min_by(|a, b| {
                a.brier_score
                    .unwrap()
                    .partial_cmp(&b.brier_score.unwrap())
                    .unwrap()
            })
        {
            println!(
                "  Best by Brier: {}  ({:.3})",
                best_brier.params.label,
                best_brier.brier_score.unwrap()
            );
        }
    }

    // Print detailed asset/side breakdown for baseline
    if let Some(baseline) = results.first() {
        if !baseline.by_asset.is_empty() {
            println!();
            println!("  --- Baseline by Asset ---");
            println!(
                "  {:<6} {:>6} {:>6} {:>6} {:>10}",
                "Asset", "Trades", "Wins", "Win%", "P&L"
            );
            let mut assets: Vec<_> = baseline.by_asset.iter().collect();
            assets.sort_by_key(|(k, _)| (*k).clone());
            for (asset, stats) in &assets {
                let wr = if stats.trades > 0 {
                    stats.wins as f64 / stats.trades as f64 * 100.0
                } else {
                    0.0
                };
                println!(
                    "  {:<6} {:>6} {:>6} {:>5.1}% {:>+10.0}",
                    asset, stats.trades, stats.wins, wr, stats.pnl
                );
            }
        }
        if !baseline.by_side.is_empty() {
            println!();
            println!("  --- Baseline by Side ---");
            println!(
                "  {:<6} {:>6} {:>6} {:>6} {:>10}",
                "Side", "Trades", "Wins", "Win%", "P&L"
            );
            let mut sides: Vec<_> = baseline.by_side.iter().collect();
            sides.sort_by_key(|(k, _)| (*k).clone());
            for (side, stats) in &sides {
                let wr = if stats.trades > 0 {
                    stats.wins as f64 / stats.trades as f64 * 100.0
                } else {
                    0.0
                };
                println!(
                    "  {:<6} {:>6} {:>6} {:>5.1}% {:>+10.0}",
                    side, stats.trades, stats.wins, wr, stats.pnl
                );
            }
        }
    }

    println!();
}

// ── Entry point ────────────────────────────────────────────────────

pub fn run(user_dirs: &[String], run_filter: Option<&str>) -> anyhow::Result<()> {
    // Reuse the same log dir / run resolution logic as --analyze
    let (effective_dirs, inline_run) = analyze::extract_inline_run_filter(user_dirs);
    let run_selector = run_filter.or(inline_run.as_deref());

    let dirs = if run_selector.is_some() && effective_dirs.is_empty() {
        let all = analyze::all_day_dirs()?;
        if all.is_empty() {
            bail!("No YYYY-MM-DD directories found in logs/");
        }
        all
    } else {
        analyze::resolve_log_dirs(&effective_dirs)?
    };
    let dir_labels: Vec<String> = dirs.iter().map(|d| d.display().to_string()).collect();
    let files = analyze::find_event_files(&dirs)?;

    // Resolve run filter
    let resolved_run_id = if let Some(selector) = run_selector {
        let runs = analyze::discover_runs(&files)?;
        let run_id = analyze::resolve_run_id(&runs, selector)?;
        Some(run_id)
    } else {
        None
    };

    // Parse backtest data
    let (evals, settled) = parse_backtest_data(&files, resolved_run_id.as_deref())?;

    if evals.is_empty() {
        if let Some(ref rid) = resolved_run_id {
            println!("No pipeline evaluations found for run: {}", rid);
        } else {
            println!("No pipeline evaluations found in the specified logs.");
        }
        return Ok(());
    }

    let trade_candidate_count = evals.iter().filter(|e| e.action_kind == "trade_candidate").count();
    let no_trade_count = evals.iter().filter(|e| e.action_kind == "no_trade").count();

    // Detect sample rate from the logged evals
    let no_trade_sample_rate = evals
        .iter()
        .find(|e| e.action_kind == "no_trade" && e.sample_rate.is_some())
        .and_then(|e| e.sample_rate);

    // Load config for baseline params
    let app_config = config::load_config("config.toml")
        .context("failed to load config.toml for baseline params")?;

    let grid = build_default_grid(&app_config.trading);

    // Run all sweep configurations
    let results: Vec<SweepResult> = grid
        .iter()
        .map(|params| run_sweep(params, &evals, &settled))
        .collect();

    print_results(
        &results,
        evals.len(),
        trade_candidate_count,
        no_trade_count,
        settled.len(),
        &dir_labels,
        resolved_run_id.as_deref(),
        no_trade_sample_rate,
    );

    Ok(())
}

// ── Tests ──────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn make_eval(
        condition_id: &str,
        asset: &str,
        tte: f64,
        net_edge: f64,
        posterior: f64,
        side: &str,
        action: &str,
    ) -> BacktestEval {
        BacktestEval {
            condition_id: condition_id.to_string(),
            asset: asset.to_string(),
            time_to_expiry_secs: tte,
            posterior: Some(posterior),
            market_implied_p: Some(0.5),
            gross_edge: Some(net_edge + 0.02),
            net_edge: Some(net_edge),
            cost_total: Some(0.02),
            executable_price: Some(0.50),
            edge_side: Some(side.to_string()),
            effective_threshold: 0.11,
            base_threshold: 0.11,
            kelly_final: Some(0.02),
            stake_usdc: Some(50.0),
            best_bid: Some(0.50),
            best_ask: Some(0.51),
            action_kind: action.to_string(),
            was_sampled: true,
            sample_rate: None,
        }
    }

    fn make_settled(condition_id: &str, asset: &str, won: bool, pnl: f64) -> SettledTrade {
        SettledTrade {
            condition_id: condition_id.to_string(),
            asset: asset.to_string(),
            side: "YES".to_string(),
            model_type: "test".to_string(),
            predicted_yes_prob: 0.6,
            model_yes_prob: Some(0.6),
            actual_yes_outcome: won,
            won,
            realized_pnl: pnl,
            avg_entry_price: 0.5,
            quantity: 100.0,
            window_duration_secs: Some(300),
        }
    }

    #[test]
    fn sweep_baseline_includes_all_trade_candidates() {
        let evals = vec![
            make_eval("a", "BTC", 200.0, 0.12, 0.6, "BUY YES", "trade_candidate"),
            make_eval("b", "BTC", 200.0, 0.13, 0.7, "BUY YES", "trade_candidate"),
        ];
        let mut settled = HashMap::new();
        settled.insert("a".to_string(), make_settled("a", "BTC", true, 50.0));
        settled.insert("b".to_string(), make_settled("b", "BTC", false, -30.0));

        let params = SweepParams {
            label: "baseline".to_string(),
            edge_threshold: 0.11,
            yes_edge_threshold_mult: 1.0,
            no_edge_threshold_mult: 1.0,
            prob_shrinkage_alpha: None,
            min_time_to_expiry_secs: 60.0,
            max_net_edge: None,
            asset_edge_mults: HashMap::new(),
        };

        let result = run_sweep(&params, &evals, &settled);
        assert_eq!(result.simulated_trades, 2);
        assert_eq!(result.matched_outcomes, 2);
        assert_eq!(result.wins, 1);
        assert_eq!(result.losses, 1);
        assert!((result.total_pnl - 20.0).abs() < 0.01);
    }

    #[test]
    fn sweep_tighter_edge_filters_low_edge() {
        let evals = vec![
            make_eval("a", "BTC", 200.0, 0.09, 0.55, "BUY YES", "trade_candidate"),
            make_eval("b", "BTC", 200.0, 0.15, 0.7, "BUY YES", "trade_candidate"),
        ];
        let mut settled = HashMap::new();
        settled.insert("a".to_string(), make_settled("a", "BTC", false, -50.0));
        settled.insert("b".to_string(), make_settled("b", "BTC", true, 80.0));

        let params = SweepParams {
            label: "edge=0.14".to_string(),
            edge_threshold: 0.14,
            yes_edge_threshold_mult: 1.0,
            no_edge_threshold_mult: 1.0,
            prob_shrinkage_alpha: None,
            min_time_to_expiry_secs: 60.0,
            max_net_edge: None,
            asset_edge_mults: HashMap::new(),
        };

        let result = run_sweep(&params, &evals, &settled);
        // Only eval "b" passes threshold=0.14 (0.09+0.03<0.14, 0.15>0.14)
        assert_eq!(result.simulated_trades, 1);
        assert_eq!(result.wins, 1);
    }

    #[test]
    fn sweep_tte_filter_removes_short_tte() {
        let evals = vec![
            make_eval("a", "BTC", 50.0, 0.15, 0.7, "BUY YES", "trade_candidate"),
            make_eval("b", "BTC", 200.0, 0.15, 0.7, "BUY YES", "trade_candidate"),
        ];
        let mut settled = HashMap::new();
        settled.insert("a".to_string(), make_settled("a", "BTC", true, 50.0));
        settled.insert("b".to_string(), make_settled("b", "BTC", true, 50.0));

        let params = SweepParams {
            label: "tte=120".to_string(),
            edge_threshold: 0.11,
            yes_edge_threshold_mult: 1.0,
            no_edge_threshold_mult: 1.0,
            prob_shrinkage_alpha: None,
            min_time_to_expiry_secs: 120.0,
            max_net_edge: None,
            asset_edge_mults: HashMap::new(),
        };

        let result = run_sweep(&params, &evals, &settled);
        assert_eq!(result.simulated_trades, 1); // only "b" passes TTE
    }

    #[test]
    fn sweep_shrinkage_changes_edge() {
        // With alpha=0.5 and market_implied=0.5, posterior=0.7 shrinks to 0.6
        // Gross edge: 0.6 - 0.50 = 0.10, net = 0.10 - 0.02 = 0.08
        // With threshold=0.11, this should be filtered out
        let evals = vec![make_eval(
            "a",
            "BTC",
            200.0,
            0.18,
            0.7,
            "BUY YES",
            "trade_candidate",
        )];
        let mut settled = HashMap::new();
        settled.insert("a".to_string(), make_settled("a", "BTC", true, 50.0));

        let params = SweepParams {
            label: "shrink=0.5".to_string(),
            edge_threshold: 0.11,
            yes_edge_threshold_mult: 1.0,
            no_edge_threshold_mult: 1.0,
            prob_shrinkage_alpha: Some(0.5),
            min_time_to_expiry_secs: 60.0,
            max_net_edge: None,
            asset_edge_mults: HashMap::new(),
        };

        let result = run_sweep(&params, &evals, &settled);
        // Shrunk posterior = 0.5*0.7 + 0.5*0.5 = 0.60
        // New gross = 0.60 - 0.50 = 0.10, new net = 0.10 - 0.02 = 0.08
        // 0.08 < 0.11 threshold -> filtered
        assert_eq!(result.simulated_trades, 0);
    }
}
