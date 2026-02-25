use std::collections::HashMap;
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};

use anyhow::{bail, Context};
use chrono::{DateTime, Utc};

// ── Data structures ────────────────────────────────────────────────

/// Extracted from `position_settled` events.
#[derive(Debug, Clone)]
#[allow(dead_code)]
struct SettledTrade {
    condition_id: String,
    asset: String,
    side: String,
    model_type: String,
    predicted_yes_prob: f64,
    model_yes_prob: Option<f64>,
    actual_yes_outcome: bool,
    won: bool,
    realized_pnl: f64,
    avg_entry_price: f64,
    quantity: f64,
    window_duration_secs: Option<i64>,
}

/// Extracted from `pipeline_evaluation` events (trade_candidate only).
#[derive(Debug, Clone)]
struct TradeEval {
    net_edge: f64,
    time_to_expiry_secs: f64,
}

/// A single price observation from the pipeline logs.
#[derive(Debug, Clone)]
struct PriceObservation {
    asset: String,
    price: f64,
    timestamp: DateTime<Utc>,
}

/// Vol/model context at the time of a traded pipeline evaluation.
#[derive(Debug, Clone)]
#[allow(dead_code)]
struct VolAtTrade {
    condition_id: String,
    asset: String,
    model_vol: f64,
    spot: f64,
    time_to_expiry: f64,
    strike: f64,
    model_type: String,
    reference_price: Option<f64>,
}

#[derive(Debug, Default, Clone)]
struct BucketStats {
    trades: u32,
    wins: u32,
    total_pnl: f64,
}

impl BucketStats {
    fn record(&mut self, won: bool, pnl: f64) {
        self.trades += 1;
        if won {
            self.wins += 1;
        }
        self.total_pnl += pnl;
    }

    fn win_pct(&self) -> f64 {
        if self.trades == 0 {
            0.0
        } else {
            self.wins as f64 / self.trades as f64 * 100.0
        }
    }

    fn avg_pnl(&self) -> f64 {
        if self.trades == 0 {
            0.0
        } else {
            self.total_pnl / self.trades as f64
        }
    }
}

// ── Calibration bin ────────────────────────────────────────────────

#[derive(Debug, Default, Clone, Copy)]
struct CalibrationBin {
    count: u32,
    sum_predicted: f64,
    actual_yes_count: u32,
}

impl CalibrationBin {
    fn record(&mut self, predicted: f64, actual_yes: bool) {
        self.count += 1;
        self.sum_predicted += predicted;
        if actual_yes {
            self.actual_yes_count += 1;
        }
    }

    fn avg_predicted(&self) -> f64 {
        if self.count == 0 {
            0.0
        } else {
            self.sum_predicted / self.count as f64
        }
    }

    fn actual_rate(&self) -> f64 {
        if self.count == 0 {
            0.0
        } else {
            self.actual_yes_count as f64 / self.count as f64
        }
    }
}

// ── Parsing ────────────────────────────────────────────────────────

fn parse_settled(payload: &serde_json::Value) -> Option<SettledTrade> {
    let p = payload.get("payload")?;
    let condition_id = p.get("condition_id")?.as_str()?.to_string();
    let asset = p.get("asset")?.as_str()?.to_string();
    let side = p.get("side")?.as_str()?.to_string();
    let model_type = p
        .get("model_type")
        .and_then(|v| v.as_str())
        .unwrap_or("unknown")
        .to_string();
    let predicted_yes_prob = p.get("predicted_yes_prob")?.as_f64()?;
    let model_yes_prob = p.get("model_yes_prob").and_then(|v| v.as_f64());
    let actual_yes_outcome = p.get("actual_yes_outcome")?.as_bool()?;
    let won = p.get("won")?.as_bool()?;
    let realized_pnl = p.get("realized_pnl")?.as_f64()?;
    let avg_entry_price = p.get("avg_entry_price")?.as_f64()?;
    let quantity = p.get("quantity")?.as_f64()?;

    // Compute window duration from end_time and window_start
    let window_duration_secs = (|| {
        let end_str = p.get("end_time")?.as_str()?;
        let start_str = p.get("window_start")?.as_str()?;
        let end = chrono::DateTime::parse_from_rfc3339(end_str).ok()?;
        let start = chrono::DateTime::parse_from_rfc3339(start_str).ok()?;
        Some((end - start).num_seconds())
    })();

    Some(SettledTrade {
        condition_id,
        asset,
        side,
        model_type,
        predicted_yes_prob,
        model_yes_prob,
        actual_yes_outcome,
        won,
        realized_pnl,
        avg_entry_price,
        quantity,
        window_duration_secs,
    })
}

fn parse_trade_eval(payload: &serde_json::Value) -> Option<(String, TradeEval)> {
    let p = payload.get("payload")?;
    let action_kind = p.get("action")?.get("kind")?.as_str()?;
    if action_kind != "trade_candidate" {
        return None;
    }
    let condition_id = p.get("market")?.get("condition_id")?.as_str()?.to_string();
    let net_edge = p.get("cost_context")?.get("net_edge")?.as_f64()?;
    let time_to_expiry_secs = p
        .get("model_context")?
        .get("time_to_expiry_secs")?
        .as_f64()?;

    Some((
        condition_id,
        TradeEval {
            net_edge,
            time_to_expiry_secs,
        },
    ))
}

/// Extract price observations from a pipeline_evaluation event.
/// Each observation in `price_context.observations` becomes a PriceObservation.
fn parse_price_observations(payload: &serde_json::Value) -> Vec<PriceObservation> {
    let mut out = Vec::new();
    let Some(p) = payload.get("payload") else {
        return out;
    };
    let asset = p
        .get("market")
        .and_then(|m| m.get("asset"))
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    if asset.is_empty() {
        return out;
    }
    let Some(obs_arr) = p
        .get("price_context")
        .and_then(|pc| pc.get("observations"))
        .and_then(|v| v.as_array())
    else {
        return out;
    };
    for obs in obs_arr {
        let Some(price) = obs.get("price").and_then(|v| v.as_f64()) else {
            continue;
        };
        let Some(ts_str) = obs.get("timestamp").and_then(|v| v.as_str()) else {
            continue;
        };
        let Ok(ts) = chrono::DateTime::parse_from_rfc3339(ts_str) else {
            continue;
        };
        if price > 0.0 {
            out.push(PriceObservation {
                asset: asset.clone(),
                price,
                timestamp: ts.with_timezone(&Utc),
            });
        }
    }
    out
}

/// Extract vol/model context from a trade_candidate pipeline_evaluation.
fn parse_vol_at_trade(payload: &serde_json::Value) -> Option<VolAtTrade> {
    let p = payload.get("payload")?;
    let action_kind = p.get("action")?.get("kind")?.as_str()?;
    if action_kind != "trade_candidate" {
        return None;
    }
    let condition_id = p.get("market")?.get("condition_id")?.as_str()?.to_string();
    let asset = p.get("market")?.get("asset")?.as_str()?.to_string();
    let mc = p.get("model_context")?;
    let model_vol = mc.get("model_volatility")?.as_f64()?;
    let spot = p
        .get("price_context")?
        .get("aggregated_spot")?
        .as_f64()?;
    let time_to_expiry = mc.get("time_to_expiry_secs")?.as_f64()?;
    let strike = mc.get("strike_price").and_then(|v| v.as_f64()).unwrap_or(0.0);
    let model_type = mc
        .get("model_type")
        .and_then(|v| v.as_str())
        .unwrap_or("unknown")
        .to_string();
    let reference_price = mc.get("reference_price").and_then(|v| v.as_f64());

    Some(VolAtTrade {
        condition_id,
        asset,
        model_vol,
        spot,
        time_to_expiry,
        strike,
        model_type,
        reference_price,
    })
}

// ── Log file discovery ─────────────────────────────────────────────

fn resolve_log_dirs(user_dirs: &[String]) -> anyhow::Result<Vec<PathBuf>> {
    if user_dirs.is_empty() {
        // Auto-find latest day directory under logs/
        let day_dirs = all_day_dirs()?;
        match day_dirs.last() {
            Some(latest) => Ok(vec![latest.clone()]),
            None => bail!("No YYYY-MM-DD directories found in logs/"),
        }
    } else {
        let dirs: Vec<PathBuf> = user_dirs.iter().map(PathBuf::from).collect();
        for d in &dirs {
            if !d.is_dir() {
                bail!("Not a directory: {}", d.display());
            }
        }
        Ok(dirs)
    }
}

/// Return ALL YYYY-MM-DD day directories under logs/, sorted chronologically.
fn all_day_dirs() -> anyhow::Result<Vec<PathBuf>> {
    let logs_root = Path::new("logs");
    if !logs_root.is_dir() {
        bail!("No logs/ directory found. Specify a path: cargo run -- --analyze <dir>");
    }
    let mut day_dirs: Vec<PathBuf> = std::fs::read_dir(logs_root)?
        .filter_map(|e| e.ok())
        .filter(|e| e.file_type().map(|ft| ft.is_dir()).unwrap_or(false))
        .filter(|e| {
            let name = e.file_name();
            let name = name.to_string_lossy();
            name.len() == 10
                && name.chars().nth(4) == Some('-')
                && name.chars().nth(7) == Some('-')
        })
        .map(|e| e.path())
        .collect();
    day_dirs.sort();
    Ok(day_dirs)
}

fn find_event_files(dirs: &[PathBuf]) -> anyhow::Result<Vec<PathBuf>> {
    let mut files = Vec::new();
    for dir in dirs {
        for entry in std::fs::read_dir(dir)
            .with_context(|| format!("Failed to read directory: {}", dir.display()))?
        {
            let entry = entry?;
            let name = entry.file_name();
            let name = name.to_string_lossy();
            // Match events-*.json but not traded-decisions-*
            if name.starts_with("events-") && name.ends_with(".json") {
                files.push(entry.path());
            }
        }
    }
    files.sort();
    if files.is_empty() {
        bail!("No events-*.json files found in the specified directories");
    }
    Ok(files)
}

// ── Run discovery ─────────────────────────────────────────────────

#[derive(Debug)]
struct RunInfo {
    run_id: String,
    bot_version: String,
    first_event_ts: String,
    last_event_ts: String,
    event_count: u64,
    settled_count: u32,
    candidate_count: u32,
    wins: u32,
    losses: u32,
    total_pnl: f64,
}

/// Lightweight first pass: scan all JSONL files and extract per-run summary info.
/// Only reads top-level fields (no deep payload parsing except for position_settled).
fn discover_runs(files: &[PathBuf]) -> anyhow::Result<Vec<RunInfo>> {
    let mut runs: HashMap<String, RunInfo> = HashMap::new();

    for file in files {
        let f = std::fs::File::open(file)
            .with_context(|| format!("Failed to open {}", file.display()))?;
        let reader = BufReader::new(f);
        for line in reader.lines() {
            let line = line?;
            if line.is_empty() {
                continue;
            }
            // Extract run_id with fast string search before full parse
            let run_id = match extract_json_str(&line, "run_id") {
                Some(id) => id,
                None => continue,
            };
            let bot_version = extract_json_str(&line, "bot_version").unwrap_or_default();
            let event_ts = extract_json_str(&line, "event_ts").unwrap_or_default();

            let info = runs.entry(run_id.clone()).or_insert_with(|| RunInfo {
                run_id,
                bot_version: bot_version.clone(),
                first_event_ts: event_ts.clone(),
                last_event_ts: event_ts.clone(),
                event_count: 0,
                settled_count: 0,
                candidate_count: 0,
                wins: 0,
                losses: 0,
                total_pnl: 0.0,
            });

            info.event_count += 1;
            if !event_ts.is_empty() {
                if event_ts < info.first_event_ts || info.first_event_ts.is_empty() {
                    info.first_event_ts = event_ts.clone();
                }
                if event_ts > info.last_event_ts {
                    info.last_event_ts = event_ts.clone();
                }
            }
            if !bot_version.is_empty() && info.bot_version.is_empty() {
                info.bot_version = bot_version;
            }

            // Check event type for settled/candidate counts
            if line.contains("\"position_settled\"") {
                info.settled_count += 1;
                // Peek into payload for won + realized_pnl
                if let Ok(val) = serde_json::from_str::<serde_json::Value>(&line) {
                    if let Some(p) = val.get("payload") {
                        if let Some(won) = p.get("won").and_then(|v| v.as_bool()) {
                            if won {
                                info.wins += 1;
                            } else {
                                info.losses += 1;
                            }
                        }
                        if let Some(pnl) = p.get("realized_pnl").and_then(|v| v.as_f64()) {
                            info.total_pnl += pnl;
                        }
                    }
                }
            } else if line.contains("\"pipeline_evaluation\"")
                && line.contains("\"trade_candidate\"")
            {
                info.candidate_count += 1;
            }
        }
    }

    let mut run_list: Vec<RunInfo> = runs.into_values().collect();
    run_list.sort_by(|a, b| a.first_event_ts.cmp(&b.first_event_ts));
    Ok(run_list)
}

/// Fast extraction of a top-level JSON string field without full parse.
fn extract_json_str(line: &str, key: &str) -> Option<String> {
    let pattern = format!("\"{}\":\"", key);
    let start = line.find(&pattern)? + pattern.len();
    let rest = &line[start..];
    let end = rest.find('"')?;
    Some(rest[..end].to_string())
}

/// Format an event_ts string (RFC3339 with offset) into a short display form.
fn format_event_ts(ts: &str) -> String {
    if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(ts) {
        // Determine EST/EDT from the offset (-05:00 = EST, -04:00 = EDT)
        let tz_label = match dt.offset().local_minus_utc() {
            -18000 => "EST",
            -14400 => "EDT",
            _ => "ET",
        };
        format!("{} {}", dt.format("%m-%d %H:%M"), tz_label)
    } else {
        ts.to_string()
    }
}

/// Resolve a --run argument to an actual run_id.
/// If it parses as a positive integer, treat as 1-based index into the chronological run list.
/// Otherwise, exact run_id match.
fn resolve_run_id(runs: &[RunInfo], selector: &str) -> anyhow::Result<String> {
    // Try as 1-based index
    if let Ok(idx) = selector.parse::<usize>() {
        if idx == 0 {
            bail!("Run index must be >= 1 (got 0)");
        }
        if idx > runs.len() {
            bail!(
                "Run index {} out of range (only {} runs found). Use --runs to list them.",
                idx,
                runs.len()
            );
        }
        return Ok(runs[idx - 1].run_id.clone());
    }

    // Try exact match
    for r in runs {
        if r.run_id == selector {
            return Ok(r.run_id.clone());
        }
    }

    // Try prefix match
    let matches: Vec<&RunInfo> = runs.iter().filter(|r| r.run_id.starts_with(selector)).collect();
    match matches.len() {
        0 => bail!(
            "No run matching '{}'. Use --runs to list available runs.",
            selector
        ),
        1 => Ok(matches[0].run_id.clone()),
        n => bail!(
            "Ambiguous selector '{}' matches {} runs. Use --runs to list them.",
            selector,
            n
        ),
    }
}

/// Entry point for `--runs`: list all runs found in the given log directories.
/// When no dirs are specified, scans ALL day directories (not just the latest).
pub fn list_runs(user_dirs: &[String]) -> anyhow::Result<()> {
    let dirs = if user_dirs.is_empty() {
        let all = all_day_dirs()?;
        if all.is_empty() {
            bail!("No YYYY-MM-DD directories found in logs/");
        }
        all
    } else {
        resolve_log_dirs(user_dirs)?
    };
    let dir_labels: Vec<String> = dirs.iter().map(|d| d.display().to_string()).collect();
    let files = find_event_files(&dirs)?;
    let runs = discover_runs(&files)?;

    if runs.is_empty() {
        println!("No runs found in: {}", dir_labels.join(", "));
        return Ok(());
    }

    println!();
    println!("  Logs: {}", dir_labels.join(", "));
    println!();
    println!(
        "  {:<4} {:<46} {:<8} {:<24} {:<24} {:>5}  {:>7}  {:>5}  {:>10}",
        "#", "Run ID", "Version", "First Event", "Last Event", "Cands", "Settled", "W/L", "Total P&L"
    );
    println!("  {}", "\u{2500}".repeat(140));

    for (i, r) in runs.iter().enumerate() {
        let wl = if r.settled_count > 0 {
            format!("{}/{}", r.wins, r.losses)
        } else {
            "-".to_string()
        };
        let pnl = if r.settled_count > 0 {
            format!("{:+.2}", r.total_pnl)
        } else {
            "-".to_string()
        };
        println!(
            "  {:<4} {:<46} {:<8} {:<24} {:<24} {:>5}  {:>7}  {:>5}  {:>10}",
            i + 1,
            r.run_id,
            r.bot_version,
            format_event_ts(&r.first_event_ts),
            format_event_ts(&r.last_event_ts),
            r.candidate_count,
            r.settled_count,
            wl,
            pnl,
        );
    }

    println!();
    println!("  Use --analyze --run <#> to analyze a specific run.");
    println!();
    Ok(())
}

// ── Single-pass JSONL reader ───────────────────────────────────────

struct ParsedLogs {
    settled: Vec<SettledTrade>,
    evals: HashMap<String, TradeEval>,
    observations_by_asset: HashMap<String, Vec<PriceObservation>>,
    vol_at_trades: HashMap<String, VolAtTrade>,
}

fn parse_logs(files: &[PathBuf], run_id_filter: Option<&str>) -> anyhow::Result<ParsedLogs> {
    let mut settled = Vec::new();
    let mut evals: HashMap<String, TradeEval> = HashMap::new();
    let mut observations_by_asset: HashMap<String, Vec<PriceObservation>> = HashMap::new();
    let mut vol_at_trades: HashMap<String, VolAtTrade> = HashMap::new();

    for file in files {
        let content = std::fs::read_to_string(file)
            .with_context(|| format!("Failed to read {}", file.display()))?;
        for line in content.lines() {
            if line.is_empty() {
                continue;
            }
            // Fast run_id filter before any JSON parsing
            if let Some(filter_id) = run_id_filter {
                match extract_json_str(line, "run_id") {
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
                    if let Some(trade) = parse_settled(&val) {
                        settled.push(trade);
                    }
                }
                "pipeline_evaluation" => {
                    // Extract price observations from ALL pipeline evals
                    for obs in parse_price_observations(&val) {
                        observations_by_asset
                            .entry(obs.asset.clone())
                            .or_default()
                            .push(obs);
                    }
                    if let Some((cid, eval)) = parse_trade_eval(&val) {
                        evals.insert(cid.clone(), eval);
                    }
                    if let Some(vat) = parse_vol_at_trade(&val) {
                        vol_at_trades.insert(vat.condition_id.clone(), vat);
                    }
                }
                _ => {}
            }
        }
    }

    // Sort and deduplicate observations by timestamp per asset
    for obs_vec in observations_by_asset.values_mut() {
        obs_vec.sort_by_key(|o| o.timestamp);
        obs_vec.dedup_by(|a, b| a.timestamp == b.timestamp && (a.price - b.price).abs() < 1e-10);
    }

    Ok(ParsedLogs {
        settled,
        evals,
        observations_by_asset,
        vol_at_trades,
    })
}

// ── Report helpers ─────────────────────────────────────────────────

fn timeframe_bucket(window_secs: Option<i64>) -> &'static str {
    match window_secs {
        Some(s) if s <= 300 => "5min",
        Some(s) if s <= 900 => "15min",
        Some(_) => "other",
        None => "unknown",
    }
}

fn tte_bucket(tte_secs: f64) -> &'static str {
    if tte_secs < 60.0 {
        "0-60s"
    } else if tte_secs < 120.0 {
        "60-120s"
    } else if tte_secs < 180.0 {
        "120-180s"
    } else if tte_secs < 300.0 {
        "180-300s"
    } else {
        "300s+"
    }
}

fn edge_bucket(edge: f64) -> &'static str {
    if edge < 0.10 {
        "<0.10"
    } else if edge < 0.15 {
        "0.10-0.15"
    } else if edge < 0.20 {
        "0.15-0.20"
    } else if edge < 0.25 {
        "0.20-0.25"
    } else {
        "0.25+"
    }
}

fn print_bucket_table(title: &str, buckets: &[(String, BucketStats)]) {
    println!("\n=== {} ===", title);
    println!(
        "  {:<20} {:>6}   {:>4}   {:>6}   {:>10}   {:>8}",
        "Category", "Trades", "Wins", "Win%", "Total P&L", "Avg P&L"
    );
    println!("  {}", "-".repeat(64));

    let mut total = BucketStats::default();
    for (label, stats) in buckets {
        if stats.trades == 0 {
            continue;
        }
        total.trades += stats.trades;
        total.wins += stats.wins;
        total.total_pnl += stats.total_pnl;
        println!(
            "  {:<20} {:>6}   {:>4}   {:>5.1}%   {:>+10.2}   {:>+8.2}",
            label,
            stats.trades,
            stats.wins,
            stats.win_pct(),
            stats.total_pnl,
            stats.avg_pnl(),
        );
    }
    println!("  {}", "-".repeat(64));
    println!(
        "  {:<20} {:>6}   {:>4}   {:>5.1}%   {:>+10.2}   {:>+8.2}",
        "TOTAL",
        total.trades,
        total.wins,
        total.win_pct(),
        total.total_pnl,
        total.avg_pnl(),
    );
}

// ── Report 1: Win Rate Breakdown ───────────────────────────────────

fn report_win_rate(settled: &[SettledTrade], evals: &HashMap<String, TradeEval>) {
    // By asset
    let mut by_asset: HashMap<String, BucketStats> = HashMap::new();
    for t in settled {
        by_asset
            .entry(t.asset.clone())
            .or_default()
            .record(t.won, t.realized_pnl);
    }
    let mut asset_buckets: Vec<_> = by_asset.into_iter().collect();
    asset_buckets.sort_by(|a, b| a.0.cmp(&b.0));
    print_bucket_table("Win Rate by Asset", &asset_buckets);

    // By model type
    let mut by_model: HashMap<String, BucketStats> = HashMap::new();
    for t in settled {
        by_model
            .entry(t.model_type.clone())
            .or_default()
            .record(t.won, t.realized_pnl);
    }
    let mut model_buckets: Vec<_> = by_model.into_iter().collect();
    model_buckets.sort_by(|a, b| a.0.cmp(&b.0));
    print_bucket_table("Win Rate by Model Type", &model_buckets);

    // By side
    let mut by_side: HashMap<String, BucketStats> = HashMap::new();
    for t in settled {
        by_side
            .entry(t.side.clone())
            .or_default()
            .record(t.won, t.realized_pnl);
    }
    let mut side_buckets: Vec<_> = by_side.into_iter().collect();
    side_buckets.sort_by(|a, b| a.0.cmp(&b.0));
    print_bucket_table("Win Rate by Side", &side_buckets);

    // By timeframe
    let mut by_tf: HashMap<String, BucketStats> = HashMap::new();
    for t in settled {
        by_tf
            .entry(timeframe_bucket(t.window_duration_secs).to_string())
            .or_default()
            .record(t.won, t.realized_pnl);
    }
    let tf_order = ["5min", "15min", "other", "unknown"];
    let tf_buckets: Vec<_> = tf_order
        .iter()
        .filter_map(|k| {
            by_tf
                .remove(*k)
                .map(|v| (k.to_string(), v))
        })
        .collect();
    print_bucket_table("Win Rate by Timeframe", &tf_buckets);

    // By time-to-expiry (needs eval data)
    let mut by_tte: HashMap<String, BucketStats> = HashMap::new();
    for t in settled {
        if let Some(eval) = evals.get(&t.condition_id) {
            by_tte
                .entry(tte_bucket(eval.time_to_expiry_secs).to_string())
                .or_default()
                .record(t.won, t.realized_pnl);
        }
    }
    let tte_order = ["0-60s", "60-120s", "120-180s", "180-300s", "300s+"];
    let tte_buckets: Vec<_> = tte_order
        .iter()
        .filter_map(|k| {
            by_tte
                .remove(*k)
                .map(|v| (k.to_string(), v))
        })
        .collect();
    print_bucket_table("Win Rate by Time-to-Expiry", &tte_buckets);

    // By edge size (needs eval data)
    let mut by_edge: HashMap<String, BucketStats> = HashMap::new();
    for t in settled {
        if let Some(eval) = evals.get(&t.condition_id) {
            by_edge
                .entry(edge_bucket(eval.net_edge).to_string())
                .or_default()
                .record(t.won, t.realized_pnl);
        }
    }
    let edge_order = ["<0.10", "0.10-0.15", "0.15-0.20", "0.20-0.25", "0.25+"];
    let edge_buckets: Vec<_> = edge_order
        .iter()
        .filter_map(|k| {
            by_edge
                .remove(*k)
                .map(|v| (k.to_string(), v))
        })
        .collect();
    print_bucket_table("Win Rate by Edge Size", &edge_buckets);
}

// ── Report 2: Calibration Curve ────────────────────────────────────

fn brier_score(settled: &[SettledTrade]) -> f64 {
    if settled.is_empty() {
        return 0.0;
    }
    let sum: f64 = settled
        .iter()
        .map(|t| {
            let outcome = if t.actual_yes_outcome { 1.0 } else { 0.0 };
            (t.predicted_yes_prob - outcome).powi(2)
        })
        .sum();
    sum / settled.len() as f64
}

fn model_brier_score(settled: &[SettledTrade]) -> Option<f64> {
    let with_model: Vec<_> = settled
        .iter()
        .filter_map(|t| t.model_yes_prob.map(|p| (p, t.actual_yes_outcome)))
        .collect();
    if with_model.is_empty() {
        return None;
    }
    let sum: f64 = with_model
        .iter()
        .map(|(p, actual)| {
            let outcome = if *actual { 1.0 } else { 0.0 };
            (p - outcome).powi(2)
        })
        .sum();
    Some(sum / with_model.len() as f64)
}

fn report_calibration(settled: &[SettledTrade]) {
    // Adjusted (blended) calibration
    println!("\n=== Calibration: Adjusted Posterior (blended with market) ===");
    println!(
        "  {:<14} {:>5}  {:>8}  {:>8}  {:>6}",
        "Predicted", "Count", "Avg Pred", "Actual", "Gap"
    );
    println!("  {}", "-".repeat(50));

    let mut bins = [CalibrationBin::default(); 10];

    for t in settled {
        let idx = ((t.predicted_yes_prob * 10.0).floor() as usize).min(9);
        bins[idx].record(t.predicted_yes_prob, t.actual_yes_outcome);
    }

    for (i, bin) in bins.iter().enumerate() {
        if bin.count == 0 {
            continue;
        }
        let lo = i as f64 / 10.0;
        let hi = (i + 1) as f64 / 10.0;
        let gap = bin.actual_rate() - bin.avg_predicted();
        let flag = if gap.abs() > 0.10 {
            if gap > 0.0 {
                "  <- OVER"
            } else {
                "  <- UNDER"
            }
        } else {
            ""
        };
        println!(
            "  [{:.1}, {:.1})   {:>5}   {:>7.3}   {:>7.3}  {:>+6.2}{}",
            lo,
            hi,
            bin.count,
            bin.avg_predicted(),
            bin.actual_rate(),
            gap,
            flag,
        );
    }

    // Model (pure Black-Scholes) calibration
    let has_model_prob = settled.iter().any(|t| t.model_yes_prob.is_some());
    if has_model_prob {
        println!("\n=== Calibration: Model Posterior (pure model, no market blend) ===");
        println!(
            "  {:<14} {:>5}  {:>8}  {:>8}  {:>6}",
            "Predicted", "Count", "Avg Pred", "Actual", "Gap"
        );
        println!("  {}", "-".repeat(50));

        let mut model_bins = [CalibrationBin::default(); 10];

        for t in settled {
            if let Some(mp) = t.model_yes_prob {
                let idx = ((mp * 10.0).floor() as usize).min(9);
                model_bins[idx].record(mp, t.actual_yes_outcome);
            }
        }

        for (i, bin) in model_bins.iter().enumerate() {
            if bin.count == 0 {
                continue;
            }
            let lo = i as f64 / 10.0;
            let hi = (i + 1) as f64 / 10.0;
            let gap = bin.actual_rate() - bin.avg_predicted();
            let flag = if gap.abs() > 0.10 {
                if gap > 0.0 {
                    "  <- OVER"
                } else {
                    "  <- UNDER"
                }
            } else {
                ""
            };
            println!(
                "  [{:.1}, {:.1})   {:>5}   {:>7.3}   {:>7.3}  {:>+6.2}{}",
                lo,
                hi,
                bin.count,
                bin.avg_predicted(),
                bin.actual_rate(),
                gap,
                flag,
            );
        }
    }

    // Brier scores
    let adjusted_bs = brier_score(settled);
    let model_bs = model_brier_score(settled);
    println!();
    println!(
        "  Adjusted Brier Score: {:.4}  (lower is better; 0.25 = random)",
        adjusted_bs
    );
    if let Some(mbs) = model_bs {
        println!(
            "  Model Brier Score:    {:.4}  (pure model, no market blend)",
            mbs
        );
    } else {
        println!("  Model Brier Score:    N/A  (no model_yes_prob in logs; run with updated bot)");
    }
}

// ── Report 3: Edge Predictiveness ──────────────────────────────────

fn report_edge_predictiveness(settled: &[SettledTrade], evals: &HashMap<String, TradeEval>) {
    println!("\n=== Edge Predictiveness ===");
    println!(
        "  {:<14} {:>6}   {:>6}   {:>10}   {:>8}",
        "Net Edge", "Trades", "Win%", "Total P&L", "Avg P&L"
    );
    println!("  {}", "-".repeat(54));

    let edge_order = ["<0.10", "0.10-0.15", "0.15-0.20", "0.20-0.25", "0.25+"];
    let mut by_edge: HashMap<String, BucketStats> = HashMap::new();

    for t in settled {
        if let Some(eval) = evals.get(&t.condition_id) {
            by_edge
                .entry(edge_bucket(eval.net_edge).to_string())
                .or_default()
                .record(t.won, t.realized_pnl);
        }
    }

    for label in &edge_order {
        if let Some(stats) = by_edge.get(*label) {
            if stats.trades > 0 {
                println!(
                    "  {:<14} {:>6}   {:>5.1}%   {:>+10.2}   {:>+8.2}",
                    label,
                    stats.trades,
                    stats.win_pct(),
                    stats.total_pnl,
                    stats.avg_pnl(),
                );
            }
        }
    }

    // Find minimum edge threshold for >= 60% cumulative win rate
    // Iterate from highest edge bucket down
    let mut cum_trades: u32 = 0;
    let mut cum_wins: u32 = 0;
    let mut min_edge_for_60 = None;
    let thresholds: &[(&str, f64)] = &[
        ("0.25+", 0.25),
        ("0.20-0.25", 0.20),
        ("0.15-0.20", 0.15),
        ("0.10-0.15", 0.10),
        ("<0.10", 0.0),
    ];

    for (label, threshold) in thresholds {
        if let Some(stats) = by_edge.get(*label) {
            cum_trades += stats.trades;
            cum_wins += stats.wins;
        }
        if cum_trades > 0 {
            let cum_rate = cum_wins as f64 / cum_trades as f64 * 100.0;
            if cum_rate >= 60.0 {
                min_edge_for_60 = Some(*threshold);
            }
        }
    }

    println!();
    match min_edge_for_60 {
        Some(edge) => println!(
            "  -> Minimum edge for >=60% win rate: {:.2}",
            edge
        ),
        None => println!("  -> No edge threshold achieves >=60% win rate in this dataset"),
    }
}

// ── Report 4: EWMA Lambda Grid Search ──────────────────────────────

fn report_ewma_lambda_grid(
    observations_by_asset: &HashMap<String, Vec<PriceObservation>>,
    settled: &[SettledTrade],
    vol_at_trades: &HashMap<String, VolAtTrade>,
) {
    use crate::bayesian::models;
    use crate::bayesian::volatility::VolatilityEstimator;

    let lambdas = [0.85, 0.88, 0.90, 0.92, 0.94, 0.97];

    // Group settled trades by asset
    let mut settled_by_asset: HashMap<String, Vec<&SettledTrade>> = HashMap::new();
    for t in settled {
        settled_by_asset.entry(t.asset.clone()).or_default().push(t);
    }

    println!("\n=== EWMA Lambda Grid Search ===");

    let mut any_printed = false;
    let mut assets: Vec<&String> = observations_by_asset.keys().collect();
    assets.sort();

    for asset in assets {
        let obs = &observations_by_asset[asset];
        let asset_settled = match settled_by_asset.get(asset) {
            Some(s) if !s.is_empty() => s,
            _ => continue,
        };

        // Need at least a few trades with vol context to be meaningful
        let trades_with_context: Vec<(&SettledTrade, &VolAtTrade)> = asset_settled
            .iter()
            .filter_map(|t| {
                vol_at_trades
                    .get(&t.condition_id)
                    .map(|vat| (*t, vat))
            })
            .collect();

        if trades_with_context.len() < 3 {
            continue;
        }

        println!(
            "\n  Asset: {}  ({} observations, {} settled trades with context)",
            asset,
            obs.len(),
            trades_with_context.len()
        );
        println!(
            "  {:<8} {:>10} {:>12} {:>12}",
            "Lambda", "Mean Vol", "Brier Score", ""
        );
        println!("  {}", "-".repeat(46));

        let mut best_lambda = 0.0f64;
        let mut best_brier = f64::MAX;

        for &lambda in &lambdas {
            // Replay all observations through a VolatilityEstimator with this lambda.
            // Use 100-obs window and record vol at each timestamp.
            let mut estimator = VolatilityEstimator::new(100, 1.0, lambda);
            let mut vol_by_ts: Vec<(DateTime<Utc>, f64)> = Vec::new();

            // Estimate tick interval from observations
            let tick_secs = if obs.len() >= 2 {
                let total_span =
                    (obs.last().unwrap().timestamp - obs.first().unwrap().timestamp).num_seconds()
                        as f64;
                (total_span / (obs.len() - 1) as f64).max(1.0)
            } else {
                3.0
            };

            for o in obs {
                estimator.add_price(o.price);
                if let Some(vol) = estimator.annualized_volatility(tick_secs) {
                    vol_by_ts.push((o.timestamp, vol));
                }
            }

            if vol_by_ts.is_empty() {
                continue;
            }

            // Mean vol over all observations
            let mean_vol: f64 =
                vol_by_ts.iter().map(|(_, v)| v).sum::<f64>() / vol_by_ts.len() as f64;

            // For each settled trade, find the vol at the closest observation
            // before/at the trade's entry and recompute Brier score.
            let mut brier_sum = 0.0f64;
            let mut brier_count = 0u32;

            for (trade, vat) in &trades_with_context {
                // Find closest vol before entry. We use vol_by_ts which is sorted.
                // We don't have the exact entry timestamp, so use the vol context's
                // spot + model params but with our simulated vol.
                let simulated_vol = if let Some(pos) =
                    vol_by_ts.iter().rposition(|_| true)
                {
                    // Use the last available vol (end of replay represents latest state)
                    // In practice, we should match by time, but we don't have exact
                    // entry timestamps. Use last vol as approximation.
                    vol_by_ts[pos].1
                } else {
                    continue;
                };

                // Recompute model probability with simulated vol
                let model_prob = if vat.model_type == "updown_directional" {
                    if let Some(ref_price) = vat.reference_price {
                        models::directional_up_probability(
                            vat.spot,
                            ref_price,
                            vat.time_to_expiry,
                            simulated_vol,
                            0.0, // no drift in replay
                        )
                    } else {
                        continue;
                    }
                } else if vat.strike > 0.0 {
                    models::binary_call_probability(
                        vat.spot,
                        vat.strike,
                        vat.time_to_expiry,
                        simulated_vol,
                    )
                } else {
                    continue;
                };

                let outcome = if trade.actual_yes_outcome { 1.0 } else { 0.0 };
                brier_sum += (model_prob - outcome).powi(2);
                brier_count += 1;
            }

            if brier_count == 0 {
                continue;
            }

            let brier = brier_sum / brier_count as f64;
            let marker = if brier < best_brier { "<- best" } else { "" };
            if brier < best_brier {
                best_brier = brier;
                best_lambda = lambda;
            }

            println!(
                "  {:<8.2} {:>9.2}% {:>11.4} {:>12}",
                lambda,
                mean_vol * 100.0,
                brier,
                marker,
            );
        }

        if best_brier < f64::MAX {
            println!(
                "\n  -> Recommended lambda for {}: {:.2} (Brier: {:.4})",
                asset, best_lambda, best_brier
            );
            any_printed = true;
        }
    }

    if !any_printed {
        println!("  No assets had enough data for lambda grid search.");
    }
}

// ── Inline run filter ──────────────────────────────────────────────

/// Check user dirs for an inline run suffix, e.g. "logs/2026-02-24-2".
/// If a path doesn't exist as a directory but stripping the last `-N` yields
/// a valid directory, treat N as a run filter for that directory.
/// Returns (cleaned dirs, optional run filter string).
fn extract_inline_run_filter(user_dirs: &[String]) -> (Vec<String>, Option<String>) {
    let mut dirs = Vec::new();
    let mut run_filter: Option<String> = None;

    for d in user_dirs {
        if Path::new(d).is_dir() {
            dirs.push(d.clone());
            continue;
        }
        // Try splitting off trailing -N
        if let Some(pos) = d.rfind('-') {
            let (base, suffix) = d.split_at(pos);
            let num_part = &suffix[1..]; // skip the '-'
            if !num_part.is_empty()
                && num_part.chars().all(|c| c.is_ascii_digit())
                && Path::new(base).is_dir()
            {
                dirs.push(base.to_string());
                run_filter = Some(num_part.to_string());
                continue;
            }
        }
        // Not a dir and no inline suffix — pass through for error handling later
        dirs.push(d.clone());
    }

    (dirs, run_filter)
}

// ── Entry point ────────────────────────────────────────────────────

pub fn run(user_dirs: &[String], run_filter: Option<&str>) -> anyhow::Result<()> {
    // Check for inline run suffix: "logs/2026-02-24-2" → dir + run #2
    let (effective_dirs, inline_run) = extract_inline_run_filter(user_dirs);
    let run_selector = run_filter.or(inline_run.as_deref());

    // When a run filter is active without explicit dirs, scan all days so index
    // numbers match `--runs` output regardless of which day is "latest".
    let dirs = if run_selector.is_some() && effective_dirs.is_empty() {
        let all = all_day_dirs()?;
        if all.is_empty() {
            bail!("No YYYY-MM-DD directories found in logs/");
        }
        all
    } else {
        resolve_log_dirs(&effective_dirs)?
    };
    let dir_labels: Vec<String> = dirs.iter().map(|d| d.display().to_string()).collect();
    let files = find_event_files(&dirs)?;

    // Resolve run filter if provided
    let resolved_run_id = if let Some(selector) = run_selector {
        let runs = discover_runs(&files)?;
        let run_id = resolve_run_id(&runs, selector)?;
        Some(run_id)
    } else {
        None
    };

    let parsed = parse_logs(&files, resolved_run_id.as_deref())?;
    let ParsedLogs {
        settled,
        evals,
        observations_by_asset,
        vol_at_trades,
    } = parsed;

    if settled.is_empty() {
        if let Some(ref rid) = resolved_run_id {
            println!("No settled trades found for run: {}", rid);
        } else {
            println!("No settled trades found in the specified logs.");
        }
        return Ok(());
    }

    let matched = settled
        .iter()
        .filter(|t| evals.contains_key(&t.condition_id))
        .count();

    // Summary header
    println!();
    println!("======================================");
    println!("  Trading Performance Analysis");
    println!("  Logs: {}", dir_labels.join(", "));
    if let Some(ref rid) = resolved_run_id {
        println!("  Run:  {}", rid);
    }
    println!("  Settled trades: {}", settled.len());
    println!("  Pipeline evals (traded): {}", evals.len());
    println!("  Matched (with edge data): {}", matched);
    println!("======================================");

    if matched < settled.len() {
        println!(
            "\n  Note: {} settled trades have no matching pipeline eval.",
            settled.len() - matched
        );
        println!("  These are excluded from edge/TTE reports but included in other breakdowns.");
    }

    report_win_rate(&settled, &evals);
    report_calibration(&settled);
    report_edge_predictiveness(&settled, &evals);

    if !observations_by_asset.is_empty() {
        report_ewma_lambda_grid(&observations_by_asset, &settled, &vol_at_trades);
    }

    println!();
    Ok(())
}

// ── Tests ──────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bucket_stats_basic_math() {
        let mut b = BucketStats::default();
        b.record(true, 10.0);
        b.record(false, -5.0);
        b.record(true, 3.0);
        assert_eq!(b.trades, 3);
        assert_eq!(b.wins, 2);
        assert!((b.win_pct() - 66.666).abs() < 0.1);
        assert!((b.avg_pnl() - 2.666).abs() < 0.1);
    }

    #[test]
    fn bucket_stats_empty() {
        let b = BucketStats::default();
        assert_eq!(b.win_pct(), 0.0);
        assert_eq!(b.avg_pnl(), 0.0);
    }

    #[test]
    fn brier_score_perfect() {
        let trades = vec![
            SettledTrade {
                condition_id: "a".into(),
                asset: "BTC".into(),
                side: "YES".into(),
                model_type: "test".into(),
                predicted_yes_prob: 1.0,
                model_yes_prob: Some(1.0),
                actual_yes_outcome: true,
                won: true,
                realized_pnl: 1.0,
                avg_entry_price: 0.5,
                quantity: 10.0,
                window_duration_secs: Some(300),
            },
            SettledTrade {
                condition_id: "b".into(),
                asset: "BTC".into(),
                side: "NO".into(),
                model_type: "test".into(),
                predicted_yes_prob: 0.0,
                model_yes_prob: Some(0.0),
                actual_yes_outcome: false,
                won: true,
                realized_pnl: 1.0,
                avg_entry_price: 0.5,
                quantity: 10.0,
                window_duration_secs: Some(300),
            },
        ];
        assert!((brier_score(&trades) - 0.0).abs() < 1e-10);
    }

    #[test]
    fn brier_score_random() {
        let trades = vec![
            SettledTrade {
                condition_id: "a".into(),
                asset: "BTC".into(),
                side: "YES".into(),
                model_type: "test".into(),
                predicted_yes_prob: 0.5,
                model_yes_prob: Some(0.5),
                actual_yes_outcome: true,
                won: true,
                realized_pnl: 1.0,
                avg_entry_price: 0.5,
                quantity: 10.0,
                window_duration_secs: Some(300),
            },
            SettledTrade {
                condition_id: "b".into(),
                asset: "BTC".into(),
                side: "NO".into(),
                model_type: "test".into(),
                predicted_yes_prob: 0.5,
                model_yes_prob: Some(0.5),
                actual_yes_outcome: false,
                won: true,
                realized_pnl: 1.0,
                avg_entry_price: 0.5,
                quantity: 10.0,
                window_duration_secs: Some(300),
            },
        ];
        assert!((brier_score(&trades) - 0.25).abs() < 1e-10);
    }

    #[test]
    fn brier_score_empty() {
        assert_eq!(brier_score(&[]), 0.0);
    }

    #[test]
    fn timeframe_bucket_categorizes_correctly() {
        assert_eq!(timeframe_bucket(Some(60)), "5min");
        assert_eq!(timeframe_bucket(Some(300)), "5min");
        assert_eq!(timeframe_bucket(Some(301)), "15min");
        assert_eq!(timeframe_bucket(Some(900)), "15min");
        assert_eq!(timeframe_bucket(Some(901)), "other");
        assert_eq!(timeframe_bucket(None), "unknown");
    }

    #[test]
    fn tte_bucket_categorizes_correctly() {
        assert_eq!(tte_bucket(30.0), "0-60s");
        assert_eq!(tte_bucket(59.9), "0-60s");
        assert_eq!(tte_bucket(60.0), "60-120s");
        assert_eq!(tte_bucket(119.9), "60-120s");
        assert_eq!(tte_bucket(120.0), "120-180s");
        assert_eq!(tte_bucket(180.0), "180-300s");
        assert_eq!(tte_bucket(300.0), "300s+");
        assert_eq!(tte_bucket(1000.0), "300s+");
    }

    #[test]
    fn edge_bucket_categorizes_correctly() {
        assert_eq!(edge_bucket(0.05), "<0.10");
        assert_eq!(edge_bucket(0.099), "<0.10");
        assert_eq!(edge_bucket(0.10), "0.10-0.15");
        assert_eq!(edge_bucket(0.149), "0.10-0.15");
        assert_eq!(edge_bucket(0.15), "0.15-0.20");
        assert_eq!(edge_bucket(0.20), "0.20-0.25");
        assert_eq!(edge_bucket(0.25), "0.25+");
        assert_eq!(edge_bucket(0.50), "0.25+");
    }

    #[test]
    fn parse_settled_from_real_json() {
        let line = r#"{"event_type":"position_settled","payload":{"actual_yes_outcome":false,"asset":"SOL","avg_entry_price":0.408,"closed_at":"2026-02-23T12:56:07.024-05:00","condition_id":"0xabc","end_time":"2026-02-23T12:45:00.000-05:00","hold_seconds":1491,"market_question":"Solana test","model_type":"updown_directional","predicted_yes_prob":0.211,"quantity":116.225,"realized_pnl":68.8052,"side":"NO","token_id":"abc","window_start":"2026-02-23T12:30:00.000-05:00","won":true}}"#;
        let val: serde_json::Value = serde_json::from_str(line).unwrap();
        let trade = parse_settled(&val).expect("should parse settled trade");
        assert_eq!(trade.asset, "SOL");
        assert_eq!(trade.side, "NO");
        assert_eq!(trade.model_type, "updown_directional");
        assert!((trade.predicted_yes_prob - 0.211).abs() < 1e-6);
        assert!(!trade.actual_yes_outcome);
        assert!(trade.won);
        assert!((trade.realized_pnl - 68.8052).abs() < 1e-4);
        assert_eq!(trade.window_duration_secs, Some(900)); // 12:30 to 12:45
    }

    #[test]
    fn parse_trade_eval_from_real_json() {
        let line = r#"{"event_type":"pipeline_evaluation","payload":{"action":{"kind":"trade_candidate","side":"BUY NO"},"cost_context":{"net_edge":0.2159},"market":{"condition_id":"0xabc"},"model_context":{"time_to_expiry_secs":296.0}}}"#;
        let val: serde_json::Value = serde_json::from_str(line).unwrap();
        let (cid, eval) = parse_trade_eval(&val).expect("should parse trade eval");
        assert_eq!(cid, "0xabc");
        assert!((eval.net_edge - 0.2159).abs() < 1e-6);
        assert!((eval.time_to_expiry_secs - 296.0).abs() < 1e-6);
    }

    #[test]
    fn parse_trade_eval_skips_no_trade() {
        let line = r#"{"event_type":"pipeline_evaluation","payload":{"action":{"kind":"no_trade","reason":"edge below threshold"},"cost_context":{"net_edge":0.05},"market":{"condition_id":"0xabc"},"model_context":{"time_to_expiry_secs":100.0}}}"#;
        let val: serde_json::Value = serde_json::from_str(line).unwrap();
        assert!(parse_trade_eval(&val).is_none());
    }

    #[test]
    fn calibration_bin_math() {
        let mut bin = CalibrationBin::default();
        bin.record(0.7, true);
        bin.record(0.8, false);
        bin.record(0.75, true);
        assert_eq!(bin.count, 3);
        assert!((bin.avg_predicted() - 0.75).abs() < 1e-10);
        assert!((bin.actual_rate() - 2.0 / 3.0).abs() < 1e-10);
    }

    // CalibrationBin array default initialization test
    #[test]
    fn calibration_bin_default_is_zeroed() {
        let bins = [CalibrationBin::default(); 10];
        for bin in &bins {
            assert_eq!(bin.count, 0);
            assert_eq!(bin.sum_predicted, 0.0);
            assert_eq!(bin.actual_yes_count, 0);
        }
    }
}
