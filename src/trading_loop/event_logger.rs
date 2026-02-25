use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use chrono::{NaiveDate, Utc};
use rust_decimal::prelude::ToPrimitive;
use serde_json::{json, Value};
use tokio::io::AsyncWriteExt;
use tokio::sync::mpsc;
use tokio::time::MissedTickBehavior;
use tracing::{error, warn};

use crate::config::JsonlLoggingConfig;
use crate::price_feeds::{AggregatedPrice, PriceAsset};
use crate::timezone::{et_abbreviation, to_et};
use crate::venue::{OrderBook, OrderEvent, Side, VenueType};

use super::pipeline::{PipelineAction, PipelineResult};
use super::pnl::{ClosedTradeRecord, OutcomeTokenSide, PnlSnapshot};

const SCHEMA_VERSION: &str = "jsonl.v1";

fn fmt_et_rfc3339(ts: chrono::DateTime<Utc>) -> String {
    to_et(ts).to_rfc3339_opts(chrono::SecondsFormat::Millis, false)
}

fn now_et_day() -> NaiveDate {
    to_et(Utc::now()).date_naive()
}

fn run_id_timestamp_et() -> String {
    let now = Utc::now();
    let now_et = to_et(now);
    format!(
        "{}{}",
        now_et.format("%Y%m%dT%H%M%S%.3f"),
        et_abbreviation(now)
    )
}

#[derive(Debug, Clone)]
pub struct RunMetadata {
    pub run_id: String,
    pub mode: String,
    pub paper_mode: bool,
    pub venue_type: String,
    pub bot_version: String,
    pub git_sha: Option<String>,
    pub config_hash: String,
}

impl RunMetadata {
    pub fn new(paper_mode: bool, venue_type: VenueType, config_debug: &str) -> Self {
        let venue = match venue_type {
            VenueType::International => "international",
            VenueType::US => "us",
        }
        .to_string();

        let mode = if paper_mode { "paper" } else { "live" }.to_string();
        let run_id = format!(
            "{}-{}-{}",
            mode,
            run_id_timestamp_et(),
            std::process::id()
        );
        let bot_version = env!("CARGO_PKG_VERSION").to_string();
        let git_sha = option_env!("GIT_SHA").map(std::string::ToString::to_string);

        let mut hasher = DefaultHasher::new();
        config_debug.hash(&mut hasher);
        let config_hash = format!("{:016x}", hasher.finish());

        Self {
            run_id,
            mode,
            paper_mode,
            venue_type: venue,
            bot_version,
            git_sha,
            config_hash,
        }
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub struct LoggerHealthSnapshot {
    pub dropped_count: u64,
    pub backpressure_incidents: u64,
    pub write_error_count: u64,
}

#[derive(Debug)]
struct LoggerState {
    enabled: bool,
    tx: Option<mpsc::Sender<LogMessage>>,
    seq: AtomicU64,
    dropped_count: AtomicU64,
    backpressure_incidents: AtomicU64,
    write_error_count: Arc<AtomicU64>,
    metadata: RunMetadata,
}

#[derive(Debug)]
struct LogMessage {
    serialized: String,
    event_type: String,
}

#[derive(Debug, Clone)]
pub struct EventLoggerHandle {
    state: Arc<LoggerState>,
    no_trade_sample_rate: f64,
    orderbook_depth_levels: usize,
    log_dir: String,
}

impl EventLoggerHandle {
    pub fn new(config: JsonlLoggingConfig, metadata: RunMetadata) -> Self {
        if !config.enabled {
            return Self::disabled(metadata);
        }

        let (tx, rx) = mpsc::channel(config.channel_capacity);
        let write_error_count = Arc::new(AtomicU64::new(0));
        let log_dir = config.dir.clone();

        tokio::spawn(writer_task(
            rx,
            config.clone(),
            Arc::clone(&write_error_count),
        ));

        Self {
            state: Arc::new(LoggerState {
                enabled: true,
                tx: Some(tx),
                seq: AtomicU64::new(0),
                dropped_count: AtomicU64::new(0),
                backpressure_incidents: AtomicU64::new(0),
                write_error_count,
                metadata,
            }),
            no_trade_sample_rate: config.no_trade_sample_rate.clamp(0.0, 1.0),
            orderbook_depth_levels: config.orderbook_depth_levels.max(1),
            log_dir,
        }
    }

    pub fn disabled(metadata: RunMetadata) -> Self {
        Self {
            state: Arc::new(LoggerState {
                enabled: false,
                tx: None,
                seq: AtomicU64::new(0),
                dropped_count: AtomicU64::new(0),
                backpressure_incidents: AtomicU64::new(0),
                write_error_count: Arc::new(AtomicU64::new(0)),
                metadata,
            }),
            no_trade_sample_rate: 0.0,
            orderbook_depth_levels: 1,
            log_dir: String::new(),
        }
    }

    pub fn is_enabled(&self) -> bool {
        self.state.enabled
    }

    pub fn log_dir(&self) -> &str {
        &self.log_dir
    }

    pub fn run_metadata(&self) -> &RunMetadata {
        &self.state.metadata
    }

    pub fn no_trade_sample_rate(&self) -> f64 {
        self.no_trade_sample_rate
    }

    pub fn orderbook_depth_levels(&self) -> usize {
        self.orderbook_depth_levels
    }

    pub fn should_log_no_trade(&self) -> bool {
        if !self.state.enabled {
            return false;
        }
        if self.no_trade_sample_rate <= 0.0 {
            return false;
        }
        if self.no_trade_sample_rate >= 1.0 {
            return true;
        }
        rand::random::<f64>() < self.no_trade_sample_rate
    }

    pub fn health_snapshot(&self) -> LoggerHealthSnapshot {
        LoggerHealthSnapshot {
            dropped_count: self.state.dropped_count.load(Ordering::Relaxed),
            backpressure_incidents: self.state.backpressure_incidents.load(Ordering::Relaxed),
            write_error_count: self.state.write_error_count.load(Ordering::Relaxed),
        }
    }

    pub fn try_log_value(&self, event_type: &str, payload: Value) {
        if !self.state.enabled {
            return;
        }

        let seq = self.state.seq.fetch_add(1, Ordering::Relaxed) + 1;

        let line = json!({
            "schema_version": SCHEMA_VERSION,
            "event_type": event_type,
            "event_ts": fmt_et_rfc3339(Utc::now()),
            "seq": seq,
            "run_id": self.state.metadata.run_id,
            "mode": self.state.metadata.mode,
            "paper_mode": self.state.metadata.paper_mode,
            "venue_type": self.state.metadata.venue_type,
            "bot_version": self.state.metadata.bot_version,
            "git_sha": self.state.metadata.git_sha,
            "config_hash": self.state.metadata.config_hash,
            "payload": payload,
        });

        let Ok(serialized) = serde_json::to_string(&line) else {
            self.state.write_error_count.fetch_add(1, Ordering::Relaxed);
            return;
        };

        let Some(tx) = &self.state.tx else {
            return;
        };

        let msg = LogMessage {
            serialized,
            event_type: event_type.to_string(),
        };

        match tx.try_send(msg) {
            Ok(()) => {}
            Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
                self.state.dropped_count.fetch_add(1, Ordering::Relaxed);
                self.state
                    .backpressure_incidents
                    .fetch_add(1, Ordering::Relaxed);
            }
            Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
                self.state.dropped_count.fetch_add(1, Ordering::Relaxed);
            }
        }
    }
}

pub fn observations_payload(price: &AggregatedPrice) -> Value {
    Value::Array(
        price
            .observations
            .iter()
            .map(|obs| {
                json!({
                    "source": obs.source.to_string(),
                    "price": obs.price.to_f64(),
                    "timestamp": fmt_et_rfc3339(obs.timestamp),
                    "bid": obs.bid.and_then(|v| v.to_f64()),
                    "ask": obs.ask.and_then(|v| v.to_f64()),
                })
            })
            .collect(),
    )
}

pub fn orderbook_payload(book: &OrderBook, depth_levels: usize) -> Value {
    let depth = depth_levels.max(1);
    json!({
        "token_id": book.token_id,
        "timestamp": fmt_et_rfc3339(book.timestamp),
        "tick_size": book.tick_size.to_f64(),
        "min_order_size": book.min_order_size.to_f64(),
        "best_bid": book.best_bid().and_then(|v| v.to_f64()),
        "best_ask": book.best_ask().and_then(|v| v.to_f64()),
        "midpoint": book.midpoint().and_then(|v| v.to_f64()),
        "bids": book
            .bids
            .iter()
            .take(depth)
            .map(|level| json!({"price": level.price.to_f64(), "size": level.size.to_f64()}))
            .collect::<Vec<_>>(),
        "asks": book
            .asks
            .iter()
            .take(depth)
            .map(|level| json!({"price": level.price.to_f64(), "size": level.size.to_f64()}))
            .collect::<Vec<_>>(),
    })
}

pub fn order_event_payload(event: &OrderEvent) -> Value {
    match event {
        OrderEvent::Filled {
            order_id,
            fill_price,
            fill_amount,
        } => json!({
            "kind": "filled",
            "order_id": order_id,
            "fill_price": fill_price.to_f64(),
            "fill_amount": fill_amount.to_f64(),
        }),
        OrderEvent::PartialFill {
            order_id,
            fill_price,
            fill_amount,
        } => json!({
            "kind": "partial_fill",
            "order_id": order_id,
            "fill_price": fill_price.to_f64(),
            "fill_amount": fill_amount.to_f64(),
        }),
        OrderEvent::Cancelled { order_id } => json!({
            "kind": "cancelled",
            "order_id": order_id,
        }),
        OrderEvent::Rejected { order_id, reason } => json!({
            "kind": "rejected",
            "order_id": order_id,
            "reason": reason,
        }),
    }
}

pub fn pnl_snapshot_payload(snapshot: &PnlSnapshot, trigger: &str) -> Value {
    json!({
        "trigger": trigger,
        "capital": snapshot.starting_capital.to_f64(),
        "realized": snapshot.realized_pnl.to_f64(),
        "unrealized": snapshot.unrealized_pnl.to_f64(),
        "total": snapshot.total_pnl.to_f64(),
        "equity": snapshot.equity.to_f64(),
        "roi_pct": snapshot.roi_pct.to_f64(),
        "fees": snapshot.total_fees_paid.to_f64(),
        "open_positions": snapshot.open_positions,
        "fills": snapshot.fills,
        "trades": snapshot.trades,
        "closed_trades": snapshot.closed_trades,
        "wins": snapshot.wins,
        "losses": snapshot.losses,
        "win_rate": snapshot.win_rate.to_f64(),
        "per_asset": snapshot
            .per_asset
            .iter()
            .map(|row| {
                json!({
                    "asset": row.asset.symbol(),
                    "realized": row.realized_pnl.to_f64(),
                    "unrealized": row.unrealized_pnl.to_f64(),
                    "total": row.total_pnl.to_f64(),
                    "open_positions": row.open_positions,
                })
            })
            .collect::<Vec<_>>(),
    })
}

pub fn closed_trade_payload(record: &ClosedTradeRecord) -> Value {
    let side = match record.outcome_side {
        OutcomeTokenSide::Yes => "YES",
        OutcomeTokenSide::No => "NO",
    };
    let won = matches!(
        (record.outcome_side, record.actual_yes_outcome),
        (OutcomeTokenSide::Yes, true) | (OutcomeTokenSide::No, false)
    );

    json!({
        "opened_at": fmt_et_rfc3339(record.opened_at),
        "closed_at": fmt_et_rfc3339(record.closed_at),
        "end_time": record.end_time.map(fmt_et_rfc3339),
        "window_start": record.window_start.map(fmt_et_rfc3339),
        "resolution_available_at": record.resolution_available_at.map(fmt_et_rfc3339),
        "hold_seconds": (record.closed_at - record.opened_at).num_seconds(),
        "asset": record.asset.symbol(),
        "token_id": record.token_id,
        "condition_id": record.condition_id,
        "market_question": record.market_question,
        "side": side,
        "avg_entry_price": record.avg_entry_price.to_f64(),
        "quantity": record.quantity.to_f64(),
        "predicted_yes_prob": record.predicted_yes_prob,
        "model_yes_prob": record.model_yes_prob,
        "actual_yes_outcome": record.actual_yes_outcome,
        "payout_price": record.payout_price.to_f64(),
        "realized_pnl": record.realized_pnl.to_f64(),
        "official_price_to_beat": record.official_price_to_beat.and_then(|v| v.to_f64()),
        "official_final_price": record.official_final_price.and_then(|v| v.to_f64()),
        "settlement_source": record.settlement_source,
        "won": won,
        "model_type": record.model_type,
    })
}

pub fn pipeline_payload(
    result: &PipelineResult,
    market_asset: PriceAsset,
    price: &AggregatedPrice,
    book: &OrderBook,
    model_uncertainty: f64,
    base_edge_threshold: f64,
    current_bankroll: rust_decimal::Decimal,
    current_exposure: rust_decimal::Decimal,
    open_order_count: u32,
    depth_levels: usize,
    sample_rate: Option<f64>,
    was_sampled: bool,
) -> Value {
    let action_kind = match &result.action {
        PipelineAction::PlaceOrder { .. } => "trade_candidate",
        PipelineAction::NoTrade { .. } => "no_trade",
    };
    let reason = match &result.action {
        PipelineAction::NoTrade { reason } => Some(reason.clone()),
        _ => None,
    };

    let d = &result.diagnostics;

    json!({
        "sample_rate": sample_rate,
        "was_sampled": was_sampled,
        "market": {
            "condition_id": result.market.condition_id,
            "token_id_yes": result.market.token_id_yes,
            "token_id_no": result.market.token_id_no,
            "question": result.market.question,
            "asset": market_asset.symbol(),
            "start_time": result.market.start_time.map(fmt_et_rfc3339),
            "end_time": fmt_et_rfc3339(result.market.end_time),
            "duration_secs": (result.market.end_time - Utc::now()).num_seconds(),
            "strike_price": result.market.strike_price.to_f64(),
            "price_to_beat": result.market.polymarket_price_to_beat.and_then(|v| v.to_f64()),
            "price_to_beat_source": result.market.polymarket_price_to_beat_source.clone(),
            "liquidity": result.market.liquidity.to_f64(),
            "volume": result.market.volume.to_f64(),
        },
        "price_context": {
            "aggregated_spot": d.aggregated_spot,
            "confidence": d.confidence_score,
            "spread_ratio": d.spread_ratio,
            "num_sources": price.num_sources,
            "quorum_met": price.quorum_met,
            "observations": observations_payload(price),
        },
        "model_context": {
            "model_type": d.model_type,
            "p_model": d.p_model,
            "market_implied_p": d.market_implied_p,
            "posterior": d.posterior,
            "model_uncertainty": model_uncertainty,
            "model_volatility": d.model_volatility,
            "ewma_variance": d.ewma_variance,
            "ewma_observations": d.ewma_observations,
            "ewma_warmed_up": d.ewma_warmed_up,
            "reference_price": d.reference_price,
            "reference_price_source": d.reference_source.clone(),
            "strike_price": d.strike_price,
            "time_to_expiry_secs": d.time_to_expiry_secs,
        },
        "execution_context": {
            "orderbook": orderbook_payload(book, depth_levels),
            "executable_price": d.executable_price,
            "probe_size_tokens": book.min_order_size.to_f64(),
            "final_size_tokens": d.kelly_size_tokens.and_then(|v| v.to_f64()),
        },
        "cost_context": {
            "gross_edge": d.gross_edge,
            "fee": d.cost_fee,
            "slippage": d.cost_slippage,
            "latency": d.cost_latency,
            "total_cost": d.cost_total,
            "net_edge": d.net_edge,
            "effective_threshold": d.edge_threshold,
            "base_threshold": base_edge_threshold,
            "fee_asymmetry_flip": d.fee_asymmetry_flip,
        },
        "sizing_risk_context": {
            "kelly_raw": d.kelly_raw,
            "kelly_adjusted": d.kelly_adjusted,
            "kelly_final": d.kelly_final,
            "stake_usdc": d.kelly_stake_usdc.and_then(|v| v.to_f64()),
            "size_tokens": d.kelly_size_tokens.and_then(|v| v.to_f64()),
            "threshold_pass": d.threshold_pass,
            "risk_check_passed": matches!(result.action, PipelineAction::PlaceOrder { .. }) || !matches!(reason.as_deref(), Some(r) if r.starts_with("risk check failed")),
            "current_bankroll": current_bankroll.to_f64(),
            "current_exposure": current_exposure.to_f64(),
            "open_order_count": open_order_count,
        },
        "action": {
            "kind": action_kind,
            "side": d.edge_side,
            "reason": reason,
        }
    })
}

async fn writer_task(
    mut rx: mpsc::Receiver<LogMessage>,
    config: JsonlLoggingConfig,
    write_error_count: Arc<AtomicU64>,
) {
    let dir = PathBuf::from(&config.dir);
    if let Err(e) = tokio::fs::create_dir_all(&dir).await {
        write_error_count.fetch_add(1, Ordering::Relaxed);
        error!(error = %e, dir = %dir.display(), "failed to create JSONL log directory");
        return;
    }

    let mut current_day = now_et_day();
    let mut current_path = active_file_path(&dir, current_day);
    let mut traded_path = traded_decisions_file_path(&dir, current_day);
    let mut file = match open_append_file(&current_path).await {
        Ok(file) => file,
        Err(e) => {
            write_error_count.fetch_add(1, Ordering::Relaxed);
            error!(error = %e, path = %current_path.display(), "failed to open JSONL log file");
            return;
        }
    };
    let mut traded_file = match open_append_file(&traded_path).await {
        Ok(file) => file,
        Err(e) => {
            write_error_count.fetch_add(1, Ordering::Relaxed);
            error!(
                error = %e,
                path = %traded_path.display(),
                "failed to open traded-decisions log file"
            );
            return;
        }
    };

    let mut flush_timer = tokio::time::interval(Duration::from_millis(config.flush_interval_ms));
    flush_timer.set_missed_tick_behavior(MissedTickBehavior::Delay);

    loop {
        tokio::select! {
            msg = rx.recv() => {
                let Some(msg) = msg else {
                    break;
                };

                if let Err(e) = maybe_roll_files(
                    &mut file,
                    &mut traded_file,
                    &mut current_day,
                    &mut current_path,
                    &mut traded_path,
                    &dir,
                    &config,
                    &write_error_count,
                ).await {
                    write_error_count.fetch_add(1, Ordering::Relaxed);
                    error!(error = %e, "failed to roll JSONL log file");
                }

                if let Err(e) = file.write_all(msg.serialized.as_bytes()).await {
                    write_error_count.fetch_add(1, Ordering::Relaxed);
                    error!(error = %e, "failed writing JSONL line");
                    continue;
                }
                if let Err(e) = file.write_all(b"\n").await {
                    write_error_count.fetch_add(1, Ordering::Relaxed);
                    error!(error = %e, "failed writing JSONL newline");
                }

                if msg.event_type == "traded_decision" {
                    if let Err(e) = traded_file.write_all(msg.serialized.as_bytes()).await {
                        write_error_count.fetch_add(1, Ordering::Relaxed);
                        error!(error = %e, "failed writing traded-decision line");
                        continue;
                    }
                    if let Err(e) = traded_file.write_all(b"\n").await {
                        write_error_count.fetch_add(1, Ordering::Relaxed);
                        error!(error = %e, "failed writing traded-decision newline");
                    }
                }
            }
            _ = flush_timer.tick() => {
                if let Err(e) = maybe_roll_files(
                    &mut file,
                    &mut traded_file,
                    &mut current_day,
                    &mut current_path,
                    &mut traded_path,
                    &dir,
                    &config,
                    &write_error_count,
                ).await {
                    write_error_count.fetch_add(1, Ordering::Relaxed);
                    error!(error = %e, "failed to roll JSONL log file");
                }
                if let Err(e) = file.flush().await {
                    write_error_count.fetch_add(1, Ordering::Relaxed);
                    error!(error = %e, "failed flushing JSONL file");
                }
                if let Err(e) = traded_file.flush().await {
                    write_error_count.fetch_add(1, Ordering::Relaxed);
                    error!(error = %e, "failed flushing traded-decisions file");
                }
            }
        }
    }

    if let Err(e) = file.flush().await {
        write_error_count.fetch_add(1, Ordering::Relaxed);
        error!(error = %e, "failed final JSONL flush");
    }
    if let Err(e) = traded_file.flush().await {
        write_error_count.fetch_add(1, Ordering::Relaxed);
        error!(error = %e, "failed final traded-decisions flush");
    }
}

async fn open_append_file(path: &Path) -> std::io::Result<tokio::fs::File> {
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }
    tokio::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .await
}

async fn maybe_roll_files(
    file: &mut tokio::fs::File,
    traded_file: &mut tokio::fs::File,
    current_day: &mut NaiveDate,
    current_path: &mut PathBuf,
    traded_path: &mut PathBuf,
    dir: &Path,
    config: &JsonlLoggingConfig,
    write_error_count: &Arc<AtomicU64>,
) -> std::io::Result<()> {
    let now_day = now_et_day();
    if now_day <= *current_day {
        return Ok(());
    }

    file.flush().await?;
    traded_file.flush().await?;

    let old_path = current_path.clone();
    let old_traded_path = traded_path.clone();
    *current_day = now_day;
    *current_path = active_file_path(dir, now_day);
    *traded_path = traded_decisions_file_path(dir, now_day);
    *file = open_append_file(current_path).await?;
    *traded_file = open_append_file(traded_path).await?;

    if config.gzip_on_roll {
        let old_path_clone = old_path.clone();
        let old_traded_path_clone = old_traded_path.clone();
        let dir_clone = dir.to_path_buf();
        let prune_days = config.prune_days;
        let write_errors = Arc::clone(write_error_count);
        tokio::task::spawn_blocking(move || {
            if let Err(e) = gzip_and_prune_sync(&old_path_clone, &dir_clone, prune_days) {
                write_errors.fetch_add(1, Ordering::Relaxed);
                warn!(error = %e, path = %old_path_clone.display(), "failed to gzip rolled JSONL log");
            }
            if let Err(e) = gzip_and_prune_sync(&old_traded_path_clone, &dir_clone, prune_days) {
                write_errors.fetch_add(1, Ordering::Relaxed);
                warn!(
                    error = %e,
                    path = %old_traded_path_clone.display(),
                    "failed to gzip rolled traded-decisions log"
                );
            }
        });
    } else if config.prune_days > 0 {
        let dir_clone = dir.to_path_buf();
        let prune_days = config.prune_days;
        let write_errors = Arc::clone(write_error_count);
        tokio::task::spawn_blocking(move || {
            if let Err(e) = prune_old_logs(&dir_clone, prune_days) {
                write_errors.fetch_add(1, Ordering::Relaxed);
                warn!(error = %e, "failed to prune old JSONL logs");
            }
        });
    }

    Ok(())
}

fn active_file_path(dir: &Path, day: NaiveDate) -> PathBuf {
    day_dir_path(dir, day).join(format!("events-{}.json", day.format("%Y-%m-%d")))
}

fn traded_decisions_file_path(dir: &Path, day: NaiveDate) -> PathBuf {
    day_dir_path(dir, day).join(format!("traded-decisions-{}.json", day.format("%Y-%m-%d")))
}

fn day_dir_path(dir: &Path, day: NaiveDate) -> PathBuf {
    dir.join(day.format("%Y-%m-%d").to_string())
}

fn gzip_and_prune_sync(path: &Path, dir: &Path, prune_days: u32) -> std::io::Result<()> {
    if path.exists() {
        let status = std::process::Command::new("gzip")
            .arg("-f")
            .arg(path)
            .status()?;
        if !status.success() {
            return Err(std::io::Error::other(format!(
                "gzip command failed with status {}",
                status
            )));
        }
    }

    if prune_days > 0 {
        prune_old_logs(dir, prune_days)?;
    }
    Ok(())
}

fn parse_event_date(name: &str) -> Option<NaiveDate> {
    let prefix = "events-";
    let stem = name.strip_prefix(prefix)?;
    let date_part = stem
        .strip_suffix(".json.gz")
        .or_else(|| stem.strip_suffix(".json"))
        .or_else(|| stem.strip_suffix(".jsonl.gz"))
        .or_else(|| stem.strip_suffix(".jsonl"))?;
    NaiveDate::parse_from_str(date_part, "%Y-%m-%d").ok()
}

fn prune_old_logs(dir: &Path, prune_days: u32) -> std::io::Result<()> {
    let today = now_et_day();
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        let file_type = entry.file_type()?;
        let name = entry.file_name();
        let name = name.to_string_lossy();

        if file_type.is_dir() {
            // New layout: logs/YYYY-MM-DD/events-YYYY-MM-DD.json(.gz)
            let Ok(day) = NaiveDate::parse_from_str(&name, "%Y-%m-%d") else {
                continue;
            };
            let age_days = (today - day).num_days();
            if age_days > i64::from(prune_days) {
                let _ = std::fs::remove_dir_all(path);
            }
            continue;
        }

        if !file_type.is_file() {
            continue;
        }

        // Legacy flat layout compatibility: logs/events-YYYY-MM-DD.{json,jsonl}(.gz)
        let Some(day) = parse_event_date(&name) else {
            continue;
        };
        let age_days = (today - day).num_days();
        if age_days > i64::from(prune_days) {
            let _ = std::fs::remove_file(path);
        }
    }
    Ok(())
}

pub fn market_scan_payload(
    tags: &[String],
    max_duration_secs: u64,
    count: usize,
    btc: usize,
    eth: usize,
    xrp: usize,
    sol: usize,
    unknown: usize,
    error: Option<&str>,
) -> Value {
    json!({
        "tags": tags,
        "max_duration_secs": max_duration_secs,
        "count": count,
        "btc": btc,
        "eth": eth,
        "xrp": xrp,
        "sol": sol,
        "unknown": unknown,
        "error": error,
    })
}

pub fn trading_eligibility_payload(
    asset: PriceAsset,
    active_markets_for_asset: usize,
    blocked_missing_reference: usize,
    blocked_idempotency: usize,
    eligible_markets_for_trading: usize,
    captured_reference_for_entry: bool,
) -> Value {
    json!({
        "asset": asset.symbol(),
        "active_markets_for_asset": active_markets_for_asset,
        "blocked_missing_reference": blocked_missing_reference,
        "blocked_idempotency": blocked_idempotency,
        "eligible_markets_for_trading": eligible_markets_for_trading,
        "captured_reference_for_entry": captured_reference_for_entry,
    })
}

pub fn run_started_payload(
    paper_mode: bool,
    edge_threshold: rust_decimal::Decimal,
    kelly_fraction: rust_decimal::Decimal,
    min_confidence: rust_decimal::Decimal,
    max_spread_ratio: rust_decimal::Decimal,
    min_liquidity: rust_decimal::Decimal,
    min_time_to_expiry_secs: u64,
    market_tags: &[String],
    market_duration_max_secs: u64,
    no_trade_sample_rate: f64,
) -> Value {
    json!({
        "paper_mode": paper_mode,
        "edge_threshold": edge_threshold.to_f64(),
        "kelly_fraction": kelly_fraction.to_f64(),
        "min_confidence": min_confidence.to_f64(),
        "max_spread_ratio": max_spread_ratio.to_f64(),
        "min_liquidity": min_liquidity.to_f64(),
        "min_time_to_expiry_secs": min_time_to_expiry_secs,
        "market_tags": market_tags,
        "market_duration_max_secs": market_duration_max_secs,
        "no_trade_sample_rate": no_trade_sample_rate,
    })
}

pub fn run_ended_payload(
    snapshot: &PnlSnapshot,
    health: LoggerHealthSnapshot,
    reason: &str,
) -> Value {
    json!({
        "reason": reason,
        "summary": pnl_snapshot_payload(snapshot, "run_end"),
        "logger_health": {
            "dropped_count": health.dropped_count,
            "backpressure_incidents": health.backpressure_incidents,
            "write_error_count": health.write_error_count,
        },
    })
}

pub fn fill_applied_payload(
    order_id: Option<&str>,
    side: Side,
    fill_price: rust_decimal::Decimal,
    fill_amount: rust_decimal::Decimal,
    applied: bool,
) -> Value {
    json!({
        "order_id": order_id,
        "side": match side {
            Side::Buy => "BUY",
            Side::Sell => "SELL",
        },
        "fill_price": fill_price.to_f64(),
        "fill_amount": fill_amount.to_f64(),
        "applied": applied,
    })
}

pub fn action_payload(
    result: &PipelineResult,
    sample_rate: Option<f64>,
    was_sampled: bool,
) -> Value {
    match &result.action {
        PipelineAction::PlaceOrder {
            token_id,
            side,
            price,
            amount,
        } => json!({
            "kind": "trade_candidate",
            "token_id": token_id,
            "side": match side {
                Side::Buy => "BUY",
                Side::Sell => "SELL",
            },
            "price": price.to_f64(),
            "amount": amount.to_f64(),
            "sample_rate": sample_rate,
            "was_sampled": was_sampled,
            "market_question": result.market.question,
        }),
        PipelineAction::NoTrade { reason } => json!({
            "kind": "no_trade",
            "reason": reason,
            "sample_rate": sample_rate,
            "was_sampled": was_sampled,
            "market_question": result.market.question,
        }),
    }
}

#[allow(clippy::too_many_arguments)]
pub fn traded_decision_payload(
    order_id: Option<&str>,
    condition_id: &str,
    token_id: &str,
    side: Side,
    price: rust_decimal::Decimal,
    amount: rust_decimal::Decimal,
    market_question: &str,
    model_type: &str,
    posterior: Option<f64>,
    net_edge: Option<f64>,
) -> Value {
    json!({
        "order_id": order_id,
        "condition_id": condition_id,
        "token_id": token_id,
        "side": match side {
            Side::Buy => "BUY",
            Side::Sell => "SELL",
        },
        "price": price.to_f64(),
        "amount": amount.to_f64(),
        "market_question": market_question,
        "model_type": model_type,
        "posterior": posterior,
        "net_edge": net_edge,
    })
}

pub fn venue_side_for_result(result: &PipelineResult) -> Option<Side> {
    match result.action {
        PipelineAction::PlaceOrder { side, .. } => Some(side),
        PipelineAction::NoTrade { .. } => None,
    }
}

pub fn asset_symbol(asset: PriceAsset) -> &'static str {
    asset.symbol()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_event_date_understands_json_and_jsonl_names() {
        let d_json = parse_event_date("events-2026-02-19.json").expect("date parse failed");
        let d_json_gz = parse_event_date("events-2026-02-19.json.gz").expect("date parse failed");
        let d_jsonl = parse_event_date("events-2026-02-19.jsonl").expect("date parse failed");
        let d_jsonl_gz =
            parse_event_date("events-2026-02-19.jsonl.gz").expect("date parse failed");
        assert_eq!(d_json, d_json_gz);
        assert_eq!(d_json, d_jsonl);
        assert_eq!(d_json, d_jsonl_gz);
        assert!(parse_event_date("not-events-file").is_none());
    }

    #[test]
    fn active_file_path_uses_date_subdirectory() {
        let day = NaiveDate::from_ymd_opt(2026, 2, 23).expect("valid date");
        let base = Path::new("logs");
        let path = active_file_path(base, day);
        assert_eq!(
            path,
            Path::new("logs/2026-02-23/events-2026-02-23.json")
        );
    }

    #[test]
    fn traded_decisions_path_uses_date_subdirectory() {
        let day = NaiveDate::from_ymd_opt(2026, 2, 23).expect("valid date");
        let base = Path::new("logs");
        let path = traded_decisions_file_path(base, day);
        assert_eq!(
            path,
            Path::new("logs/2026-02-23/traded-decisions-2026-02-23.json")
        );
    }

    #[tokio::test]
    async fn no_trade_sampling_respects_boundaries() {
        let meta = RunMetadata::new(true, VenueType::International, "cfg");

        let mut cfg = JsonlLoggingConfig::default();
        cfg.enabled = true;
        cfg.no_trade_sample_rate = 0.0;
        let h0 = EventLoggerHandle::new(cfg.clone(), meta.clone());
        assert!(!h0.should_log_no_trade());

        cfg.no_trade_sample_rate = 1.0;
        let h1 = EventLoggerHandle::new(cfg, meta);
        assert!(h1.should_log_no_trade());
    }

    #[tokio::test]
    async fn writer_emits_valid_jsonl_line() {
        let run_suffix = Utc::now()
            .timestamp_nanos_opt()
            .unwrap_or_default()
            .to_string();
        let dir = std::env::temp_dir().join(format!("polymarket-jsonl-test-{}", run_suffix));
        let mut cfg = JsonlLoggingConfig::default();
        cfg.enabled = true;
        cfg.dir = dir.to_string_lossy().to_string();
        cfg.flush_interval_ms = 25;
        cfg.channel_capacity = 8;
        cfg.gzip_on_roll = false;
        cfg.prune_days = 0;

        let handle = EventLoggerHandle::new(
            cfg.clone(),
            RunMetadata::new(true, VenueType::International, "cfg"),
        );
        handle.try_log_value("test_event", json!({"hello": "world"}));

        tokio::time::sleep(Duration::from_millis(100)).await;
        drop(handle);
        tokio::time::sleep(Duration::from_millis(100)).await;

        let file = active_file_path(Path::new(&cfg.dir), now_et_day());
        let content = tokio::fs::read_to_string(&file)
            .await
            .expect("expected jsonl file to be written");
        let first_line = content.lines().next().expect("expected at least one line");
        let parsed: Value = serde_json::from_str(first_line).expect("invalid JSONL line");
        assert_eq!(parsed["event_type"], "test_event");
        let event_ts = parsed["event_ts"]
            .as_str()
            .expect("event_ts should be a string");
        let parsed_ts = chrono::DateTime::parse_from_rfc3339(event_ts)
            .expect("event_ts should be RFC3339");
        assert_ne!(parsed_ts.offset().local_minus_utc(), 0);

        let _ = tokio::fs::remove_dir_all(&dir).await;
    }

    #[tokio::test]
    async fn writer_emits_traded_decisions_to_dedicated_file() {
        let run_suffix = Utc::now()
            .timestamp_nanos_opt()
            .unwrap_or_default()
            .to_string();
        let dir = std::env::temp_dir().join(format!("polymarket-traded-test-{}", run_suffix));
        let mut cfg = JsonlLoggingConfig::default();
        cfg.enabled = true;
        cfg.dir = dir.to_string_lossy().to_string();
        cfg.flush_interval_ms = 25;
        cfg.channel_capacity = 8;
        cfg.gzip_on_roll = false;
        cfg.prune_days = 0;

        let handle = EventLoggerHandle::new(
            cfg.clone(),
            RunMetadata::new(true, VenueType::International, "cfg"),
        );

        handle.try_log_value("pipeline_action", json!({"kind":"no_trade"}));
        handle.try_log_value("traded_decision", json!({"kind":"trade_candidate"}));

        tokio::time::sleep(Duration::from_millis(120)).await;
        drop(handle);
        tokio::time::sleep(Duration::from_millis(100)).await;

        let day = now_et_day();
        let events_file = active_file_path(Path::new(&cfg.dir), day);
        let traded_file = traded_decisions_file_path(Path::new(&cfg.dir), day);

        let events_content = tokio::fs::read_to_string(&events_file)
            .await
            .expect("expected events file");
        let traded_content = tokio::fs::read_to_string(&traded_file)
            .await
            .expect("expected traded-decisions file");

        assert!(events_content.lines().count() >= 2);
        assert_eq!(traded_content.lines().count(), 1);

        let parsed: Value = serde_json::from_str(
            traded_content
                .lines()
                .next()
                .expect("expected one traded line"),
        )
        .expect("invalid JSON line");
        assert_eq!(parsed["event_type"], "traded_decision");

        let _ = tokio::fs::remove_dir_all(&dir).await;
    }
}
