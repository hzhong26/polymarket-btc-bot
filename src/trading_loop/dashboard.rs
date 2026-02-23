use chrono::Utc;
use rust_decimal::Decimal;
use std::sync::atomic::{AtomicBool, Ordering};

use super::pipeline::{PipelineAction, PipelineResult};
use super::pnl::{ClosedTradeRecord, OutcomeTokenSide, PnlSnapshot};
use crate::timezone::{et_abbreviation, now_et, to_et};

const RESET: &str = "\x1b[0m";
const DIM: &str = "\x1b[2m";
const GREEN: &str = "\x1b[32m";
const YELLOW: &str = "\x1b[33m";
const CYAN: &str = "\x1b[36m";
const RED: &str = "\x1b[31m";
const WHITE: &str = "\x1b[97m";

static HEADER_PRINTED: AtomicBool = AtomicBool::new(false);

fn fmt_opt_f64(value: Option<f64>, decimals: usize) -> String {
    match value {
        Some(v) => format!("{:.*}", decimals, v),
        None => "n/a".to_string(),
    }
}

fn fmt_opt_decimal(value: Option<Decimal>) -> String {
    match value {
        Some(v) => v.round_dp(2).to_string(),
        None => "n/a".to_string(),
    }
}

fn fmt_decimal(value: Decimal, decimals: u32) -> String {
    value.round_dp(decimals).to_string()
}

fn now_et_hms() -> String {
    now_et().format("%H:%M:%S").to_string()
}

fn fmt_et(ts_utc: chrono::DateTime<Utc>, fmt: &str) -> String {
    let et = to_et(ts_utc);
    format!("{} {}", et.format(fmt), et_abbreviation(ts_utc))
}

pub fn print_market_dashboard(result: &PipelineResult, eval_count: u64, trade_count: u64) {
    if !HEADER_PRINTED.swap(true, Ordering::SeqCst) {
        println!(
            "{dim}===================================================================={reset}",
            dim = DIM,
            reset = RESET
        );
        println!(
            "{green}Polymarket Bot - Step-by-Step Pipeline Dashboard{reset}",
            green = GREEN,
            reset = RESET
        );
        println!(
            "{dim}Mode: paper-trading dashboard (set trading.dashboard_enabled=false to disable){reset}",
            dim = DIM,
            reset = RESET
        );
        println!(
            "{dim}===================================================================={reset}",
            dim = DIM,
            reset = RESET
        );
    }

    let ts = now_et_hms();
    let d = &result.diagnostics;

    println!();
    println!(
        "{dim}===================================================================={reset}",
        dim = DIM,
        reset = RESET
    );
    println!(
        "{cyan}[{ts}] PIPELINE TEST #{eval_count} ({mode}){reset}",
        cyan = CYAN,
        ts = ts,
        eval_count = eval_count,
        mode = match result.action {
            PipelineAction::PlaceOrder { .. } => "TRADE",
            PipelineAction::NoTrade { .. } => "NO-TRADE",
        },
        reset = RESET
    );
    println!(
        "{white}Market:{reset} {}",
        result.market.question,
        white = WHITE,
        reset = RESET
    );

    println!(
        "1) Aggregated {} spot            : {}",
        d.market_asset,
        match d.aggregated_spot {
            Some(v) => format!("${}", fmt_opt_f64(Some(v), 2)),
            None => "n/a".to_string(),
        }
    );
    println!(
        "2) Confidence score              : {}",
        fmt_opt_f64(Some(d.confidence_score), 3)
    );
    let up_down_model = d.model_type == "updown_directional";
    let threshold_label = if up_down_model {
        "Polymarket Reference"
    } else {
        "Strike"
    };
    let threshold_value = if up_down_model {
        fmt_opt_f64(d.reference_price, 2)
    } else {
        fmt_opt_f64(Some(d.strike_price), 2)
    };
    let threshold_display = if threshold_value == "n/a" {
        "n/a".to_string()
    } else {
        format!("${threshold_value}")
    };
    println!(
        "3) {} / Expiry            : {} / {} (t={}s)",
        threshold_label,
        threshold_display,
        fmt_et(d.expiry_time, "%Y-%m-%d %I:%M:%S %p"),
        d.time_to_expiry_secs.round()
    );
    if up_down_model {
        println!(
            "4) p_model computation           : p_model={} from f(spot={}, reference={}, t={}s, vol={}, model={})",
            fmt_opt_f64(d.p_model, 4),
            fmt_opt_f64(d.aggregated_spot, 2),
            fmt_opt_f64(d.reference_price, 2),
            d.time_to_expiry_secs.round(),
            fmt_opt_f64(d.model_volatility, 4),
            d.model_type
        );
    } else {
        println!(
            "4) p_model computation           : p_model={} from f(spot={}, strike={}, t={}s, vol={}, model={})",
            fmt_opt_f64(d.p_model, 4),
            fmt_opt_f64(d.aggregated_spot, 2),
            fmt_opt_f64(Some(d.strike_price), 2),
            d.time_to_expiry_secs.round(),
            fmt_opt_f64(d.model_volatility, 4),
            d.model_type
        );
    }
    println!(
        "5) Posterior blend               : posterior={} (model={} market={} conf={})",
        fmt_opt_f64(d.posterior, 4),
        fmt_opt_f64(d.p_model, 4),
        fmt_opt_f64(d.market_implied_p, 4),
        fmt_opt_f64(Some(d.confidence_score), 3)
    );
    println!(
        "6) Best executable ask/bid price : {} [{}]",
        fmt_opt_f64(d.executable_price, 4),
        d.edge_side.unwrap_or("n/a")
    );
    println!(
        "7) Gross/Cost/Net edge           : gross={} fee={} slip={} lat={} total={} net={}",
        fmt_opt_f64(d.gross_edge, 4),
        fmt_opt_f64(d.cost_fee, 4),
        fmt_opt_f64(d.cost_slippage, 4),
        fmt_opt_f64(d.cost_latency, 4),
        fmt_opt_f64(d.cost_total, 4),
        fmt_opt_f64(d.net_edge, 4)
    );
    println!(
        "11) Threshold check              : net={} vs threshold={} => {}",
        fmt_opt_f64(d.net_edge, 4),
        fmt_opt_f64(Some(d.edge_threshold), 4),
        match d.threshold_pass {
            Some(true) => format!(
                "{green}TRADE CANDIDATE{reset}",
                green = GREEN,
                reset = RESET
            ),
            Some(false) => format!("{red}NO TRADE{reset}", red = RED, reset = RESET),
            None => "n/a".to_string(),
        }
    );
    println!(
        "12) Kelly sizing                 : raw={} adjusted={} final={} stake_usdc={} size_tokens={}",
        fmt_opt_f64(d.kelly_raw, 4),
        fmt_opt_f64(d.kelly_adjusted, 4),
        fmt_opt_f64(d.kelly_final, 4),
        fmt_opt_decimal(d.kelly_stake_usdc),
        fmt_opt_decimal(d.kelly_size_tokens)
    );

    match &result.action {
        PipelineAction::PlaceOrder {
            token_id,
            side,
            price,
            amount,
        } => {
            println!(
                "{yellow}ACTION:{reset} PLACE ORDER #{trade_count} side={side:?} token={} price={} amount={}",
                token_id,
                price,
                amount,
                yellow = YELLOW,
                reset = RESET
            );
        }
        PipelineAction::NoTrade { reason } => {
            println!(
                "{yellow}ACTION:{reset} NO TRADE ({})",
                reason,
                yellow = YELLOW,
                reset = RESET
            );
        }
    }
}

pub fn print_pnl_dashboard(
    snapshot: &PnlSnapshot,
    mode: &str,
    trigger: &str,
    recent_settlements: Option<&[ClosedTradeRecord]>,
) {
    println!(
        "{dim}--------------------------- P&L SNAPSHOT ---------------------------{reset}",
        dim = DIM,
        reset = RESET
    );
    println!(
        "{cyan}[{}] mode={} trigger={}{reset}",
        now_et_hms(),
        mode,
        trigger,
        cyan = CYAN,
        reset = RESET
    );
    println!(
        "Capital={} Realized={} Unrealized={} Total={} Equity={} ROI={}%",
        fmt_decimal(snapshot.starting_capital, 2),
        fmt_decimal(snapshot.realized_pnl, 2),
        fmt_decimal(snapshot.unrealized_pnl, 2),
        fmt_decimal(snapshot.total_pnl, 2),
        fmt_decimal(snapshot.equity, 2),
        fmt_decimal(snapshot.roi_pct, 2)
    );
    println!(
        "Fees={} OpenPositions={} Fills={} Trades={} ClosedTrades={} Wins={} Losses={} WinRate={}%",
        fmt_decimal(snapshot.total_fees_paid, 2),
        snapshot.open_positions,
        snapshot.fills,
        snapshot.trades,
        snapshot.closed_trades,
        snapshot.wins,
        snapshot.losses,
        fmt_decimal(snapshot.win_rate, 2)
    );

    println!("Asset Breakdown:");
    for row in &snapshot.per_asset {
        println!(
            "  {}: realized={} unrealized={} total={} open_positions={}",
            row.asset.symbol(),
            fmt_decimal(row.realized_pnl, 2),
            fmt_decimal(row.unrealized_pnl, 2),
            fmt_decimal(row.total_pnl, 2),
            row.open_positions
        );
    }

    if let Some(records) = recent_settlements {
        if !records.is_empty() {
            println!("Recent Settlements:");
            for rec in records.iter().rev().take(5) {
                let side = match rec.outcome_side {
                    OutcomeTokenSide::Yes => "YES",
                    OutcomeTokenSide::No => "NO",
                };
                let outcome = if rec.actual_yes_outcome { "YES" } else { "NO" };
                println!(
                    "  [{}] {} {} -> {} payout={} realized={}",
                    fmt_et(rec.closed_at, "%H:%M:%S"),
                    rec.asset.symbol(),
                    side,
                    outcome,
                    fmt_decimal(rec.payout_price, 2),
                    fmt_decimal(rec.realized_pnl, 2)
                );
            }
        }
    }
}
