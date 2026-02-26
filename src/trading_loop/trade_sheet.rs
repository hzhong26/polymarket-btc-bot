use std::collections::{HashMap, HashSet};
use std::path::PathBuf;

use chrono::{DateTime, Utc};
use rust_decimal::prelude::ToPrimitive;
use rust_decimal::Decimal;
use rust_xlsxwriter::{Color, Format, Workbook};

use crate::timezone::to_et;
use crate::trading_loop::pnl::{ClosedTradeRecord, OutcomeTokenSide};

#[derive(Clone)]
struct TradeRow {
    run_number: u32,
    coin: String,
    opened_at: DateTime<Utc>,
    end_time: Option<DateTime<Utc>>,
    window_start: Option<DateTime<Utc>>,
    side: String,
    kelly_stake_usdc: Option<f64>,
    cost_fee_rate: Option<f64>,
    status: String,
    result: Option<bool>,
    won: Option<bool>,
    realized_pnl: Option<Decimal>,
}

pub struct TradeSheetWriter {
    rows: Vec<TradeRow>,
    by_token: HashMap<String, usize>,
    path: PathBuf,
    current_run_number: u32,
    current_bot_version: String,
    /// (run_number, version) for all runs seen (including current)
    run_versions: Vec<(u32, String)>,
}

impl TradeSheetWriter {
    pub fn new(log_dir: &str, bot_version: &str) -> Self {
        let today = to_et(Utc::now()).format("%Y-%m-%d").to_string();
        let path = PathBuf::from(log_dir)
            .join(&today)
            .join(format!("trades-{}.xlsx", today));
        Self {
            rows: Vec::new(),
            by_token: HashMap::new(),
            path,
            current_run_number: 1,
            current_bot_version: bot_version.to_string(),
            run_versions: vec![(1, bot_version.to_string())],
        }
    }

    pub fn record_opened(
        &mut self,
        token_id: String,
        coin: String,
        opened_at: DateTime<Utc>,
        end_time: Option<DateTime<Utc>>,
        window_start: Option<DateTime<Utc>>,
        side: String,
        kelly_stake_usdc: Option<f64>,
        cost_fee_rate: Option<f64>,
    ) {
        if self.by_token.contains_key(&token_id) {
            return;
        }
        let idx = self.rows.len();
        self.by_token.insert(token_id.clone(), idx);
        self.rows.push(TradeRow {
            run_number: self.current_run_number,
            coin,
            opened_at,
            end_time,
            window_start,
            side,
            kelly_stake_usdc,
            cost_fee_rate,
            status: "OPEN".to_string(),
            result: None,
            won: None,
            realized_pnl: None,
        });
        self.spawn_write();
    }

    pub fn record_closed(&mut self, rec: &ClosedTradeRecord) {
        let won = match rec.outcome_side {
            OutcomeTokenSide::Yes => rec.actual_yes_outcome,
            OutcomeTokenSide::No => !rec.actual_yes_outcome,
        };
        if let Some(&idx) = self.by_token.get(&rec.token_id) {
            let row = &mut self.rows[idx];
            row.status = "CLOSED".to_string();
            row.result = Some(rec.actual_yes_outcome);
            row.won = Some(won);
            row.realized_pnl = Some(rec.realized_pnl);
        } else {
            // Opened before this session — create a minimal closed row
            let idx = self.rows.len();
            self.by_token.insert(rec.token_id.clone(), idx);
            self.rows.push(TradeRow {
                run_number: self.current_run_number,
                coin: rec.asset.symbol().to_string(),
                opened_at: rec.opened_at,
                end_time: rec.end_time,
                window_start: rec.window_start,
                side: match rec.outcome_side {
                    OutcomeTokenSide::Yes => "YES".to_string(),
                    OutcomeTokenSide::No => "NO".to_string(),
                },
                kelly_stake_usdc: None,
                cost_fee_rate: None,
                status: "CLOSED".to_string(),
                result: Some(rec.actual_yes_outcome),
                won: Some(won),
                realized_pnl: Some(rec.realized_pnl),
            });
        }
        self.spawn_write();
    }

    /// Read today's events JSONL and pre-populate rows for any trades placed
    /// before this process started (e.g. after a same-day restart).
    /// Safe to call with an empty or missing log file — returns silently.
    pub async fn seed_from_today_log(&mut self, log_dir: &str) {
        use tokio::io::{AsyncBufReadExt, BufReader};

        let today = to_et(Utc::now()).format("%Y-%m-%d").to_string();
        let events_path = PathBuf::from(log_dir)
            .join(&today)
            .join(format!("events-{}.json", today));

        let Ok(file) = tokio::fs::File::open(&events_path).await else {
            return;
        };

        let mut lines = BufReader::new(file).lines();
        let mut seen: HashSet<String> = HashSet::new();
        // ordered list of (token_id, run_id, row) from pipeline_evaluation events
        let mut candidates: Vec<(String, Option<String>, TradeRow)> = Vec::new();
        // token_id → (actual_yes_outcome, won, realized_pnl)
        let mut settlements: HashMap<String, (bool, bool, Decimal)> = HashMap::new();
        // Track distinct run_ids in order of first appearance → (run_id, bot_version)
        let mut discovered_runs: Vec<(String, String)> = Vec::new();

        while let Ok(Some(line)) = lines.next_line().await {
            let Ok(val) = serde_json::from_str::<serde_json::Value>(&line) else {
                continue;
            };

            // Track run_id from every event line (not just trade_candidate)
            if let Some(rid) = val["run_id"].as_str() {
                if !discovered_runs.iter().any(|(r, _)| r == rid) {
                    let ver = val["bot_version"]
                        .as_str()
                        .unwrap_or("?")
                        .to_string();
                    discovered_runs.push((rid.to_string(), ver));
                }
            }

            match val["event_type"].as_str() {
                Some("pipeline_evaluation") => {
                    let p = &val["payload"];
                    if p["action"]["kind"].as_str() != Some("trade_candidate") {
                        continue;
                    }
                    let edge_side = p["action"]["side"].as_str().unwrap_or("");
                    let is_yes = edge_side.contains("YES");
                    let token_key = if is_yes { "token_id_yes" } else { "token_id_no" };
                    let Some(token_id) = p["market"][token_key].as_str() else {
                        continue;
                    };
                    if seen.contains(token_id) || self.by_token.contains_key(token_id) {
                        continue;
                    }
                    seen.insert(token_id.to_string());

                    let run_id = val["run_id"].as_str().map(|s| s.to_string());

                    let coin = p["market"]["asset"]
                        .as_str()
                        .unwrap_or("?")
                        .to_uppercase();
                    let opened_at = val["event_ts"]
                        .as_str()
                        .and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok())
                        .map(|dt| dt.with_timezone(&Utc))
                        .unwrap_or_else(Utc::now);
                    let end_time = p["market"]["end_time"]
                        .as_str()
                        .and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok())
                        .map(|dt| dt.with_timezone(&Utc));
                    let window_start = p["market"]["start_time"]
                        .as_str()
                        .and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok())
                        .map(|dt| dt.with_timezone(&Utc));
                    let side = if is_yes { "YES" } else { "NO" }.to_string();
                    let kelly_stake_usdc =
                        p["sizing_risk_context"]["stake_usdc"].as_f64();
                    let cost_fee_rate = p["cost_context"]["fee"].as_f64();

                    candidates.push((
                        token_id.to_string(),
                        run_id,
                        TradeRow {
                            run_number: 0, // assigned below after we know all runs
                            coin,
                            opened_at,
                            end_time,
                            window_start,
                            side,
                            kelly_stake_usdc,
                            cost_fee_rate,
                            status: "OPEN".to_string(),
                            result: None,
                            won: None,
                            realized_pnl: None,
                        },
                    ));
                }
                Some("position_settled") => {
                    let p = &val["payload"];
                    let Some(token_id) = p["token_id"].as_str() else {
                        continue;
                    };
                    let actual_yes = p["actual_yes_outcome"].as_bool().unwrap_or(false);
                    let won = p["won"].as_bool().unwrap_or(false);
                    let pnl = p["realized_pnl"]
                        .as_f64()
                        .and_then(|v| Decimal::try_from(v).ok())
                        .unwrap_or_default();
                    settlements.insert(token_id.to_string(), (actual_yes, won, pnl));
                }
                _ => {}
            }
        }

        // Build run_id → run_number mapping (1-based, by first appearance order)
        let run_id_to_number: HashMap<&str, u32> = discovered_runs
            .iter()
            .enumerate()
            .map(|(i, (rid, _))| (rid.as_str(), (i + 1) as u32))
            .collect();

        // Populate run_versions from discovered runs
        if !discovered_runs.is_empty() {
            self.run_versions = discovered_runs
                .iter()
                .enumerate()
                .map(|(i, (_, ver))| ((i + 1) as u32, ver.clone()))
                .collect();
            self.current_run_number = discovered_runs.len() as u32 + 1;
            // Add current run to run_versions
            self.run_versions
                .push((self.current_run_number, self.current_bot_version.clone()));
        }

        for (token_id, run_id, mut row) in candidates {
            // Assign run_number based on the run_id this trade came from
            row.run_number = run_id
                .as_deref()
                .and_then(|rid| run_id_to_number.get(rid).copied())
                .unwrap_or(1);

            if let Some(&(actual_yes, won, pnl)) = settlements.get(&token_id) {
                row.status = "CLOSED".to_string();
                row.result = Some(actual_yes);
                row.won = Some(won);
                row.realized_pnl = Some(pnl);
            }
            let idx = self.rows.len();
            self.by_token.insert(token_id, idx);
            self.rows.push(row);
        }

        if !self.rows.is_empty() {
            self.spawn_write();
        }
    }

    fn spawn_write(&self) {
        let path = self.path.clone();
        let content = self.build_xlsx();
        tokio::spawn(async move {
            if let Some(parent) = path.parent() {
                let _ = tokio::fs::create_dir_all(parent).await;
            }
            let _ = tokio::fs::write(&path, content).await;
        });
    }

    pub(crate) fn build_xlsx(&self) -> Vec<u8> {
        let mut workbook = Workbook::new();

        // ── Formats ──────────────────────────────────────────────────────
        let fmt_bold = Format::new().set_bold();
        let fmt_run_header = Format::new().set_bold().set_font_size(12);
        let fmt_dollar = Format::new().set_num_format("$#,##0.00");
        let fmt_pct = Format::new().set_num_format("0.0%");
        let fmt_win = Format::new()
            .set_font_color(Color::RGB(0x227722))
            .set_bold();
        let fmt_loss = Format::new()
            .set_font_color(Color::RGB(0xCC2222))
            .set_bold();
        let fmt_pnl_pos = Format::new()
            .set_num_format("$#,##0.00")
            .set_font_color(Color::RGB(0x227722));
        let fmt_pnl_neg = Format::new()
            .set_num_format("$#,##0.00")
            .set_font_color(Color::RGB(0xCC2222));
        let fmt_section_header = Format::new().set_bold().set_font_size(11);

        // ══════════════════════════════════════════════════════════════════
        //  Sheet 1: Trades
        // ══════════════════════════════════════════════════════════════════
        let trades_sheet = workbook.add_worksheet();
        let _ = trades_sheet.set_name("Trades");

        let headers = [
            "#", "Coin", "Date & Time", "Timeframe", "Status", "Decision",
            "Result", "W/L", "Stake ($)", "P&L ($)", "Fee (~$)",
        ];
        let col_widths = [5.0, 8.0, 16.0, 10.0, 9.0, 10.0, 8.0, 7.0, 12.0, 12.0, 12.0];

        for (c, &w) in col_widths.iter().enumerate() {
            let _ = trades_sheet.set_column_width(c as u16, w);
        }

        // Collect run numbers
        let mut run_numbers: Vec<u32> = self
            .rows
            .iter()
            .map(|r| r.run_number)
            .collect::<HashSet<_>>()
            .into_iter()
            .collect();
        if !run_numbers.contains(&self.current_run_number) {
            run_numbers.push(self.current_run_number);
        }
        run_numbers.sort();

        let mut row: u32 = 0;

        for &run_num in &run_numbers {
            let version = self
                .run_versions
                .iter()
                .find(|(n, _)| *n == run_num)
                .map(|(_, v)| v.as_str())
                .unwrap_or("?");

            // Run header (merged across all columns)
            let run_label = format!("Run {} (v{})", run_num, version);
            let _ = trades_sheet.merge_range(
                row, 0, row, (headers.len() - 1) as u16,
                &run_label, &fmt_run_header,
            );
            row += 1;

            // Column headers
            for (c, &h) in headers.iter().enumerate() {
                let _ = trades_sheet.write_string_with_format(row, c as u16, h, &fmt_bold);
            }
            row += 1;

            // Data rows (newest first)
            let run_rows: Vec<&TradeRow> = self
                .rows
                .iter()
                .filter(|r| r.run_number == run_num)
                .rev()
                .collect();

            for (rev_i, tr) in run_rows.iter().enumerate() {
                let n = (rev_i + 1) as f64;
                let dt = to_et(tr.opened_at).format("%m/%d %H:%M:%S").to_string();
                let tf = fmt_timeframe(tr.end_time, tr.window_start);
                let decision = if tr.side == "YES" { "UP" } else { "DOWN" };
                let result_str = tr.result.map(|r| if r { "UP" } else { "DOWN" }).unwrap_or("");
                let wl = match tr.won {
                    Some(true) => "WIN",
                    Some(false) => "LOSS",
                    None => "",
                };

                let _ = trades_sheet.write_number(row, 0, n);
                let _ = trades_sheet.write_string(row, 1, &tr.coin);
                let _ = trades_sheet.write_string(row, 2, &dt);
                let _ = trades_sheet.write_string(row, 3, &tf);
                let _ = trades_sheet.write_string(row, 4, &tr.status);
                let _ = trades_sheet.write_string(row, 5, decision);
                let _ = trades_sheet.write_string(row, 6, result_str);

                // W/L with conditional color
                match tr.won {
                    Some(true) => {
                        let _ = trades_sheet.write_string_with_format(row, 7, wl, &fmt_win);
                    }
                    Some(false) => {
                        let _ = trades_sheet.write_string_with_format(row, 7, wl, &fmt_loss);
                    }
                    None => {
                        let _ = trades_sheet.write_string(row, 7, wl);
                    }
                }

                // Stake
                if let Some(stake) = tr.kelly_stake_usdc {
                    let _ = trades_sheet.write_number_with_format(row, 8, stake, &fmt_dollar);
                }

                // P&L with conditional color
                if let Some(pnl) = tr.realized_pnl.and_then(|v| v.to_f64()) {
                    let fmt = if pnl >= 0.0 { &fmt_pnl_pos } else { &fmt_pnl_neg };
                    let _ = trades_sheet.write_number_with_format(row, 9, pnl, fmt);
                }

                // Fee
                if let (Some(rate), Some(stake)) = (tr.cost_fee_rate, tr.kelly_stake_usdc) {
                    let _ = trades_sheet.write_number_with_format(row, 10, rate * stake, &fmt_dollar);
                }

                row += 1;
            }

            row += 1; // blank row between runs
        }

        // ══════════════════════════════════════════════════════════════════
        //  Sheet 2: Analysis
        // ══════════════════════════════════════════════════════════════════
        let closed: Vec<&TradeRow> = self.rows.iter().filter(|r| r.status == "CLOSED").collect();

        if !closed.is_empty() {
            let analysis = workbook.add_worksheet();
            let _ = analysis.set_name("Analysis");

            let coins = ordered_coins(&closed);
            let mut arow: u32 = 0;

            // ── Totals table ─────────────────────────────────────────────
            let total = closed.len();
            let wins = closed.iter().filter(|r| r.won == Some(true)).count();
            let losses = total - wins;
            let win_pct = wins as f64 / total as f64;

            let total_pnl: f64 = closed
                .iter()
                .filter_map(|r| r.realized_pnl.and_then(|v| v.to_f64()))
                .sum();
            let total_staked: f64 = closed.iter().filter_map(|r| r.kelly_stake_usdc).sum();
            let total_fee: f64 = closed
                .iter()
                .filter_map(|r| match (r.cost_fee_rate, r.kelly_stake_usdc) {
                    (Some(rate), Some(stake)) => Some(rate * stake),
                    _ => None,
                })
                .sum();
            let roi_pct = if total_staked > 0.0 { total_pnl / total_staked } else { 0.0 };
            let net_profit = total_pnl - total_fee;

            let _ = analysis.merge_range(arow, 0, arow, 7, "Totals", &fmt_section_header);
            arow += 1;
            let totals_headers = ["Trades", "Wins", "Losses", "Win %", "Total P&L", "ROI %", "Fees", "Net Profit"];
            for (c, &h) in totals_headers.iter().enumerate() {
                let _ = analysis.write_string_with_format(arow, c as u16, h, &fmt_bold);
            }
            arow += 1;
            let _ = analysis.write_number(arow, 0, total as f64);
            let _ = analysis.write_number(arow, 1, wins as f64);
            let _ = analysis.write_number(arow, 2, losses as f64);
            let _ = analysis.write_number_with_format(arow, 3, win_pct, &fmt_pct);
            let pnl_fmt = if total_pnl >= 0.0 { &fmt_pnl_pos } else { &fmt_pnl_neg };
            let _ = analysis.write_number_with_format(arow, 4, total_pnl, pnl_fmt);
            let _ = analysis.write_number_with_format(arow, 5, roi_pct, &fmt_pct);
            let _ = analysis.write_number_with_format(arow, 6, total_fee, &fmt_dollar);
            let np_fmt = if net_profit >= 0.0 { &fmt_pnl_pos } else { &fmt_pnl_neg };
            let _ = analysis.write_number_with_format(arow, 7, net_profit, np_fmt);
            arow += 2;

            // ── Win Rate by Coin ─────────────────────────────────────────
            let _ = analysis.merge_range(arow, 0, arow, 3, "Win Rate by Coin", &fmt_section_header);
            arow += 1;
            let wr_headers = ["Coin", "Wins", "Losses", "Win %"];
            for (c, &h) in wr_headers.iter().enumerate() {
                let _ = analysis.write_string_with_format(arow, c as u16, h, &fmt_bold);
            }
            arow += 1;
            for coin in &coins {
                let subset: Vec<&&TradeRow> = closed.iter().filter(|r| r.coin == *coin).collect();
                let c_total = subset.len();
                let c_wins = subset.iter().filter(|r| r.won == Some(true)).count();
                let c_wr = c_wins as f64 / c_total as f64;
                let _ = analysis.write_string(arow, 0, coin);
                let _ = analysis.write_number(arow, 1, c_wins as f64);
                let _ = analysis.write_number(arow, 2, (c_total - c_wins) as f64);
                let _ = analysis.write_number_with_format(arow, 3, c_wr, &fmt_pct);
                arow += 1;
            }
            arow += 1;

            // ── P&L by Coin ──────────────────────────────────────────────
            let _ = analysis.merge_range(arow, 0, arow, 3, "P&L by Coin", &fmt_section_header);
            arow += 1;
            let pnl_headers = ["Coin", "Total P&L", "Staked", "ROI %"];
            for (c, &h) in pnl_headers.iter().enumerate() {
                let _ = analysis.write_string_with_format(arow, c as u16, h, &fmt_bold);
            }
            arow += 1;
            for coin in &coins {
                let subset: Vec<&&TradeRow> = closed.iter().filter(|r| r.coin == *coin).collect();
                let c_pnl: f64 = subset
                    .iter()
                    .filter_map(|r| r.realized_pnl.and_then(|v| v.to_f64()))
                    .sum();
                let c_staked: f64 = subset.iter().filter_map(|r| r.kelly_stake_usdc).sum();
                let c_roi = if c_staked > 0.0 { c_pnl / c_staked } else { 0.0 };
                let _ = analysis.write_string(arow, 0, coin);
                let cpnl_fmt = if c_pnl >= 0.0 { &fmt_pnl_pos } else { &fmt_pnl_neg };
                let _ = analysis.write_number_with_format(arow, 1, c_pnl, cpnl_fmt);
                let _ = analysis.write_number_with_format(arow, 2, c_staked, &fmt_dollar);
                let _ = analysis.write_number_with_format(arow, 3, c_roi, &fmt_pct);
                arow += 1;
            }
            arow += 1;

            // ── Coin x Timeframe ─────────────────────────────────────────
            let _ = analysis.merge_range(arow, 0, arow, 5, "Coin x Timeframe", &fmt_section_header);
            arow += 1;
            let ct_headers = ["Coin", "Timeframe", "Wins", "Losses", "Win %", "Total P&L"];
            for (c, &h) in ct_headers.iter().enumerate() {
                let _ = analysis.write_string_with_format(arow, c as u16, h, &fmt_bold);
            }
            arow += 1;
            for coin in &coins {
                let subset: Vec<&&TradeRow> = closed.iter().filter(|r| r.coin == *coin).collect();
                let timeframes = ordered_timeframes(&subset);
                for tf in &timeframes {
                    let tf_sub: Vec<&&&TradeRow> = subset
                        .iter()
                        .filter(|r| fmt_timeframe(r.end_time, r.window_start) == *tf)
                        .collect();
                    let t_total = tf_sub.len();
                    let t_wins = tf_sub.iter().filter(|r| r.won == Some(true)).count();
                    let t_wr = t_wins as f64 / t_total as f64;
                    let t_pnl: f64 = tf_sub
                        .iter()
                        .filter_map(|r| r.realized_pnl.and_then(|v| v.to_f64()))
                        .sum();
                    let _ = analysis.write_string(arow, 0, coin);
                    let _ = analysis.write_string(arow, 1, tf);
                    let _ = analysis.write_number(arow, 2, t_wins as f64);
                    let _ = analysis.write_number(arow, 3, (t_total - t_wins) as f64);
                    let _ = analysis.write_number_with_format(arow, 4, t_wr, &fmt_pct);
                    let tpnl_fmt = if t_pnl >= 0.0 { &fmt_pnl_pos } else { &fmt_pnl_neg };
                    let _ = analysis.write_number_with_format(arow, 5, t_pnl, tpnl_fmt);
                    arow += 1;
                }
            }
            arow += 1;

            // ── Cumulative P&L ───────────────────────────────────────────
            let cum_pnls = cumulative_pnls(&closed);
            if cum_pnls.len() >= 2 {
                let _ = analysis.merge_range(arow, 0, arow, 1, "Cumulative P&L", &fmt_section_header);
                arow += 1;
                let _ = analysis.write_string_with_format(arow, 0, "Trade #", &fmt_bold);
                let _ = analysis.write_string_with_format(arow, 1, "Cumulative P&L", &fmt_bold);
                arow += 1;
                for (i, cum) in cum_pnls.iter().enumerate() {
                    let _ = analysis.write_number(arow, 0, (i + 1) as f64);
                    let cfmt = if *cum >= 0.0 { &fmt_pnl_pos } else { &fmt_pnl_neg };
                    let _ = analysis.write_number_with_format(arow, 1, *cum, cfmt);
                    arow += 1;
                }
            }

            // Set column widths for analysis sheet
            let _ = analysis.set_column_width(0, 14.0);
            let _ = analysis.set_column_width(1, 14.0);
            let _ = analysis.set_column_width(2, 10.0);
            let _ = analysis.set_column_width(3, 10.0);
            let _ = analysis.set_column_width(4, 12.0);
            let _ = analysis.set_column_width(5, 12.0);
            let _ = analysis.set_column_width(6, 10.0);
            let _ = analysis.set_column_width(7, 12.0);
        }

        workbook.save_to_buffer().unwrap_or_default()
    }
}

pub(crate) fn fmt_timeframe(end: Option<DateTime<Utc>>, start: Option<DateTime<Utc>>) -> String {
    match (end, start) {
        (Some(e), Some(s)) => {
            let secs = (e - s).num_seconds();
            if (secs - 300).abs() <= 30 {
                "5min".to_string()
            } else if (secs - 900).abs() <= 30 {
                "15min".to_string()
            } else if (secs - 3600).abs() <= 60 {
                "1h".to_string()
            } else {
                format!("{}min", secs / 60)
            }
        }
        _ => "?".to_string(),
    }
}

/// Build a cumulative P&L series from closed trades (in chronological order).
fn cumulative_pnls(closed: &[&TradeRow]) -> Vec<f64> {
    let mut cum = Vec::with_capacity(closed.len());
    let mut running = 0.0_f64;
    for row in closed {
        if let Some(pnl) = row.realized_pnl.and_then(|v| v.to_f64()) {
            running += pnl;
        }
        cum.push(running);
    }
    cum
}

fn ordered_coins(rows: &[&TradeRow]) -> Vec<String> {
    let mut seen = Vec::new();
    for r in rows {
        if !seen.contains(&r.coin) {
            seen.push(r.coin.clone());
        }
    }
    seen.sort();
    seen
}

fn ordered_timeframes(rows: &[&&TradeRow]) -> Vec<String> {
    let mut seen = Vec::new();
    for r in rows {
        let tf = fmt_timeframe(r.end_time, r.window_start);
        if !seen.contains(&tf) {
            seen.push(tf);
        }
    }
    seen.sort_by(|a, b| parse_tf_minutes(a).cmp(&parse_tf_minutes(b)));
    seen
}

fn parse_tf_minutes(s: &str) -> u32 {
    if s == "1h" {
        return 60;
    }
    s.trim_end_matches("min").parse().unwrap_or(999)
}

#[cfg(test)]
mod tests {
    use super::*;

    use chrono::TimeZone;
    use rust_decimal::Decimal;

    use crate::price_feeds::PriceAsset;
    use crate::trading_loop::pnl::ClosedTradeRecord;

    // ── helpers ──────────────────────────────────────────────────────────────

    fn ts(h: u32, m: u32, s: u32) -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 2, 23, h, m, s).unwrap()
    }

    /// Build a writer without a real path — we only call build_xlsx() in these tests.
    fn writer() -> TradeSheetWriter {
        TradeSheetWriter {
            rows: Vec::new(),
            by_token: HashMap::new(),
            path: PathBuf::from("/tmp/trade-sheet-test.xlsx"),
            current_run_number: 1,
            current_bot_version: "0.0.0-test".to_string(),
            run_versions: vec![(1, "0.0.0-test".to_string())],
        }
    }

    fn open_row(
        coin: &str,
        opened_at: DateTime<Utc>,
        window_secs: i64,
        side: &str,
        stake: f64,
        fee_rate: f64,
    ) -> TradeRow {
        TradeRow {
            run_number: 1,
            coin: coin.to_string(),
            opened_at,
            end_time: Some(opened_at + chrono::Duration::seconds(window_secs)),
            window_start: Some(opened_at),
            side: side.to_string(),
            kelly_stake_usdc: Some(stake),
            cost_fee_rate: Some(fee_rate),
            status: "OPEN".to_string(),
            result: None,
            won: None,
            realized_pnl: None,
        }
    }

    fn closed_rec(
        token_id: &str,
        outcome_side: OutcomeTokenSide,
        actual_yes: bool,
        pnl: Decimal,
    ) -> ClosedTradeRecord {
        ClosedTradeRecord {
            opened_at: ts(10, 0, 0),
            closed_at: ts(10, 5, 0),
            end_time: Some(ts(10, 5, 0)),
            window_start: Some(ts(10, 0, 0)),
            asset: PriceAsset::Btc,
            token_id: token_id.to_string(),
            condition_id: None,
            market_question: "BTC UP?".to_string(),
            outcome_side,
            avg_entry_price: Decimal::new(50, 2),
            quantity: Decimal::from(10),
            predicted_yes_prob: 0.6,
            model_yes_prob: 0.58,
            actual_yes_outcome: actual_yes,
            payout_price: Decimal::ONE,
            realized_pnl: pnl,
            model_type: "binary_call".to_string(),
            official_price_to_beat: None,
            official_final_price: None,
            settlement_source: "test".to_string(),
            resolution_available_at: None,
        }
    }

    /// Helper: get a row reference by token_id
    fn get_row<'a>(w: &'a TradeSheetWriter, token_id: &str) -> &'a TradeRow {
        let idx = w.by_token[token_id];
        &w.rows[idx]
    }

    // ── fmt_timeframe ─────────────────────────────────────────────────────────

    #[test]
    fn timeframe_exactly_5min() {
        let s = ts(10, 0, 0);
        assert_eq!(fmt_timeframe(Some(s + chrono::Duration::seconds(300)), Some(s)), "5min");
    }

    #[test]
    fn timeframe_5min_within_tolerance() {
        let s = ts(10, 0, 0);
        assert_eq!(fmt_timeframe(Some(s + chrono::Duration::seconds(320)), Some(s)), "5min");
        assert_eq!(fmt_timeframe(Some(s + chrono::Duration::seconds(280)), Some(s)), "5min");
    }

    #[test]
    fn timeframe_exactly_15min() {
        let s = ts(10, 0, 0);
        assert_eq!(fmt_timeframe(Some(s + chrono::Duration::seconds(900)), Some(s)), "15min");
    }

    #[test]
    fn timeframe_exactly_1h() {
        let s = ts(10, 0, 0);
        assert_eq!(fmt_timeframe(Some(s + chrono::Duration::seconds(3600)), Some(s)), "1h");
    }

    #[test]
    fn timeframe_arbitrary_falls_back_to_nmin() {
        let s = ts(10, 0, 0);
        // 2700s = 45 min, not close to 5/15/60
        assert_eq!(fmt_timeframe(Some(s + chrono::Duration::seconds(2700)), Some(s)), "45min");
    }

    #[test]
    fn timeframe_missing_timestamps_returns_question_mark() {
        assert_eq!(fmt_timeframe(None, None), "?");
        assert_eq!(fmt_timeframe(Some(ts(10, 5, 0)), None), "?");
        assert_eq!(fmt_timeframe(None, Some(ts(10, 0, 0))), "?");
    }

    // ── build_xlsx produces valid output ──────────────────────────────────────

    #[test]
    fn empty_writer_produces_valid_xlsx() {
        let w = writer();
        let bytes = w.build_xlsx();
        // XLSX files start with PK zip magic bytes
        assert!(bytes.len() > 100, "xlsx should not be empty");
        assert_eq!(&bytes[0..2], b"PK", "xlsx should start with PK zip header");
    }

    #[test]
    fn xlsx_with_closed_trades_is_larger() {
        let w_empty = writer();
        let empty_size = w_empty.build_xlsx().len();

        let mut w = writer();
        w.rows.push(TradeRow {
            status: "CLOSED".to_string(),
            result: Some(true),
            won: Some(true),
            realized_pnl: Some(Decimal::try_from(29.0_f64).unwrap()),
            ..open_row("BTC", ts(10, 30, 15), 900, "YES", 31.0, 0.02)
        });
        w.by_token.insert("tok1".into(), 0);

        let bytes = w.build_xlsx();
        // With closed trades, xlsx should be bigger (has Analysis sheet + trade data)
        assert!(bytes.len() > empty_size, "xlsx with closed trades should be larger than empty");
        assert_eq!(&bytes[0..2], b"PK", "should be valid zip/xlsx");
    }

    // ── OPEN row fields ───────────────────────────────────────────────────────

    #[test]
    fn open_row_has_correct_fields() {
        let mut w = writer();
        w.rows.push(open_row("BTC", ts(10, 35, 22), 300, "YES", 52.0, 0.02));
        w.by_token.insert("tok1".into(), 0);

        let row = get_row(&w, "tok1");
        assert_eq!(row.coin, "BTC");
        assert_eq!(row.side, "YES");
        assert_eq!(row.status, "OPEN");
        assert_eq!(row.kelly_stake_usdc, Some(52.0));
        assert_eq!(row.cost_fee_rate, Some(0.02));
        assert_eq!(fmt_timeframe(row.end_time, row.window_start), "5min");
        assert!(row.result.is_none());
        assert!(row.won.is_none());
        assert!(row.realized_pnl.is_none());
    }

    #[test]
    fn no_side_maps_correctly() {
        let mut w = writer();
        w.rows.push(open_row("ETH", ts(10, 0, 0), 300, "NO", 31.0, 0.02));
        w.by_token.insert("tok1".into(), 0);

        let row = get_row(&w, "tok1");
        assert_eq!(row.side, "NO");
    }

    // ── CLOSED row fields ─────────────────────────────────────────────────────

    #[test]
    fn closed_win_row_has_correct_fields() {
        let mut w = writer();
        w.rows.push(TradeRow {
            status: "CLOSED".to_string(),
            result: Some(true),
            won: Some(true),
            realized_pnl: Some(Decimal::try_from(29.0_f64).unwrap()),
            ..open_row("BTC", ts(10, 30, 15), 900, "YES", 31.0, 0.02)
        });
        w.by_token.insert("tok1".into(), 0);

        let row = get_row(&w, "tok1");
        assert_eq!(row.status, "CLOSED");
        assert_eq!(row.result, Some(true));
        assert_eq!(row.won, Some(true));
        assert_eq!(row.realized_pnl.and_then(|v| v.to_f64()), Some(29.0));
        assert_eq!(row.kelly_stake_usdc, Some(31.0));
    }

    #[test]
    fn closed_loss_row_has_negative_pnl() {
        let mut w = writer();
        w.rows.push(TradeRow {
            status: "CLOSED".to_string(),
            result: Some(false),
            won: Some(false),
            realized_pnl: Some(Decimal::try_from(-45.0_f64).unwrap()),
            ..open_row("BTC", ts(10, 15, 3), 300, "YES", 45.0, 0.02)
        });
        w.by_token.insert("tok1".into(), 0);

        let row = get_row(&w, "tok1");
        assert_eq!(row.result, Some(false));
        assert_eq!(row.won, Some(false));
        assert!(row.realized_pnl.and_then(|v| v.to_f64()).unwrap() < 0.0);
    }

    // ── row ordering ──────────────────────────────────────────────────────────

    #[test]
    fn rows_stored_in_insertion_order() {
        let mut w = writer();
        w.rows.push(open_row("BTC", ts(10, 0, 0), 300, "YES", 50.0, 0.02));
        w.by_token.insert("tok1".into(), 0);
        w.rows.push(open_row("ETH", ts(10, 5, 0), 300, "NO", 30.0, 0.02));
        w.by_token.insert("tok2".into(), 1);

        assert_eq!(w.rows.len(), 2);
        assert_eq!(w.rows[0].coin, "BTC");
        assert_eq!(w.rows[1].coin, "ETH");
        // build_xlsx reverses within each run (newest first)
        // just verify the xlsx is produced successfully
        let bytes = w.build_xlsx();
        assert!(bytes.len() > 100);
    }

    // ── record_opened deduplication ───────────────────────────────────────────

    #[tokio::test]
    async fn duplicate_open_is_ignored() {
        let mut w = writer();
        let opened = ts(10, 0, 0);
        let end = Some(opened + chrono::Duration::seconds(300));

        w.record_opened("tok1".into(), "BTC".into(), opened, end, Some(opened), "YES".into(), Some(52.0), Some(0.02));
        w.record_opened("tok1".into(), "BTC".into(), opened, end, Some(opened), "YES".into(), Some(52.0), Some(0.02));

        assert_eq!(w.rows.len(), 1, "duplicate should be ignored");
    }

    // ── record_closed: known token updates in-place ───────────────────────────

    #[tokio::test]
    async fn close_known_token_updates_status_and_pnl() {
        let mut w = writer();
        let opened = ts(10, 0, 0);
        let end = Some(opened + chrono::Duration::seconds(300));

        w.record_opened("tok1".into(), "BTC".into(), opened, end, Some(opened), "YES".into(), Some(52.0), Some(0.02));

        let rec = closed_rec("tok1", OutcomeTokenSide::Yes, true, Decimal::try_from(48.0_f64).unwrap());
        w.record_closed(&rec);

        let row = get_row(&w, "tok1");
        assert_eq!(row.status, "CLOSED");
        assert_eq!(row.result, Some(true));
        assert_eq!(row.won, Some(true));
        assert_eq!(row.realized_pnl.and_then(|v| v.to_f64()), Some(48.0));
        // Stake and fee preserved from open
        assert_eq!(row.kelly_stake_usdc, Some(52.0));
        assert_eq!(row.cost_fee_rate, Some(0.02));
    }

    #[tokio::test]
    async fn close_no_side_yes_outcome_is_loss() {
        let mut w = writer();
        let opened = ts(10, 0, 0);
        let end = Some(opened + chrono::Duration::seconds(300));

        w.record_opened("tok1".into(), "BTC".into(), opened, end, Some(opened), "NO".into(), Some(31.0), Some(0.02));

        // Held NO token but YES resolved — that's a loss
        let rec = closed_rec("tok1", OutcomeTokenSide::No, true, Decimal::try_from(-31.0_f64).unwrap());
        w.record_closed(&rec);

        let row = get_row(&w, "tok1");
        assert_eq!(row.won, Some(false));
    }

    // ── record_closed: unknown token creates fallback row ─────────────────────

    #[tokio::test]
    async fn close_unknown_token_creates_fallback_row() {
        let mut w = writer();

        let rec = closed_rec("tok-prev-session", OutcomeTokenSide::Yes, true, Decimal::try_from(20.0_f64).unwrap());
        w.record_closed(&rec);

        assert_eq!(w.rows.len(), 1, "expected 1 row");
        let row = get_row(&w, "tok-prev-session");
        assert_eq!(row.coin, "BTC");       // from rec.asset.symbol()
        assert_eq!(row.status, "CLOSED");
        assert_eq!(row.side, "YES");
        assert_eq!(row.won, Some(true));
        assert_eq!(row.realized_pnl.and_then(|v| v.to_f64()), Some(20.0));
        assert!(row.kelly_stake_usdc.is_none()); // no stake (not opened this session)
        assert!(row.cost_fee_rate.is_none());     // no fee
    }

    // ── seed_from_today_log ───────────────────────────────────────────────────

    /// Return a unique temp directory for this test (cleaned up by caller).
    fn temp_log_dir(suffix: &str) -> PathBuf {
        let dir = std::env::temp_dir()
            .join(format!("trade-sheet-test-{}", suffix));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    /// Write a minimal events JSONL file that seed_from_today_log can parse.
    fn write_events_file(dir: &std::path::Path, lines: &[serde_json::Value]) {
        let today = to_et(Utc::now()).format("%Y-%m-%d").to_string();
        let day_dir = dir.join(&today);
        std::fs::create_dir_all(&day_dir).unwrap();
        let path = day_dir.join(format!("events-{}.json", today));
        let content: String = lines
            .iter()
            .map(|v| serde_json::to_string(v).unwrap())
            .collect::<Vec<_>>()
            .join("\n");
        std::fs::write(&path, content).unwrap();
    }

    const TEST_RUN_ID: &str = "test-run-1";
    const TEST_BOT_VERSION: &str = "0.0.0-test";

    fn pipeline_eval_line(
        token_id_yes: &str,
        token_id_no: &str,
        edge_side: &str,   // "BUY YES" or "BUY NO"
        asset: &str,
        stake: f64,
        fee: f64,
        seq: u64,
        opened_at: DateTime<Utc>,
        end_time: DateTime<Utc>,
        start_time: DateTime<Utc>,
    ) -> serde_json::Value {
        pipeline_eval_line_with_run(
            token_id_yes, token_id_no, edge_side, asset, stake, fee, seq,
            opened_at, end_time, start_time, TEST_RUN_ID, TEST_BOT_VERSION,
        )
    }

    fn pipeline_eval_line_with_run(
        token_id_yes: &str,
        token_id_no: &str,
        edge_side: &str,
        asset: &str,
        stake: f64,
        fee: f64,
        seq: u64,
        opened_at: DateTime<Utc>,
        end_time: DateTime<Utc>,
        start_time: DateTime<Utc>,
        run_id: &str,
        bot_version: &str,
    ) -> serde_json::Value {
        serde_json::json!({
            "event_type": "pipeline_evaluation",
            "event_ts": opened_at.to_rfc3339(),
            "seq": seq,
            "run_id": run_id,
            "bot_version": bot_version,
            "payload": {
                "market": {
                    "token_id_yes": token_id_yes,
                    "token_id_no": token_id_no,
                    "asset": asset,
                    "end_time": end_time.to_rfc3339(),
                    "start_time": start_time.to_rfc3339(),
                },
                "action": {
                    "kind": "trade_candidate",
                    "side": edge_side,
                },
                "sizing_risk_context": { "stake_usdc": stake },
                "cost_context": { "fee": fee },
            }
        })
    }

    fn settlement_line(token_id: &str, actual_yes: bool, won: bool, pnl: f64) -> serde_json::Value {
        settlement_line_with_run(token_id, actual_yes, won, pnl, TEST_RUN_ID, TEST_BOT_VERSION)
    }

    fn settlement_line_with_run(
        token_id: &str,
        actual_yes: bool,
        won: bool,
        pnl: f64,
        run_id: &str,
        bot_version: &str,
    ) -> serde_json::Value {
        serde_json::json!({
            "event_type": "position_settled",
            "event_ts": Utc::now().to_rfc3339(),
            "run_id": run_id,
            "bot_version": bot_version,
            "payload": {
                "token_id": token_id,
                "actual_yes_outcome": actual_yes,
                "won": won,
                "realized_pnl": pnl,
            }
        })
    }

    #[tokio::test]
    async fn seed_loads_open_trade_from_log() {
        let dir = temp_log_dir("open");
        let opened = ts(10, 30, 0);
        let end = opened + chrono::Duration::seconds(300);

        write_events_file(&dir, &[
            pipeline_eval_line("yes-tok", "no-tok", "BUY YES", "BTC", 52.0, 0.02, 1, opened, end, opened),
        ]);

        let mut w = TradeSheetWriter::new(dir.to_str().unwrap(), TEST_BOT_VERSION);
        w.seed_from_today_log(dir.to_str().unwrap()).await;

        assert_eq!(w.rows.len(), 1, "expected 1 row");
        let row = get_row(&w, "yes-tok");
        assert_eq!(row.coin, "BTC");
        assert_eq!(fmt_timeframe(row.end_time, row.window_start), "5min");
        assert_eq!(row.status, "OPEN");
        assert_eq!(row.side, "YES");
        assert_eq!(row.kelly_stake_usdc, Some(52.0));
        assert_eq!(row.cost_fee_rate, Some(0.02));
    }

    #[tokio::test]
    async fn seed_marks_settled_trade_as_closed() {
        let dir = temp_log_dir("closed");
        let opened = ts(10, 0, 0);
        let end = opened + chrono::Duration::seconds(300);

        write_events_file(&dir, &[
            pipeline_eval_line("yes-tok", "no-tok", "BUY YES", "BTC", 31.0, 0.02, 1, opened, end, opened),
            settlement_line("yes-tok", true, true, 29.0),
        ]);

        let mut w = TradeSheetWriter::new(dir.to_str().unwrap(), TEST_BOT_VERSION);
        w.seed_from_today_log(dir.to_str().unwrap()).await;

        let row = get_row(&w, "yes-tok");
        assert_eq!(row.status, "CLOSED");
        assert_eq!(row.result, Some(true));
        assert_eq!(row.won, Some(true));
        assert_eq!(row.realized_pnl.and_then(|v| v.to_f64()), Some(29.0));
    }

    #[tokio::test]
    async fn seed_handles_no_side_trade() {
        let dir = temp_log_dir("noside");
        let opened = ts(10, 0, 0);
        let end = opened + chrono::Duration::seconds(900);

        write_events_file(&dir, &[
            pipeline_eval_line("yes-tok", "no-tok", "BUY NO", "ETH", 40.0, 0.02, 1, opened, end, opened),
        ]);

        let mut w = TradeSheetWriter::new(dir.to_str().unwrap(), TEST_BOT_VERSION);
        w.seed_from_today_log(dir.to_str().unwrap()).await;

        let row = get_row(&w, "no-tok");
        assert_eq!(row.coin, "ETH");
        assert_eq!(fmt_timeframe(row.end_time, row.window_start), "15min");
        assert_eq!(row.side, "NO");
    }

    #[tokio::test]
    async fn seed_skips_no_trade_evaluations() {
        let dir = temp_log_dir("notrade");
        let opened = ts(10, 0, 0);
        let end = opened + chrono::Duration::seconds(300);

        write_events_file(&dir, &[
            serde_json::json!({
                "event_type": "pipeline_evaluation",
                "event_ts": opened.to_rfc3339(),
                "seq": 1,
                "payload": {
                    "market": { "token_id_yes": "y", "token_id_no": "n", "asset": "BTC",
                                "end_time": end.to_rfc3339(), "start_time": opened.to_rfc3339() },
                    "action": { "kind": "no_trade", "side": null },
                    "sizing_risk_context": { "stake_usdc": 50.0 },
                    "cost_context": { "fee": 0.02 },
                }
            }),
        ]);

        let mut w = TradeSheetWriter::new(dir.to_str().unwrap(), TEST_BOT_VERSION);
        w.seed_from_today_log(dir.to_str().unwrap()).await;

        assert_eq!(w.rows.len(), 0, "no_trade rows should be skipped");
    }

    #[tokio::test]
    async fn seed_deduplicates_repeated_token_ids() {
        let dir = temp_log_dir("dedup");
        let opened = ts(10, 0, 0);
        let end = opened + chrono::Duration::seconds(300);
        let ev = pipeline_eval_line("yes-tok", "no-tok", "BUY YES", "BTC", 52.0, 0.02, 1, opened, end, opened);

        write_events_file(&dir, &[ev.clone(), ev]);

        let mut w = TradeSheetWriter::new(dir.to_str().unwrap(), TEST_BOT_VERSION);
        w.seed_from_today_log(dir.to_str().unwrap()).await;

        assert_eq!(w.rows.len(), 1, "duplicate token_id should only add one row");
    }

    #[tokio::test]
    async fn seed_does_not_overwrite_already_recorded_rows() {
        let dir = temp_log_dir("nooverwrite");
        let opened = ts(10, 0, 0);
        let end = Some(opened + chrono::Duration::seconds(300));

        // Manually add a row before seeding
        let mut w = TradeSheetWriter::new(dir.to_str().unwrap(), TEST_BOT_VERSION);
        w.record_opened("yes-tok".into(), "BTC".into(), opened, end, Some(opened), "YES".into(), Some(99.0), Some(0.05));

        // Log has different stake for same token
        write_events_file(&dir, &[
            pipeline_eval_line("yes-tok", "no-tok", "BUY YES", "BTC", 52.0, 0.02, 1, opened, end.unwrap(), opened),
        ]);
        w.seed_from_today_log(dir.to_str().unwrap()).await;

        assert_eq!(w.rows.len(), 1);
        let row = get_row(&w, "yes-tok");
        assert_eq!(row.kelly_stake_usdc, Some(99.0), "pre-recorded row should not be overwritten by seed");
    }

    #[tokio::test]
    async fn seed_returns_silently_when_log_missing() {
        let dir = temp_log_dir("missing");
        let mut w = TradeSheetWriter::new(dir.to_str().unwrap(), TEST_BOT_VERSION);
        // No events file written
        w.seed_from_today_log(dir.to_str().unwrap()).await;
        assert_eq!(w.rows.len(), 0);
    }

    // ── fee edge cases ────────────────────────────────────────────────────────

    #[test]
    fn fee_missing_when_either_value_absent() {
        let mut w = writer();
        w.rows.push(TradeRow {
            cost_fee_rate: None,
            ..open_row("BTC", ts(10, 0, 0), 300, "YES", 50.0, 0.0)
        });
        w.by_token.insert("tok1".into(), 0);

        let row = get_row(&w, "tok1");
        assert!(row.cost_fee_rate.is_none());
    }

    // ── multi-run grouping ───────────────────────────────────────────────────

    #[tokio::test]
    async fn seed_discovers_multiple_runs_and_groups_them() {
        let dir = temp_log_dir("multirun");
        let opened1 = ts(10, 0, 0);
        let end1 = opened1 + chrono::Duration::seconds(300);
        let opened2 = ts(14, 0, 0);
        let end2 = opened2 + chrono::Duration::seconds(300);

        write_events_file(&dir, &[
            pipeline_eval_line_with_run(
                "yes-tok1", "no-tok1", "BUY YES", "BTC", 52.0, 0.02, 1,
                opened1, end1, opened1, "run-morning", "1.0.0",
            ),
            settlement_line_with_run("yes-tok1", true, true, 29.0, "run-morning", "1.0.0"),
            pipeline_eval_line_with_run(
                "yes-tok2", "no-tok2", "BUY NO", "ETH", 40.0, 0.02, 2,
                opened2, end2, opened2, "run-afternoon", "1.1.0",
            ),
        ]);

        let mut w = TradeSheetWriter::new(dir.to_str().unwrap(), "1.1.0");
        w.seed_from_today_log(dir.to_str().unwrap()).await;

        // Run 1 trade (BTC, closed)
        let r1 = get_row(&w, "yes-tok1");
        assert_eq!(r1.coin, "BTC");
        assert_eq!(r1.status, "CLOSED");
        assert_eq!(r1.run_number, 1);

        // Run 2 trade (ETH, open, NO side)
        let r2 = get_row(&w, "no-tok2");
        assert_eq!(r2.coin, "ETH");
        assert_eq!(r2.status, "OPEN");
        assert_eq!(r2.side, "NO");
        assert_eq!(r2.run_number, 2);

        // Current run should be 3
        assert_eq!(w.current_run_number, 3);

        // Verify xlsx still builds successfully with multi-run data
        let bytes = w.build_xlsx();
        assert!(bytes.len() > 100);
    }
}
