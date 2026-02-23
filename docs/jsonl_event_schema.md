# JSONL Event Schema (`jsonl.v1`)

This bot writes one JSON object per line into:

- Active: `logs/YYYY-MM-DD/events-YYYY-MM-DD.json`
- Traded-only: `logs/YYYY-MM-DD/traded-decisions-YYYY-MM-DD.json`
- Rolled (optional): `logs/YYYY-MM-DD/*.json.gz`

## Envelope fields (all events)

- `schema_version`: string, currently `jsonl.v1`
- `event_type`: string (see catalog below)
- `event_ts`: RFC3339 ET timestamp (offset included, e.g. `-05:00` or `-04:00`)
- `seq`: monotonic sequence number per run
- `run_id`: run/session identifier
- `mode`: `paper` or `live`
- `paper_mode`: boolean
- `venue_type`: `international` or `us`
- `bot_version`: Cargo package version
- `git_sha`: optional build-time git sha
- `config_hash`: hash of startup config debug payload
- `payload`: event-specific object

## Event catalog

- `run_started`
- `market_scan`
- `pipeline_evaluation`
- `pipeline_action`
- `order_submitted`
- `order_submit_failed`
- `order_event`
- `fill_applied`
- `position_settled`
- `pnl_snapshot`
- `logger_health`
- `run_ended`

## Example rows

### `pipeline_evaluation`

```json
{
  "schema_version": "jsonl.v1",
  "event_type": "pipeline_evaluation",
  "event_ts": "2026-02-19T15:01:22.101-05:00",
  "seq": 1902,
  "run_id": "paper-20260219T150101.001EST-12345",
  "mode": "paper",
  "paper_mode": true,
  "venue_type": "international",
  "bot_version": "0.1.0",
  "git_sha": null,
  "config_hash": "abcd1234ef567890",
  "payload": {
    "sample_rate": 0.15,
    "was_sampled": true,
    "market": {
      "condition_id": "...",
      "token_id_yes": "...",
      "token_id_no": "...",
      "question": "Ethereum Up or Down - ...",
      "asset": "ETH",
      "start_time": "2026-02-19T15:00:00.000-05:00",
      "end_time": "2026-02-19T15:05:00.000-05:00",
      "price_to_beat": 66899.60,
      "price_to_beat_source": "field:priceToBeat"
    },
    "price_context": {
      "aggregated_spot": 1944.93,
      "confidence": 0.986,
      "spread_ratio": 0.0004,
      "num_sources": 4,
      "observations": []
    },
    "model_context": {
      "model_type": "updown_directional",
      "p_model": 0.6167,
      "posterior": 0.6150,
      "reference_price": 66899.60,
      "reference_price_source": "field:priceToBeat"
    },
    "cost_context": {
      "gross_edge": 0.1174,
      "net_edge": 0.0893,
      "effective_threshold": 0.0800
    },
    "action": {
      "kind": "trade_candidate",
      "side": "BUY NO"
    }
  }
}
```

### `position_settled`

```json
{
  "event_type": "position_settled",
  "payload": {
    "opened_at": "2026-02-19T15:00:04.000-05:00",
    "closed_at": "2026-02-19T15:05:12.000-05:00",
    "resolution_available_at": "2026-02-19T15:05:11.500-05:00",
    "hold_seconds": 308,
    "asset": "BTC",
    "side": "YES",
    "predicted_yes_prob": 0.58,
    "actual_yes_outcome": true,
    "payout_price": 1.0,
    "official_price_to_beat": 66899.60,
    "official_final_price": null,
    "settlement_source": "field:outcomePrices",
    "realized_pnl": 42.3,
    "won": true
  }
}
```

### `pnl_snapshot`

```json
{
  "event_type": "pnl_snapshot",
  "payload": {
    "trigger": "settlement",
    "capital": 1000.0,
    "realized": 120.4,
    "unrealized": -5.1,
    "total": 115.3,
    "equity": 1115.3,
    "win_rate": 54.55,
    "per_asset": [
      {"asset": "BTC", "realized": 80.1, "unrealized": 0.0, "total": 80.1}
    ]
  }
}
```

## Join keys for later analysis

- Primary run join: `run_id`
- Event order join: `seq`
- Market join: `payload.market.condition_id`, `payload.market.token_id_yes`, `payload.market.token_id_no`
- Order lifecycle join: `payload.order_id`
- Asset-level grouping: `payload.asset` or `payload.market.asset`

## Suggested analysis queries

- Win rate by confidence bucket (e.g., deciles of `payload.price_context.confidence`).
- Net edge vs realized P&L by asset and side.
- Cost decomposition drift: fee/slippage/latency over time.
- Calibration drift by asset using `predicted_yes_prob` vs `actual_yes_outcome`.
- No-trade reason frequency and false-negative review around threshold boundaries.
