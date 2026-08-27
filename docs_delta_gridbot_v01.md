# DeltaGridBot V0.1

DeltaGridBot V0.1 is a Delta Exchange India Demo/Testnet-only execution engine. Its purpose is execution quality, reliability, observability, accounting correctness, and research-grade data collection.

It deliberately does not integrate Probability Engine V2, AI recommendations, automatic range selection, automatic adaptive regridding, hedge execution, live Delta execution, or concurrent grids.

## Safety

Execution clients are hard restricted to:

- REST: `https://cdn-ind.testnet.deltaex.org`
- Private WebSocket: `wss://socket-ind.testnet.deltaex.org`
- Public WebSocket: `wss://socket-ind-pub.testnet.deltaex.org`

The client validates hostnames before private REST calls and order submission. Production India, global production, and global testnet endpoints are rejected.

## Architecture

The package follows:

Grid Strategy -> Order Proposal -> GridBot Risk Controls -> Execution -> Delta Testnet

Key modules:

- `grid_bot/grid_builder.py`: arithmetic/geometric levels, tick rounding, preview
- `grid_bot/risk.py`: V0.1 risk states and order checks
- `grid_bot/accounting.py`: Delta-reported fee/funding/cost ledger and net trading P&L
- `grid_bot/delta_testnet_client.py`: hard-guarded Testnet REST/auth helpers
- `grid_bot/reconciliation.py`: exchange-authoritative mismatch detection
- `grid_bot/engine.py`: create/start/pause/resume/stop and immutable summaries
- `grid_bot/supabase_repository.py`: Supabase-backed GridBot durable state, history, and idempotency records

## Durable Persistence

V0.1 treats Supabase as the durable GridBot source of truth and Delta Exchange as the authoritative execution state. The local `grid_bot_state_v01.json` file is only an emergency/local cache once the Supabase schema is applied.

Startup recovery queries Supabase for an active run, loads the active configuration version, persisted rounded grid levels, known GridBot orders, known fills, and then reconciles against Delta open orders, recent fills, and the ETHUSD position. Recovery must not depend on regenerating historical grid levels from config alone.

External exchange operations cannot be perfectly atomic with database writes, so order placement uses an intent-first workflow:

1. Generate deterministic `client_order_id`.
2. Persist `grid_order_proposals` before calling Delta.
3. Submit the post-only Delta order.
4. Persist the resulting `grid_orders` identity immediately.
5. If the post-submit persistence step fails, cancel the submitted Delta order and fail closed.
6. Reconciliation can recover uncertain results by matching the deterministic `client_order_id`.

If Supabase persistence is unavailable while Supabase mode is enabled, lifecycle operations fail before normal trading continues.

## Risk Formulas

Inventory Utilisation v0.1:

`abs(net inventory) / max inventory`

Grid nature defines the permitted net inventory range:

- Neutral: `-MaxInventory <= inventory <= +MaxInventory`
- Long / Bullish Bias: `0 <= inventory <= +MaxInventory`
- Short / Bearish Bias: `-MaxInventory <= inventory <= 0`

Max Inventory is net inventory capacity, not the sum of all mathematical grid
levels. Outstanding opening orders reserve capacity before they fill. For
example, in Long mode, filled long inventory plus remaining outstanding opening
BUY quantity must stay within Max Inventory. In Short mode, filled short
inventory plus remaining outstanding opening SELL quantity must stay within Max
Inventory. Neutral mode tracks long-side and short-side opening capacity
separately; opposite outstanding risk is not treated as safe netting.

Risk-reducing orders remain allowed when they do not cross through the permitted
inventory range. Max Inventory is an accumulation brake, not a trap.

Arithmetic and Geometric level generation is independent of grid nature:

- Arithmetic: `(upper - lower) / (grid_count - 1)`
- Geometric: `(upper / lower) ** (1 / (grid_count - 1))`

`grid_count = N` means N total mathematical price levels. The number of active
opening orders can be lower than N when grid nature, inventory reservation, or
post-only safety defers some levels. Long mode does not force a market long at
startup; it only places eligible opening BUY orders below market. Short mode
does not force a market short at startup; it only places eligible opening SELL
orders above market.

Post-only execution safety is separate from the mathematical grid level. BUY
execution prices are rounded down to the valid Delta tick, SELL execution prices
are rounded up, and orders that would cross the current best ask/bid are
deferred rather than converted into taker orders.

Grid Risk Ratio v0.1:

`projected adverse grid exposure / configured risk capital`

GRR is an experimental V0.1 telemetry metric, not an optimality claim.

Risk states are `GREEN`, `YELLOW`, `ORANGE`, `RED`, and `CRITICAL`. RED/CRITICAL block new risk-increasing activity in the V0.1 controller. No automatic market liquidation is performed.

## Account-Health GRR V0.1a

The operator-facing dashboard no longer asks for manual allocated capital or risk capital buckets. Preview uses exchange/product metadata and the current account response where available.

Dashboard GRR v0.1a:

`projected_grid_exposure / account_equity`

Where:

- `projected_grid_exposure = (max potential long inventory + max potential short inventory) * reference ETH price * contract multiplier`
- `reference ETH price` is derived from current Delta bid/ask and rounded to tick size
- `account_equity` is read from Delta account/profile fields when available

If account equity is unavailable, the dashboard shows `UNKNOWN` and treats the exposure estimate as informational. This is a transparent V0.1 operator-health metric, not an optimal portfolio margin model.

## Accounting

No GST, personal income tax, or inferred tax model is included.

Net Trading P&L Before Income Tax:

`gross realised P&L - trading fees + net funding - other exchange costs + other exchange credits`

Maker/taker roles are recorded only when Delta reports or reconciliation can reliably infer them; otherwise fills remain `unknown`.

## Immutable Summary

Stopping a run performs reconciliation events first, then generates an immutable Grid Run Summary. Normal updates are rejected by both application repository logic and the Supabase trigger in `migrations/grid_bot_v01_schema.sql`.

## Known V0.1 Limitations

This implementation establishes the execution architecture, safety guard, API/UI surface, persistence schema, and deterministic core logic. Actual long-running market observation and natural fill/replacement behavior must be validated against Delta Testnet over repeated runs before V0.2 work begins.

## REST Fallback

When authenticated REST and public WebSocket are healthy but private WebSocket is unavailable, V0.1 may enter `REST_FALLBACK` with operational state `DEGRADED`. This is Delta Demo/Testnet-only validation behavior and is not an automatic approval for live V1 execution.

REST fallback polling reconciles open orders, recent fills, current ETHUSD position, and account/margin state. Fills use Delta exchange fill IDs as an idempotency barrier. Each poll uses an overlapping lookback from the last confirmed fill timestamp plus fill ID deduplication, so repeated fills across polls or restarts are ignored.

Private WebSocket remains the preferred account-event transport. REST fallback is being validated in V0.1 as a resilience/degraded-mode path.

## Exchange Truth Reconciliation

Durable reconciliation uses exchange truth in this order: current open orders,
bounded/paginated order history, bounded/paginated fills, then current ETHUSD
position. A GridBot fill is attributed only through a persisted GridBot order by
`client_order_id` or exchange `order_id`; ETHUSD symbol or nearby timestamps are
not enough. Missing fills are persisted before terminal order state is assigned,
so an order absent from open orders is not treated as manually cancelled unless
order-history evidence supports cancellation.

Fill-derived GridBot inventory is computed from persisted fills with BUY as
positive and SELL as negative, then compared with the observed Delta ETHUSD
position. Any unexplained difference is reported as `POSITION_MISMATCH`. The
query pattern is active-run and relevant-order scoped; reconciliation does not
download all historical GridBot rows.
