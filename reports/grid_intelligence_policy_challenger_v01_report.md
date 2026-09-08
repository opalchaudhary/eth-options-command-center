# Grid Intelligence V1 - Policy Challenger Research V0.1

## Scope

- Research only. No live Grid Parameter Engine, Probability V2, GridBot, order, UI, deployment, or production policy code was changed.
- Focus horizon: `12H`.
- Champion: stored current Grid Parameter Recommender V0.1 ranges from `grid_parameter_recommendations`.
- Outcome source: `grid_parameter_recommendation_outcomes` plus persisted `eth_ohlcv` `5m` candles for challenger breach timing and trailing pre-recommendation buffers.
- Usable snapshots with recommended 12H ranges: 30.
- Matched 12H outcomes: 30 available; missing for usable snapshot set: 0.
- OHLCV windows read: 60 read-only windows.

## Challenger Definitions

1. `champion_v0_1`: use stored `recommended_lower_price` and `recommended_upper_price` exactly.
2. `expansion_widen`: keep champion midpoint; set width to champion width x 1.10 if expansion < 0.55, x 1.20 if 0.55 <= expansion < 0.72, x 1.35 if 0.72 <= expansion < 0.86, and x 1.60 if expansion >= 0.86.
3. `v2_range70_floor`: use `min(champion_lower, range_70_lower)` and `max(champion_upper, range_70_upper)`. Wider V2 ranges such as range_90 were not present in the allowed point-in-time V2 snapshot inputs, so this challenger only uses available range_70.
4. `v2_range70_trailing_buffer`: keep champion midpoint; base width is `max(champion_width, range_70_width)` where range_70 exists; add `0.35 * trailing_12h_path_width * (0.50 + expansion_probability)` using only persisted OHLCV before the recommendation timestamp.
5. `stress_filter_no_grid`: emit `NO_GRID` when `path_inside_70 <= 0.38 and expansion_probability >= 0.72`; otherwise use champion range.

Balance score is research-only: `containment_rate * grid_participation_rate - 0.20 * median_extra_width_vs_champion - 0.10 * max(0, 0.35 - median_range_utilization)`.

## 12H Results

| policy | usable_snapshots | grid_recommendations | no_grid | grid_participation_rate | abstention_rate | no_grid_avoided_champion_breach_rate | containment_rate | breach_rate | median_time_to_first_breach | median_actual_recommended_width_ratio | median_range_utilization | median_recommended_width | median_width_vs_champion | wide_range_penalty | balance_score |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| v2_range70_trailing_buffer | 30 | 30 | 0 | 1.0000 | 0.0000 |  | 0.4667 | 0.5333 | 319.6448 | 0.7470 | 0.5697 | 69.1564 | 1.5766 | 0.5766 | 0.3513 |
| champion_v0_1 | 30 | 30 | 0 | 1.0000 | 0.0000 |  | 0.3333 | 0.6667 | 114.8151 | 1.2012 | 0.6585 | 46.7750 | 1.0000 | 0.0000 | 0.3333 |
| v2_range70_floor | 30 | 30 | 0 | 1.0000 | 0.0000 |  | 0.3333 | 0.6667 | 114.8151 | 1.2012 | 0.6585 | 46.7750 | 1.0000 | 0.0000 | 0.3333 |
| expansion_widen | 30 | 30 | 0 | 1.0000 | 0.0000 |  | 0.4000 | 0.6000 | 222.2577 | 0.8898 | 0.6174 | 63.1463 | 1.3500 | 0.3500 | 0.3300 |
| stress_filter_no_grid | 30 | 5 | 25 | 0.1667 | 0.8333 | 0.8000 | 1.0000 | 0.0000 |  | 0.4743 | 0.4743 | 70.8000 | 1.0000 | 0.0000 | 0.1667 |

## Regime Findings

| segment | bucket | policy | n | containment_rate | median_width_vs_champion | median_actual_recommended_width_ratio | median_range_utilization |
| --- | --- | --- | --- | --- | --- | --- | --- |
| confidence_band | MEDIUM | champion_v0_1 | 30 | 0.3333 | 1.0000 | 1.2012 | 0.6585 |
| confidence_band | MEDIUM | expansion_widen | 30 | 0.4000 | 1.3500 | 0.8898 | 0.6174 |
| confidence_band | MEDIUM | stress_filter_no_grid | 5 | 1.0000 | 1.0000 | 0.4743 | 0.4743 |
| confidence_band | MEDIUM | v2_range70_floor | 30 | 0.3333 | 1.0000 | 1.2012 | 0.6585 |
| confidence_band | MEDIUM | v2_range70_trailing_buffer | 30 | 0.4667 | 1.5766 | 0.7470 | 0.5697 |
| directional_state | UNKNOWN | champion_v0_1 | 30 | 0.3333 | 1.0000 | 1.2012 | 0.6585 |
| directional_state | UNKNOWN | expansion_widen | 30 | 0.4000 | 1.3500 | 0.8898 | 0.6174 |
| directional_state | UNKNOWN | stress_filter_no_grid | 5 | 1.0000 | 1.0000 | 0.4743 | 0.4743 |
| directional_state | UNKNOWN | v2_range70_floor | 30 | 0.3333 | 1.0000 | 1.2012 | 0.6585 |
| directional_state | UNKNOWN | v2_range70_trailing_buffer | 30 | 0.4667 | 1.5766 | 0.7470 | 0.5697 |
| expansion_band | expansion_HIGH | champion_v0_1 | 10 | 0.2000 | 1.0000 | 1.7361 | 0.7302 |
| expansion_band | expansion_HIGH | expansion_widen | 10 | 0.2000 | 1.3500 | 1.2860 | 0.6705 |
| expansion_band | expansion_HIGH | v2_range70_floor | 10 | 0.2000 | 1.0000 | 1.7361 | 0.7302 |
| expansion_band | expansion_HIGH | v2_range70_trailing_buffer | 10 | 0.2000 | 1.6280 | 0.9310 | 0.6035 |
| expansion_band | expansion_LOW | champion_v0_1 | 10 | 0.5000 | 1.0000 | 0.6112 | 0.5869 |
| expansion_band | expansion_LOW | expansion_widen | 10 | 0.6000 | 1.2750 | 0.5061 | 0.5061 |
| expansion_band | expansion_LOW | stress_filter_no_grid | 5 | 1.0000 | 1.0000 | 0.4743 | 0.4743 |
| expansion_band | expansion_LOW | v2_range70_floor | 10 | 0.5000 | 1.0000 | 0.6111 | 0.5868 |
| expansion_band | expansion_LOW | v2_range70_trailing_buffer | 10 | 0.6000 | 1.5363 | 0.3393 | 0.3393 |
| expansion_band | expansion_MID | champion_v0_1 | 10 | 0.3000 | 1.0000 | 1.0174 | 0.6000 |
| expansion_band | expansion_MID | expansion_widen | 10 | 0.4000 | 1.3500 | 0.7536 | 0.5664 |
| expansion_band | expansion_MID | v2_range70_floor | 10 | 0.3000 | 1.0000 | 1.0174 | 0.6000 |
| expansion_band | expansion_MID | v2_range70_trailing_buffer | 10 | 0.6000 | 1.6202 | 0.6728 | 0.4991 |
| inside_band | inside_HIGH | champion_v0_1 | 10 | 0.5000 | 1.0000 | 0.6112 | 0.5869 |
| inside_band | inside_HIGH | expansion_widen | 10 | 0.7000 | 1.2750 | 0.5061 | 0.5061 |
| inside_band | inside_HIGH | stress_filter_no_grid | 5 | 1.0000 | 1.0000 | 0.4743 | 0.4743 |
| inside_band | inside_HIGH | v2_range70_floor | 10 | 0.5000 | 1.0000 | 0.6111 | 0.5868 |
| inside_band | inside_HIGH | v2_range70_trailing_buffer | 10 | 0.7000 | 1.5653 | 0.3393 | 0.3393 |
| inside_band | inside_LOW | champion_v0_1 | 10 | 0.2000 | 1.0000 | 1.5621 | 0.6701 |
| inside_band | inside_LOW | expansion_widen | 10 | 0.2000 | 1.3500 | 1.1571 | 0.6260 |
| inside_band | inside_LOW | v2_range70_floor | 10 | 0.2000 | 1.0000 | 1.5621 | 0.6701 |
| inside_band | inside_LOW | v2_range70_trailing_buffer | 10 | 0.3000 | 1.7324 | 0.8451 | 0.5858 |
| inside_band | inside_MID | champion_v0_1 | 10 | 0.3000 | 1.0000 | 1.8828 | 0.8216 |
| inside_band | inside_MID | expansion_widen | 10 | 0.3000 | 1.3500 | 1.3947 | 0.6734 |
| inside_band | inside_MID | v2_range70_floor | 10 | 0.3000 | 1.0000 | 1.8828 | 0.8216 |
| inside_band | inside_MID | v2_range70_trailing_buffer | 10 | 0.4000 | 1.4415 | 1.1756 | 0.6124 |

## Interpretation

- Best research balance by the defined score: `v2_range70_trailing_buffer`.
- Champion 12H containment: 0.3333 with median width ratio 1.2012 if champion data is available.
- Wider V2 range use is currently constrained because the allowed point-in-time recommendation snapshots preserve `range_70` only; no `range_90` or broader V2 interval was available for this replay.
- The stress filter is useful as a risk gate signal, but it abstained on many snapshots; its high conditional containment should not be compared to full-participation policies without the participation penalty.
- The trailing-buffer challenger tests whether recent realized path width can cover expansion regimes without blindly making every grid huge.

## V1.1 Readiness

Evidence is directional but not strong enough by itself to ship Parameter Engine V1.1. The sample is only 30 actionable 12H snapshots, all from one recommender version and a short calendar period. A candidate V1.1 can be proposed for paper trading/shadow replay if it improves containment materially without excessive width penalty, but production policy should wait for more matured snapshots and a non-overlapping validation period.

## Limitations

- Sample size is small; segment buckets use a minimum n=5.
- Challenger policies use stored snapshots and pre-recommendation OHLCV only; no future path information is used to choose ranges.
- 5-minute OHLCV cannot reconstruct true tick order inside a candle.
- Wider V2 ranges beyond `range_70` were unavailable in the allowed point-in-time inputs.
- This is research evidence, not a live recommendation policy change.
