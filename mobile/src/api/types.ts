export type Greeks = {
  delta: number | null;
  gamma: number | null;
  theta: number | null;
  vega: number | null;
};

export type MobileError = {
  ok: false;
  error: {
    code: string;
    message: string;
  };
  timestamp: string;
};

export type Position = {
  symbol: string | null;
  contract_type: string | null;
  size: number | null;
  entry_price: number | null;
  mark_price: number | null;
  liquidation_price: number | null;
  margin: number | null;
  realized_pnl: number | null;
  unrealized_pnl: number | null;
  computed_delta: number | null;
  computed_gamma: number | null;
  computed_theta: number | null;
  computed_vega: number | null;
};

export type Balance = {
  asset_symbol: string;
  balance: number | null;
  available_balance: number | null;
  blocked_margin: number | null;
  order_margin: number | null;
  position_margin: number | null;
};

export type SubwalletAccount = {
  id: string;
  label: string;
  kind: string;
  ok: boolean;
  error?: string | null;
  net_equity: number | null;
  balance: number | null;
  available_balance: number | null;
  blocked_margin: number | null;
  order_margin: number | null;
  position_margin: number | null;
  margin_utilization_pct: number | null;
  position_count: number;
  greeks: Greeks;
  balances: Balance[];
  positions: Position[];
};

export type SubwalletsResponse = {
  ok: boolean;
  last_updated: string;
  aggregate: {
    net_equity: number | null;
    balance: number | null;
    available_balance: number | null;
    blocked_margin: number | null;
    order_margin: number | null;
    position_margin: number | null;
    margin_utilization_pct: number | null;
    greeks: Greeks;
  };
  accounts: SubwalletAccount[];
};

export type IronFlyLeg = {
  action: string | null;
  option_type: string | null;
  strike: number | null;
  quantity: number | null;
  mark_price: number | null;
  open_interest: number | null;
  volume: number | null;
  iv: number | null;
  delta: number | null;
  gamma: number | null;
  theta: number | null;
  vega: number | null;
};

export type IronFlyCandidate = {
  expiry: string | null;
  dte: number | null;
  center_strike: number | null;
  wing_width: number | null;
  score: number | null;
  status: string | null;
  ranking_reason: string | null;
  liquidity_score: number | null;
  expected_move: number | null;
  median_iv: number | null;
  realized_vol_pct: number | null;
  iv_rv_spread: number | null;
  component_scores: Record<string, number>;
  net_greeks: Greeks;
  payoff: {
    net_credit: number | null;
    max_profit: number | null;
    max_loss: number | null;
    lower_breakeven: number | null;
    upper_breakeven: number | null;
    return_on_risk: number | null;
    wing_width: number | null;
  };
  legs?: IronFlyLeg[];
};

export type IronFlyResponse = {
  ok: boolean;
  last_updated: string;
  generated_at: string | null;
  symbol: string | null;
  recommendation: string | null;
  iron_fly_score: number | null;
  confidence: string | null;
  selected: IronFlyCandidate | null;
  top_alternatives: IronFlyCandidate[];
  expiry_comparison: unknown[];
  risk_factors: string[];
  entry_conditions: string[];
  adjustment_triggers: string[];
  stop_loss_logic: string | null;
  profit_booking_logic: string | null;
  time_based_exit: string | null;
  research_only: boolean;
};

export type HomeResponse = {
  ok: boolean;
  last_updated: string;
  backend: {
    ok: boolean;
    service: string;
    version: string;
  };
  market: {
    ok: boolean;
    symbol: string | null;
    spot_price: number | null;
    mark_price: number | null;
    last_updated: string;
  };
  subwallets: {
    ok: boolean;
    account_count: number;
    healthy_account_count: number;
    total_positions: number;
    aggregate: SubwalletsResponse["aggregate"];
    last_updated: string;
  };
  iron_fly: {
    ok: boolean;
    generated_at: string | null;
    recommendation: string | null;
    iron_fly_score: number | null;
    confidence: string | null;
    selected: Pick<IronFlyCandidate, "expiry" | "dte" | "center_strike" | "wing_width"> | null;
    last_updated: string;
  };
};
