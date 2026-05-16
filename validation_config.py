INR_PER_USDT = 85
ETH_LOT_SIZE = 0.01
PAPER_WALLET_CAPITAL_INR = 50000
PAPER_WALLET_CAPITAL_USDT = PAPER_WALLET_CAPITAL_INR / INR_PER_USDT

MAX_RISK_PER_TRADE_PCT = 0.02
MAX_MARGIN_USAGE_PCT = 0.35


def usdt_to_inr(value):
    return round(float(value or 0) * INR_PER_USDT, 2)


def inr_to_usdt(value):
    return round(float(value or 0) / INR_PER_USDT, 4)


def lot_notional_eth(lots):
    return round(float(lots or 0) * ETH_LOT_SIZE, 4)
