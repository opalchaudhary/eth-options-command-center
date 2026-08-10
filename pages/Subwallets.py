import pandas as pd
import streamlit as st

from api_client import api_get, backend_url
from ui_styles import load_css


st.set_page_config(
    page_title="Subwallets | ETH Options Command Center",
    layout="wide",
)

load_css()

st.title("Subwallets")
st.caption("Main account and subwallet positions, computed Greeks, balances, and aggregate exposure.")


def _fmt_number(value, digits=4):
    try:
        if value is None:
            return "NA"

        return f"{float(value):,.{digits}f}"
    except (TypeError, ValueError):
        return "NA"


def _fmt_money(value):
    try:
        if value is None:
            return "NA"

        return f"{float(value):,.2f}"
    except (TypeError, ValueError):
        return "NA"


def _flatten_balance_summary(balance_summary):
    rows = []

    for asset, values in (balance_summary.get("by_asset") or {}).items():
        row = {"asset_symbol": asset}
        row.update(values)
        rows.append(row)

    return rows


def _positions_table(positions):
    df = pd.DataFrame(positions or [])

    if df.empty:
        return df

    preferred_columns = [
        "symbol",
        "contract_type",
        "size",
        "entry_price",
        "mark_price",
        "margin",
        "liquidation_price",
        "realized_pnl",
        "unrealized_pnl",
        "computed_delta",
        "computed_gamma",
        "computed_theta",
        "computed_vega",
    ]
    columns = [column for column in preferred_columns if column in df.columns]

    return df[columns]


try:
    snapshot = api_get("/accounts/subwallets", timeout=15)
except Exception as exc:
    st.error(f"FastAPI backend unavailable at {backend_url()}: {exc}")
    st.stop()


if not snapshot.get("accounts"):
    st.warning(snapshot.get("error") or "No Delta account credentials are configured.")
    st.info(
        "Configure `DELTA_API_KEY` and `DELTA_API_SECRET` for the main account. "
        "For subwallets, add `DELTA_SUBWALLET_1_API_KEY`, `DELTA_SUBWALLET_1_API_SECRET`, "
        "`DELTA_SUBWALLET_2_API_KEY`, and `DELTA_SUBWALLET_2_API_SECRET`."
    )
    st.stop()


aggregate = snapshot.get("aggregate") or {}
aggregate_greeks = aggregate.get("greeks") or {}

st.subheader("Aggregate")

ag1, ag2, ag3, ag4 = st.columns(4)
ag1.metric("Net Delta", _fmt_number(aggregate_greeks.get("delta")))
ag2.metric("Net Gamma", _fmt_number(aggregate_greeks.get("gamma"), digits=6))
ag3.metric("Net Theta", _fmt_number(aggregate_greeks.get("theta")))
ag4.metric("Net Vega", _fmt_number(aggregate_greeks.get("vega")))

ab1, ab2, ab3, ab4 = st.columns(4)
ab1.metric("Net Equity", _fmt_money(aggregate.get("net_equity")))
ab2.metric("Total Balance", _fmt_money(aggregate.get("balance")))
ab3.metric("Available Balance", _fmt_money(aggregate.get("available_balance")))
ab4.metric("Blocked Margin", _fmt_money(aggregate.get("blocked_margin")))

aggregate_balance_rows = _flatten_balance_summary({"by_asset": aggregate.get("balances_by_asset") or {}})

with st.expander("Aggregate Balance By Asset", expanded=True):
    if aggregate_balance_rows:
        st.dataframe(pd.DataFrame(aggregate_balance_rows), use_container_width=True, hide_index=True)
    else:
        st.warning("No aggregate wallet balances available.")


st.divider()
st.subheader("Accounts")

for account in snapshot.get("accounts") or []:
    label = account.get("label") or account.get("id")

    with st.container(border=True):
        st.markdown(f"### {label}")

        if not account.get("ok"):
            st.error(account.get("error") or "Account snapshot unavailable.")
            continue

        greeks = account.get("greeks") or {}
        balance_summary = account.get("balance_summary") or {}

        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Positions", account.get("position_count", 0))
        c2.metric("Net Delta", _fmt_number(greeks.get("delta")))
        c3.metric("Net Gamma", _fmt_number(greeks.get("gamma"), digits=6))
        c4.metric("Net Equity", _fmt_money(balance_summary.get("net_equity")))

        c5, c6, c7, c8 = st.columns(4)
        c5.metric("Net Theta", _fmt_number(greeks.get("theta")))
        c6.metric("Net Vega", _fmt_number(greeks.get("vega")))
        c7.metric("Available Balance", _fmt_money(sum(
            (values.get("available_balance") or 0)
            for values in (balance_summary.get("by_asset") or {}).values()
        )))
        c8.metric("Blocked Margin", _fmt_money(sum(
            (values.get("blocked_margin") or 0)
            for values in (balance_summary.get("by_asset") or {}).values()
        )))

        positions_df = _positions_table(account.get("positions"))
        balance_rows = _flatten_balance_summary(balance_summary)

        tab_positions, tab_balances = st.tabs(["Positions & Greeks", "Balances"])

        with tab_positions:
            if positions_df.empty:
                st.info("No open margined positions.")
            else:
                st.dataframe(positions_df, use_container_width=True, hide_index=True)

        with tab_balances:
            if balance_rows:
                st.dataframe(pd.DataFrame(balance_rows), use_container_width=True, hide_index=True)
            else:
                st.info("No wallet balances returned.")


st.caption(
    "Greeks are computed from each open position size multiplied by the current Delta ticker Greeks "
    "for the same product symbol."
)
