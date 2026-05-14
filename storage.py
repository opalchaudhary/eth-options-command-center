import os
import json
import requests
from dotenv import load_dotenv
from datetime import datetime, timezone

try:
    import streamlit as st
except Exception:
    st = None


load_dotenv()


def get_secret(key):
    if st is not None:
        try:
            if key in st.secrets:
                return st.secrets[key]
        except Exception:
            pass

    return os.getenv(key)


SUPABASE_URL = get_secret("SUPABASE_URL")
SUPABASE_KEY = get_secret("SUPABASE_KEY")


if not SUPABASE_URL or not SUPABASE_KEY:
    print("❌ Supabase credentials missing. Check .env or Streamlit Secrets.")


HEADERS = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json",
    "Prefer": "return=minimal"
}


def post_to_supabase(table_name, payload):
    if not SUPABASE_URL or not SUPABASE_KEY:
        print("❌ Supabase not configured.")
        return False

    url = f"{SUPABASE_URL}/rest/v1/{table_name}"

    try:
        response = requests.post(
            url,
            headers=HEADERS,
            json=payload,
            timeout=10
        )

        if response.status_code in [200, 201, 204]:
            return True

        print(f"❌ Supabase insert failed: {table_name}")
        print("Status:", response.status_code)
        print("Response:", response.text)
        print("Payload:", payload)
        return False

    except requests.exceptions.Timeout:
        print(f"⚠️ Supabase timeout while saving to {table_name}")
        return False

    except requests.exceptions.RequestException as e:
        print(f"⚠️ Supabase request error in {table_name}:", str(e))
        return False

    except Exception as e:
        print(f"❌ Unexpected Supabase error in {table_name}:", str(e))
        return False


def save_analytics_snapshot(analytics, expiry_label):
    row = {
        "snapshot_time": datetime.now(timezone.utc).isoformat(),
        "expiry_label": expiry_label,
        "spot_price": analytics.get("spot_price"),
        "max_pain": analytics.get("max_pain"),
        "atm_strike": analytics.get("atm_strike"),
        "pcr": analytics.get("pcr"),
        "atm_straddle_price": analytics.get("atm_straddle_price"),
        "expected_move_pct": analytics.get("expected_move_pct"),
        "expected_move_upper": analytics.get("expected_move_upper"),
        "expected_move_lower": analytics.get("expected_move_lower")
    }

    if post_to_supabase("analytics_snapshots", row):
        print("✅ Analytics Snapshot Saved")
        return True

    return False


def save_premium_decay_snapshot(
    expiry_label,
    atm_strike,
    atm_ce_price,
    atm_pe_price,
    atm_straddle_price
):
    row = {
        "snapshot_time": datetime.now(timezone.utc).isoformat(),
        "expiry_label": expiry_label,
        "atm_strike": atm_strike,
        "atm_ce_price": atm_ce_price,
        "atm_pe_price": atm_pe_price,
        "atm_straddle_price": atm_straddle_price
    }

    if post_to_supabase("premium_decay_snapshots", row):
        print("✅ Premium Decay Snapshot Saved")
        return True

    return False


def save_option_chain_snapshot(expiry_df, expiry_label):
    if expiry_df is None or expiry_df.empty:
        print("⚠️ Option Chain Snapshot Skipped: Empty dataframe")
        return False

    snapshot_time = datetime.now(timezone.utc).isoformat()
    rows = []

    for _, row in expiry_df.iterrows():
        rows.append({
            "snapshot_time": snapshot_time,
            "expiry_label": expiry_label,
            "expiry_date": str(row.get("expiry")),
            "strike": row.get("strike"),
            "option_type": row.get("type"),
            "mark_price": row.get("mark_price"),
            "oi": row.get("oi"),
            "volume": row.get("volume"),
            "iv": row.get("iv"),
            "delta": row.get("delta"),
            "gamma": row.get("gamma"),
            "theta": row.get("theta"),
            "vega": row.get("vega")
        })

    if post_to_supabase("option_chain_snapshots", rows):
        print(f"✅ Option Chain Snapshot Saved: {len(rows)} rows")
        return True

    return False


def save_orderbook_insights(insights):
    if not insights or insights.get("status") != "ok":
        print("⚠️ Order Book Snapshot Skipped: Invalid insights")
        return False

    row = {
        "timestamp": insights.get("timestamp"),

        # Kept None to avoid timestamptz format errors from Delta API
        "last_updated_at": None,

        "symbol": insights.get("symbol"),

        "eth_price": insights.get("mid_price"),
        "best_bid": insights.get("best_bid"),
        "best_ask": insights.get("best_ask"),

        "spread": insights.get("spread"),
        "spread_pct": insights.get("spread_pct"),
        "spread_quality": insights.get("spread_quality"),

        "bid_depth": insights.get("bid_depth"),
        "ask_depth": insights.get("ask_depth"),
        "imbalance_ratio": insights.get("imbalance_ratio"),

        "bias": insights.get("bias"),

        "nearest_bid_wall_price": insights.get("nearest_bid_wall_price"),
        "nearest_bid_wall_size": insights.get("nearest_bid_wall_size"),

        "nearest_ask_wall_price": insights.get("nearest_ask_wall_price"),
        "nearest_ask_wall_size": insights.get("nearest_ask_wall_size"),

        "trap_risk": insights.get("trap_risk"),
        "execution_signal": insights.get("execution_signal"),
    }

    if post_to_supabase("orderbook_insights", row):
        print("✅ Order Book Insight Snapshot Saved")
        return True

    return False

def save_ohlcv_data(df, symbol="ETHUSD", resolution="5m"):
    """
    Save OHLCV candle data into Supabase.
    Uses upsert to avoid duplicate candle entries.
    """

    if df is None or df.empty:
        print("No OHLCV data to save.")
        return False

    records = []

    for _, row in df.iterrows():
        records.append(
            {
                "symbol": symbol,
                "resolution": resolution,
                "candle_time": row["timestamp"].isoformat(),
                "epoch_time": int(row["time"]),
                "open": float(row["open"]),
                "high": float(row["high"]),
                "low": float(row["low"]),
                "close": float(row["close"]),
                "volume": float(row["volume"]),
            }
        )

    url = f"{SUPABASE_URL}/rest/v1/eth_ohlcv"

    try:
        response = requests.post(
            url,
            headers={
                **HEADERS,
                "Content-Type": "application/json",
                "Prefer": "resolution=merge-duplicates",
            },
            params={
                "on_conflict": "symbol,resolution,candle_time"
            },
            json=records,
            timeout=15,
        )

        if response.status_code not in [200, 201]:
            print("Failed to save OHLCV data:", response.status_code, response.text)
            return False

        print(f"Saved/updated {len(records)} OHLCV candles.")
        return True

    except Exception as e:
        print("Error saving OHLCV data:", e)
        return False
    
def save_market_events(events, symbol="ETHUSD", resolution="5m"):
    if not events:
        print("No market events to save.")
        return False

    records = []

    for e in events:
        records.append({
            "symbol": symbol,
            "resolution": resolution,
            "event_type": e.get("event_type"),
            "direction": e.get("direction"),
            "event_time": e.get("event_time").isoformat(),
            "price": e.get("price"),
            "reference_price": e.get("reference_price"),
            "strength": e.get("strength"),
            "metadata": e.get("metadata", {}),
        })

    url = f"{SUPABASE_URL}/rest/v1/eth_market_events"

    response = requests.post(
        url,
        headers={
            **HEADERS,
            "Content-Type": "application/json",
            "Prefer": "resolution=merge-duplicates",
        },
        params={"on_conflict": "symbol,resolution,event_type,event_time,price"},
        json=records,
        timeout=15,
    )

    if response.status_code not in [200, 201]:
        print("Failed to save market events:", response.status_code, response.text)
        return False

    print(f"Saved/updated {len(records)} market events.")
    return True


def save_smc_zones(zones, symbol="ETHUSD", resolution="5m"):
    if not zones:
        print("No SMC zones to save.")
        return False

    records = []
    seen = set()

    for z in zones:
        key = (
            symbol,
            resolution,
            z.get("zone_type"),
            str(z.get("start_time")),
            round(float(z.get("price_low")), 4),
            round(float(z.get("price_high")), 4),
        )

        if key in seen:
            continue

        seen.add(key)

        records.append({
            "symbol": symbol,
            "resolution": resolution,
            "zone_type": z.get("zone_type"),
            "direction": z.get("direction"),
            "start_time": z.get("start_time").isoformat(),
            "end_time": z.get("end_time").isoformat(),
            "price_low": z.get("price_low"),
            "price_high": z.get("price_high"),
            "strength": z.get("strength"),
            "status": z.get("status", "active"),
            "metadata": z.get("metadata", {}),
        })

    if not records:
        print("No unique SMC zones to save.")
        return False

    url = f"{SUPABASE_URL}/rest/v1/eth_smc_zones"

    response = requests.post(
        url,
        headers={
            **HEADERS,
            "Content-Type": "application/json",
            "Prefer": "resolution=merge-duplicates",
        },
        params={
            "on_conflict": "symbol,resolution,zone_type,start_time,price_low,price_high"
        },
        json=records,
        timeout=15,
    )

    if response.status_code not in [200, 201]:
        print("Failed to save SMC zones:", response.status_code, response.text)
        return False

    print(f"Saved/updated {len(records)} SMC zones.")
    return True

def save_volume_profile(profile, symbol="ETHUSD", resolution="5m"):
    if not profile:
        print("No volume profile to save.")
        return False

    records = []

    for p in profile:
        records.append({
            "symbol": symbol,
            "resolution": resolution,
            "price_level": p.get("price_level"),
            "volume": p.get("volume"),
            "profile_type": p.get("profile_type", "ohlcv_approx"),
            "metadata": p.get("metadata", {}),
        })

    url = f"{SUPABASE_URL}/rest/v1/eth_volume_profile"

    response = requests.post(
        url,
        headers={
            **HEADERS,
            "Content-Type": "application/json",
            "Prefer": "resolution=merge-duplicates",
        },
        params={"on_conflict": "symbol,resolution,price_level"},
        json=records,
        timeout=15,
    )

    if response.status_code not in [200, 201]:
        print("Failed to save volume profile:", response.status_code, response.text)
        return False

    print(f"Saved/updated {len(records)} volume profile rows.")
    return True
