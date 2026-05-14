import os
import requests
from dotenv import load_dotenv
from datetime import datetime, timezone

try:
    import streamlit as st
except Exception:
    st = None


load_dotenv()


def get_secret(key):
    """
    Works both locally and on Streamlit Cloud.
    Local: reads from .env
    Streamlit Cloud: reads from st.secrets
    """
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
    """
    Central safe POST function.
    Prevents app crash if Supabase fails temporarily.
    """
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


def save_option_chain_snapshot(expiry_df, expiry_label):
    if expiry_df is None or expiry_df.empty:
        print("⚠️ Option Chain Snapshot Skipped: Empty dataframe")
        return

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