import os
import requests
from dotenv import load_dotenv
from datetime import datetime, timezone

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

HEADERS = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json",
    "Prefer": "return=minimal"
}


def save_analytics_snapshot(analytics, expiry_label):
    url = f"{SUPABASE_URL}/rest/v1/analytics_snapshots"

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

    try:
        response = requests.post(url, headers=HEADERS, json=row)

        if response.status_code in [200, 201]:
            print("✅ Analytics Snapshot Saved")
        else:
            print("❌ Analytics Snapshot Save Failed")
            print("Status:", response.status_code)
            print("Response:", response.text)

    except Exception as e:
        print("❌ Analytics Snapshot Error:", str(e))


def save_premium_decay_snapshot(
    expiry_label,
    atm_strike,
    atm_ce_price,
    atm_pe_price,
    atm_straddle_price
):
    url = f"{SUPABASE_URL}/rest/v1/premium_decay_snapshots"

    row = {
        "snapshot_time": datetime.now(timezone.utc).isoformat(),
        "expiry_label": expiry_label,
        "atm_strike": atm_strike,
        "atm_ce_price": atm_ce_price,
        "atm_pe_price": atm_pe_price,
        "atm_straddle_price": atm_straddle_price
    }

    try:
        response = requests.post(url, headers=HEADERS, json=row)

        if response.status_code in [200, 201]:
            print("✅ Premium Decay Snapshot Saved")
        else:
            print("❌ Premium Decay Save Failed")
            print("Status:", response.status_code)
            print("Response:", response.text)

    except Exception as e:
        print("❌ Premium Decay Error:", str(e))


def save_option_chain_snapshot(expiry_df, expiry_label):
    if expiry_df is None or expiry_df.empty:
        print("⚠️ Option Chain Snapshot Skipped: Empty dataframe")
        return

    url = f"{SUPABASE_URL}/rest/v1/option_chain_snapshots"
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

    try:
        response = requests.post(url, headers=HEADERS, json=rows)

        if response.status_code in [200, 201]:
            print(f"✅ Option Chain Snapshot Saved: {len(rows)} rows")
        else:
            print("❌ Option Chain Snapshot Save Failed")
            print("Status:", response.status_code)
            print("Response:", response.text)

    except Exception as e:
        print("❌ Option Chain Snapshot Error:", str(e))