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
            print("✅ Snapshot Saved Successfully")
        else:
            print("❌ Snapshot Save Failed")
            print("Status:", response.status_code)
            print("Response:", response.text)

    except Exception as e:
        print("❌ Supabase Error:", str(e))