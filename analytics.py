import pandas as pd


def clean_options_df(df):
    df = df.copy()

    numeric_cols = [
        "strike", "mark_price", "oi", "volume",
        "iv", "delta", "gamma", "theta", "vega"
    ]

    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    return df


def basic_expiry_analytics(df):
    df = clean_options_df(df)

    calls = df[df["type"] == "call_options"]
    puts = df[df["type"] == "put_options"]

    total_call_oi = calls["oi"].sum()
    total_put_oi = puts["oi"].sum()

    pcr = total_put_oi / total_call_oi if total_call_oi != 0 else None

    highest_call_oi = calls.sort_values("oi", ascending=False).head(1)
    highest_put_oi = puts.sort_values("oi", ascending=False).head(1)

    return {
        "total_call_oi": total_call_oi,
        "total_put_oi": total_put_oi,
        "pcr": pcr,
        "highest_call_oi_strike": highest_call_oi["strike"].iloc[0] if not highest_call_oi.empty else None,
        "highest_put_oi_strike": highest_put_oi["strike"].iloc[0] if not highest_put_oi.empty else None,
        "net_gamma": df["gamma"].sum(),
        "net_delta": df["delta"].sum(),
        "net_theta": df["theta"].sum(),
        "net_vega": df["vega"].sum(),
    }


def calculate_max_pain(df):
    df = clean_options_df(df)

    strikes = sorted(df["strike"].dropna().unique())
    pain_data = []

    for expiry_price in strikes:
        total_pain = 0

        for _, row in df.iterrows():
            option_strike = row["strike"]
            oi = row["oi"]

            if pd.isna(option_strike) or pd.isna(oi):
                continue

            if row["type"] == "call_options":
                pain = max(0, expiry_price - option_strike) * oi
            else:
                pain = max(0, option_strike - expiry_price) * oi

            total_pain += pain

        pain_data.append({
            "strike": expiry_price,
            "pain": total_pain
        })

    pain_df = pd.DataFrame(pain_data)

    if pain_df.empty:
        return None, pain_df

    max_pain_strike = pain_df.sort_values("pain").iloc[0]["strike"]

    return max_pain_strike, pain_df


def calculate_atm_and_expected_move(df, spot_price=None):
    df = clean_options_df(df)

    calls = df[df["type"] == "call_options"].copy()
    puts = df[df["type"] == "put_options"].copy()

    if calls.empty or puts.empty:
        return None, None, None, None

    if spot_price is not None:
        atm_reference_price = float(spot_price)
    else:
        atm_reference_price = df["strike"].median()

    atm_strike = df.iloc[
        (df["strike"] - atm_reference_price).abs().argsort()[:1]
    ]["strike"].iloc[0]

    atm_call = calls.iloc[
        (calls["strike"] - atm_strike).abs().argsort()[:1]
    ]

    atm_put = puts.iloc[
        (puts["strike"] - atm_strike).abs().argsort()[:1]
    ]

    ce_price = atm_call["mark_price"].iloc[0]
    pe_price = atm_put["mark_price"].iloc[0]

    expected_move = ce_price + pe_price

    return atm_strike, expected_move, ce_price, pe_price