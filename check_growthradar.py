import os
import requests
import pandas as pd

# =========================
# ENV
# =========================
FMP_API_KEY = os.environ.get("FMP_API_KEY")
WEBHOOK_URL = os.environ.get("WEBHOOK_URL_GROWTHRADAR")

if not FMP_API_KEY:
    raise ValueError("FMP_API_KEY missing")


# =========================
# SAFE REQUEST
# =========================
def safe_get(url, params=None):
    try:
        r = requests.get(url, params=params, timeout=10)
        data = r.json()

        # ★ FMPエラー検出
        if not isinstance(data, list):
            print("API ERROR:", data)
            return None

        return data
    except Exception as e:
        print("REQUEST ERROR:", e)
        return None


# =========================
# SCREENER（1 call）
# =========================
def get_candidates():
    url = "https://financialmodelingprep.com/api/v3/stock-screener"

    params = {
        "marketCapLowerThan": 3_000_000_000,
        "priceMoreThan": 3,
        "exchange": "NASDAQ",
        "limit": 1000,
        "apikey": FMP_API_KEY
    }

    data = safe_get(url, params)

    if not data:
        print("Screener failed")
        return []

    print(f"Screener fetched: {len(data)}")
    return data


# =========================
# SELECT
# =========================
def select_top(candidates):
    if not candidates:
        return []

    df = pd.DataFrame(candidates)

    # 必須カラムチェック
    for col in ["marketCap", "price", "symbol"]:
        if col not in df.columns:
            print("Missing column:", col)
            return []

    df = df.dropna(subset=["marketCap", "price"])

    # 小型優先
    df = df.sort_values("marketCap")

    df = df.head(200)

    print(f"Selected: {len(df)}")

    return df.to_dict("records")


# =========================
# GROWTH（最大200 calls）
# =========================
def fetch_growth(ticker):
    url = f"https://financialmodelingprep.com/api/v3/income-statement/{ticker}"

    params = {
        "limit": 2,
        "apikey": FMP_API_KEY
    }

    data = safe_get(url, params)

    if not data or len(data) < 2:
        return None

    rev1 = data[0].get("revenue", 0)
    rev0 = data[1].get("revenue", 0)

    if rev0 <= 0:
        return None

    return (rev1 - rev0) / rev0


# =========================
# SCORE v5
# =========================
def score(d):
    s = 0

    mcap = d["marketCap"]
    price = d["price"]

    # SIZE
    if mcap < 200_000_000:
        s += 7
    elif mcap < 500_000_000:
        s += 6
    elif mcap < 1_000_000_000:
        s += 4
    else:
        s += 2

    # GROWTH
    rev = d.get("revenue_growth")
    if rev is not None:
        if rev > 0.5:
            s += 8
        elif rev > 0.3:
            s += 6
        elif rev > 0.15:
            s += 4
        elif rev > 0.05:
            s += 2

    # MOMENTUM proxy
    if price > 20:
        s += 2

    # SECTOR
    if d.get("sector") in ["Technology", "Healthcare", "Communication Services"]:
        s += 2

    return s


# =========================
# DISCORD
# =========================
def notify(df, stats):
    if not WEBHOOK_URL:
        print(df)
        return

    try:
        if df.empty:
            msg = "⚠️ GrowthRadar v5: No candidates\n\n"
        else:
            msg = "🚀 GrowthRadar v5 (Stable)\n\n"

            for _, r in df.iterrows():
                msg += (
                    f"{r['symbol']} | Score:{r['score']}\n"
                    f"MCap:{round(r['marketCap']/1e9,2)}B "
                    f"| Rev:{r.get('revenue_growth',0):.2f}\n\n"
                )

        msg += (
            "--- Stats ---\n"
            f"Screener: {stats['screener']}\n"
            f"Selected: {stats['selected']}\n"
            f"Growth: {stats['growth']}\n"
        )

        requests.post(WEBHOOK_URL, json={"content": msg}, timeout=10)

    except Exception as e:
        print("Discord error:", e)


# =========================
# MAIN
# =========================
def main():
    stats = {"screener": 0, "selected": 0, "growth": 0}

    # ① Screener
    candidates = get_candidates()
    stats["screener"] = len(candidates)

    if not candidates:
        notify(pd.DataFrame(), stats)
        return

    # ② Select
    selected = select_top(candidates)
    stats["selected"] = len(selected)

    if not selected:
        notify(pd.DataFrame(), stats)
        return

    results = []

    # ③ Growth
    for d in selected:
        g = fetch_growth(d["symbol"])
        if g is not None:
            d["revenue_growth"] = g
            stats["growth"] += 1

        d["score"] = score(d)
        results.append(d)

    df = pd.DataFrame(results)

    if not df.empty:
        df = df[df["score"] >= 6] \
            .sort_values("score", ascending=False) \
            .head(15)

    notify(df, stats)


if __name__ == "__main__":
    main()
