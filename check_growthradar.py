import os
import requests
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed

# =========================
# ENV
# =========================
FMP_API_KEY = os.environ.get("FMP_API_KEY")
WEBHOOK_URL = os.environ.get("WEBHOOK_URL_GROWTHRADAR")

if not FMP_API_KEY:
    raise ValueError("FMP_API_KEY missing")


# =========================
# SAFE UNIVERSE (壊れないRussell代替)
# =========================
def get_tickers():
    """
    Russell 3000代替（安定版）
    ※外部壊れCSVを排除
    """
    url = "https://datahub.io/core/nasdaq-listings/r/nasdaq-listed-symbols.csv"
    df = pd.read_csv(url)

    tickers = df["Symbol"].dropna().tolist()

    # 少し拡張（NYSE系も混ぜる）
    url2 = "https://datahub.io/core/nyse-other-listings/r/nyse-listed-symbols.csv"
    df2 = pd.read_csv(url2)

    tickers += df2["ACT Symbol"].dropna().tolist()

    tickers = list(set(tickers))

    print("Universe loaded:", len(tickers))
    return tickers


# =========================
# FETCH DATA
# =========================
def fetch(ticker):
    try:
        url = f"https://financialmodelingprep.com/api/v3/profile/{ticker}?apikey={FMP_API_KEY}"
        r = requests.get(url, timeout=10).json()

        if not isinstance(r, list) or not r:
            return None

        mcap = r[0].get("mktCap")
        price = r[0].get("price")
        sector = r[0].get("sector")

        if not mcap or not price:
            return None

        return {
            "ticker": ticker,
            "mcap": mcap,
            "price": price,
            "sector": sector
        }

    except:
        return None


# =========================
# SCORE (テンバガー寄り)
# =========================
def score(d):
    s = 0

    # 小型ほど強く加点
    if d["mcap"] < 500_000_000:
        s += 5
    elif d["mcap"] < 1_000_000_000:
        s += 4
    elif d["mcap"] < 5_000_000_000:
        s += 2
    else:
        s += 1

    # セクター補正
    if d["sector"] in ["Technology", "Healthcare"]:
        s += 1

    return s


# =========================
# DISCORD
# =========================
def notify(df):
    if not WEBHOOK_URL:
        print(df)
        return

    if df.empty:
        msg = "⚠️ GrowthRadar v2: No candidates"
    else:
        msg = "🚀 GrowthRadar v2 (Attack Mode)\n\n"
        for _, r in df.iterrows():
            msg += (
                f"{r['ticker']} | Score:{r['score']}\n"
                f"MCap:{r['mcap_b']}B | Sector:{r['sector']}\n\n"
            )

    try:
        requests.post(WEBHOOK_URL, json={"content": msg}, timeout=10)
    except Exception as e:
        print("Webhook error:", e)


# =========================
# MAIN
# =========================
def main():
    tickers = get_tickers()

    results = []

    with ThreadPoolExecutor(max_workers=6) as ex:
        futures = [ex.submit(fetch, t) for t in tickers]

        for f in as_completed(futures):
            d = f.result()
            if not d:
                continue

            d["score"] = score(d)
            d["mcap_b"] = round(d["mcap"] / 1e9, 2)

            results.append(d)

    df = pd.DataFrame(results)

    if df.empty:
        notify(df)
        return

    df = df.sort_values("score", ascending=False).head(20)

    notify(df)


if __name__ == "__main__":
    main()
