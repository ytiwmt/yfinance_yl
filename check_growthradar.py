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
# UNIVERSE
# =========================
def get_tickers():
    url = "https://datahub.io/core/nasdaq-listings/r/nasdaq-listed-symbols.csv"
    df = pd.read_csv(url)

    tickers = df["Symbol"].dropna().tolist()
    print(f"Tickers loaded: {len(tickers)}")

    return tickers


# =========================
# FETCH
# =========================
def fetch(ticker):
    try:
        url = f"https://financialmodelingprep.com/api/v3/profile/{ticker}?apikey={FMP_API_KEY}"
        r = requests.get(url, timeout=10).json()

        if not isinstance(r, list) or not r:
            return None

        d = r[0]

        mcap = d.get("mktCap")
        price = d.get("price")
        sector = d.get("sector")

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
# SCORE
# =========================
def score(d):
    s = 0

    if d["mcap"] < 500_000_000:
        s += 5
    elif d["mcap"] < 1_000_000_000:
        s += 4
    elif d["mcap"] < 5_000_000_000:
        s += 2
    else:
        s += 1

    if d["sector"] in ["Technology", "Healthcare"]:
        s += 1

    return s


# =========================
# NOTIFY
# =========================
def notify(df, total, processed, valid):
    if not WEBHOOK_URL:
        print(df)
        return

    if df.empty:
        msg = "⚠️ GrowthRadar v2: No candidates\n\n"
    else:
        msg = "🚀 GrowthRadar v2\n\n"

        for _, r in df.iterrows():
            msg += (
                f"{r['ticker']} | Score:{r['score']}\n"
                f"MCap:{r['mcap_b']}B | Sector:{r['sector']}\n\n"
            )

    msg += (
        "--------------------\n"
        f"Total tickers: {total}\n"
        f"Processed: {processed}\n"
        f"Valid: {valid}\n"
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

    total = len(tickers)
    processed = 0
    valid = 0

    results = []

    with ThreadPoolExecutor(max_workers=6) as ex:
        futures = [ex.submit(fetch, t) for t in tickers]

        for f in as_completed(futures):
            processed += 1

            if processed % 500 == 0:
                print(f"Processed: {processed}/{total}")

            d = f.result()
            if not d:
                continue

            valid += 1

            d["score"] = score(d)
            d["mcap_b"] = round(d["mcap"] / 1e9, 2)

            results.append(d)

    df = pd.DataFrame(results)

    if not df.empty:
        df = df.sort_values("score", ascending=False).head(20)

    notify(df, total, processed, valid)


if __name__ == "__main__":
    main()
