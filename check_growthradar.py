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
    raise ValueError("FMP_API_KEY is missing")

# =========================
# SETTINGS
# =========================
TOP_N = 20
MAX_WORKERS = 5

MIN_MARKET_CAP = 200_000_000
MIN_GROWTH = 0.10
MIN_GROSS_MARGIN = 0.20

# =========================
# ① 母集団（安定版：FMP依存なし）
# =========================
def get_tickers():
    url = "https://raw.githubusercontent.com/datasets/nasdaq-listings/master/data/nasdaq-listed-symbols.csv"
    df = pd.read_csv(url)

    tickers = df["Symbol"].dropna().tolist()

    print(f"Tickers loaded: {len(tickers)}")
    return tickers

# =========================
# ② 財務取得（FMPのみ使用）
# =========================
def fetch_data(ticker):
    try:
        # income statement
        url = f"https://financialmodelingprep.com/api/v3/income-statement/{ticker}?limit=3&apikey={FMP_API_KEY}"
        fin = requests.get(url, timeout=10).json()

        if not isinstance(fin, list) or len(fin) < 3:
            return None

        rev0 = fin[0].get("revenue")
        rev1 = fin[1].get("revenue")
        rev2 = fin[2].get("revenue")

        if not all([rev0, rev1, rev2]):
            return None

        yoy = (rev0 - rev1) / rev1
        cagr = (rev0 / rev2) ** (1/2) - 1

        # profile
        url2 = f"https://financialmodelingprep.com/api/v3/profile/{ticker}?apikey={FMP_API_KEY}"
        prof = requests.get(url2, timeout=10).json()

        if not isinstance(prof, list) or not prof:
            return None

        gross = prof[0].get("grossProfitMargin")
        mcap = prof[0].get("mktCap")

        if gross is None or mcap is None:
            return None

        return {
            "ticker": ticker,
            "yoy": yoy,
            "cagr": cagr,
            "gross": gross,
            "mcap": mcap
        }

    except:
        return None

# =========================
# ③ フィルタ
# =========================
def filter_stock(d):
    return (
        d["mcap"] >= MIN_MARKET_CAP and
        d["yoy"] >= MIN_GROWTH and
        d["gross"] >= MIN_GROSS_MARGIN
    )

# =========================
# ④ スコア
# =========================
def score(d):
    s = 0

    # growth
    if d["yoy"] > 0.6:
        s += 5
    elif d["yoy"] > 0.4:
        s += 4
    elif d["yoy"] > 0.25:
        s += 3

    if d["cagr"] > 0.4:
        s += 2
    elif d["cagr"] > 0.25:
        s += 1

    # quality
    if d["gross"] > 0.7:
        s += 3
    elif d["gross"] > 0.5:
        s += 2
    elif d["gross"] > 0.4:
        s += 1

    return s

# =========================
# ⑤ Discord
# =========================
def notify(df):
    if not WEBHOOK_URL:
        print("No webhook")
        return

    if df.empty:
        msg = "No candidates"
    else:
        msg = "🚀 GrowthRadar TOP\n\n"
        for _, r in df.iterrows():
            msg += f"{r['Ticker']} | Score:{r['Score']}\nYoY:{r['YoY%']}% CAGR:{r['CAGR%']}%\n\n"

    requests.post(WEBHOOK_URL, json={"content": msg})

# =========================
# MAIN
# =========================
def main():
    tickers = get_tickers()

    results = []

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futures = [ex.submit(fetch_data, t) for t in tickers]

        for f in as_completed(futures):
            d = f.result()
            if not d:
                continue

            if not filter_stock(d):
                continue

            results.append({
                "Ticker": d["ticker"],
                "YoY%": round(d["yoy"] * 100, 1),
                "CAGR%": round(d["cagr"] * 100, 1),
                "Gross%": round(d["gross"] * 100, 1),
                "Score": score(d),
                "MarketCap(B)": round(d["mcap"] / 1e9, 2)
            })

    df = pd.DataFrame(results)

    if df.empty:
        print("No results")
        notify(df)
        return

    df = df.sort_values("Score", ascending=False).head(TOP_N)

    print(df)
    df.to_csv("growthradar.csv", index=False)

    notify(df)

if __name__ == "__main__":
    main()
