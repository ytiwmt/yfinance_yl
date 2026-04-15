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

# =========================
# ① 母集団（安定CSV）
# =========================
def get_tickers():
    url = "https://raw.githubusercontent.com/datasets/nasdaq-listings/master/data/nasdaq-listed-symbols.csv"
    df = pd.read_csv(url)

    tickers = df["Symbol"].dropna().tolist()

    print("Tickers:", len(tickers))
    return tickers

# =========================
# ② 財務取得（欠損耐性版）
# =========================
def fetch_data(ticker):
    try:
        url = f"https://financialmodelingprep.com/api/v3/income-statement/{ticker}?limit=2&apikey={FMP_API_KEY}"
        fin = requests.get(url, timeout=10).json()

        if not isinstance(fin, list) or len(fin) < 2:
            return None

        rev0 = fin[0].get("revenue")
        rev1 = fin[1].get("revenue")

        if not rev0 or not rev1:
            return None

        yoy = (rev0 - rev1) / rev1

        # profile
        url2 = f"https://financialmodelingprep.com/api/v3/profile/{ticker}?apikey={FMP_API_KEY}"
        prof = requests.get(url2, timeout=10).json()

        if not isinstance(prof, list) or not prof:
            return None

        mcap = prof[0].get("mktCap")
        gross = prof[0].get("grossProfitMargin")  # optional

        if not mcap:
            return None

        return {
            "ticker": ticker,
            "yoy": yoy,
            "mcap": mcap,
            "gross": gross
        }

    except:
        return None

# =========================
# ③ フィルタ（緩め）
# =========================
def filter_stock(d):
    return (
        d["mcap"] >= MIN_MARKET_CAP and
        d["yoy"] >= MIN_GROWTH
    )

# =========================
# ④ スコア（現実寄り）
# =========================
def score(d):
    s = 0

    # growth
    if d["yoy"] > 0.5:
        s += 5
    elif d["yoy"] > 0.3:
        s += 4
    elif d["yoy"] > 0.2:
        s += 3
    elif d["yoy"] > 0.1:
        s += 2

    # quality bonus（ある場合のみ）
    g = d["gross"]
    if g:
        if g > 0.7:
            s += 3
        elif g > 0.5:
            s += 2
        elif g > 0.3:
            s += 1

    # size penalty（小型加点）
    if d["mcap"] < 1_000_000_000:
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
        msg = "No GrowthRadar candidates"
    else:
        msg = "🚀 GrowthRadar v2\n\n"
        for _, r in df.iterrows():
            msg += (
                f"{r['Ticker']} | Score:{r['Score']}\n"
                f"YoY:{r['YoY%']}% | MCap:{r['MCapB']}B\n\n"
            )

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
                "MCapB": round(d["mcap"] / 1e9, 2),
                "Score": score(d)
            })

    df = pd.DataFrame(results)

    if df.empty:
        print("No results")
        notify(df)
        return

    df = df.sort_values("Score", ascending=False).head(TOP_N)

    print(df)
    notify(df)

# =========================
if __name__ == "__main__":
    main()
