import os
import requests
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed

# =========================
# ENV
# =========================
FMP_API_KEY = os.environ.get("FMP_API_KEY")

if not FMP_API_KEY:
    raise ValueError("FMP_API_KEY missing")

# =========================
# SETTINGS
# =========================
TOP_N = 20
MAX_WORKERS = 5

MIN_MARKET_CAP = 100_000_000  # 超重要：小型も残す
MIN_PRICE = 1                  # ペニー除外最低限

# =========================
# ① Russell 3000（実データ）
# =========================
def get_tickers():
    url = "https://www.stockmarketmba.com/databases/Russell3000.csv"
    df = pd.read_csv(url)

    # 列名ゆれ対策
    col = [c for c in df.columns if "ticker" in c.lower()][0]
    tickers = df[col].dropna().tolist()

    print("Russell 3000 loaded:", len(tickers))
    return tickers

# =========================
# ② 財務取得（欠損耐性MAX）
# =========================
def fetch_data(ticker):
    try:
        # profile
        url = f"https://financialmodelingprep.com/api/v3/profile/{ticker}?apikey={FMP_API_KEY}"
        prof = requests.get(url, timeout=10).json()

        if not isinstance(prof, list) or not prof:
            return None

        mcap = prof[0].get("mktCap")
        price = prof[0].get("price")
        sector = prof[0].get("sector")

        if not mcap or not price:
            return None

        if mcap < MIN_MARKET_CAP or price < MIN_PRICE:
            return None

        # income statement（あればラッキー）
        url2 = f"https://financialmodelingprep.com/api/v3/income-statement/{ticker}?limit=2&apikey={FMP_API_KEY}"
        fin = requests.get(url2, timeout=10).json()

        yoy = None
        if isinstance(fin, list) and len(fin) >= 2:
            r0 = fin[0].get("revenue")
            r1 = fin[1].get("revenue")

            if r0 and r1:
                yoy = (r0 - r1) / r1

        return {
            "ticker": ticker,
            "mcap": mcap,
            "price": price,
            "sector": sector,
            "yoy": yoy
        }

    except:
        return None

# =========================
# ③ スコア（テンバガー寄り）
# =========================
def score(d):
    s = 0

    # growth（最重要）
    if d["yoy"] is not None:
        if d["yoy"] > 0.8:
            s += 5
        elif d["yoy"] > 0.5:
            s += 4
        elif d["yoy"] > 0.3:
            s += 3
        elif d["yoy"] > 0.1:
            s += 2

    # size（小さいほど加点）
    if d["mcap"] < 1_000_000_000:
        s += 3
    elif d["mcap"] < 5_000_000_000:
        s += 2

    # sector bias（テンバガー寄り）
    if d["sector"] in ["Technology", "Healthcare"]:
        s += 1

    return s

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

            s = score(d)

            results.append({
                "Ticker": d["ticker"],
                "Score": s,
                "YoY": None if d["yoy"] is None else round(d["yoy"] * 100, 1),
                "MCap(B)": round(d["mcap"] / 1e9, 2),
                "Sector": d["sector"]
            })

    df = pd.DataFrame(results)

    if df.empty:
        print("No results")
        return

    df = df.sort_values("Score", ascending=False).head(TOP_N)

    print(df)
    df.to_csv("r3000_tenbagger.csv", index=False)

if __name__ == "__main__":
    main()
