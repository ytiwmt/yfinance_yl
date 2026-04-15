import os
import requests
import pandas as pd
import yfinance as yf
from concurrent.futures import ThreadPoolExecutor, as_completed

# =========================
# ENV
# =========================
WEBHOOK_URL = os.environ.get("WEBHOOK_URL_GROWTHRADAR")

# =========================
# CONFIG
# =========================
MAX_WORKERS = 5
SCAN_LIMIT = 200  # API負荷回避

# =========================
# TICKERS
# =========================
def get_tickers():
    url = "https://datahub.io/core/nasdaq-listings/r/nasdaq-listed-symbols.csv"
    df = pd.read_csv(url)
    tickers = df["Symbol"].dropna().tolist()
    return tickers[:SCAN_LIMIT]

# =========================
# FETCH DATA（yfinance）
# =========================
def fetch_growth(ticker):
    try:
        t = yf.Ticker(ticker)
        df = t.quarterly_financials

        if df is None or df.empty:
            return None

        if "Total Revenue" not in df.index:
            return None

        rev = df.loc["Total Revenue"].dropna().values

        if len(rev) < 4:
            return None

        r0, r1, r2, r3 = rev[:4]

        if min(r0, r1, r2, r3) <= 0:
            return None

        # YoY
        yoy = (r0 - r2) / r2

        # 加速
        qoq_now = (r0 - r1) / r1
        qoq_prev = (r1 - r2) / r2
        accel = qoq_now - qoq_prev

        return {
            "ticker": ticker,
            "yoy": yoy,
            "accel": accel
        }

    except:
        return None

# =========================
# SCORE
# =========================
def score(d):
    s = 0

    yoy = d["yoy"]
    accel = d["accel"]

    # 成長
    if yoy > 0.5:
        s += 5
    elif yoy > 0.3:
        s += 4
    elif yoy > 0.15:
        s += 3
    elif yoy > 0.05:
        s += 1

    # 加速（重要）
    if accel > 0.2:
        s += 6
    elif accel > 0.1:
        s += 4
    elif accel > 0.05:
        s += 2

    # フィルタ
    if yoy < 0.05:
        s -= 3

    return s

# =========================
# NOTIFY
# =========================
def notify(df, stats):
    msg = "🚀 GrowthRadar v6 (yfinance)\n\n"

    if df.empty:
        msg += "No candidates\n\n"
    else:
        for _, r in df.iterrows():
            msg += (
                f"{r['ticker']} | Score:{r['score']}\n"
                f"YoY:{r['yoy']:.2f} | Accel:{r['accel']:.2f}\n\n"
            )

    msg += (
        "--- Stats ---\n"
        f"Checked: {stats['checked']}\n"
        f"Valid: {stats['valid']}\n"
    )

    if WEBHOOK_URL:
        requests.post(WEBHOOK_URL, json={"content": msg}, timeout=10)
    else:
        print(msg)

# =========================
# ERROR NOTIFY
# =========================
def notify_error(e, stats):
    msg = f"🔥 ERROR\n{str(e)}\nChecked:{stats['checked']}"

    if WEBHOOK_URL:
        try:
            requests.post(WEBHOOK_URL, json={"content": msg}, timeout=10)
        except:
            print("Discord送信失敗")
    else:
        print(msg)

# =========================
# MAIN
# =========================
def main(stats):
    tickers = get_tickers()

    results = []

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futures = {ex.submit(fetch_growth, t): t for t in tickers}

        for f in as_completed(futures):
            stats["checked"] += 1

            res = f.result()
            if not res:
                continue

            stats["valid"] += 1

            res["score"] = score(res)
            results.append(res)

            if stats["checked"] % 50 == 0:
                print(f"Processed: {stats['checked']}")

    df = pd.DataFrame(results)

    if not df.empty:
        df = df.sort_values("score", ascending=False).head(15)

    notify(df, stats)

# =========================
# WRAPPER
# =========================
def main_wrapper():
    stats = {"checked": 0, "valid": 0}

    try:
        main(stats)
    except Exception as e:
        notify_error(e, stats)

# =========================
# ENTRY
# =========================
if __name__ == "__main__":
    main_wrapper()
