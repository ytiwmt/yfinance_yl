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
SCAN_LIMIT = 300

MIN_REVENUE = 50_000_000
MIN_MCAP = 200_000_000
MAX_MCAP = 5_000_000_000

MIN_YOY = 0.10
MIN_MOMENTUM = 0.20   # 3ヶ月 +20%

# =========================
# TICKERS
# =========================
def get_tickers():
    url = "https://datahub.io/core/nasdaq-listings/r/nasdaq-listed-symbols.csv"
    df = pd.read_csv(url)
    return df["Symbol"].dropna().tolist()[:SCAN_LIMIT]

# =========================
# FETCH
# =========================
def fetch_data(ticker):
    try:
        t = yf.Ticker(ticker)

        # --- 財務 ---
        fin = t.quarterly_financials
        if fin is None or fin.empty or "Total Revenue" not in fin.index:
            return None

        rev = fin.loc["Total Revenue"].dropna().values
        if len(rev) < 4:
            return None

        r0, r1, r2, r3 = rev[:4]

        if min(r0, r1, r2, r3) <= 0:
            return None

        if r0 < MIN_REVENUE:
            return None

        # --- 成長 ---
        yoy = (r0 - r2) / r2
        if yoy < MIN_YOY:
            return None

        qoq_now = (r0 - r1) / r1
        qoq_prev = (r1 - r2) / r2
        accel = qoq_now - qoq_prev

        if accel <= 0 or accel > 1.0:
            return None

        # --- 市場データ ---
        info = t.info
        mcap = info.get("marketCap", 0)
        if not mcap or mcap < MIN_MCAP or mcap > MAX_MCAP:
            return None

        # --- モメンタム ---
        hist = t.history(period="3mo")

        if hist is None or hist.empty or len(hist) < 20:
            return None

        price_now = hist["Close"].iloc[-1]
        price_3m = hist["Close"].iloc[0]

        momentum = (price_now - price_3m) / price_3m
        if momentum < MIN_MOMENTUM:
            return None

        # --- 出来高 ---
        vol_now = hist["Volume"].tail(5).mean()
        vol_prev = hist["Volume"].head(5).mean()

        if vol_prev == 0:
            return None

        vol_trend = vol_now / vol_prev

        return {
            "ticker": ticker,
            "yoy": yoy,
            "accel": accel,
            "momentum": momentum,
            "vol_trend": vol_trend,
            "mcap": mcap
        }

    except:
        return None

# =========================
# SCORE v8
# =========================
def score(d):
    s = 0

    # 成長
    if d["yoy"] > 0.5: s += 5
    elif d["yoy"] > 0.3: s += 4
    else: s += 3

    # 加速
    if d["accel"] > 0.3: s += 4
    elif d["accel"] > 0.15: s += 3
    else: s += 2

    # モメンタム
    if d["momentum"] > 0.5: s += 5
    elif d["momentum"] > 0.3: s += 4
    else: s += 3

    # 出来高
    if d["vol_trend"] > 1.5: s += 2
    elif d["vol_trend"] > 1.2: s += 1

    return s

# =========================
# NOTIFY
# =========================
def notify(df, stats):
    msg = "🚀 GrowthRadar v8 (Momentum)\n\n"

    if df.empty:
        msg += "No candidates\n\n"
    else:
        for _, r in df.iterrows():
            msg += (
                f"{r['ticker']} | Score:{r['score']}\n"
                f"YoY:{r['yoy']:.2f} Accel:{r['accel']:.2f}\n"
                f"Mom:{r['momentum']:.2f} Vol:{r['vol_trend']:.2f}\n\n"
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
        futures = {ex.submit(fetch_data, t): t for t in tickers}

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
