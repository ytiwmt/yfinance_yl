import os
import requests
import pandas as pd
import numpy as np
import random
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

# =========================
# CONFIG
# =========================
WEBHOOK_URL = os.environ.get("WEBHOOK_URL_GROWTHRADAR")

MAX_WORKERS = 10
SCAN_SIZE = 1500

MIN_PRICE = 2.0
MIN_MCAP = 5e7  # $50M

HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "application/json"
}

class GrowthRadarV26_3_Plus:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update(HEADERS)

    # =========================
    # UNIVERSE
    # =========================
    def load_universe(self):
        symbols = []
        sources = [
            "https://raw.githubusercontent.com/rreichel3/US-Stock-Symbols/main/all_tickers.txt",
            "https://datahub.io/core/nasdaq-listings/r/nasdaq-listed-symbols.csv",
        ]

        for url in sources:
            try:
                r = self.session.get(url, timeout=10)
                if r.status_code == 200:
                    if url.endswith(".txt"):
                        found = r.text.split("\n")
                    else:
                        df = pd.read_csv(url)
                        found = df["Symbol"].tolist()
                    symbols.extend(found)
            except:
                pass

        clean = list(set([
            str(s).strip().upper()
            for s in symbols
            if isinstance(s, str) and re.match(r"^[A-Z]{1,5}$", str(s).strip())
        ]))

        random.shuffle(clean)
        return clean

    # =========================
    # CORE FETCH（両方共通）
    # =========================
    def fetch(self, ticker):
        try:
            url = f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}?range=1y&interval=1d"
            r = self.session.get(url, timeout=6).json()
            data = r["chart"]["result"][0]

            last_trade_ts = data["meta"].get("regularMarketTime", 0)
            if (time.time() - last_trade_ts) > 86400 * 5:
                return None

            close = [c for c in data["indicators"]["quote"][0]["close"] if c]
            volume = [v for v in data["indicators"]["quote"][0]["volume"] if v]

            if len(close) < 126:
                return None

            price = close[-1]
            if price < MIN_PRICE:
                return None

            m1 = price / close[-21] - 1
            m3 = price / close[-63] - 1
            m6 = price / close[-126] - 1

            if m6 < 0.3 or m1 > 1.5:
                return None

            accel = m1 - m3

            volat = np.std(close[-21:]) / np.mean(close[-21:])
            if volat > 0.25:
                return None

            vol_short = np.mean(volume[-5:])
            vol_mid = np.mean(volume[-21:])
            vol_long = np.mean(volume[-63:])

            trend = np.mean(close[-10:]) / np.mean(close[-30:-10]) - 1

            return {
                "ticker": ticker,
                "price": price,
                "m6": m6,
                "accel": accel,
                "trend": trend,
                "vol_short": vol_short,
                "vol_mid": vol_mid,
                "vol_long": vol_long
            }

        except:
            return None

    # =========================
    # META
    # =========================
    def fetch_meta(self, tickers):
        meta = {}
        try:
            for i in range(0, len(tickers), 100):
                chunk = tickers[i:i+100]
                url = f"https://query1.finance.yahoo.com/v7/finance/quote?symbols={','.join(chunk)}"
                r = self.session.get(url, timeout=10).json()

                for res in r.get("quoteResponse", {}).get("result", []):
                    meta[res["symbol"]] = {
                        "name": res.get("longName", res.get("shortName", res["symbol"])),
                        "mcap": res.get("marketCap", 0)
                    }
        except:
            pass
        return meta

    # =========================
    # RUN
    # =========================
    def run(self):
        universe = self.load_universe()
        batch = universe[:SCAN_SIZE]

        print(f"Scanning {len(batch)} symbols...")

        raw = []
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
            futures = {ex.submit(self.fetch, t): t for t in batch}
            for f in as_completed(futures):
                r = f.result()
                if r:
                    raw.append(r)

        print(f"Base valid: {len(raw)}")

        meta = self.fetch_meta([r["ticker"] for r in raw])

        data = []
        for r in raw:
            m = meta.get(r["ticker"], {"name": r["ticker"], "mcap": 0})
            if m["mcap"] > 0 and m["mcap"] < MIN_MCAP:
                continue
            r.update(m)
            data.append(r)

        df = pd.DataFrame(data)

        # =========================
        # Tier2（v26.2）
        # =========================
        df_loose = df[
            (df["accel"] >= 0.15) &
            (df["trend"] >= 0.1) &
            (df["vol_mid"] >= df["vol_long"] * 1.2) &
            (df["vol_short"] >= df["vol_mid"] * 0.8)
        ].copy()

        # =========================
        # Tier1（v26.3）
        # =========================
        df_strict = df[
            (df["accel"] >= 0.2) &
            (df["trend"] >= 0.2) &
            (df["vol_mid"] >= df["vol_long"] * 1.3) &
            (df["vol_short"] >= df["vol_mid"] * 0.9)
        ].copy()

        def score(df_):
            if df_.empty:
                return df_
            for col in ["m6", "accel", "trend"]:
                df_[col] = df_[col].astype(float)

            df_["vol"] = df_["vol_short"] / (df_["vol_mid"] + 1e-9)

            df_["score"] = (
                df_["m6"].rank(pct=True) * 0.4 +
                df_["accel"].rank(pct=True) * 0.2 +
                df_["trend"].rank(pct=True) * 0.25 +
                df_["vol"].rank(pct=True) * 0.15
            )
            return df_

        df_loose = score(df_loose)
        df_strict = score(df_strict)

        self.report(df_strict, df_loose, len(batch))

    # =========================
    # REPORT
    # =========================
    def report(self, strict_df, loose_df, scanned):
        now = datetime.now().strftime("%Y-%m-%d %H:%M")

        msg = [
            f"🚀 GrowthRadar v26.3+ (2-Layer)",
            f"Scanned: {scanned} | Strict: {len(strict_df)} | Loose: {len(loose_df)} | {now}\n"
        ]

        msg.append("=== Tier1 (Strict) ===\n")
        for r in strict_df.sort_values("score", ascending=False).head(10).to_dict("records"):
            msg.append(
                f"{r['ticker']} | Score:{r['score']:.2f}\n"
                f"Price:${r['price']:.2f} | M6:{r['m6']:+.1%} | "
                f"Accel:{r['accel']:.2f} | Trend:{r['trend']:.2f}\n"
            )

        msg.append("\n=== Tier2 (Watchlist) ===\n")
        for r in loose_df.sort_values("score", ascending=False).head(15).to_dict("records"):
            msg.append(
                f"{r['ticker']} | Score:{r['score']:.2f}\n"
                f"Price:${r['price']:.2f} | M6:{r['m6']:+.1%} | "
                f"Accel:{r['accel']:.2f} | Trend:{r['trend']:.2f}\n"
            )

        text = "\n".join(msg)

        if WEBHOOK_URL:
            requests.post(WEBHOOK_URL, json={"content": text})

        print(text)


if __name__ == "__main__":
    GrowthRadarV26_3_Plus().run()
