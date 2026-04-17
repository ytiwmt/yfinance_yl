import os
import requests
import pandas as pd
import numpy as np
import random
import re
import time
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

# =========================
# CONFIG
# =========================
WEBHOOK_URL = os.environ.get("WEBHOOK_URL_GROWTHRADAR")

MAX_WORKERS = 10
SCAN_SIZE = 1500

MIN_PRICE = 2.0
MIN_MCAP = 5e7
MIN_AVG_VOL_VAL = 5e5

UNIVERSE_FILE = "universe_v26_5.json"
LOG_FILE = "growthradar_v26_5.log"

HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "application/json"
}

class GrowthRadarV26_5:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update(HEADERS)

    # =========================
    # UNIVERSE (fixed)
    # =========================
    def load_universe(self):
        if os.path.exists(UNIVERSE_FILE):
            with open(UNIVERSE_FILE, "r") as f:
                return json.load(f)

        print("Creating universe...")

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

        # 固定コアユニバース
        core = clean[:3000]

        with open(UNIVERSE_FILE, "w") as f:
            json.dump(core, f)

        return core

    # =========================
    # FETCH TECHNICAL
    # =========================
    def fetch(self, ticker):
        try:
            url = f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}?range=1y&interval=1d"
            r = self.session.get(url, timeout=6).json()
            res = r["chart"]["result"][0]

            if (time.time() - res["meta"].get("regularMarketTime", 0)) > 86400 * 5:
                return None

            close = [c for c in res["indicators"]["quote"][0]["close"] if c]
            volume = [v for v in res["indicators"]["quote"][0]["volume"] if v]

            if len(close) < 126:
                return None

            price = close[-1]
            if price < MIN_PRICE:
                return None

            avg_vol_val = np.mean(close[-21:]) * np.mean(volume[-21:])
            if avg_vol_val < MIN_AVG_VOL_VAL:
                return None

            m1 = price / close[-21] - 1
            m3 = price / close[-63] - 1
            m6 = price / close[-126] - 1

            if m6 < 0.3 or m1 > 1.5:
                return None

            volat = np.std(close[-21:]) / np.mean(close[-21:])
            if volat > 0.25:
                return None

            return {
                "ticker": ticker,
                "price": price,
                "m6": m6,
                "accel": m1 - m3,
                "trend": np.mean(close[-10:]) / np.mean(close[-30:-10]) - 1,
                "vol_short": np.mean(volume[-5:]),
                "vol_mid": np.mean(volume[-21:]),
                "vol_long": np.mean(volume[-63:])
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
    # LOG
    # =========================
    def log(self, text):
        with open(LOG_FILE, "a", encoding="utf-8") as f:
            f.write(f"\n\n[{datetime.now()}]\n{text}")

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

        if not raw:
            print("No candidates.")
            return

        meta = self.fetch_meta([r["ticker"] for r in raw])

        data = []
        for r in raw:
            m = meta.get(r["ticker"], {"name": r["ticker"], "mcap": 0})
            if 0 < m["mcap"] < MIN_MCAP:
                continue
            r.update(m)
            data.append(r)

        df = pd.DataFrame(data)
        if df.empty:
            return

        # Tier1
        df_t1 = df[
            (df["accel"] >= 0.20) &
            (df["trend"] >= 0.20) &
            (df["vol_mid"] >= df["vol_long"] * 1.3) &
            (df["vol_short"] >= df["vol_mid"] * 0.9)
        ].copy()

        # Tier2
        df_t2 = df[
            (df["accel"] >= 0.18) &
            (df["trend"] >= 0.15) &
            (df["vol_mid"] >= df["vol_long"] * 1.1)
        ].copy()

        def score(d):
            if d.empty:
                return d
            d["vol_ratio"] = d["vol_short"] / (d["vol_mid"] + 1e-9)
            d["score"] = (
                d["m6"].rank(pct=True) * 0.40 +
                d["accel"].rank(pct=True) * 0.20 +
                d["trend"].rank(pct=True) * 0.25 +
                d["vol_ratio"].rank(pct=True) * 0.15
            )
            return d.sort_values("score", ascending=False)

        t1 = score(df_t1)
        t2 = score(df_t2)

        output = self.report(t1, t2, len(batch), len(df))

        self.log(output)

    # =========================
    # REPORT
    # =========================
    def report(self, t1, t2, scanned, base):
        now = datetime.now().strftime("%Y-%m-%d %H:%M")

        msg = [
            f"🚀 GrowthRadar v26.5 (Tracking Engine)",
            f"Scanned:{scanned} | Base:{base} | Tier1:{len(t1)} | Tier2:{len(t2)} | {now}\n"
        ]

        msg.append("🏆 Tier1")
        for r in t1.head(10).to_dict("records"):
            msg.append(f"{r['ticker']} | S:{r['score']:.2f} | P:${r['price']:.2f}")

        msg.append("\n👀 Tier2")
        for r in t2.head(10).to_dict("records"):
            msg.append(f"{r['ticker']} | S:{r['score']:.2f} | P:${r['price']:.2f}")

        text = "\n".join(msg)

        if WEBHOOK_URL:
            requests.post(WEBHOOK_URL, json={"content": text})

        print(text)
        return text


if __name__ == "__main__":
    GrowthRadarV26_5().run()
