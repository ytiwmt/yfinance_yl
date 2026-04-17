import os
import requests
import pandas as pd
import numpy as np
import random
import re
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

# =========================
# CONFIG
# =========================
WEBHOOK_URL = os.environ.get("WEBHOOK_URL_GROWTHRADAR")

SCAN_SIZE = 2000
MAX_WORKERS = 10

MIN_PRICE = 2.0
MIN_MCAP = 5e7
MIN_AVG_VOL_VAL = 5e5

HEADERS = {"User-Agent": "Mozilla/5.0"}

# =========================
# METRICS HOLDER（重要）
# =========================
class Metrics:
    def __init__(self):
        self.universe_size = 0
        self.scanned_size = 0
        self.base_size = 0
        self.tier1 = 0
        self.tier2 = 0

# =========================
def send_discord(webhook_url, text):
    if not webhook_url:
        print("[DISCORD] missing webhook")
        return

    chunks = [text[i:i+1800] for i in range(0, len(text), 1800)]
    for c in chunks:
        try:
            r = requests.post(webhook_url, json={"content": c}, timeout=10)
            print("[DISCORD]", r.status_code, r.text[:80])
        except Exception as e:
            print("[DISCORD ERROR]", e)

# =========================
class GrowthRadarV26_8:

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update(HEADERS)

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
                    found = r.text.split("\n") if url.endswith(".txt") else pd.read_csv(url)["Symbol"].tolist()
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
    def fetch(self, ticker):
        try:
            url = f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}?range=1y&interval=1d"
            r = self.session.get(url, timeout=6).json()
            res = r["chart"]["result"][0]

            close = [c for c in res["indicators"]["quote"][0]["close"] if c]
            volume = [v for v in res["indicators"]["quote"][0]["volume"] if v]

            if len(close) < 126:
                return None

            price = close[-1]
            if price < MIN_PRICE:
                return None

            avg_vol_val = np.mean(close[-21:]) * np.mean(volume[-21:])
            if np.isnan(avg_vol_val) or avg_vol_val < MIN_AVG_VOL_VAL:
                return None

            m1 = price / close[-21] - 1
            m3 = price / close[-63] - 1
            m6 = price / close[-126] - 1

            if m6 < 0.3:
                return None

            trend = np.mean(close[-10:]) / (np.mean(close[-30:-10]) + 1e-9) - 1

            return {
                "ticker": ticker,
                "price": price,
                "m6": m6,
                "accel": m1 - m3,
                "trend": trend,
                "vol_short": np.mean(volume[-5:]),
                "vol_mid": np.mean(volume[-21:]),
                "vol_long": np.mean(volume[-63:])
            }

        except:
            return None

    # =========================
    def score(self, df):
        df["vol_ratio"] = df["vol_short"] / (df["vol_mid"] + 1e-9)

        df["score"] = (
            df["m6"].rank(pct=True) * 0.40 +
            df["accel"].rank(pct=True) * 0.20 +
            df["trend"].rank(pct=True) * 0.25 +
            df["vol_ratio"].rank(pct=True) * 0.15
        )

        return df

    # =========================
    def run(self):
        m = Metrics()

        # ===== ① Universe（母集団）=====
        universe = self.load_universe()
        m.universe_size = len(universe)

        batch = universe[:SCAN_SIZE]
        m.scanned_size = len(batch)

        # ===== ② Fetch（生存データ）=====
        raw = []
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
            futures = {ex.submit(self.fetch, t): t for t in batch}
            for f in as_completed(futures):
                r = f.result()
                if r:
                    raw.append(r)

        m.base_size = len(raw)

        if m.base_size == 0:
            print("NO DATA")
            return

        # ===== ③ Score =====
        df = self.score(pd.DataFrame(raw))

        df = df.sort_values("score", ascending=False)

        m.tier1 = len(df[df["score"] > 0.80])
        m.tier2 = len(df[(df["score"] <= 0.80) & (df["score"] > 0.60)])

        # ===== ④ REPORT =====
        now = datetime.now().strftime("%Y-%m-%d %H:%M")

        msg = [
            "🚀 GrowthRadar v26.8 (Metrics Fixed)",
            f"Scanned:{m.universe_size} | Base:{m.base_size} | Tier1:{m.tier1} | Tier2:{m.tier2} | {now}\n",
            "🔥 Tier1"
        ]

        for r in df[df["score"] > 0.80].head(8).to_dict("records"):
            msg.append(f"{r['ticker']} S:{r['score']:.2f}")

        msg.append("\n👀 Tier2")
        for r in df[(df["score"] <= 0.80) & (df["score"] > 0.60)].head(8).to_dict("records"):
            msg.append(f"{r['ticker']} S:{r['score']:.2f}")

        text = "\n".join(msg)

        print(text)
        send_discord(WEBHOOK_URL, text)


if __name__ == "__main__":
    GrowthRadarV26_8().run()
