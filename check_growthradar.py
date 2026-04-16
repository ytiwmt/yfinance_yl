import os
import requests
import pandas as pd
import numpy as np
import random
import time
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

# =========================
# CONFIG
# =========================
WEBHOOK_URL = os.environ.get("WEBHOOK_URL_GROWTHRADAR")

UNIVERSE_FILE = "universe.json"
HISTORY_FILE = "history.json"

SCAN_SPLIT = 3
MAX_WORKERS = 6

MIN_PRICE = 3
MIN_MCAP = 1e8
MAX_MCAP = 5e9
MIN_REV = 0.10

HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "application/json"
}

# =========================
# ENGINE
# =========================
class GrowthRadarV20:

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update(HEADERS)

    # =========================
    # UNIVERSE
    # =========================
    def load_universe(self):
        if os.path.exists(UNIVERSE_FILE):
            with open(UNIVERSE_FILE) as f:
                return json.load(f)

        url = "https://datahub.io/core/nasdaq-listings/r/nasdaq-listed-symbols.csv"
        df = pd.read_csv(url)
        df = df.dropna(subset=["Symbol"])
        df = df[~df["Symbol"].str.contains(r"[\$\.\-\=]", na=False)]

        symbols = df["Symbol"].tolist()
        random.shuffle(symbols)

        universe = symbols[:3000]

        with open(UNIVERSE_FILE, "w") as f:
            json.dump(universe, f)

        return universe

    # =========================
    # ROTATION
    # =========================
    def get_batch(self, universe):
        idx = datetime.utcnow().day % SCAN_SPLIT
        size = len(universe) // SCAN_SPLIT
        return universe[idx*size:(idx+1)*size]

    # =========================
    # FETCH
    # =========================
    def fetch(self, ticker):
        try:
            # price
            p_url = f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}?range=1y&interval=1d"
            r = self.session.get(p_url, timeout=10)
            j = r.json()["chart"]["result"][0]

            close = [c for c in j["indicators"]["quote"][0]["close"] if c]
            vol = [v for v in j["indicators"]["quote"][0]["volume"] if v]

            if len(close) < 120:
                return None

            price = close[-1]
            if price < MIN_PRICE:
                return None

            m1 = price / close[-21] - 1
            m3 = price / close[-63] - 1
            m12 = price / close[0] - 1

            accel = m1 - m3
            vol_ratio = (sum(vol[-5:])/5)/(sum(vol[-30:])/30 + 1e-9)

            # fundamentals（軽量1回）
            f_url = f"https://query1.finance.yahoo.com/v10/finance/quoteSummary/{ticker}?modules=financialData,defaultKeyStatistics"
            f = self.session.get(f_url, timeout=10).json()

            res = f["quoteSummary"]["result"][0]

            rev = res.get("financialData", {}).get("revenueGrowth", {}).get("raw", 0)
            mcap = res.get("defaultKeyStatistics", {}).get("marketCap", {}).get("raw", 0)

            # フィルタ（ここが核心）
            if rev < MIN_REV:
                return None

            if not (MIN_MCAP < mcap < MAX_MCAP):
                return None

            return {
                "ticker": ticker,
                "price": price,
                "m1": m1,
                "m12": m12,
                "accel": accel,
                "vol": vol_ratio,
                "rev": rev,
                "mcap": mcap
            }

        except:
            return None

    # =========================
    # RUN
    # =========================
    def run(self):
        universe = self.load_universe()
        batch = self.get_batch(universe)

        results = []

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
            futures = {ex.submit(self.fetch, t): t for t in batch}
            for f in as_completed(futures):
                r = f.result()
                if r:
                    results.append(r)

        df = pd.DataFrame(results)
        if df.empty:
            print("No candidates")
            return

        # =========================
        # Z-SCORE
        # =========================
        for col in ["accel", "m1", "vol"]:
            df[f"z_{col}"] = (df[col] - df[col].mean()) / (df[col].std() + 1e-9)

        # テンバガー特化スコア
        df["score"] = (
            0.5 * df["z_accel"] +
            0.25 * df["z_vol"] +
            0.15 * df["z_m1"] +
            0.10 * df["rev"]
        )

        # 一発屋排除
        df = df[df["m12"] > -0.3]

        df = df.sort_values("score", ascending=False).head(15)

        self.report(df, len(batch))

    # =========================
    # REPORT
    # =========================
    def report(self, df, batch_size):
        msg = [
            f"🚀 GrowthRadar v20 (Tenbagger Core)",
            f"Scanned: {batch_size} | Candidates: {len(df)}\n"
        ]

        for r in df.to_dict("records"):
            msg.append(
                f"{r['ticker']} | Score:{r['score']:.2f}\n"
                f"Price:{r['price']:.2f} | MC:{r['mcap']/1e9:.2f}B\n"
                f"Rev:{r['rev']:.1%} | Accel:{r['accel']:.2f} | M1:{r['m1']:.1%}\n"
            )

        text = "\n".join(msg)

        if WEBHOOK_URL:
            requests.post(WEBHOOK_URL, json={"content": text})
        else:
            print(text)


if __name__ == "__main__":
    GrowthRadarV20().run()
