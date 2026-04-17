import os
import requests
import pandas as pd
import numpy as np
import random
import time
import json
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

WEBHOOK_URL = os.environ.get("WEBHOOK_URL_GROWTHRADAR")
MAX_WORKERS = 12

MIN_PRICE = 1.0

HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "application/json"
}

class GrowthRadarV25:

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update(HEADERS)

    # -----------------------
    # Universe
    # -----------------------
    def load_universe(self):
        symbols = []

        sources = [
            "https://raw.githubusercontent.com/rreichel3/US-Stock-Symbols/main/all_tickers.txt",
            "https://datahub.io/core/nasdaq-listings/r/nasdaq-listed-symbols.csv"
        ]

        print("Fetching universe...")
        for url in sources:
            try:
                r = self.session.get(url, timeout=10)
                if "csv" in url:
                    df = pd.read_csv(url)
                    symbols.extend(df["Symbol"].dropna().tolist())
                else:
                    symbols.extend([s.strip() for s in r.text.split("\n") if s.strip()])
            except:
                pass

        clean = list(set([
            s.upper() for s in symbols
            if re.match(r"^[A-Z]{1,5}$", str(s))
        ]))

        random.shuffle(clean)
        print(f"Universe size: {len(clean)}")
        return clean

    # -----------------------
    # Fetch
    # -----------------------
    def fetch(self, ticker):
        try:
            url = f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}?range=1y&interval=1d"
            r = self.session.get(url, timeout=5)
            j = r.json()["chart"]["result"][0]

            close = [c for c in j["indicators"]["quote"][0]["close"] if c is not None]
            vol = [v for v in j["indicators"]["quote"][0]["volume"] if v is not None]

            if len(close) < 60:
                return None

            price = close[-1]
            if price < MIN_PRICE:
                return None

            m1 = price / close[-21] - 1
            m3 = price / close[-63] - 1 if len(close) > 63 else m1
            accel = m1 - m3

            vol_ratio = (sum(vol[-5:]) / 5) / (sum(vol[-21:]) / 21 + 1e-9)

            # mcapは「使えたら使う」扱いに変更
            mcap = 0
            try:
                durl = f"https://query1.finance.yahoo.com/v7/finance/quote?symbols={ticker}"
                d = self.session.get(durl, timeout=3).json()
                mcap = d["quoteResponse"]["result"][0].get("marketCap", 0)
            except:
                pass

            return {
                "ticker": ticker,
                "price": price,
                "m1": m1,
                "m3": m3,
                "accel": accel,
                "vol": vol_ratio,
                "mcap": mcap
            }

        except:
            return None

    # -----------------------
    # Run
    # -----------------------
    def run(self):
        universe = self.load_universe()
        batch = universe[:1500]

        print(f"Scanning {len(batch)} symbols...")

        results = []
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
            futures = {ex.submit(self.fetch, t): t for t in batch}
            for f in as_completed(futures):
                r = f.result()
                if r:
                    results.append(r)

        print(f"Valid results: {len(results)}")

        if not results:
            print("No data.")
            return

        df = pd.DataFrame(results)

        # -----------------------
        # Z-score
        # -----------------------
        for col in ["accel", "m1", "vol"]:
            df[f"z_{col}"] = (df[col] - df[col].mean()) / (df[col].std() + 1e-9)

        # -----------------------
        # Soft scoring
        # -----------------------
        df["score"] = (
            df["z_accel"] * 0.5 +
            df["z_m1"] * 0.3 +
            df["z_vol"] * 0.2
        )

        # -----------------------
        # Tenbagger bias (重要)
        # -----------------------
        df["bonus"] = 0

        # 小型優遇
        df.loc[(df["mcap"] > 0) & (df["mcap"] < 2e9), "bonus"] += 0.5

        # 加速が明確
        df.loc[df["accel"] > 0.3, "bonus"] += 0.5

        # 初動ブレイク
        df.loc[df["m1"] > 0.5, "bonus"] += 0.5

        df["final_score"] = df["score"] + df["bonus"]

        # -----------------------
        # 出力（最低15件保証）
        # -----------------------
        top = df.sort_values("final_score", ascending=False).head(15)

        self.report(top, len(batch), len(df))

    def report(self, df, scanned, valid):
        now = datetime.now().strftime("%Y-%m-%d %H:%M")

        msg = [
            f"🚀 GrowthRadar v25",
            f"Scanned: {scanned} | Valid: {valid} | {now}\n"
        ]

        for r in df.to_dict("records"):
            mc = f"{r['mcap']/1e9:.2f}B" if r["mcap"] > 0 else "N/A"
            msg.append(
                f"{r['ticker']} | Score:{r['final_score']:.2f}\n"
                f"Price:{r['price']:.2f} | MC:{mc}\n"
                f"M1:{r['m1']:+.1%} | Accel:{r['accel']:.2f} | Vol:{r['vol']:.1f}x\n"
            )

        text = "\n".join(msg)

        if WEBHOOK_URL:
            requests.post(WEBHOOK_URL, json={"content": text})
        else:
            print(text)


if __name__ == "__main__":
    GrowthRadarV25().run()
