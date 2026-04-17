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

# =========================
# CONFIG (v25.2 Discovery Mode)
# =========================
WEBHOOK_URL = os.environ.get("WEBHOOK_URL_GROWTHRADAR")
MAX_WORKERS = 12

MIN_PRICE = 1.0
MIN_MCAP = 5e7

HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "application/json"
}

class GrowthRadarV25_2:

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update(HEADERS)

        # ===== 観測ログ =====
        self.stats = {
            "total": 0,
            "details_ok": 0,
            "price_ok": 0,
            "passed_filters": 0
        }

    # =========================
    # UNIVERSE
    # =========================
    def load_universe(self):
        symbols = []

        sources = [
            "https://raw.githubusercontent.com/rreichel3/US-Stock-Symbols/main/all_tickers.txt",
            "https://datahub.io/core/nasdaq-listings/r/nasdaq-listed-symbols.csv",
            "https://raw.githubusercontent.com/yannick-cw/stock-tickers/master/data/tickers.json"
        ]

        print("Fetching universe...")

        for url in sources:
            try:
                r = self.session.get(url, timeout=10)

                if url.endswith(".txt"):
                    found = [s.strip().upper() for s in r.text.split("\n") if s.strip()]

                elif url.endswith(".csv"):
                    df = pd.read_csv(url)
                    found = df["Symbol"].dropna().astype(str).tolist()

                else:
                    data = r.json()
                    found = [item["symbol"] if isinstance(item, dict) else item for item in data]

                symbols.extend(found)

            except:
                continue

        # クリーニング
        symbols = list(set([
            s for s in symbols
            if re.match(r"^[A-Z]{1,5}$", str(s))
        ]))

        random.shuffle(symbols)

        print(f"Universe size: {len(symbols)}")
        return symbols[:1500]

    # =========================
    # DETAILS
    # =========================
    def fetch_details(self, ticker):
        try:
            url = f"https://query1.finance.yahoo.com/v7/finance/quote?symbols={ticker}"
            r = self.session.get(url, timeout=5).json()
            d = r["quoteResponse"]["result"][0]

            return {
                "mcap": d.get("marketCap", 0),
                "name": d.get("longName", ""),
                "type": d.get("quoteType", "")
            }
        except:
            return None

    def is_noise(self, name):
        noise = ["WARRANT", "UNIT", "ACQUISITION", "RIGHT", "ETF"]
        return any(k in name.upper() for k in noise)

    # =========================
    # ANALYZE
    # =========================
    def analyze(self, ticker):
        self.stats["total"] += 1

        try:
            d = self.fetch_details(ticker)
            if not d:
                return None

            self.stats["details_ok"] += 1

            if d["type"] != "EQUITY":
                return None

            if self.is_noise(d["name"]):
                return None

            mcap = d["mcap"]
            if not mcap or mcap < MIN_MCAP:
                return None

            # 価格データ
            url = f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}?range=1y&interval=1d"
            r = self.session.get(url, timeout=5)
            j = r.json()["chart"]["result"][0]

            close = [c for c in j["indicators"]["quote"][0]["close"] if c is not None]
            vol   = [v for v in j["indicators"]["quote"][0]["volume"] if v is not None]

            if len(close) < 120:
                return None

            self.stats["price_ok"] += 1

            price = close[-1]
            if price < MIN_PRICE:
                return None

            # 指標
            m1  = price / close[-21] - 1
            m3  = price / close[-63] - 1
            m12 = price / close[0] - 1

            accel = m1 - m3
            vol_ratio = (sum(vol[-5:])/5) / (sum(vol[-30:])/30 + 1e-9)

            # ===== 緩和フィルタ =====
            if m3 < 0.15:
                return None

            if accel < 0.03:
                return None

            if vol_ratio < 1.0:
                return None

            self.stats["passed_filters"] += 1

            # スコア（ランキング用）
            score = (
                (m3 * 8) +
                (accel * 25) +
                (vol_ratio * 4)
            )

            return {
                "ticker": ticker,
                "price": price,
                "mcap": mcap,
                "m1": m1,
                "m3": m3,
                "m12": m12,
                "accel": accel,
                "vol": vol_ratio,
                "score": score
            }

        except:
            return None

    # =========================
    # RUN
    # =========================
    def run(self):
        universe = self.load_universe()

        print(f"Scanning {len(universe)} symbols...")

        results = []

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
            futures = {ex.submit(self.analyze, t): t for t in universe}

            for f in as_completed(futures):
                r = f.result()
                if r:
                    results.append(r)

        # ===== ログ =====
        print("\n=== SCAN STATS ===")
        print(f"Total:          {self.stats['total']}")
        print(f"Details OK:     {self.stats['details_ok']}")
        print(f"Price OK:       {self.stats['price_ok']}")
        print(f"Passed Filters: {self.stats['passed_filters']}")
        print("==================\n")

        if not results:
            self.output("No candidates.")
            return

        df = pd.DataFrame(results)
        df = df.sort_values("score", ascending=False).head(50)

        self.report(df, len(universe), len(results))

    # =========================
    # REPORT
    # =========================
    def report(self, df, total, valid):
        now = datetime.now().strftime("%Y/%m/%d %H:%M")

        msg = [
            f"🚀 GrowthRadar v25.2 Discovery",
            f"Universe: {total} | Hits: {valid} | {now}\n"
        ]

        for r in df.to_dict("records"):
            msg.append(
                f"{r['ticker']} | Score:{r['score']:.2f}\n"
                f"Price:{r['price']:.2f} | MC:{r['mcap']/1e9:.2f}B\n"
                f"M3:{r['m3']:.1%} | Accel:{r['accel']:.2f} | Vol:{r['vol']:.1f}x\n"
            )

        self.output("\n".join(msg))

    def output(self, text):
        if WEBHOOK_URL:
            try:
                requests.post(WEBHOOK_URL, json={"content": text}, timeout=10)
            except:
                pass
        print(text)


if __name__ == "__main__":
    GrowthRadarV25_2().run()
