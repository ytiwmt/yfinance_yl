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
STATE_FILE = "growth_radar_timeseries.json"

MIN_PRICE = 2.0
HEADERS = {"User-Agent": "Mozilla/5.0"}

# =========================
# TIME SERIES STATE
# =========================
class TimeSeriesState:

    def __init__(self, path):
        self.path = path
        self.data = self._load()

    def _load(self):
        if os.path.exists(self.path):
            try:
                with open(self.path, "r") as f:
                    return json.load(f)
            except:
                return {}
        return {}

    def update(self, df):
        today = datetime.now().strftime("%Y-%m-%d")

        for r in df.to_dict("records"):
            t = r["ticker"]

            if t not in self.data:
                self.data[t] = []

            self.data[t].append({
                "date": today,
                "momentum": float(r["momentum"]),
                "price": float(r["price"])
            })

            # メモリ制限（直近90日）
            self.data[t] = self.data[t][-90:]

    def get_series(self, ticker):
        return self.data.get(ticker, [])

    def save(self):
        with open(self.path, "w") as f:
            json.dump(self.data, f)

# =========================
def send(webhook, text):
    if not webhook:
        print(text)
        return
    try:
        requests.post(webhook, json={"content": text}, timeout=10)
    except:
        pass

# =========================
class GrowthRadarV29:

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update(HEADERS)
        self.state = TimeSeriesState(STATE_FILE)

    # =========================
    def load_universe(self):
        url = "https://raw.githubusercontent.com/rreichel3/US-Stock-Symbols/main/all_tickers.txt"
        r = self.session.get(url, timeout=10).text.split("\n")
        clean = list(set([x.strip().upper() for x in r if re.match(r"^[A-Z]{1,5}$", x)]))
        random.shuffle(clean)
        return clean[:SCAN_SIZE]

    # =========================
    def fetch(self, t):
        try:
            url = f"https://query1.finance.yahoo.com/v8/finance/chart/{t}?range=1y&interval=1d"
            r = self.session.get(url, timeout=6).json()
            res = r["chart"]["result"][0]

            close = [c for c in res["indicators"]["quote"][0]["close"] if c]
            volume = [v for v in res["indicators"]["quote"][0]["volume"] if v]

            if len(close) < 120:
                return None

            price = close[-1]
            if price < MIN_PRICE:
                return None

            m1 = price / close[-21] - 1
            m3 = price / close[-63] - 1
            m6 = price / close[-120] - 1

            trend = np.mean(close[-10:]) / (np.mean(close[-30:-10]) + 1e-9) - 1

            accel = m1 - m3

            return {
                "ticker": t,
                "price": price,
                "m1": m1,
                "m3": m3,
                "m6": m6,
                "trend": trend,
                "accel": accel,
                "vol": np.mean(volume[-21:])
            }

        except:
            return None

    # =========================
    def score(self, df):

        df["momentum"] = (
            df["m6"].rank(pct=True) * 0.35 +
            df["accel"].rank(pct=True) * 0.35 +
            df["trend"].rank(pct=True) * 0.30
        )

        return df

    # =========================
    def detect_reentry(self, df):

        reentry = []

        for r in df.to_dict("records"):
            series = self.state.get_series(r["ticker"])

            if len(series) < 5:
                continue

            # slope（直近5日トレンド）
            recent = [x["momentum"] for x in series[-5:]]
            slope = recent[-1] - recent[0]

            prev_slope = series[-2]["momentum"] - series[-5]["momentum"]

            # “沈んでから再上昇”
            if prev_slope < 0 and slope > 0.15 and r["momentum"] > 0.75:
                reentry.append(r)

        return reentry

    # =========================
    def run(self):

        universe = self.load_universe()

        raw = []
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
            futures = {ex.submit(self.fetch, t): t for t in universe}
            for f in as_completed(futures):
                r = f.result()
                if r:
                    raw.append(r)

        if not raw:
            print("NO DATA")
            return

        df = self.score(pd.DataFrame(raw)).sort_values("momentum", ascending=False)

        tier1 = df[df["momentum"] > 0.85]
        tier2 = df[(df["momentum"] <= 0.85) & (df["momentum"] > 0.7)]

        reentry = self.detect_reentry(df)

        # STATE UPDATE（時系列）
        self.state.update(df)
        self.state.save()

        now = datetime.now().strftime("%Y-%m-%d %H:%M")

        msg = [
            "📈 GrowthRadar v29 (Time-Series Engine)",
            f"Live:{len(df)} Tier1:{len(tier1)} Tier2:{len(tier2)} ReEntry:{len(reentry)} {now}\n",
            "🔥 Tier1"
        ]

        for r in tier1.head(10).to_dict("records"):
            msg.append(f"{r['ticker']} S:{r['momentum']:.2f}")

        msg.append("\n👀 Tier2")
        for r in tier2.head(10).to_dict("records"):
            msg.append(f"{r['ticker']} S:{r['momentum']:.2f}")

        if reentry:
            msg.append("\n♻️ Re-Entry (Slope Break)")
            for r in reentry[:8]:
                msg.append(f"{r['ticker']} S:{r['momentum']:.2f}")

        text = "\n".join(msg)

        print(text)
        send(WEBHOOK_URL, text)


if __name__ == "__main__":
    GrowthRadarV29().run()
