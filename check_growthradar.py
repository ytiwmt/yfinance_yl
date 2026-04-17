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
SCAN_SIZE = 2000

STATE_FILE = "state_store_v26_6.json"

MIN_PRICE = 2.0
MIN_MCAP = 5e7
MIN_AVG_VOL_VAL = 5e5

HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "application/json"
}

# =========================
# STATE STORE
# =========================
def load_state():
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE, "r") as f:
            return json.load(f)
    return {}

def save_state(state):
    with open(STATE_FILE, "w") as f:
        json.dump(state, f)

# =========================
class GrowthRadarV26_6:
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
        return clean[:3000]

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
            if avg_vol_val < MIN_AVG_VOL_VAL:
                return None

            m1 = price / close[-21] - 1
            m3 = price / close[-63] - 1
            m6 = price / close[-126] - 1

            if m6 < 0.3 or m1 > 1.5:
                return None

            trend = np.mean(close[-10:]) / np.mean(close[-30:-10]) - 1

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
    def compute_score(self, df):
        df["vol_ratio"] = df["vol_short"] / (df["vol_mid"] + 1e-9)

        raw_score = (
            df["m6"].rank(pct=True) * 0.40 +
            df["accel"].rank(pct=True) * 0.20 +
            df["trend"].rank(pct=True) * 0.25 +
            df["vol_ratio"].rank(pct=True) * 0.15
        )

        df["raw_score"] = raw_score
        return df

    # =========================
    def update_state(self, df, state):
        now = datetime.now().strftime("%Y-%m-%d")

        for _, r in df.iterrows():
            t = r["ticker"]

            if t not in state:
                state[t] = {
                    "history": []
                }

            state[t]["history"].append({
                "date": now,
                "score": float(r["raw_score"]),
                "trend": float(r["trend"]),
                "m6": float(r["m6"])
            })

            # 最新10日だけ保持
            state[t]["history"] = state[t]["history"][-10:]

        return state

    # =========================
    def state_score(self, state):
        rows = []

        for t, s in state.items():
            hist = s["history"]
            if len(hist) < 3:
                continue

            scores = [h["score"] for h in hist]
            trends = [h["trend"] for h in hist]

            state_score = (
                np.mean(scores) * 0.7 +
                np.max(scores) * 0.3
            )

            stability = 1 - np.std(scores)

            rows.append({
                "ticker": t,
                "state_score": state_score,
                "stability": stability,
                "last_score": scores[-1],
                "trend": trends[-1]
            })

        return pd.DataFrame(rows)

    # =========================
    def run(self):
        universe = self.load_universe()
        batch = universe[:SCAN_SIZE]

        raw = []
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
            futures = {ex.submit(self.fetch, t): t for t in batch}
            for f in as_completed(futures):
                r = f.result()
                if r:
                    raw.append(r)

        if not raw:
            print("No data")
            return

        df = pd.DataFrame(raw)
        df = self.compute_score(df)

        state = load_state()
        state = self.update_state(df, state)
        save_state(state)

        st_df = self.state_score(state)

        if st_df.empty:
            return

        # フィルタ（状態保持型）
        t1 = st_df[
            (st_df["state_score"] > 0.7) &
            (st_df["stability"] > 0.2)
        ].sort_values("state_score", ascending=False)

        t2 = st_df[
            (st_df["state_score"] > 0.5)
        ].sort_values("state_score", ascending=False)

        self.report(t1, t2)

    # =========================
    def report(self, t1, t2):
        now = datetime.now().strftime("%Y-%m-%d %H:%M")

        msg = [
            f"🚀 GrowthRadar v26.6 (State-Aware)",
            f"Tier1:{len(t1)} | Tier2:{len(t2)} | {now}\n"
        ]

        msg.append("🏆 Tier1 (Persistent)")
        for r in t1.head(10).to_dict("records"):
            msg.append(f"{r['ticker']} | S:{r['state_score']:.2f} | ST:{r['stability']:.2f}")

        msg.append("\n👀 Tier2 (Tracking)")
        for r in t2.head(10).to_dict("records"):
            msg.append(f"{r['ticker']} | S:{r['state_score']:.2f}")

        text = "\n".join(msg)

        if WEBHOOK_URL:
            requests.post(WEBHOOK_URL, json={"content": text})

        print(text)


if __name__ == "__main__":
    GrowthRadarV26_6().run()
