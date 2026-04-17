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

MIN_PRICE = 2.0
MIN_MCAP = 5e7
MIN_AVG_VOL_VAL = 5e5

STATE_FILE = "state_v266.json"
LOG_FILE = "trace_v266.log"

HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "application/json"
}

# =========================
# LOGGING
# =========================
def log(msg):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] {msg}"
    print(line)
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(line + "\n")

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
class GrowthRadarV26_6_Debug:
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
        return clean[:SCAN_SIZE]

    # =========================
    def fetch(self, ticker):
        try:
            log(f"[FETCH START] {ticker}")

            url = f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}?range=1y&interval=1d"
            r = self.session.get(url, timeout=6).json()
            res = r["chart"]["result"][0]

            close = [c for c in res["indicators"]["quote"][0]["close"] if c]
            volume = [v for v in res["indicators"]["quote"][0]["volume"] if v]

            if len(close) < 126:
                log(f"[DROP LEN] {ticker}")
                return None

            price = close[-1]
            if price < MIN_PRICE:
                log(f"[DROP PRICE] {ticker} price={price}")
                return None

            avg_vol_val = np.mean(close[-21:]) * np.mean(volume[-21:])
            if avg_vol_val < MIN_AVG_VOL_VAL:
                log(f"[DROP LIQUIDITY] {ticker}")
                return None

            m1 = price / close[-21] - 1
            m3 = price / close[-63] - 1
            m6 = price / close[-126] - 1

            if m6 < 0.3:
                log(f"[DROP M6] {ticker} m6={m6:.2f}")
                return None

            if m1 > 1.5:
                log(f"[DROP OVERHEAT] {ticker}")
                return None

            trend = np.mean(close[-10:]) / np.mean(close[-30:-10]) - 1

            volat = np.std(close[-21:]) / np.mean(close[-21:])
            if volat > 0.25:
                log(f"[DROP VOLAT] {ticker}")
                return None

            log(f"[PASS] {ticker} m6={m6:.2f} trend={trend:.2f}")

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

        except Exception as e:
            log(f"[ERROR] {ticker} {e}")
            return None

    # =========================
    def compute_score(self, df):
        df["vol_ratio"] = df["vol_short"] / (df["vol_mid"] + 1e-9)

        df["score"] = (
            df["m6"].rank(pct=True) * 0.40 +
            df["accel"].rank(pct=True) * 0.20 +
            df["trend"].rank(pct=True) * 0.25 +
            df["vol_ratio"].rank(pct=True) * 0.15
        )

        return df

    # =========================
    def update_state(self, df, state):
        now = datetime.now().strftime("%Y-%m-%d")

        for _, r in df.iterrows():
            t = r["ticker"]

            if t not in state:
                state[t] = {"history": []}

            state[t]["history"].append({
                "date": now,
                "score": float(r["score"]),
                "trend": float(r["trend"]),
                "m6": float(r["m6"])
            })

            state[t]["history"] = state[t]["history"][-10:]

            log(f"[STATE UPDATE] {t} hist={len(state[t]['history'])}")

        return state

    # =========================
    def run(self):
        log("===== RUN START =====")

        universe = self.load_universe()
        log(f"[UNIVERSE] {len(universe)}")

        batch = universe[:SCAN_SIZE]
        log(f"[BATCH] {len(batch)}")

        raw = []
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
            futures = {ex.submit(self.fetch, t): t for t in batch}
            for f in as_completed(futures):
                r = f.result()
                if r:
                    raw.append(r)

        log(f"[RAW PASSED] {len(raw)}")

        if not raw:
            log("NO DATA END")
            return

        df = pd.DataFrame(raw)
        df = self.compute_score(df)

        # =========================
        # STATE
        # =========================
        state = load_state()
        state = self.update_state(df, state)
        save_state(state)

        # =========================
        # REPORT
        # =========================
        t1 = df[df["score"] > 0.75].sort_values("score", ascending=False)
        t2 = df[(df["score"] <= 0.75) & (df["score"] > 0.55)].sort_values("score", ascending=False)

        log(f"[TIER1] {len(t1)}")
        log(f"[TIER2] {len(t2)}")

        msg = [
            "🚀 GrowthRadar v26.6 DEBUG",
            f"Tier1:{len(t1)} Tier2:{len(t2)}\n",
            "TOP TIER1"
        ]

        for r in t1.head(10).to_dict("records"):
            msg.append(f"{r['ticker']} S:{r['score']:.2f}")

        msg.append("\nTOP TIER2")

        for r in t2.head(10).to_dict("records"):
            msg.append(f"{r['ticker']} S:{r['score']:.2f}")

        text = "\n".join(msg)

        if WEBHOOK_URL:
            requests.post(WEBHOOK_URL, json={"content": text})

        log("===== RUN END =====")
        print(text)


if __name__ == "__main__":
    GrowthRadarV26_6_Debug().run()
