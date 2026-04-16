import os
import requests
import pandas as pd
import random
import time
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from io import StringIO

# =========================
# CONFIG
# =========================
WEBHOOK_URL = os.environ.get("WEBHOOK_URL_GROWTHRADAR")

MAX_WORKERS = 12
TIMEOUT = 15

# 毎日1回だが「循環スキャン」で市場全体カバー
ROTATION_SIZE = 1000  # 1日で処理する母数（重要）

MIN_PRICE = 2.0
MAX_PRICE = 1000.0

MIN_MCAP = 50_000_000
MAX_MCAP = 15_000_000_000

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)


class GrowthRadarV14:

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0"
        })

    # =========================
    # UNIVERSE（固定＋拡張）
    # =========================
    def get_universe(self):
        core = [
            "PLTR","NVDA","RKLB","ASTS","OKLO","HIMS","CELH","UPST",
            "COIN","HOOD","SMCI","RDDT","LUNR","IONQ","APP","SOUN",
            "DUOL","MSTR","TSLA","MARA"
        ]

        try:
            url = "https://datahub.io/core/nasdaq-listed-symbols/r/nasdaq-listed-symbols.csv"
            df = pd.read_csv(StringIO(self.session.get(url, timeout=10).text))

            df = df.dropna()
            df = df[~df["Symbol"].str.contains(r"[\$\.\-\=]", na=False)]
            df = df[~df["Symbol"].str.endswith(("W","U","R","Z"))]

            universe = df["Symbol"].tolist()
            random.shuffle(universe)

            merged = list(dict.fromkeys(core + universe))

            return merged

        except Exception:
            return core

    # =========================
    # ROTATION（最重要）
    # =========================
    def rotate_universe(self, universe):
        """
        毎日違う部分を見ることで、
        3日〜5日で市場全体をカバーする
        """
        day = int(time.time() // 86400)  # 日単位の擬似シード
        random.seed(day)

        start = (day * ROTATION_SIZE) % max(len(universe), 1)
        rotated = universe[start:start + ROTATION_SIZE]

        if len(rotated) < ROTATION_SIZE:
            rotated += universe[:ROTATION_SIZE - len(rotated)]

        return rotated

    # =========================
    # DATA FETCH
    # =========================
    def fetch(self, url):
        try:
            r = self.session.get(url, timeout=TIMEOUT)
            return r.json() if r.status_code == 200 else None
        except:
            return None

    # =========================
    # ANALYZE
    # =========================
    def analyze(self, ticker):
        p_url = f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}?range=1y&interval=1d"
        f_url = f"https://query1.finance.yahoo.com/v10/finance/quoteSummary/{ticker}?modules=financialData,defaultKeyStatistics"

        p = self.fetch(p_url)
        if not p:
            return None

        try:
            res = p["chart"]["result"][0]
            closes = [c for c in res["indicators"]["quote"][0]["close"] if c]

            if len(closes) < 120:
                return None

            price = closes[-1]

            if not (MIN_PRICE <= price <= MAX_PRICE):
                return None

            m1 = price / closes[-20] - 1
            m3 = price / closes[-60] - 1
            m6 = price / closes[-120] - 1

            accel = m1 - m3

            f = self.fetch(f_url) or {}
            rev = 0
            mcap = 0

            try:
                r0 = f["quoteSummary"]["result"][0]
                rev = r0.get("financialData", {}).get("revenueGrowth", {}).get("raw", 0)
                mcap = r0.get("defaultKeyStatistics", {}).get("marketCap", {}).get("raw", 0)
            except:
                pass

            if mcap and not (MIN_MCAP <= mcap <= MAX_MCAP):
                return None

            # =========================
            # ANOMALY SCORE（核心）
            # =========================
            score = 0

            # 成長
            if rev > 0.30:
                score += 8
            elif rev > 0.15:
                score += 4

            # 加速度（最重要）
            if accel > 0.25:
                score += 12
            elif accel > 0.10:
                score += 6

            # トレンド持続
            if m1 > m3:
                score += 5

            # 既存トレンド
            if m6 > 1.0:
                score += 5

            # 小型株
            if mcap and mcap < 2e9:
                score += 4

            if score < 14:
                return None

            return {
                "ticker": ticker,
                "price": price,
                "score": score,
                "rev": rev,
                "mcap": mcap,
                "accel": accel,
                "m1": m1,
                "m3": m3,
                "m6": m6
            }

        except:
            return None

    # =========================
    # RUN
    # =========================
    def run(self):
        universe = self.get_universe()
        universe = self.rotate_universe(universe)

        log.info(f"Scan universe: {len(universe)}")

        results = []

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
            futures = {ex.submit(self.analyze, t): t for t in universe}

            for f in as_completed(futures):
                r = f.result()
                if r:
                    results.append(r)

        df = pd.DataFrame(results)

        if not df.empty:
            df = df.sort_values("score", ascending=False).head(20)

        self.notify(df, len(universe), len(results))

    # =========================
    # OUTPUT
    # =========================
    def notify(self, df, total, hits):
        msg = []
        msg.append("🚀 GrowthRadar v14 (Discovery Engine)")
        msg.append(f"Universe: {total} | Hits: {hits}\n")

        if df.empty:
            msg.append("No anomalies detected.")
        else:
            for r in df.to_dict("records"):
                msg.append(
                    f"**{r['ticker']}** | Score:{r['score']}\n"
                    f"Price:{r['price']:.2f} | MC:{r['mcap']/1e9:.2f}B\n"
                    f"Rev:{r['rev']:.1%} | Accel:{r['accel']:.2f}\n"
                )

        out = "\n".join(msg)

        if WEBHOOK_URL:
            requests.post(WEBHOOK_URL, json={"content": out}, timeout=10)
        else:
            print(out)


if __name__ == "__main__":
    GrowthRadarV14().run()
