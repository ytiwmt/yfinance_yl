import os
import requests
import pandas as pd
import numpy as np
import random
import time
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

# =========================
# CONFIG (v17.0 Direct Hijack)
# =========================
WEBHOOK_URL = os.environ.get("WEBHOOK_URL_GROWTHRADAR")
MAX_WORKERS = 10
SCAN_LIMIT = 500  # 質の高い母集団に絞る

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

class GrowthRadarV17:
    def __init__(self):
        self.session = requests.Session()
        # ブラウザに完全になりすます
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9"
        })

    def get_universe(self):
        """
        [v17 核心] 
        外部リストが死んでいるなら、Yahoo自体の『値上がり率ランキング』から
        現在進行系で動いている母集団を直接引っこ抜く。
        """
        core = ["PLTR","NVDA","RKLB","ASTS","OKLO","HIMS","CELH","UPST","COIN","HOOD","SMCI","RDDT","LUNR","IONQ","APP","SOUN","DUOL","MSTR","TSLA","MARA"]
        
        scraped_tickers = []
        # Yahoo FinanceのDay Gainers / Most Active / Trending から300-500件程度狙う
        predefined_scanners = [
            "day_gainers", "most_active", "trending_tickers"
        ]
        
        for scrub in predefined_scanners:
            try:
                url = f"https://query1.finance.yahoo.com/v1/finance/screener/predefined/saved?formatted=false&dist=200&scrIds={scrub}"
                r = self.session.get(url, timeout=10)
                if r.status_code == 200:
                    data = r.json()
                    results = data.get("finance", {}).get("result", [{}])[0].get("quotes", [])
                    tickers = [q.get("symbol") for q in results if q.get("symbol")]
                    scraped_tickers.extend(tickers)
                    log.info(f"Scraped {len(tickers)} symbols from {scrub}")
            except Exception as e:
                log.warning(f"Screener {scrub} failed: {e}")

        # 重複排除 & 合体
        total_pool = list(dict.fromkeys(core + scraped_tickers))
        final_list = [t for t in total_pool if t and t.isalpha() and len(t) <= 5]
        
        log.info(f"Final Universe Size: {len(final_list)}")
        return final_list[:SCAN_LIMIT]

    def analyze(self, ticker):
        url = f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}?range=1y&interval=1d"
        try:
            r = self.session.get(url, timeout=10)
            if r.status_code != 200: return None
            
            res = r.json()["chart"]["result"][0]
            meta = res.get("meta", {})
            mcap = meta.get("marketCap", 0)
            
            quote = res.get("indicators", {}).get("quote", [{}])[0]
            closes = [c for c in quote.get("close", []) if c is not None]
            vols = [v for v in quote.get("volume", []) if v is not None]

            if len(closes) < 130: return None

            price = closes[-1]
            if price < 2.0: return None # ゴミ株排除

            # 出来高急増チェック (直近3日 avg / 20日 avg)
            v_spike = (sum(vols[-3:]) / 3) / (sum(vols[-20:]) / 20 + 1e-9)

            m1 = price / closes[-20] - 1
            m3 = price / closes[-60] - 1
            m12 = price / closes[0] - 1
            accel = m1 - m3

            return {
                "ticker": ticker, "price": price, "mcap": mcap,
                "m1": m1, "m3": m3, "m12": m12, "accel": accel, "v_spike": v_spike
            }
        except:
            return None

    def run(self):
        universe = self.get_universe()
        if len(universe) < 50:
            log.warning("Universe too small. Expanding via backup...")
            # ここでも少なかったらもうお手上げなので、Universeを増やすための代替策を検討
            
        results = []
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
            futures = {ex.submit(self.analyze, t): t for t in universe}
            for f in as_completed(futures):
                res = f.result()
                if res: results.append(res)

        df = pd.DataFrame(results)
        if len(df) < 15:
            self.notify_error("Critical failure: Could not build a valid dataset.")
            return

        # Z-Score
        for col in ["accel", "m1", "v_spike"]:
            df[f"z_{col}"] = (df[col] - df[col].mean()) / (df[col].std() + 1e-9)
        
        # 出来高スパイクを重み付けに加える
        df["score"] = (df["z_accel"] * 0.4) + (df["z_m1"] * 0.3) + (df["z_v_spike"] * 0.3)

        top = df.sort_values("score", ascending=False).head(15)
        self.notify(top, len(universe), len(df))

    def notify(self, df, total, valid):
        msg = [f"🔥 **GrowthRadar v17.0 (Screener Hijack)**", f"Universe: {total} | Analyzed: {valid}\n"]
        for r in df.to_dict("records"):
            msg.append(
                f"**{r['ticker']}** | Score: {r['score']:.2f}\n"
                f"└ Price: ${r['price']:.2f} | M1: {r['m1']:+.1%} | Vol: {r['v_spike']:.1f}x"
            )
        
        out = "\n".join(msg)
        if WEBHOOK_URL:
            requests.post(WEBHOOK_URL, json={"content": out})
        else:
            print(out)

    def notify_error(self, err):
        if WEBHOOK_URL:
            requests.post(WEBHOOK_URL, json={"content": f"❌ {err}"})

if __name__ == "__main__":
    GrowthRadarV17().run()
