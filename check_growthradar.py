import os
import requests
import pandas as pd
import numpy as np
import random
import time
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

# =========================
# CONFIG (v15.3 Robust Universe)
# =========================
WEBHOOK_URL = os.environ.get("WEBHOOK_URL_GROWTHRADAR")

MAX_WORKERS = 12
SCAN_LIMIT = 2000

# ノイズ除去閾値
MIN_PRICE = 3.0
MIN_MARKET_CAP = 50_000_000

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

class GrowthRadarV15_3:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": "Mozilla/5.0"})

    def get_universe(self):
        """
        [修正ポイント] 
        1つでも多くの銘柄を確実に拾うため、複数の信頼できるソースを統合。
        """
        core = ["PLTR","NVDA","RKLB","ASTS","OKLO","HIMS","CELH","UPST","COIN","HOOD","SMCI","RDDT","LUNR","IONQ","APP","SOUN","DUOL","MSTR","TSLA","MARA"]
        
        # 複数の信頼できる生データソース
        sources = [
            "https://raw.githubusercontent.com/rreichel3/US-Stock-Symbols/main/all_tickers.txt",
            "https://raw.githubusercontent.com/shilewenuw/get_all_tickers/master/get_all_tickers/tickers.csv",
            "https://pkgstore.datahub.io/core/nasdaq-listed-symbols/nasdaq-listed-symbols_csv/data/59b09030234148646b18e03fbc5ce30f/nasdaq-listed-symbols_csv.csv"
        ]
        
        ticker_pool = set(core)
        
        for url in sources:
            try:
                log.info(f"Attempting to fetch from: {url}")
                res = self.session.get(url, timeout=10)
                if res.status_code == 200:
                    if "csv" in url:
                        # CSV形式のパース
                        temp_df = pd.read_csv(requests.compat.StringIO(res.text))
                        # Symbol, Ticker, ABBRなどカラム名のゆらぎをカバー
                        col = next((c for c in temp_df.columns if c.lower() in ["symbol", "ticker", "abbr"]), None)
                        if col:
                            ticker_pool.update(temp_df[col].dropna().astype(str).tolist())
                    else:
                        # テキスト形式のパース
                        ticker_pool.update([t.strip() for t in res.text.split('\n') if t.strip()])
                
                # 1つでも300件以上取れたら、ある程度の母集団として認める
                if len(ticker_pool) > 300:
                    break
            except Exception as e:
                log.warning(f"Failed to fetch {url}: {e}")

        # クレンジング
        final_list = [t.upper() for t in ticker_pool if t.isalpha() and 1 <= len(t) <= 5]
        random.shuffle(final_list)
        
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
            
            # 物理フィルタ（時価総額）
            if mcap > 0 and mcap < MIN_MARKET_CAP: return None

            quote = res.get("indicators", {}).get("quote", [{}])[0]
            closes = [c for c in quote.get("close", []) if c is not None]
            
            if len(closes) < 130: return None

            price = closes[-1]
            if price < MIN_PRICE: return None

            # 特徴量
            m1 = price / closes[-20] - 1
            m3 = price / closes[-60] - 1
            m12 = price / closes[0] - 1
            accel = m1 - m3

            return {
                "ticker": ticker, "price": price, "mcap": mcap,
                "m1": m1, "m3": m3, "m12": m12, "accel": accel
            }
        except:
            return None

    def run(self):
        universe = self.get_universe()
        results = []
        
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
            futures = {ex.submit(self.analyze, t): t for t in universe}
            for f in as_completed(futures):
                res = f.result()
                if res: results.append(res)

        df = pd.DataFrame(results)
        if len(df) < 20:
            log.warning(f"Only {len(df)} valid results. Too few for reliable Z-Score.")
            # 統計なしでモメンタム順に出す
            df["score"] = df["m1"] * 100 
        else:
            # Z-Scoreによる異常検知
            for col in ["accel", "m1", "m3"]:
                df[f"z_{col}"] = (df[col] - df[col].mean()) / (df[col].std() + 1e-9)
            df["score"] = (df["z_accel"] * 0.5) + (df["z_m1"] * 0.3) + (df["z_m3"] * 0.2)

        top = df.sort_values("score", ascending=False).head(15)
        self.notify(top, len(universe), len(df))

    def notify(self, df, total, valid):
        msg = [f"🛡️ **GrowthRadar v15.3 (Anomaly Detector)**", f"Universe: {total} | Valid: {valid}\n"]
        for r in df.to_dict("records"):
            # 統計計算ができたかどうかで表示を変える
            score_label = f"Score: {r['score']:.2f}" if "z_accel" in r else f"M1: {r['m1']:.1%}"
            msg.append(f"**{r['ticker']}** | {score_label}\n└ Price: ${r['price']:.2f} | M1: {r['m1']:+.1%} | M12: {r['m12']:+.1%}")
        
        out = "\n".join(msg)
        if WEBHOOK_URL:
            requests.post(WEBHOOK_URL, json={"content": out})
        else:
            print(out)

if __name__ == "__main__":
    GrowthRadarV15_3().run()
