import os
import requests
import pandas as pd
import yfinance as yf
from concurrent.futures import ThreadPoolExecutor, as_completed
import random
import time

# =========================
# CONFIG (GitHub Actions Optimized)
# =========================
WEBHOOK_URL = os.environ.get("WEBHOOK_URL_GROWTHRADAR")

# GitHub Actionsでのレート制限対策：スキャン数を抑えつつ精度を高める
SCAN_LIMIT = 800 
MAX_WORKERS = 4  # 並列数を下げて接続安定性を確保

MIN_MCAP = 100_000_000
MAX_MCAP = 4_000_000_000
MIN_PRICE = 2.0  # 少し閾値を下げて母数を確保
MIN_YOY = 0.15   # 15%成長以上に緩和

class GrowthRadarCloudRunner:
    def __init__(self):
        self.session = requests.Session()
        # GitHub ActionsのIPが弾かれないよう、より一般的なヘッダーを設定
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36'
        })

    def get_tickers(self):
        # 確実に動くよう、手動厳選リストを優先的に混ぜる
        core_growth = ["PLTR", "CELH", "RKLB", "IONQ", "HIMS", "UPST", "DUOL", "APP", "SOUN", "BROS"]
        try:
            url = "https://datahub.io/core/nasdaq-listings/r/nasdaq-listed-symbols.csv"
            nasdaq = pd.read_csv(url)["Symbol"].dropna().tolist()
            return list(set(core_growth + nasdaq))
        except:
            return core_growth

    def prefilter(self, ticker):
        """リトライ付きのデータ取得"""
        for _ in range(3): # 最大3回リトライ
            try:
                t = yf.Ticker(ticker, session=self.session)
                # periodを1moから60dに伸ばし、データの欠落を防ぐ
                hist = t.history(period="60d")
                if hist.empty or len(hist) < 10:
                    time.sleep(random.uniform(0.5, 1.5))
                    continue

                p_now = hist["Close"].iloc[-1]
                vol_avg = hist["Volume"].mean()
                
                # S1通過条件を少し緩和
                if p_now < MIN_PRICE or vol_avg < 50_000:
                    return None
                
                return {"ticker": ticker, "price": p_now}
            except:
                time.sleep(1)
        return None

    def analyze(self, ticker):
        try:
            t = yf.Ticker(ticker, session=self.session)
            # infoの代わりにfast_infoを使用（GitHub Actionsで安定）
            f = t.fast_info
            mcap = f.market_cap
            if not mcap or mcap < MIN_MCAP or mcap > MAX_MCAP: return None

            hist = t.history(period="1y")
            if hist.empty or len(hist) < 100: return None
            
            p_now = hist["Close"].iloc[-1]
            high_1y = hist["Close"].max()
            dist_high = (high_1y - p_now) / (high_1y + 1e-9)

            # 財務データの取得（ここが最も失敗しやすい）
            fin = t.quarterly_financials
            if fin is None or fin.empty: return None
            
            fin.index = fin.index.str.replace(" ", "").str.upper()
            if "TOTALREVENUE" not in fin.index: return None
            
            rev = fin.loc["TOTALREVENUE"].dropna().values
            if len(rev) < 3: return None

            g0 = (rev[0] - rev[1]) / rev[1] if rev[1] > 0 else 0
            g1 = (rev[1] - rev[2]) / rev[2] if rev[2] > 0 else 0
            accel = g0 - g1
            if g0 < MIN_YOY: return None

            margin_boost = 0
            if "OPERATINGINCOME" in fin.index:
                op_inc = fin.loc["OPERATINGINCOME"].dropna().values
                if len(op_inc) >= 2:
                    margin_boost = (op_inc[0] / rev[0]) - (op_inc[1] / rev[1])

            return {
                "ticker": ticker, "price": p_now, "mcap": mcap,
                "yoy": g0, "accel": accel, "margin_boost": margin_boost,
                "dist_high": dist_high
            }
        except: return None

    def run(self):
        start_time = time.time()
        tickers = self.get_tickers()
        random.shuffle(tickers)
        
        targets = tickers[:SCAN_LIMIT]
        p1_results = []
        
        print("Stage 1 Scanning...")
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
            futures = {ex.submit(self.prefilter, t): t for t in targets}
            for f in as_completed(futures):
                res = f.result()
                if res: p1_results.append(res)

        print("Stage 1 Complete. Found:", len(p1_results))
        
        final_list = []
        if p1_results:
            candidates = [x["ticker"] for x in p1_results]
            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
                futures = {ex.submit(self.analyze, t): t for t in candidates}
                for f in as_completed(futures):
                    res = f.result()
                    if res:
                        # シンプルなスコアリング
                        score = 0
                        if res["accel"] > 0: score += 10
                        if res["yoy"] > 0.4: score += 10
                        if res["dist_high"] < 0.1: score += 5
                        res["score"] = score
                        final_list.append(res)

        df = pd.DataFrame(final_list)
        if not df.empty:
            df = df.sort_values("score", ascending=False).head(10)
        
        self.notify(df, len(p1_results), len(final_list))
        print("Done.")

    def notify(self, df, s1, s2):
        out = []
        out.append("GrowthRadar v10.5 Cloud Runner")
        out.append("----------------------------")
        
        if df.empty:
            out.append("No candidates found in this run.")
        else:
            for _, r in df.iterrows():
                line = "{}: Score {} | YoY {:.0%} | MCap {:.1f}B".format(
                    r['ticker'], r['score'], r['yoy'], r['mcap']/1e9
                )
                out.append(line)

        out.append("----------------------------")
        out.append("Stats: S1_Pass={} | Final_Hits={}".format(s1, s2))
        
        full_msg = "\n".join(out)
        if WEBHOOK_URL:
            requests.post(WEBHOOK_URL, json={"content": full_msg}, timeout=10)
        else:
            print(full_msg)

if __name__ == "__main__":
    GrowthRadarCloudRunner().run()
