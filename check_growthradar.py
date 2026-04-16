import os
import requests
import pandas as pd
import yfinance as yf
from concurrent.futures import ThreadPoolExecutor, as_completed
import random
import time
from io import StringIO

# =========================
# CONFIG (GitHub Actions Survival Mode)
# =========================
WEBHOOK_URL = os.environ.get("WEBHOOK_URL_GROWTHRADAR")

SCAN_LIMIT = 500   
MAX_WORKERS = 2    # レート制限を考慮

class GrowthRadarBruteForce:
    def __init__(self):
        self.session = requests.Session()
        self.agents = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
            'Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Mobile/15E148 Safari/604.1'
        ]

    def update_headers(self):
        self.session.headers.update({
            'User-Agent': random.choice(self.agents),
            'Accept': '*/*',
            'Referer': 'https://finance.yahoo.com/'
        })

    def get_tickers(self):
        # 確実にヒットさせたい注目銘柄
        core_growth = [
            "PLTR", "NVDA", "CELH", "RKLB", "IONQ", "HIMS", "UPST", "DUOL", 
            "APP", "SOUN", "BROS", "MSTR", "HOOD", "VRT", "SMCI", "RDDT", 
            "LUNR", "OKLO", "ASTS", "SERV", "NNE", "TSLA", "MARA", "COIN"
        ]
        try:
            url = "https://datahub.io/core/nasdaq-listings/r/nasdaq-listed-symbols.csv"
            res = self.session.get(url, timeout=5)
            if res.status_code == 200:
                df = pd.read_csv(StringIO(res.text))
                nasdaq = df["Symbol"].dropna().tolist()
                return list(set(core_growth + nasdaq))
            return core_growth
        except:
            return core_growth

    def analyze(self, ticker):
        """極限まで条件を緩めた解析ロジック"""
        try:
            self.update_headers()
            time.sleep(random.uniform(1.0, 2.0))
            
            t = yf.Ticker(ticker, session=self.session)
            
            # ヒストリカルデータ取得 (これが一番確実)
            hist = t.history(period="6mo")
            if hist.empty or len(hist) < 20:
                return None
            
            p_now = hist["Close"].iloc[-1]
            p_prev = hist["Close"].iloc[-2]
            p_start = hist["Close"].iloc[0]
            
            # モメンタム計算 (6ヶ月騰落率)
            momentum = (p_now - p_start) / p_start
            
            # 出来高の活況度
            vol_now = hist["Volume"].iloc[-1]
            vol_avg = hist["Volume"].mean()
            vol_ratio = vol_now / vol_avg if vol_avg > 0 else 1.0

            # 財務データ（オプション）
            yoy = 0
            try:
                # 取得を試みるが、失敗しても無視
                fin = t.quarterly_financials
                if fin is not None and not fin.empty:
                    fin.index = fin.index.str.replace(" ", "").str.upper()
                    rev_key = next((k for k in ["TOTALREVENUE", "REVENUE"] if k in fin.index), None)
                    if rev_key:
                        rev = fin.loc[rev_key].dropna().values
                        if len(rev) >= 2:
                            yoy = (rev[0] - rev[1]) / rev[1] if rev[1] > 0 else 0
            except:
                pass

            # スコアリング: モメンタム重視
            score = 0
            if momentum > 0.5: score += 10 # 6ヶ月で50%以上上昇
            elif momentum > 0.2: score += 5
            
            if vol_ratio > 1.5: score += 5 # 出来高急増
            if yoy > 0.2: score += 10       # 20%以上の成長
            elif yoy > 0: score += 5

            # 1日でもプラスなら生存点
            if p_now > p_prev: score += 2

            return {
                "ticker": ticker,
                "price": round(p_now, 2),
                "momentum": momentum,
                "yoy": yoy,
                "score": score
            }
        except:
            return None

    def run(self):
        start_time = time.time()
        tickers = self.get_tickers()
        random.shuffle(tickers)
        
        targets = tickers[:SCAN_LIMIT]
        print(f"[*] 解析開始: {len(targets)} 銘柄")
        
        results = []
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
            futures = {ex.submit(self.analyze, t): t for t in targets}
            for f in as_completed(futures):
                res = f.result()
                if res:
                    results.append(res)

        df = pd.DataFrame(results)
        if not df.empty:
            # とにかくスコアが高い順に15件出す
            df = df.sort_values("score", ascending=False).head(15)
        
        self.notify(df, len(targets), len(results))
        print(f"[*] 完了: {time.time() - start_time:.1f}s")

    def notify(self, df, total, valid):
        msg = []
        msg.append("🚀 **GrowthRadar v10.8 Brute Force**")
        msg.append("----------------------------")
        
        if df.empty:
            msg.append("⚠️ データ取得エラーまたは対象なし")
        else:
            for _, r in df.iterrows():
                yoy_str = "{:.0%}".format(r['yoy']) if r['yoy'] != 0 else "N/A"
                mom_str = "{:+.0%}".format(r['momentum'])
                line = "**{}** | Score: {} | Mom: {} | YoY: {} | ${}".format(
                    r['ticker'], r['score'], mom_str, yoy_str, r['price']
                )
                msg.append(line)

        msg.append("----------------------------")
        msg.append("Stats: Scanned={} | Valid_Data={}".format(total, valid))
        
        full_msg = "\n".join(msg)
        if WEBHOOK_URL:
            try: requests.post(WEBHOOK_URL, json={"content": full_msg}, timeout=15)
            except: print("Webhook Error")
        else:
            print(full_msg)

if __name__ == "__main__":
    GrowthRadarBruteForce().run()
