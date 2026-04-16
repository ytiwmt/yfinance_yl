import os
import requests
import pandas as pd
import numpy as np
import random
import time
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from io import StringIO

# =========================
# 設定 (不確実性への対応)
# =========================
WEBHOOK_URL = os.environ.get("WEBHOOK_URL_GROWTHRADAR")

# システムリソース・レート制限対策
MAX_WORKERS = 10 
SCAN_LIMIT = 2500  # 負荷分散のため

# フィルタリング基準
MIN_PRICE = 2.0
MIN_MARKET_CAP = 30_000_000
MIN_VOLUME_USD = 500_000

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

class GrowthRadarStabilized:
    """
    15回以上の修正を経て到達した、エラー耐性重視の分析スクリプト。
    """
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0",
            "Accept": "application/json"
        })

    def get_ticker_list(self):
        """銘柄取得の多重化（単一障害点の回避）"""
        sources = [
            "https://raw.githubusercontent.com/rreichel3/US-Stock-Symbols/main/all_tickers.txt",
            "https://datahub.io/core/nasdaq-listed-symbols/r/nasdaq-listed-symbols.csv"
        ]
        # バックアップ用コア銘柄
        core_list = ["PLTR","NVDA","RKLB","ASTS","OKLO","UPST","COIN","SMCI","RDDT","LUNR","MSTR","TSLA","VRT"]

        for url in sources:
            try:
                r = self.session.get(url, timeout=10)
                if r.status_code == 200:
                    if ".csv" in url:
                        df = pd.read_csv(StringIO(r.text))
                        tickers = df["Symbol"].dropna().astype(str).tolist()
                    else:
                        tickers = [t.strip() for t in r.text.split('\n') if t.strip()]
                    
                    clean_tickers = [t for t in tickers if t.isalpha() and 1 <= len(t) <= 5]
                    if len(clean_tickers) > 100:
                        log.info(f"Source success: {url}")
                        random.shuffle(clean_tickers)
                        return list(set(core_list + clean_tickers))[:SCAN_LIMIT]
            except Exception as e:
                log.warning(f"Source failed {url}: {e}")
        
        return core_list

    def fetch_analysis(self, ticker):
        """単一銘柄の取得とスコアリング (物理的例外のトラップ)"""
        url = f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}?range=1y&interval=1d"
        try:
            time.sleep(random.uniform(0.05, 0.1)) # レート制限対策
            r = self.session.get(url, timeout=10)
            if r.status_code != 200: return None
            
            data = r.json()["chart"]["result"][0]
            meta = data.get("meta", {})
            mcap = meta.get("marketCap", 0)
            
            # 初期フィルタ
            if mcap > 0 and mcap < MIN_MARKET_CAP: return None

            quote = data["indicators"]["quote"][0]
            adj_close = data.get("indicators", {}).get("adjclose", [{}])[0].get("adjclose", [])
            closes = adj_close if adj_close else quote.get("close", [])
            volumes = quote.get("volume", [])

            # Noneの除去とデータ長チェック
            closes = [c for c in closes if c is not None]
            if len(closes) < 120: return None

            price = closes[-1]
            if price < MIN_PRICE: return None
            
            # 売買代金チェック
            avg_vol = np.mean(volumes[-5:]) if volumes else 0
            if (price * avg_vol) < MIN_VOLUME_USD: return None

            # 特徴量計算
            returns = np.diff(np.log(closes))
            m1 = price / closes[-20] - 1
            m3 = price / closes[-60] - 1
            volatility = np.std(returns[-20:]) * np.sqrt(252)

            return {
                "ticker": ticker,
                "price": price,
                "mcap": mcap,
                "m1": m1,
                "m3": m3,
                "accel": m1 - m3,
                "vol": volatility
            }
        except:
            return None

    def run(self):
        tickers = self.get_ticker_list()
        log.info(f"Start scanning {len(tickers)} tickers...")

        results = []
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            future_to_ticker = {executor.submit(self.fetch_analysis, t): t for t in tickers}
            for future in as_completed(future_to_ticker):
                res = future.result()
                if res: results.append(res)

        if len(results) < 5:
            log.error("Insufficient data points collected.")
            return

        df = pd.DataFrame(results)
        
        # 統計的異常値の算出 (Z-Score)
        for col in ["accel", "m1", "m3"]:
            df[f"z_{col}"] = (df[col] - df[col].mean()) / df[col].std()

        # 総合スコア（モメンタム加速 + 順張り適性）
        df["score"] = (df["z_accel"] * 0.5) + (df["z_m1"] * 0.3) + (df["z_m3"] * 0.2)
        
        top_picks = df.sort_values("score", ascending=False).head(12)
        self.send_notification(top_picks)

    def send_notification(self, df):
        summary = [f"📊 **GrowthRadar Stability Scan** ({len(df)} picks)\n"]
        for _, r in df.iterrows():
            summary.append(
                f"**{r['ticker']}** | Score: {r['score']:.2f}\n"
                f"└ Price: ${r['price']:.2f} | M1: {r['m1']:.1%} | Accel(z): {r['z_accel']:.2f}"
            )
        
        content = "\n".join(summary)
        if WEBHOOK_URL:
            requests.post(WEBHOOK_URL, json={"content": content})
        else:
            print(content)

if __name__ == "__main__":
    GrowthRadarStabilized().run()
