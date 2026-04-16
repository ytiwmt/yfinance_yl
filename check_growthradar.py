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
# CONFIG (v15.1 Noise Filtered)
# =========================
WEBHOOK_URL = os.environ.get("WEBHOOK_URL_GROWTHRADAR")

MAX_WORKERS = 10
SCAN_LIMIT = 2500

# ノイズ除去用の物理閾値
MIN_PRICE = 3.0           # ペニーストック除外
MIN_MARKET_CAP = 50_000_000  # 50Mドル以下の超小型株を除外
MIN_VOLUME_USD = 500_000  # 1日の売買代金（価格×出来高）が最低50万ドル以上

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

class GrowthRadarV15_1:

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"})

    def get_universe(self):
        core = [
            "PLTR","NVDA","RKLB","ASTS","OKLO","HIMS","CELH","UPST",
            "COIN","HOOD","SMCI","RDDT","LUNR","IONQ","APP","SOUN",
            "DUOL","MSTR","TSLA","MARA","VRT","SERV","NNE","BROS"
        ]
        try:
            # 安定したリストソースを使用
            url = "https://raw.githubusercontent.com/rreichel3/US-Stock-Symbols/main/all_tickers.txt"
            res = self.session.get(url, timeout=10)
            if res.status_code == 200:
                full_list = [t.strip() for t in res.text.split('\n') if t.strip() and len(t.strip()) <= 5]
                random.shuffle(full_list)
                return list(dict.fromkeys(core + full_list[:SCAN_LIMIT]))
            return core
        except:
            return core

    def analyze(self, ticker):
        url = f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}?range=1y&interval=1d"
        try:
            r = self.session.get(url, timeout=12)
            if r.status_code != 200: return None
            
            res = r.json()["chart"]["result"][0]
            meta = res.get("meta", {})
            
            # 1. 通貨・市場チェック
            if meta.get("currency") != "USD": return None
            
            # 2. 物理的な時価総額フィルタ（取れる場合のみ。取れない場合は後続の統計に任せるが、極端なゴミはmeta段階で弾ける）
            mcap = meta.get("marketCap", 0)
            if mcap > 0 and mcap < MIN_MARKET_CAP: return None

            quotes = res["indicators"]["quote"][0]
            closes = [c for c in quotes["close"] if c]
            volumes = [v for v in quotes["volume"] if v]

            if len(closes) < 130: return None

            price = closes[-1]
            volume_last = volumes[-1]

            # 3. 物理価格 & 流動性フィルタ (価格 * 出来高 = 売買代金)
            if price < MIN_PRICE: return None
            if (price * volume_last) < MIN_VOLUME_USD: return None

            # 特徴量生成
            m1 = price / closes[-20] - 1
            m3 = price / closes[-60] - 1
            m6 = price / closes[-120] - 1
            m12 = price / closes[0] - 1

            accel = m1 - m3
            slope = m3 - m6

            return {
                "ticker": ticker,
                "price": price,
                "mcap": mcap,
                "m1": m1, "m3": m3, "m6": m6, "m12": m12,
                "accel": accel,
                "slope": slope
            }
        except:
            return None

    def add_zscores(self, df):
        """統計的な正規化処理"""
        def z(col):
            # ゼロ除算回避と極端な外れ値のクリッピング
            mean = df[col].mean()
            std = df[col].std() + 1e-9
            return ((df[col] - mean) / std).clip(-4, 4) # ±4以上の異常値は丸める

        df["z_accel"] = z("accel")
        df["z_slope"] = z("slope")
        df["z_m1"] = z("m1")
        df["z_m3"] = z("m3")
        df["z_m12"] = z("m12")
        return df

    def score(self, r):
        """異常値スコアリング: 重み付け調整済み"""
        return (
            0.40 * r["z_accel"] +  # 加速度を最重視
            0.20 * r["z_slope"] +  # トレンドの継続性
            0.15 * r["z_m3"] +     # 中期的な強さ
            0.15 * r["z_m1"] +     # 短期的な強さ
            0.10 * r["z_m12"]      # 長期（地盤）
        )

    def run(self):
        universe = self.get_universe()
        log.info(f"Scan Start: {len(universe)} symbols.")

        results = []
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
            futures = {ex.submit(self.analyze, t): t for t in universe}
            for f in as_completed(futures):
                r = f.result()
                if r: results.append(r)

        df = pd.DataFrame(results)

        if df.empty or len(df) < 50:
            log.warning("Insufficient data for statistical analysis.")
            return

        # 正規化とスコアリング
        df = self.add_zscores(df)
        df["score"] = df.apply(self.score, axis=1)

        # 上位2%を抽出し、さらに「プラスの異常値」のみに限定
        threshold = max(df["score"].quantile(0.98), 1.5) 
        df = df[df["score"] >= threshold]
        df = df.sort_values("score", ascending=False).head(15)

        self.notify(df, len(universe), len(results))

    def notify(self, df, total, valid):
        msg = [f"📊 **GrowthRadar v15.1 (Clean Anomaly)**", f"Universe: {total} | Filtered: {valid}\n"]

        if df.empty:
            msg.append("No high-confidence anomalies detected.")
        else:
            for r in df.to_dict("records"):
                mc_str = f"${r['mcap']/1e6:.0f}M" if r['mcap'] > 0 else "N/A"
                msg.append(
                    f"**{r['ticker']}** (Score: {r['score']:.2f})\n"
                    f"└ Price: ${r['price']:.2f} | MC: {mc_str}\n"
                    f"└ M1:{r['m1']:+.1%} | M3:{r['m3']:+.1%} | Accel(z):{r['z_accel']:.2f}"
                )

        out = "\n".join(msg)
        if WEBHOOK_URL:
            requests.post(WEBHOOK_URL, json={"content": out}, timeout=10)
        else:
            print(out)

if __name__ == "__main__":
    GrowthRadarV15_1().run()
