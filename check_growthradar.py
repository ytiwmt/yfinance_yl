import os
import requests
import pandas as pd
import random
import time
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from io import StringIO
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

# =========================
# CONFIG (v13.4 Deep Discovery)
# =========================
WEBHOOK_URL = os.environ.get("WEBHOOK_URL_GROWTHRADAR")

# 1回でスキャンする最大数。3000件あれば市場の主要成長株をほぼカバー可能
SCAN_LIMIT = 3000
MAX_WORKERS = 8  # ネットワークI/O待ちが多いため、少し多めに設定

# フィルタ条件
MIN_PRICE = 2.0
MAX_PRICE = 1000.0
MIN_MCAP = 50_000_000      # 5000万ドル（超小型も含める）
MAX_MCAP = 10_000_000_000  # 100億ドル（中堅まで）

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

class GrowthRadarV13_4:
    def __init__(self):
        self.session = requests.Session()
        # リトライ戦略の設定
        retry_strategy = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("https://", adapter)
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
            "Accept": "application/json"
        })

    def get_universe(self):
        """
        NASDAQ/NYSE等から母集団を形成。
        ノイズ（Warrant, Unit, ETF等）を可能な限り排除。
        """
        core = ["PLTR","NVDA","RKLB","ASTS","OKLO","HIMS","CELH","UPST","COIN","HOOD","SMCI","RDDT","LUNR","IONQ","APP","SOUN","DUOL","MSTR","TSLA","MARA"]
        try:
            url = "https://datahub.io/core/nasdaq-listed-symbols/r/nasdaq-listed-symbols.csv"
            df = pd.read_csv(StringIO(self.session.get(url).text))
            df = df.dropna(subset=['Symbol'])
            
            # 優先株、ワラント、ユニット、テスト銘柄を除外
            # 通常、成長株は末尾に記号がつかない（例: TSLA, AAPL）
            df = df[~df["Symbol"].str.contains(r"[\$\.\-\=]", na=False)]
            # NASDAQの慣習：末尾Wはワラント、Uはユニット等
            df = df[~df["Symbol"].str.endswith(('W', 'U', 'R', 'Z'))]
            
            universe = df["Symbol"].tolist()
            random.shuffle(universe)
            
            merged = list(dict.fromkeys(core + universe))
            return merged[:SCAN_LIMIT]
        except Exception as e:
            log.warning(f"Universe creation failed: {e}")
            return core

    def fetch_json(self, url):
        try:
            r = self.session.get(url, timeout=12)
            return r.json() if r.status_code == 200 else None
        except:
            return None

    def analyze(self, ticker):
        """軽量・高速解析: 1銘柄あたり最大2リクエスト"""
        p_url = f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}?range=1y&interval=1d"
        f_url = f"https://query1.finance.yahoo.com/v10/finance/quoteSummary/{ticker}?modules=financialData,defaultKeyStatistics"

        p_data = self.fetch_json(p_url)
        if not p_data: return None

        try:
            res = p_data["chart"]["result"][0]
            closes = [c for c in res["indicators"]["quote"][0]["close"] if c is not None]
            if len(closes) < 120: return None

            price = closes[-1]
            if not (MIN_PRICE <= price <= MAX_PRICE): return None

            # モメンタム指標
            m3 = price / closes[-60] - 1
            m6 = price / closes[-120] - 1
            m12 = price / closes[0] - 1
            accel = m3 - m6

            # 財務/時価総額取得
            f_data = self.fetch_json(f_url)
            rev, mcap = 0, 0
            if f_data:
                r0 = f_data["quoteSummary"]["result"][0]
                rev = r0.get("financialData", {}).get("revenueGrowth", {}).get("raw", 0)
                mcap = r0.get("defaultKeyStatistics", {}).get("marketCap", {}).get("raw", 0)

            # 時価総額フィルタ
            if mcap > 0 and not (MIN_MCAP <= mcap <= MAX_MCAP): return None

            # スコアリングロジック (爆発力重視)
            score = 0
            if rev > 0.30: score += 10
            elif rev > 0.15: score += 5
            
            if accel > 0.20: score += 12 # 加速度が最大の武器
            elif accel > 0.05: score += 6
            
            if m3 > 0.4: score += 5      # 短期的な熱量
            if m12 > 1.0: score += 5     # 既に火がついているか
            if mcap < 2e9: score += 3     # 小型株プレミアム

            if score < 15: return None

            return {
                "ticker": ticker, "price": price, "score": score,
                "rev": rev, "mcap": mcap, "accel": accel, "m12": m12
            }
        except:
            return None

    def run(self):
        universe = self.get_universe()
        log.info(f"Initiating Wide Scan: {len(universe)} symbols.")
        
        results = []
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
            futures = {ex.submit(self.analyze, t): t for t in universe}
            for f in as_completed(futures):
                res = f.result()
                if res: results.append(res)

        df = pd.DataFrame(results)
        if not df.empty:
            df = df.sort_values("score", ascending=False).head(15)
        
        self.notify(df, len(universe), len(results))

    def notify(self, df, total, hits):
        msg = [f"🛡️ **GrowthRadar v13.4 Deep Discovery**", f"Universe: {total} | Anomalies: {hits}\n"]
        if df.empty:
            msg.append("No explosive growth setups detected today.")
        else:
            for r in df.to_dict("records"):
                rev_s = f"{r['rev']:.1%}" if r['rev'] != 0 else "N/A"
                msg.append(
                    f"**{r['ticker']}** (Score: {r['score']})\n"
                    f"└ Price: ${r['price']:.2f} | MC: {r['mcap']/1e6:.0f}M\n"
                    f"└ Rev: {rev_s} | Accel: {r['accel']:.2f} | 1Y: {r['m12']:+.0%}"
                )
        
        full_msg = "\n".join(msg)
        if WEBHOOK_URL:
            requests.post(WEBHOOK_URL, json={"content": full_msg}, timeout=10)
        else:
            print(full_msg)

if __name__ == "__main__":
    GrowthRadarV13_4().run()
