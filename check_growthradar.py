import os
import requests
import pandas as pd
import numpy as np
import random
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

# =========================
# CONFIG (v26.4 Ultimate Edition)
# =========================
WEBHOOK_URL = os.environ.get("WEBHOOK_URL_GROWTHRADAR")
MAX_WORKERS = 10
SCAN_SIZE = 1500  # 宇宙から抽出するサンプル数
MIN_PRICE = 2.0
MIN_MCAP = 5e7    # $50M (マイクロキャップ以上)
MIN_AVG_VOL_VAL = 5e5 # 1日の平均売買代金 $500,000 (流動性確保)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept": "application/json"
}

class GrowthRadarV26_4:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update(HEADERS)

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
            except: pass
        
        clean = list(set([str(s).strip().upper() for s in symbols if isinstance(s, str) and re.match(r"^[A-Z]{1,5}$", str(s).strip())]))
        random.shuffle(clean)
        return clean

    def fetch_technical(self, ticker):
        """テクニカル指標の抽出"""
        try:
            url = f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}?range=1y&interval=1d"
            r = self.session.get(url, timeout=6).json()
            res = r["chart"]["result"][0]
            
            # ゾンビ排除 (最終取引が5日以上前ならスキップ)
            if (time.time() - res["meta"].get("regularMarketTime", 0)) > 86400 * 5:
                return None

            close = [c for c in res["indicators"]["quote"][0]["close"] if c]
            volume = [v for v in res["indicators"]["quote"][0]["volume"] if v]
            if len(close) < 126: return None

            price = close[-1]
            if price < MIN_PRICE: return None

            # 平均売買代金のチェック
            avg_vol_val = np.mean(close[-21:]) * np.mean(volume[-21:])
            if avg_vol_val < MIN_AVG_VOL_VAL: return None

            # リターン計算
            m1, m3, m6 = price/close[-21]-1, price/close[-63]-1, price/close[-126]-1
            
            # 基本フィルタ (爆騰しすぎ排除 & 長期上昇トレンド必須)
            if m6 < 0.3 or m1 > 1.5: return None
            
            # ボラティリティ制限 (荒すぎる銘柄はTenbaggerの器ではない)
            volat = np.std(close[-21:]) / np.mean(close[-21:])
            if volat > 0.25: return None

            return {
                "ticker": ticker, "price": price, "m6": m6, "accel": m1 - m3,
                "trend": np.mean(close[-10:]) / np.mean(close[-30:-10]) - 1,
                "vol_short": np.mean(volume[-5:]),
                "vol_mid": np.mean(volume[-21:]),
                "vol_long": np.mean(volume[-63:])
            }
        except: return None

    def fetch_meta(self, tickers):
        """メタデータを一括取得"""
        meta = {}
        if not tickers: return meta
        try:
            for i in range(0, len(tickers), 100):
                chunk = tickers[i:i+100]
                url = f"https://query1.finance.yahoo.com/v7/finance/quote?symbols={','.join(chunk)}"
                r = self.session.get(url, timeout=10).json()
                for res in r.get("quoteResponse", {}).get("result", []):
                    meta[res["symbol"]] = {
                        "name": res.get("longName", res.get("shortName", res["symbol"])),
                        "mcap": res.get("marketCap", 0)
                    }
        except: pass
        return meta

    def run(self):
        universe = self.load_universe()
        batch = universe[:SCAN_SIZE]
        print(f"📡 Scanning {len(batch)} symbols...")

        raw_results = []
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
            futures = {ex.submit(self.fetch_technical, t): t for t in batch}
            for f in as_completed(futures):
                r = f.result()
                if r: raw_results.append(r)

        if not raw_results:
            print("No candidates found."); return

        # メタデータ統合 & 時価総額フィルタ
        meta_map = self.fetch_meta([r["ticker"] for r in raw_results])
        data = []
        for r in raw_results:
            m = meta_map.get(r["ticker"], {"name": r["ticker"], "mcap": 0})
            if 0 < m["mcap"] < MIN_MCAP: continue
            r.update(m)
            data.append(r)

        df = pd.DataFrame(data)
        if df.empty: return

        # 層別フィルタリング
        # Tier1: 非常に強い加速と出来高の伴う上昇
        df_strict = df[
            (df["accel"] >= 0.20) & (df["trend"] >= 0.20) &
            (df["vol_mid"] >= df["vol_long"] * 1.3) & (df["vol_short"] >= df["vol_mid"] * 0.9)
        ].copy()

        # Tier2: 良好なトレンドを維持している監視対象
        df_loose = df[
            (df["accel"] >= 0.15) & (df["trend"] >= 0.10) &
            (df["vol_mid"] >= df["vol_long"] * 1.1)
        ].copy()

        def apply_scoring(target_df):
            if target_df.empty: return target_df
            target_df["vol_ratio"] = target_df["vol_short"] / (target_df["vol_mid"] + 1e-9)
            # 順位によるスコアリング (0.0 - 1.0)
            target_df["score"] = (
                target_df["m6"].rank(pct=True) * 0.40 +
                target_df["accel"].rank(pct=True) * 0.20 +
                target_df["trend"].rank(pct=True) * 0.25 +
                target_df["vol_ratio"].rank(pct=True) * 0.15
            )
            return target_df.sort_values("score", ascending=False)

        t1 = apply_scoring(df_strict)
        t2 = apply_scoring(df_loose)

        self.report(t1, t2, len(batch), len(df))

    def report(self, t1, t2, scanned, valid):
        now = datetime.now().strftime("%Y-%m-%d %H:%M")
        msg = [
            f"🚀 **GrowthRadar v26.4 Ultimate**",
            f"Scanned:{scanned} | Base:{valid} | Tier1:{len(t1)} | {now}\n",
            "🏆 **Tier 1 (High Conviction)**"
        ]

        for r in t1.head(10).to_dict("records"):
            mcap = f"${r['mcap']/1e9:.1f}B" if r['mcap'] > 0 else "N/A"
            msg.append(f"**{r['ticker']}** ({r['name'][:18]}) | S:{r['score']:.2f}\n└ P:${r['price']:.2f} | MC:{mcap} | M6:{r['m6']:+.0%} | T:{r['trend']:.2f}")

        msg.append("\n👀 **Tier 2 (Watchlist)**")
        for r in t2.head(10).to_dict("records"):
            msg.append(f"**{r['ticker']}** | S:{r['score']:.2f} | P:${r['price']:.2f} | M6:{r['m6']:+.0%}")

        text = "\n".join(msg)
        if WEBHOOK_URL:
            requests.post(WEBHOOK_URL, json={"content": text})
        print(text)

if __name__ == "__main__":
    GrowthRadarV26_4().run()
