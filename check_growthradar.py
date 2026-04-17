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
# CONFIG (v26.1 Tenbagger Hybrid)
# =========================
WEBHOOK_URL = os.environ.get("WEBHOOK_URL_GROWTHRADAR")
MAX_WORKERS = 10
SCAN_SIZE = 1500
MIN_PRICE = 2.0
MIN_MCAP = 5e7  # $50M

HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "application/json"
}

class GrowthRadarV26:
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
        """テクニカル指標の抽出（一次選別）"""
        try:
            url = f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}?range=1y&interval=1d"
            r = self.session.get(url, timeout=6).json()
            data = r["chart"]["result"][0]
            
            # 最終取引日のチェック (ゾンビ排除)
            last_trade_ts = data["meta"].get("regularMarketTime", 0)
            if (time.time() - last_trade_ts) > 86400 * 5: # 5日以上取引なしは死亡
                return None

            close = [c for c in data["indicators"]["quote"][0]["close"] if c]
            volume = [v for v in data["indicators"]["quote"][0]["volume"] if v]

            if len(close) < 126: return None
            price = close[-1]
            if price < MIN_PRICE: return None

            # 指標計算
            m1, m3, m6 = price/close[-21]-1, price/close[-63]-1, price/close[-126]-1
            
            # --- 厳格フィルタ ---
            if m6 < 0.3 or m1 > 1.5: return None # トレンド不足 or 過熱すぎ排除
            accel = m1 - m3
            if accel < 0.05: return None # 加速なし排除
            
            volat = np.std(close[-21:]) / np.mean(close[-21:])
            if volat > 0.25: return None # 荒れすぎ排除
            
            vol_short, vol_mid, vol_long = np.mean(volume[-5:]), np.mean(volume[-21:]), np.mean(volume[-63:])
            if vol_mid < vol_long: return None # 流入衰退排除
            
            trend_smooth = np.mean(close[-10:]) / np.mean(close[-30:-10]) - 1
            if trend_smooth < 0: return None

            return {
                "ticker": ticker, "price": price, "m1": m1, "m3": m3, "m6": m6,
                "accel": accel, "vol": vol_short / (vol_mid + 1e-9), "trend": trend_smooth
            }
        except: return None

    def fetch_bulk_meta(self, tickers):
        """一次選別を通過した銘柄の正体を一括確認"""
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
        print(f"Scanning {len(batch)} symbols...")

        tech_results = []
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
            futures = {ex.submit(self.fetch_technical, t): t for t in batch}
            for f in as_completed(futures):
                r = f.result()
                if r: tech_results.append(r)

        print(f"Tech valid: {len(tech_results)}. Fetching meta...")
        
        # 二次選別: メタデータと時価総額
        valid_tickers = [tr["ticker"] for tr in tech_results]
        meta_data = self.fetch_bulk_meta(valid_tickers)
        
        final_list = []
        for tr in tech_results:
            m = meta_data.get(tr["ticker"], {"name": tr["ticker"], "mcap": 0})
            if m["mcap"] > 0 and m["mcap"] < MIN_MCAP: continue # 極小ゴミ排除
            tr.update(m)
            final_list.append(tr)

        if not final_list:
            print("No real candidates."); return

        df = pd.DataFrame(final_list)
        for col in ["m6", "accel", "trend", "vol"]:
            df[f"z_{col}"] = (df[col] - df[col].mean()) / (df[col].std() + 1e-9)

        df["score"] = df["z_m6"]*0.4 + df["z_accel"]*0.2 + df["z_trend"]*0.25 + df["z_vol"]*0.15
        top = df.sort_values("score", ascending=False).head(15)
        self.report(top, len(batch), len(df))

    def report(self, df, scanned, valid):
        msg = [f"🚀 GrowthRadar v26.1 (Tenbagger Engine)", f"Scanned: {scanned} | Valid: {valid}\n"]
        for r in df.to_dict("records"):
            mcap_str = f"${r['mcap']/1e9:.2f}B" if r['mcap'] > 0 else "N/A"
            msg.append(
                f"**{r['ticker']}** ({r['name'][:15]}) | Score:{r['score']:.2f}\n"
                f"└ Price:${r['price']:.2f} | MC:{mcap_str}\n"
                f"└ M6:{r['m6']:+.1%} | Accel:{r['accel']:.2f} | Trend:{r['trend']:.2f} | Vol:{r['vol']:.1f}x\n"
            )
        text = "\n".join(msg)
        if WEBHOOK_URL: requests.post(WEBHOOK_URL, json={"content": text})
        print(text)

if __name__ == "__main__":
    GrowthRadarV26().run()
