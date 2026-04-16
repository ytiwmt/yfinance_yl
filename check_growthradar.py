import os
import requests
import pandas as pd
import numpy as np
import random
import time
import json
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

# =========================
# CONFIG (v24.1 Ironclad Full)
# =========================
WEBHOOK_URL = os.environ.get("WEBHOOK_URL_GROWTHRADAR")
UNIVERSE_FILE = "universe_v24.json"
MAX_WORKERS = 12

# フィルタ基準
MIN_PRICE = 1.0
MIN_MCAP = 5e7   # 50Mドル
MAX_MCAP = 1.5e12 # 超巨大株も一応許容

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Accept": "application/json"
}

class GrowthRadarV24_1:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update(HEADERS)
        self.start_time = datetime.now()

    def load_universe(self):
        """銘柄リストを複数のソースから執拗に取得する"""
        symbols = []
        
        # 内蔵バックアップ（外部が全滅しても最低限これを回す）
        hot_list = [
            "IONQ","RKLB","ASTS","OKLO","LUNR","QUBT","RGTI","QBTS","EOSE","MAAS","BULL","UPST","TEM","MLYS","AUR","LUMN","HOOD","COIN","MARA","MSTR",
            "PLTR","SOUN","BBAI","NNE","SMR","GGE","HITI","CGC","PLUG","RUN","ENPH","TSLA","RIVN","LCID","AFRM","SOFI","SQ","PYPL","SHOP","SE",
            "NVDA","AMD","SMCI","ARM","SNOW","U","NET","CRWD","DDOG","ZS","OKTA","MDB","PATH","AHR","RDDT","ALAB","VRT","NXT","GCT","CELH"
        ]

        # 外部ソースの巡回
        sources = [
            {"url": "https://raw.githubusercontent.com/rreichel3/US-Stock-Symbols/main/all_tickers.txt", "type": "txt"},
            {"url": "https://datahub.io/core/nasdaq-listings/r/nasdaq-listed-symbols.csv", "type": "csv"},
            {"url": "https://raw.githubusercontent.com/yannick-cw/stock-tickers/master/data/tickers.json", "type": "json"}
        ]

        print("Fetching universe from external sources...")
        for src in sources:
            try:
                res = self.session.get(src["url"], timeout=10)
                if res.status_code == 200:
                    if src["type"] == "txt":
                        found = [s.strip().upper() for s in res.text.split('\n') if s.strip()]
                    elif src["type"] == "csv":
                        df = pd.read_csv(src["url"])
                        found = df["Symbol"].dropna().astype(str).tolist()
                    elif src["type"] == "json":
                        data = res.json()
                        found = [item["symbol"] if isinstance(item, dict) else item for item in data]
                    
                    symbols.extend(found)
                    print(f"Loaded {len(found)} symbols from {src['url']}")
                    if len(symbols) > 1000: break
            except Exception as e:
                print(f"Source failed ({src['url']}): {e}")

        # クリーニング（1-5文字の英字のみ）
        clean_symbols = list(set([str(s).upper().strip() for s in (symbols + hot_list) 
                                 if re.match(r"^[A-Z]{1,5}$", str(s).upper().strip())]))
        
        random.shuffle(clean_symbols)
        with open(UNIVERSE_FILE, "w") as f:
            json.dump(clean_symbols, f)
            
        return clean_symbols

    def fetch_details(self, ticker):
        """時価総額と正式名称を軽量APIで取得"""
        try:
            url = f"https://query1.finance.yahoo.com/v7/finance/quote?symbols={ticker}"
            r = self.session.get(url, timeout=3).json()
            data = r["quoteResponse"]["result"][0]
            return {
                "mcap": data.get("marketCap", 0),
                "name": data.get("longName", ticker),
                "type": data.get("quoteType", "")
            }
        except:
            return {"mcap": 0, "name": ticker, "type": ""}

    def is_noise(self, name):
        """SPACやワラントなどのノイズを検知"""
        noise_keys = ["WARRANT", "UNIT", "ACQUISITION", "RIGHTS", "REDEMPTION"]
        return any(k in name.upper() for k in noise_keys)

    def fetch(self, ticker):
        """テクニカルデータの取得と判定"""
        try:
            # チャートデータ取得
            p_url = f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}?range=1y&interval=1d"
            r = self.session.get(p_url, timeout=5)
            j = r.json()["chart"]["result"][0]
            
            close = [c for c in j["indicators"]["quote"][0]["close"] if c is not None]
            vol = [v for v in j["indicators"]["quote"][0]["volume"] if v is not None]

            if len(close) < 60: return None
            price = close[-1]
            if price < MIN_PRICE: return None

            # 指標計算
            m1 = price / close[-21] - 1
            m3 = price / close[-63] - 1 if len(close) > 63 else m1
            accel = m1 - (price / close[-10] - 1) # 直近10日比の加速
            vol_ratio = (sum(vol[-5:])/5) / (sum(vol[-21:])/21 + 1e-9)

            # 詳細情報の取得
            details = self.fetch_details(ticker)
            
            # ノイズフィルタ
            if self.is_noise(details["name"]): return None
            if details["mcap"] > 0 and (details["mcap"] < MIN_MCAP or details["mcap"] > MAX_MCAP): return None

            return {
                "ticker": ticker, 
                "name": details["name"],
                "price": price, 
                "m1": m1, 
                "accel": accel, 
                "vol": vol_ratio, 
                "mcap": details["mcap"]
            }
        except:
            return None

    def run(self):
        try:
            universe = self.load_universe()
            # 1回のリミットを1200件に設定（API制限回避と網羅性のバランス）
            batch = universe[:1200]
            print(f"Scanning {len(batch)} symbols...")

            results = []
            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
                futures = {ex.submit(self.fetch, t): t for t in batch}
                for f in as_completed(futures):
                    r = f.result()
                    if r: results.append(r)

            if not results:
                self.send_webhook(f"⚠️ **GrowthRadar v24.1**\nスキャン対象 {len(batch)} 件中、有効な銘柄が見つかりませんでした。")
                return

            df = pd.DataFrame(results)
            
            # スコアリング（加速0.5、1ヶ月モメンタム0.3、出来高0.2）
            for col in ["accel", "m1", "vol"]:
                df[f"z_{col}"] = (df[col] - df[col].mean()) / (df[col].std() + 1e-9)

            df["score"] = (df["z_accel"] * 0.5 + df["z_m1"] * 0.3 + df["z_vol"] * 0.2)
            
            top_df = df.sort_values("score", ascending=False).head(15)
            self.report(top_df, len(batch), len(df))

        except Exception as e:
            self.send_webhook(f"🚨 **GrowthRadar Fatal Error**\n`{str(e)}`")

    def report(self, df, scanned, valid):
        now = datetime.now().strftime("%Y/%m/%d %H:%M")
        msg = [
            f"🚀 **GrowthRadar v24.1 (Ironclad)**",
            f"Universe: {scanned} | Valid: {valid} | {now}\n"
        ]

        for r in df.to_dict("records"):
            mcap_str = f"${r['mcap']/1e9:.2f}B" if r['mcap'] > 0 else "N/A"
            msg.append(
                f"**{r['ticker']}** ({r['name'][:15]}) | **Score: {r['score']:.2f}**\n"
                f"└ Price: ${r['price']:.2f} | MC: {mcap_str}\n"
                f"└ M1: {r['m1']:+.1%} | Accel: {r['accel']:+.2f} | Vol: {r['vol']:.1f}x"
            )

        self.send_webhook("\n".join(msg))

    def send_webhook(self, text):
        if WEBHOOK_URL:
            try: requests.post(WEBHOOK_URL, json={"content": text}, timeout=10)
            except: pass
        print(text)

if __name__ == "__main__":
    GrowthRadarV24_1().run()
