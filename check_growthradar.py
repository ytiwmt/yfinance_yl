import os
import requests
import pandas as pd
import yfinance as yf
from concurrent.futures import ThreadPoolExecutor, as_completed
import random
import time

# =========================
# CONFIG (The Tenbagger Rules)
# =========================
WEBHOOK_URL = os.environ.get("WEBHOOK_URL_GROWTHRADAR")

SCAN_LIMIT = 1500
MAX_WORKERS = 8

MIN_MCAP = 150_000_000
MAX_MCAP = 2_500_000_000
MIN_PRICE = 4.0
MIN_YOY = 0.25  # 25%成長は最低ライン

# =========================
# SCANNER ENGINE
# =========================
class IgnitionScanner:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({'User-Agent': 'Mozilla/5.0'})

    def get_tickers(self):
        try:
            url = "https://datahub.io/core/nasdaq-listings/r/nasdaq-listed-symbols.csv"
            return pd.read_csv(url)["Symbol"].dropna().tolist()
        except Exception:
            # Fallback tickers in case of URL failure
            return ["NVDA", "TSLA", "CELH", "RKLB", "IONQ", "HIMS", "PLTR", "UPST"]

    def prefilter(self, ticker):
        """Phase 1: 生存確認と最低限のトレンド"""
        try:
            t = yf.Ticker(ticker, session=self.session)
            hist = t.history(period="1mo")
            if len(hist) < 15: return None

            p_now = hist["Close"].iloc[-1]
            vol_avg = hist["Volume"].mean()

            # 基本条件フィルタ
            if p_now < MIN_PRICE or vol_avg < 150_000: return None
            if p_now < hist["Close"].iloc[0]: return None

            return {"ticker": ticker, "price": p_now}
        except Exception:
            return None

    def analyze(self, ticker):
        """Phase 2: 財務の爆発力とテクニカルの完成度"""
        try:
            t = yf.Ticker(ticker, session=self.session)
            fast = t.fast_info
            mcap = fast.market_cap
            if not mcap or mcap < MIN_MCAP or mcap > MAX_MCAP: return None

            # 1. テクニカル分析
            hist = t.history(period="1y")
            if len(hist) < 200: return None
            
            p_now = hist["Close"].iloc[-1]
            high_1y = hist["Close"].max()
            dist_high = (high_1y - p_now) / high_1y
            
            # 買い集め（Accumulation/Distribution）の質
            recent = hist.tail(20)
            up_vol = recent[recent['Close'] > recent['Open']]['Volume'].sum()
            down_vol = recent[recent['Close'] <= recent['Open']]['Volume'].sum()
            acc_dist = up_vol / (down_vol + 1)

            # 2. 財務分析（利益率の改善と加速）
            fin = t.quarterly_financials
            if fin is None or fin.empty: return None
            fin.index = fin.index.str.replace(" ", "").str.upper()
            
            if "TOTALREVENUE" not in fin.index: return None
            rev = fin.loc["TOTALREVENUE"].dropna().values
            if len(rev) < 4: return None

            # 成長率とその変化
            g0 = (rev[0] - rev[1]) / rev[1] if rev[1] > 0 else 0
            g1 = (rev[1] - rev[2]) / rev[2] if rev[2] > 0 else 0
            accel = g0 - g1
            
            if g0 < MIN_YOY: return None

            # 営業利益率の改善チェック
            margin_boost = 0
            if "OPERATINGINCOME" in fin.index:
                op_inc = fin.loc["OPERATINGINCOME"].dropna().values
                if len(op_inc) >= 2:
                    m0 = op_inc[0] / rev[0]
                    m1 = op_inc[1] / rev[1]
                    margin_boost = m0 - m1

            return {
                "ticker": ticker,
                "price": p_now,
                "mcap": mcap,
                "yoy": g0,
                "accel": accel,
                "margin_boost": margin_boost,
                "dist_high": dist_high,
                "acc_dist": acc_dist
            }
        except Exception:
            return None

    def score(self, d):
        s = 0
        # 加速評価
        if d["accel"] > 0.1: s += 8
        elif d["accel"] > 0: s += 4
        
        # 利益率の改善（高評価）
        if d["margin_boost"] > 0.05: s += 6
        elif d["margin_boost"] > 0: s += 2

        # ブレイクアウト構造
        if d["dist_high"] < 0.03: s += 7
        elif d["dist_high"] < 0.1: s += 3
        
        # 出来高の質
        if d["acc_dist"] > 1.5: s += 5
        
        # 時価総額ボーナス
        if d["mcap"] < 1_000_000_000: s += 3

        return s

    def run(self):
        start_time = time.time()
        all_tickers = self.get_tickers()
        random.shuffle(all_tickers)
        
        print(f"Ignition scan started on {SCAN_LIMIT} symbols...")

        # Step 1: Prefilter
        p1_results = []
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
            futures = {ex.submit(self.prefilter, t): t for t in all_tickers[:SCAN_LIMIT]}
            for f in as_completed(futures):
                res = f.result()
                if res: p1_results.append(res)

        # Step 2: Deep Analysis
        final_results = []
        targets = [x["ticker"] for x in p1_results]
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
            futures = {ex.submit(self.analyze, t): t for t in targets}
            for f in as_completed(futures):
                res = f.result()
                if res:
                    res["score"] = self.score(res)
                    if res["score"] >= 16:
                        final_results.append(res)

        df = pd.DataFrame(final_results)
        if not df.empty:
            df = df.sort_values("score", ascending=False).head(10)
        
        self.notify(df, len(p1_results), len(final_results))
        print(f"Process took {time.time()-start_time:.1f}s")

    def notify(self, df, s1, s2):
        header = "🔥 **GrowthRadar v10.1 (The Ignition)**\n--- 利益構造の変化とブレイクアウトを捕捉 ---\n\n"
        
        body = ""
        if df.empty:
            body = "❌ 条件を満たす発火寸前銘柄は不在。"
        else:
            for _, r in df.iterrows():
                boost_icon = "⚡" if r['margin_boost'] > 0 else "➖"
                body += f"**{r['ticker']}** | Score: **{r['score']}**\n"
                body += f"売上増: {r['yoy']:.1%} (加速: {r['accel']:.1%})\n"
                body += f"利幅改善: {r['margin_boost']:.1%} {boost_icon} | 集積比: {r['acc_dist']:.1f}\n"
                body += f"高値まで: -{r['dist_high']:.1%} | MCap: {r['mcap']/1e8:.1f}億ドル\n\n"

        footer = f"
