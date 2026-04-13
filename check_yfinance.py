import yfinance as yf
import pandas as pd
import requests
import json
import os
import numpy as np

webhook_url_yfinance = os.getenv("WEBHOOK_URL_YFINANCE")

# -----------------------------
# S&P500取得
# -----------------------------
def get_sp500_tickers():
    url = 'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies'
    headers = {"User-Agent": "Mozilla/5.0"}
    try:
        response = requests.get(url, headers=headers)
        table = pd.read_html(response.text)[0]
        return [t.replace('.', '-') for t in table['Symbol'].tolist()]
    except:
        return []

# -----------------------------
# 利回り統計
# -----------------------------
def calc_stats(stock, div_rate):
    hist = stock.history(period="2y")

    if hist.empty:
        return None

    prices = hist['Close'].dropna()

    if len(prices) < 100:
        return None

    yields = (div_rate / prices) * 100

    mean = yields.mean()
    std = yields.std()

    if std == 0 or np.isnan(std):
        return None

    cur_price = prices.iloc[-1]
    cur_yield = (div_rate / cur_price) * 100
    z = (cur_yield - mean) / std

    return cur_yield, mean, z

# -----------------------------
# FCF取得
# -----------------------------
def get_fcf(stock):
    try:
        cf = stock.cashflow
        if cf is None or cf.empty:
            return None

        op_cf = None
        capex = None

        for label in cf.index:
            if "Operating" in label:
                op_cf = cf.loc[label].iloc[0]
            if "Capital" in label:
                capex = cf.loc[label].iloc[0]

        if op_cf is None or capex is None:
            return None

        return op_cf + capex

    except:
        return None

# -----------------------------
# メイン
# -----------------------------
def analyze_market():
    if not webhook_url_yfinance:
        return

    tickers = get_sp500_tickers()

    income_dislocation = []
    quality_discount = []

    for symbol in tickers:
        try:
            stock = yf.Ticker(symbol)
            info = stock.info

            price = info.get('currentPrice') or info.get('regularMarketPrice')
            div_rate = info.get('trailingAnnualDividendRate') or info.get('dividendRate')

            if not price or not div_rate or div_rate <= 0:
                continue

            stats = calc_stats(stock, div_rate)
            if not stats:
                continue

            cur_yield, avg_yield, z = stats
            delta = cur_yield - avg_yield

            payout = info.get('payoutRatio')
            debt = info.get('totalDebt')
            ebitda = info.get('ebitda')
            shares = info.get('sharesOutstanding')
            fcf = get_fcf(stock)

            # =========================
            # ① インカム異常
            # =========================
            if cur_yield > 4 and z > 1.2:

                if payout and payout > 0.8:
                    continue

                if fcf and shares:
                    total_div = div_rate * shares
                    if fcf < total_div * 0.8:
                        continue

                if debt and ebitda and ebitda > 0:
                    if debt / ebitda > 4:
                        continue

                income_dislocation.append({
                    "Symbol": symbol,
                    "Yield": f"{cur_yield:.2f}%",
                    "Avg": f"{avg_yield:.2f}%",
                    "Z": f"{z:.2f}"
                })

            # =========================
            # ② クオリティ・ディスカウント（修正版）
            # =========================

            # 排他性：高配当は除外（HPQ重複防止）
            if cur_yield > 4:
                continue

            # 最低利回り
            if cur_yield < 2:
                continue

            # 絶対差フィルタ（強化版）
            if delta < 1.0:
                continue

            # 相対乖離
            if avg_yield > 0:
                ratio = cur_yield / avg_yield
            else:
                continue

            if ratio > 1.4 and z > 1.0:

                if payout and payout < 0.6:

                    rev_growth = info.get('revenueGrowth')
                    if rev_growth and rev_growth > 0:

                        if debt and ebitda and ebitda > 0:
                            if debt / ebitda < 3:
                                quality_discount.append({
                                    "Symbol": symbol,
                                    "Yield": f"{cur_yield:.2f}%",
                                    "Avg": f"{avg_yield:.2f}%",
                                    "Z": f"{z:.2f}"
                                })
                        else:
                            quality_discount.append({
                                "Symbol": symbol,
                                "Yield": f"{cur_yield:.2f}%",
                                "Avg": f"{avg_yield:.2f}%",
                                "Z": f"{z:.2f}"
                            })

        except:
            continue

    # ソート
    income_dislocation = sorted(
        income_dislocation,
        key=lambda x: float(x['Yield'][:-1]),
        reverse=True
    )[:3]

    quality_discount = sorted(
        quality_discount,
        key=lambda x: float(x['Z']),
        reverse=True
    )[:3]

    send_notification(income_dislocation, quality_discount)

# -----------------------------
# 通知
# -----------------------------
def send_notification(income, quality):
    if not income and not quality:
        payload = {"content": "📡 検知なし（市場平常）"}
    else:
        embeds = []

        for d in income:
            embeds.append({
                "title": f"🔥 インカム異常: {d['Symbol']}",
                "color": 15158332,
                "fields": [
                    {"name": "利回り", "value": d['Yield'], "inline": True},
                    {"name": "平均", "value": d['Avg'], "inline": True},
                    {"name": "Z", "value": d['Z'], "inline": True}
                ]
            })

        for d in quality:
            embeds.append({
                "title": f"💎 クオリティ・ディスカウント: {d['Symbol']}",
                "color": 3447003,
                "fields": [
                    {"name": "利回り", "value": d['Yield'], "inline": True},
                    {"name": "平均", "value": d['Avg'], "inline": True},
                    {"name": "Z", "value": d['Z'], "inline": True}
                ]
            })

        payload = {
            "content": "📊 デュアル検知レポート",
            "embeds": embeds
        }

    requests.post(
        webhook_url_yfinance,
        data=json.dumps(payload),
        headers={"Content-Type": "application/json"}
    )

# -----------------------------
# 実行
# -----------------------------
if __name__ == "__main__":
    analyze_market()
