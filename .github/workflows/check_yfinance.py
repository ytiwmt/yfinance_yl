import yfinance as yf
import pandas as pd
import requests
import json
import os  # 環境変数取得用

# GitHubにURLを晒さないよう、環境変数から読み込む
# Render等の設定画面で 'WEBHOOK_URL_YFINANCE' をキーとしてURLを登録してください
webhook_url_yfinance = os.getenv("WEBHOOK_URL_YFINANCE")

def get_sp500_tickers():
    """WikipediaからS&P500のティッカーリストを自動取得"""
    try:
        url = 'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies'
        tables = pd.read_html(url)
        df = tables[0]
        return [t.replace('.', '-') for t in df['Symbol'].tolist()]
    except Exception as e:
        print(f"List acquisition error: {e}")
        return []

def analyze_market():
    if not webhook_url_yfinance:
        print("Error: WEBHOOK_URL_YFINANCE is not set in environment variables.")
        return

    tickers = get_sp500_tickers()
    found_opportunities = []

    print(f"Scanning {len(tickers)} stocks...")

    for symbol in tickers:
        try:
            stock = yf.Ticker(symbol)
            info = stock.info
            
            # 利回り3%以上、配当性向80%以下を足切りラインに設定
            cur_yield = info.get('dividendYield', 0) * 100
            payout = info.get('payoutRatio', 0) * 100
            
            if cur_yield < 3.0 or payout > 80 or payout <= 0:
                continue

            # 過去平均との比較（2年）
            hist = stock.history(period="2y")
            if hist.empty:
                continue
                
            avg_price = hist['Close'].mean()
            annual_div = info.get('trailingAnnualDividendRate', 0)
            avg_yield = (annual_div / avg_price) * 100 if avg_price > 0 else 0
            
            # バグ検知：現在の利回りが過去平均より20%以上高い場合
            if cur_yield > (avg_yield * 1.2):
                found_opportunities.append({
                    "Symbol": symbol,
                    "Yield": cur_yield,
                    "AvgYield": avg_yield,
                    "Payout": payout
                })

        except:
            continue

    # 通知処理
    top_deals = sorted(found_opportunities, key=lambda x: x['Yield'], reverse=True)[:10]
    
    if top_deals:
        msg = "【米国株・流動的バグ検知レポート】\n"
        for d in top_deals:
            msg += f"✅ **{d['Symbol']}**: 利回り{d['Yield']:.2f}% (平均{d['AvgYield']:.2f}%) / 配当性向{d['Payout']:.1f}%\n"
        send_discord_message(msg)
    else:
        print("No opportunities found today.")

def send_discord_message(content):
    payload = {"content": content}
    headers = {"Content-Type": "application/json"}
    requests.post(webhook_url_yfinance, data=json.dumps(payload), headers=headers)

if __name__ == "__main__":
    analyze_market()
