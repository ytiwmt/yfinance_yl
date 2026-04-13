import yfinance as yf
import pandas as pd
import requests
import json
import os

# GitHub Secretsから取得
webhook_url_yfinance = os.getenv("WEBHOOK_URL_YFINANCE")

def get_sp500_tickers():
    """WikipediaからS&P500銘柄を403回避しつつ取得"""
    url = 'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies'
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    }
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        tables = pd.read_html(response.text)
        df = tables[0]
        return [t.replace('.', '-') for t in df['Symbol'].tolist()]
    except Exception as e:
        print(f"List acquisition error: {e}")
        return []

def analyze_market():
    if not webhook_url_yfinance:
        print("Error: WEBHOOK_URL_YFINANCE is not set.")
        return

    tickers = get_sp500_tickers()
    if not tickers:
        return

    found_opportunities = []
    print(f"Scanning {len(tickers)} stocks...")

    for symbol in tickers:
        try:
            stock = yf.Ticker(symbol)
            info = stock.info
            
            # --- 利回りの取得と正規化（バグ修正箇所） ---
            raw_yield = info.get('dividendYield')
            if raw_yield is None or raw_yield == 0:
                continue
                
            # yfinanceの仕様揺れを吸収 (0.05 なら 5.0 に、5.0 ならそのままに)
            cur_yield = raw_yield * 100 if raw_yield < 1 else raw_yield
            
            # 配当性向
            payout = info.get('payoutRatio', 0) *
