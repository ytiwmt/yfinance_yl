import os, requests, pandas as pd, numpy as np, random, re, json, time
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

# =========================
# CONFIG
# =========================
WEBHOOK_URL = os.environ.get("WEBHOOK_URL_GROWTHRADAR")
STATE_FILE = "growth_state_v32_5.json"
SCAN_SIZE = 1500
MAX_WORKERS = 10
MIN_PRICE = 2.0
HEADERS = {"User-Agent": "Mozilla/5.0"}

# =========================
# STATE
# =========================
class State:
    def __init__(self):
        self.data = self.load()

    def load(self):
        if os.path.exists(STATE_FILE):
            try:
                return json.load(open(STATE_FILE))
            except:
                return {}
        return {}

    def save(self):
        try:
            json.dump(self.data, open(STATE_FILE, "w"))
        except Exception as e:
            print(f"[STATE SAVE ERROR] {e}")

    def update(self, ticker, score):
        if not np.isfinite(score):
            return
        hist = self.data.get(ticker, [])
        hist.append({"t": time.time(), "s": float(score)})
        self.data[ticker] = hist[-30:]

# =========================
# UNIVERSE
# =========================
def load_universe():
    symbols = []

    try:
        df = pd.read_csv("https://datahub.io/core/nasdaq-listings/r/nasdaq-listed-symbols.csv")
        symbols += df["Symbol"].tolist()
        print(f"[NASDAQ OK] {len(df)}")
    except:
        print("[NASDAQ FAIL]")

    try:
        txt = requests.get(
            "https://raw.githubusercontent.com/rreichel3/US-Stock-Symbols/main/all_tickers.txt",
            timeout=10
        ).text.split("\n")
        symbols += txt
        print(f"[GitHub OK] {len(txt)}")
    except:
        print("[GitHub FAIL]")

    clean = list(set([
        s.strip().upper()
        for s in symbols
        if isinstance(s, str) and re.match(r"^[A-Z]{1,5}$", s)
    ]))

    random.shuffle(clean)
    return clean[:SCAN_SIZE]

# =========================
# FETCH
# =========================
def fetch(session, ticker):
    try:
        url = f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}?range=1y&interval=1d"
        r = session.get(url, timeout=5).json()
        res = r["chart"]["result"][0]

        close = [c for c in res["indicators"]["quote"][0]["close"] if c is not None]
        if len(close) < 120:
            return None

        price = close[-1]
        if price < MIN_PRICE:
            return None

        def ret(a, b):
            return (a / b - 1) if b > 0 else 0

        m6 = ret(price, close[-120])
        m3 = ret(price, close[-63])
        m1 = ret(price, close[-21])

        ma10 = np.mean(close[-10:])
        ma30 = np.mean(close[-30:])
        trend = ret(ma10, ma30)

        accel = m1 - (m3 / 3)

        score = (0.3 * m6) + (0.3 * trend) + (0.4 * accel)

        return {"ticker": ticker, "score": score, "price": price}

    except:
        return None

# =========================
# DETECTOR（モメンタム版）
# =========================
def detect(state):
    early = []
    expansion = []
    expansion_strict = []

    for t, hist in state.data.items():
        if len(hist) < 3:
            continue

        scores = [h["s"] for h in hist]
        latest = scores[-1]
        prev = scores[-2]
        avg = np.mean(scores)

        momentum = latest - prev

        # -----------------
        # EARLY（今上がってる）
        # -----------------
        if momentum > 0.03:
            early.append({
                "ticker": t,
                "score": latest,
                "momentum": momentum
            })

        # -----------------
        # EXPANSION（強い＋上昇）
        # -----------------
        if latest > 0.65 and momentum > 0.02:
            expansion.append({
                "ticker": t,
                "score": latest,
                "momentum": momentum
            })

        # -----------------
        # STRONG（高強度）
        # -----------------
        if latest > 0.80 and avg > 0.60:
            expansion_strict.append({
                "ticker": t,
                "score": latest,
                "momentum": momentum
            })

    early = sorted(early, key=lambda x: x["momentum"], reverse=True)
    expansion = sorted(expansion, key=lambda x: x["score"], reverse=True)
    expansion_strict = sorted(expansion_strict, key=lambda x: x["score"], reverse=True)

    return early, expansion, expansion_strict

# =========================
# REPORT
# =========================
def report(early, expansion, expansion_strict, scanned, valid):
    now = datetime.now().strftime("%Y-%m-%d %H:%M")

    msg = [
        f"🚀 GrowthRadar v32.5",
        f"Scanned:{scanned} Valid:{valid}",
        f"EARLY:{len(early)} EXP:{len(expansion)} STRONG:{len(expansion_strict)}",
        f"Time:{now}",
        ""
    ]

    msg.append("🔥 EARLY (上昇検知)")
    for c in early[:10]:
        msg.append(f"{c['ticker']} S:{c['score']:.2f} M:{c['momentum']:.3f}")

    msg.append("\n🚀 EXPANSION (強い+上昇)")
    for c in expansion[:10]:
        msg.append(f"{c['ticker']} S:{c['score']:.2f} M:{c['momentum']:.3f}")

    msg.append("\n💎 STRONG (高強度)")
    for c in expansion_strict[:5]:
        msg.append(f"{c['ticker']} S:{c['score']:.2f} M:{c['momentum']:.3f}")

    text = "\n".join(msg)
    print(text)

    if WEBHOOK_URL:
        try:
            requests.post(WEBHOOK_URL, json={"content": text[:1900]})
        except:
            pass

# =========================
# MAIN
# =========================
def run():
    session = requests.Session()
    session.headers.update(HEADERS)

    state = State()
    universe = load_universe()

    print(f"Scanning {len(universe)} tickers...")

    raw = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futures = {ex.submit(fetch, session, t): t for t in universe}
        for f in as_completed(futures):
            res = f.result()
            if res:
                raw.append(res)

    if not raw:
        print("NO DATA")
        return

    # 相対スコア化（維持）
    df = pd.DataFrame(raw)
    df["score"] = df["score"].rank(pct=True)

    for _, r in df.iterrows():
        state.update(r["ticker"], r["score"])

    state.save()

    early, expansion, expansion_strict = detect(state)

    report(early, expansion, expansion_strict, len(universe), len(df))


if __name__ == "__main__":
    run()
