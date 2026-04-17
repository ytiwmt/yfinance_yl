import os, requests, pandas as pd, numpy as np, random, re, json, time
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

# =========================
# CONFIG
# =========================
WEBHOOK_URL = os.environ.get("WEBHOOK_URL_GROWTHRADAR")
STATE_FILE = "growth_state_v32_1.json"
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
                with open(STATE_FILE, "r") as f:
                    return json.load(f)
            except:
                return {}
        return {}

    def save(self):
        try:
            with open(STATE_FILE, "w") as f:
                json.dump(self.data, f)
        except Exception as e:
            print(f"[STATE SAVE ERROR] {e}")

    def update(self, ticker, score):
        if not np.isfinite(score):
            return
        hist = self.data.get(ticker, [])
        hist.append({"t": time.time(), "s": float(score)})
        self.data[ticker] = hist[-30:]

# =========================
# UNIVERSE（冗長化）
# =========================
def load_universe():
    symbols = []

    # NASDAQ (安定)
    try:
        df = pd.read_csv("https://datahub.io/core/nasdaq-listings/r/nasdaq-listed-symbols.csv")
        symbols += df["Symbol"].tolist()
        print(f"[NASDAQ OK] {len(df)}")
    except:
        print("[NASDAQ FAIL]")

    # GitHub backup
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

        # ★ 修正スコア（加速重視）
        score = (0.3 * m6) + (0.3 * trend) + (0.4 * accel)

        return {"ticker": ticker, "score": score, "price": price}

    except:
        return None

# =========================
# DETECTOR（簡素化＆安定）
# =========================
def detect(state):
    results = []

    for t, hist in state.data.items():
        if len(hist) < 7:
            continue

        scores = [h["s"] for h in hist]
        x = np.arange(len(scores))

        try:
            slope = np.polyfit(x, scores, 1)[0]
            latest = scores[-1]
            avg = np.mean(scores)

            boost = latest - avg

            # ★ シンプル条件
            if slope > 0 and boost > 0:
                results.append({
                    "ticker": t,
                    "score": latest,
                    "slope": slope,
                    "boost": boost
                })
        except:
            continue

    return sorted(results, key=lambda x: (x["score"], x["slope"]), reverse=True)

# =========================
# REPORT（必ず送る）
# =========================
def report(candidates, scanned, valid):
    now = datetime.now().strftime("%Y-%m-%d %H:%M")

    msg = [
        f"🚀 GrowthRadar v32.1",
        f"Scanned:{scanned} Valid:{valid} Candidates:{len(candidates)}",
        f"Time:{now}",
        ""
    ]

    if candidates:
        msg.append("🔥 TOP CANDIDATES")
        for c in candidates[:10]:
            msg.append(
                f"{c['ticker']} "
                f"S:{c['score']:.2f} "
                f"Slope:{c['slope']:.4f} "
                f"Boost:{c['boost']:.3f}"
            )
    else:
        msg.append("⚠️ NO CANDIDATES (market contraction or early phase)")

    text = "\n".join(msg)
    print(text)

    if WEBHOOK_URL:
        try:
            res = requests.post(WEBHOOK_URL, json={"content": text[:1900]})
            print(f"[WEBHOOK] status={res.status_code}")
        except Exception as e:
            print(f"[WEBHOOK ERROR] {e}")

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
                state.update(res["ticker"], res["score"])

    state.save()

    candidates = detect(state)

    report(candidates, len(universe), len(raw))


if __name__ == "__main__":
    run()
