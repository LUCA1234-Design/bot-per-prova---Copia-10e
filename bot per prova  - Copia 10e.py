# ============================================================
# 🤖 BOT V15 "ANTIFRAGILE" — QUANTUM EDITION
# ============================================================
# ARCHITETTURA:
# 1. GENERALE (Regime Detection): Decide SE combattere (Trend, Range, Shock).
# 2. SCOUT (AI Veloce): Controlla rapidamente il terreno.
# 3. ANALISTA (AI Profonda): Valida la strategia complessa.
# 4. QUANT ENGINE: Z-Score (0.5), Kelly Criterion, Volatility Target.
# ============================================================

import time, json, requests, datetime, numpy as np, pandas as pd
import threading, queue, traceback, websocket, ssl, logging, gc, os, csv, random, sys, math, sqlite3
from io import BytesIO
from threading import Lock

# --- LIBRERIE SCIENTIFICHE ---
from scipy.stats import zscore 
from scipy.signal import argrelextrema

# --- SILENZIA AVVISI MATPLOTLIB (BOMBA NUCLEARE) ---
import warnings
warnings.filterwarnings("ignore", category=UserWarning)

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

# --- CONNESSIONE BINANCE ---
from binance.client import Client

# ============================
# CONFIGURAZIONE V15
# ============================
API_KEY = os.getenv("BINANCE_API_KEY", "v5lsKf3Ajri6DXZPkUuD8zMWCHN861vMk3fTTrDA19UnOZtKvabmJHH6x3DkpumZ")
API_SECRET = os.getenv("BINANCE_API_SECRET", "XW0MnFlgNg40v8EvIuJQSAyo9hxWseXzKKPsnj1IrhqpAAyRsZyqBNmff7ZgMI")
TELEGRAM_TOKEN = "8436199553:AAEJAYyl3HCbeg3hzT1m9DhYIo_WniLjyVI"
TELEGRAM_CHAT_ID = "675648539"

# DUAL AI CONFIG
AI_ENABLED = True
AI_SYNC_ON_SIGNAL = True
AI_SECTION_IN_MSG = True

# --- SCOUT: Leggero e Istantaneo ---
AI_URL_SCOUT = "http://127.0.0.1:1234/v1/chat/completions"
AI_MODEL_SCOUT = "qwen2.5-1.5b-instruct"

# --- ANALYST: Pesante, Intelligente e Preciso ---
AI_URL_ANALYST = "http://127.0.0.1:1234/v1/chat/completions"
AI_MODEL_ANALYST = "qwen2.5-coder-7b-instruct"  # <--- INSERISCI QUI IL NOME ESATTO DEL MODELLO GRANDE
AI_TIMEOUT = 45

# PARAMETRI SNIPER V15
THRESHOLD_BASE = 0.45
ACCOUNT_BALANCE = 1000.0
HG_ENABLED = True
HG_MONITOR_ALL = True
HG_TF = ["1h", "15m"]
HG_TF_SECONDS = {"1h": 3600, "15m": 900}
HG_COOLDOWN = 180
HG_RVOL_PARTIAL_MIN = 2.2
HG_RVOL_BAR_MIN = 1.3
HG_SQUEEZE_MIN_BARS = 10
HG_NR7_RVOL_MIN = 1.2
HG_RS_SLOPE_MIN = 0.0014
HG_LOOKBACK_RS = 48
HG_LOOKBACK_HIGH = 20
HG_SQZ_ON = True
HG_NR7_ON = True
HG_RS_ON = True
HG_MIN_QUOTE_VOL = 70000 
HG_QVOL_LOOKBACK = 20
HG_CFG = {
    "1h": {"rvol_partial_min": 1.5, "rvol_bar_min": 1.3, "min_score": 0.72, "cooldown": HG_COOLDOWN},
    "15m": {"rvol_partial_min": 2.5, "rvol_bar_min": 1.6, "min_score": 0.85, "cooldown": 900},
}
# Cooldown Segnali
SIGNAL_COOLDOWN = 1800
SIGNAL_COOLDOWN_BY_TF = {"15m": 900, "1h": 3600, "4h": 7200}
DIVERGENCE_MAX_AGE_HOURS = 4
DIVERGENCE_MAX_AGE_CANDLES = 3
DIVERGENCE_MAX_AGE_BY_TF = {"15m": 2, "1h": 2, "4h": 1}
BREAKOUT_RULES = {
    "1h": {"vol_min": 0.6, "break_mult": 1.001, "min_closes": 1, "atr_mult": 0.08},
    "15m": {"vol_min": 0.6, "break_mult": 1.0004, "min_closes": 1, "atr_mult": 0.05},
}
ORARI_VIETATI_UTC  = [3, 4]  # Solo le 2 ore più morte
ORARI_MIGLIORI_UTC = list(range(7, 17)) + list(range(19, 24)) + [0, 1]  # Ore operative
ORARI_MEDIOCRI_UTC = [2, 5, 6, 17, 18]  # Ore di transizione

# ============================
# 🎯 SOGLIE SNIPER CONFIGURABILI
# ============================
MIN_RR = 1.80  # Reward:Risk minimo accettato
MIN_FUSION_SCORE = 0.50
MIN_AGENT_CONFIRMATIONS = 4
NON_OPTIMAL_HOUR_PENALTY = 0.05

# ============================
# ⚙️ RISK ENGINE ISTITUZIONALE
# ============================
MAX_PORTFOLIO_HEAT = 0.06
MAX_SINGLE_TRADE_RISK = 0.015
MAX_DAILY_DRAWDOWN = 0.03
MAX_DAILY_TRADES = 8
VOLATILITY_TARGET_ANNUAL = 0.15
CONSECUTIVE_LOSS_SCALE = {0: 1.0, 1: 0.85, 2: 0.70, 3: 0.50, 4: 0.0}

# ============================
# 🏛️ REGIME → STRATEGY MAP
# ============================
REGIME_STRATEGY_MAP = {
    "LONG_TREND":     {"allowed": ["Squeeze", "IGNITION", "NR7", "RS Leader", "Breakout"], "forbidden": ["mean_reversion"]},
    "SHORT_TREND":    {"allowed": ["Squeeze", "IGNITION", "NR7", "RS Leader", "Breakdown"], "forbidden": ["mean_reversion"]},
    "MEAN_REVERSION": {"allowed": ["Divergen", "hammer", "inverted"], "forbidden": ["Breakout", "IGNITION", "Breakdown"]},
    "SHOCK":          {"allowed": [], "forbidden": ["all"]},
}

# ============================
# 📊 WEIGHTED CONFLUENCE
# ============================
CONFLUENCE_WEIGHTS = {
    "rsi_alignment": 1.0,
    "ema_stack": 1.5,
    "macd_alignment": 1.2,
    "volume_strong": 2.0,
    "cmf_concorde": 1.5,
    "stoch_zone_ok": 0.8,
    "tsi_concorde": 0.8,
    "wavetrend_concorde": 0.7,
    "structure_ok": 1.8,
    "rvol_ok": 2.0,
}

def is_good_trading_hour() -> bool:
    ora = datetime.datetime.utcnow().hour
    if ora in ORARI_VIETATI_UTC: return False
    return True

# Logging
logging.basicConfig(level=logging.DEBUG, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("BOT_V15")
logging.getLogger("websocket").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)
logging.getLogger("matplotlib").setLevel(logging.WARNING)
logging.getLogger("matplotlib.font_manager").disabled = True

client = Client(API_KEY, API_SECRET)
client.API_URL = "https://fapi.binance.com"

# Strutture Dati
historical_data = {}
divergence_state = {}
last_signal_time = {}
last_ai_call_per_symbol = {}
symbols_whitelist = []
symbols_hg_all = []
WS_HEALTH = {}
WS_FAILCOUNT = {}
LAST_MESSAGE_TIME = {}
LAST_MESSAGE_LOCK = threading.Lock()
WS_STALE_TIMEOUT = 60       # Secondi senza messaggi prima che il watchdog riavvii il WS
WATCHDOG_GRACE_PERIOD = 30  # Secondi di attesa iniziale prima che il watchdog inizi a monitorare
WATCHDOG_CHECK_INTERVAL = 15  # Secondi tra ogni controllo del watchdog
ACTIVE_HEARTBEATS = set()
LOGBOOK_FILE = "signals_log_v15.csv"
LOGBOOK_LOCK = threading.Lock()
SYMBOL_LOCKS = {}
LAST_KLINE_TIME = {}
LAST_PROCESSED_CLOSED = {}
LAST_PROCESSED_LOCK = threading.Lock()
last_hg_immediate_time = {}
last_hg_validated_time = {}
last_hg_bar_immediate = {}
signal_queue = queue.Queue()

risk_engine_state = {
    "daily_pnl_pct": 0.0,
    "open_heat": 0.0,
    "trades_today": 0,
    "consecutive_losses": 0,
    "last_reset_day": "",
}

DB_FILE = "bot_state_v15.sqlite"
DB_LOCK = Lock()

def get_db_connection():
    conn = sqlite3.connect(DB_FILE, check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL;")
    return conn

def init_database():
    with DB_LOCK:
        conn = get_db_connection()
        conn.execute("CREATE TABLE IF NOT EXISTS last_signal_time (symbol TEXT PRIMARY KEY, ts REAL);")
        conn.execute("CREATE TABLE IF NOT EXISTS last_ai_call_per_symbol (symbol TEXT PRIMARY KEY, ts REAL);")
        conn.execute("CREATE TABLE IF NOT EXISTS ws_health (name TEXT PRIMARY KEY, alive INTEGER, last_msg REAL, fail_count INTEGER);")
        conn.execute("CREATE TABLE IF NOT EXISTS last_message_time (name TEXT PRIMARY KEY, ts REAL);")
        conn.execute("""
            CREATE TABLE IF NOT EXISTS trade_journal (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT, symbol TEXT, tf TEXT, direction TEXT, kind TEXT,
                entry REAL, sl REAL, tp1 REAL, tp2 REAL, tp3 REAL,
                probability REAL, confluence_score INTEGER, regime TEXT,
                micro_score INTEGER, ai_forza TEXT, ai_successo INTEGER,
                position_size_usd REAL, risk_pct REAL,
                outcome TEXT DEFAULT 'pending', pnl_pct REAL DEFAULT 0.0, notes TEXT DEFAULT ''
            );
        """)
        conn.commit()
        conn.close()

def db_save_last_signal_time():
    if not last_signal_time: return
    with DB_LOCK:
        conn = get_db_connection()
        for s, t in last_signal_time.items():
            conn.execute("INSERT OR REPLACE INTO last_signal_time (symbol, ts) VALUES (?, ?);", (s, float(t)))
        conn.commit()
        conn.close()

def db_load_last_signal_time():
    with DB_LOCK:
        conn = get_db_connection()
        rows = conn.execute("SELECT symbol, ts FROM last_signal_time;").fetchall()
        conn.close()
    for s, t in rows: last_signal_time[s] = float(t)

# (Altre funzioni DB semplificate per brevità - lasciale vuote o copia le tue se vuoi)
def db_save_ws_health(): pass 
def db_load_ws_health(): pass
def db_save_last_message_time(): pass
def db_load_last_message_time(): pass
def db_save_last_ai_call_per_symbol(): pass
def db_load_last_ai_call_per_symbol(): pass

def get_symbol_lock(symbol):
    if symbol not in SYMBOL_LOCKS: SYMBOL_LOCKS[symbol] = threading.Lock()
    return SYMBOL_LOCKS[symbol]

# ============================
# INDICATORI BASE
# ============================
def calculate_z_score_series(series, window=20):
    if len(series) < window: return pd.Series(0, index=series.index)
    mean = series.rolling(window=window).mean()
    std = series.rolling(window=window).std(ddof=0)
    return (series - mean) / std.replace(0, 1)

def calculate_kelly_position(win_rate, reward_to_risk, balance):
    if reward_to_risk <= 0: return 0.0, 0.0
    kelly_fraction = win_rate - ((1 - win_rate) / reward_to_risk)
    safe_kelly = max(0.0, kelly_fraction * 0.5) 
    allocation_pct = min(safe_kelly, 0.12) 
    return balance * allocation_pct, allocation_pct * 100

def calc_rsi(series, period=14):
    delta = series.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    rs = gain.rolling(period).mean() / loss.rolling(period).mean()
    return 100 - (100 / (1 + rs))

def calc_atr(df, period=14):
    high_low = df["high"] - df["low"]
    high_close = (df["high"] - df["close"].shift()).abs()
    low_close = (df["low"] - df["close"].shift()).abs()
    tr = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
    return tr.rolling(period).mean()

def calc_adx(df, period=14):
    try:
        if df is None or len(df) < period * 2: return 0
        df = df.copy()
        df['up'] = df['high'] - df['high'].shift(1)
        df['down'] = df['low'].shift(1) - df['low']
        df['pos_dm'] = np.where((df['up'] > df['down']) & (df['up'] > 0), df['up'], 0.0)
        df['neg_dm'] = np.where((df['down'] > df['up']) & (df['down'] > 0), df['down'], 0.0)
        tr = calc_atr(df, period)
        pos_dm_s = df['pos_dm'].ewm(alpha=1/period, min_periods=period).mean()
        neg_dm_s = df['neg_dm'].ewm(alpha=1/period, min_periods=period).mean()
        tr_s = tr.ewm(alpha=1/period, min_periods=period).mean()
        pos_di = 100 * (pos_dm_s / tr_s)
        neg_di = 100 * (neg_dm_s / tr_s)
        dx = (abs(pos_di - neg_di) / abs(pos_di + neg_di)) * 100
        return dx.ewm(alpha=1/period, min_periods=period).mean().iloc[-1]
    except: return 0

def calc_obv(df):
    obv = [0]
    for i in range(1, len(df)):
        if df["close"].iloc[i] > df["close"].iloc[i-1]: obv.append(obv[-1] + df["volume"].iloc[i])
        elif df["close"].iloc[i] < df["close"].iloc[i-1]: obv.append(obv[-1] - df["volume"].iloc[i])
        else: obv.append(obv[-1])
    return pd.Series(obv, index=df.index)

def calc_macd(series):
    ema12 = series.ewm(span=12).mean()
    ema26 = series.ewm(span=26).mean()
    macd = ema12 - ema26
    signal = macd.ewm(span=9).mean()
    return macd, signal, macd - signal

def calc_wavetrend(df, chlen=10, avg=21, malen=4):
    try:
        hlc3 = (df["high"] + df["low"] + df["close"]) / 3
        esa = hlc3.ewm(span=chlen, adjust=False).mean()
        d = abs(hlc3 - esa).ewm(span=chlen, adjust=False).mean()
        ci = (hlc3 - esa) / (0.015 * d).replace(0, np.nan)
        wt1 = ci.ewm(span=avg, adjust=False).mean()
        wt2 = wt1.ewm(span=malen, adjust=False).mean()
        return wt1.fillna(0), wt2.fillna(0)
    except:
        return pd.Series(0, index=df.index), pd.Series(0, index=df.index)

def calc_cci(df, period=20):
    try:
        tp = (df["high"] + df["low"] + df["close"]) / 3
        sma = tp.rolling(period).mean()
        mad = (tp - sma).abs().rolling(period).mean()
        return ((tp - sma) / (0.015 * mad)).fillna(0)
    except:
        return pd.Series(0, index=df.index)

def calc_stoch(df, k_period=14, d_period=3):
    try:
        low_min = df["low"].rolling(k_period).min()
        high_max = df["high"].rolling(k_period).max()
        den = (high_max - low_min).replace(0, np.nan)
        stoch_k = 100 * (df["close"] - low_min) / den
        stoch_d = stoch_k.rolling(d_period).mean()
        return stoch_k.fillna(50), stoch_d.fillna(50)
    except:
        return pd.Series(50, index=df.index), pd.Series(50, index=df.index)

def calc_cmf(df, period=20):
    try:
        den = (df["high"] - df["low"]).replace(0, np.nan)
        mfm = ((df["close"] - df["low"]) - (df["high"] - df["close"])) / den
        mfv = mfm.fillna(0) * df["volume"]
        vol_sum = df["volume"].rolling(period).sum().replace(0, np.nan)
        return (mfv.rolling(period).sum() / vol_sum).fillna(0)
    except:
        return pd.Series(0, index=df.index)

def calc_roc(series, period=10):
    try:
        prev = series.shift(period)
        return ((series - prev) / prev.replace(0, np.nan) * 100).fillna(0)
    except:
        return pd.Series(0, index=series.index)

def calc_tsi(series, r=25, s=13):
    try:
        diff = series.diff()
        abs_diff = diff.abs()
        num = diff.ewm(span=r, adjust=False).mean().ewm(span=s, adjust=False).mean()
        den = abs_diff.ewm(span=r, adjust=False).mean().ewm(span=s, adjust=False).mean()
        return (100 * num / den.replace(0, np.nan)).fillna(0)
    except:
        return pd.Series(0, index=series.index)

def compute_indicators(df):
    if df is None or df.empty: return df
    df = df.copy()
    try:
        df["rsi"] = calc_rsi(df["close"])
        df["atr"] = calc_atr(df)
        df["obv"] = calc_obv(df)
        df["macd"], df["macd_signal"], df["macd_hist"] = calc_macd(df["close"])
        df["wt1"], df["wt2"] = calc_wavetrend(df)
        df["cci"] = calc_cci(df)
        df["stoch_k"], df["stoch_d"] = calc_stoch(df)
        df["cmf"] = calc_cmf(df)
        df["roc"] = calc_roc(df["close"])
        df["tsi"] = calc_tsi(df["close"])
    except: pass
    return df
# ============================
# HELPER TELEGRAM & FILTRI (BLOCCO SALVA-VITA)
# ============================

# 1. TELEGRAM
def send_telegram(msg):
    try:
        requests.post(f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
                      data={"chat_id": TELEGRAM_CHAT_ID, "text": msg}, timeout=5)
    except Exception as e:
        logger.error(f"Telegram Error: {e}")

def send_telegram_photo(photo_path, caption=""):
    try:
        with open(photo_path, "rb") as img:
            requests.post(f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendPhoto",
                          data={"chat_id": TELEGRAM_CHAT_ID, "caption": caption},
                          files={"photo": img}, timeout=10)
    except Exception as e:
        logger.error(f"Telegram Photo Error: {e}")

def test_telegram():
    send_telegram("✅ V15 Antifragile: Telegram Connesso")

# 2. FILTRI SIMBOLO E ARROTONDAMENTI
SYMBOL_FILTER_CACHE = {}

def get_symbol_filters(symbol):
    try:
        if symbol in SYMBOL_FILTER_CACHE: return SYMBOL_FILTER_CACHE[symbol]
        info = client.futures_exchange_info()
        for s in info["symbols"]:
            if s["symbol"] == symbol:
                f = {f["filterType"]: f for f in s["filters"]}
                tick = float(f.get("PRICE_FILTER", {}).get("tickSize", 0.0001))
                step = float(f.get("LOT_SIZE", {}).get("stepSize", 0.001))
                min_qty = float(f.get("LOT_SIZE", {}).get("minQty", 0.001))
                SYMBOL_FILTER_CACHE[symbol] = {"tickSize": tick, "stepSize": step, "minQty": min_qty}
                return SYMBOL_FILTER_CACHE[symbol]
    except: pass
    return {"tickSize": 0.0001, "stepSize": 0.001, "minQty": 0.001}

def round_to(value, step):
    try:
        if step <= 0: return float(value)
        return float(np.floor(float(value) / step) * step)
    except: return float(value)


# ============================
# 🏛️ REGIME DETECTION PROBABILISTICO (IL GENERALE)
# ============================
def get_probabilistic_regime(df):
    """V15: Calcola la probabilità dei 4 regimi (Trend, Range, Shock)."""
    try:
        if df is None or len(df) < 50: return "MEAN_REVERSION", {}
        adx = calc_adx(df)
        ema50 = df['close'].ewm(span=50).mean()
        ema200 = df['close'].ewm(span=200).mean()
        m, bbu, bbl, bbw = _bbands(df['close'])
        ke, kcu, kcl, kcw = _keltner(df)
        vol_ratio = (bbw / kcw.replace(0, 0.0001)).iloc[-1]
        z_score = calculate_z_score_series(df['close']).iloc[-1]
        
        scores = {"LONG_TREND": 0, "SHORT_TREND": 0, "MEAN_REVERSION": 0, "SHOCK": 0}

        # Logica Punti
        if adx > 25: 
            if ema50.iloc[-1] > ema200.iloc[-1]: scores["LONG_TREND"] += 30
            else: scores["SHORT_TREND"] += 30
        
        if vol_ratio > 1.8 or abs(z_score) > 3.0: scores["SHOCK"] += 60
        if adx < 20 and vol_ratio < 1.0: scores["MEAN_REVERSION"] += 50

        return max(scores, key=scores.get), scores
    except: return "MEAN_REVERSION", {}

# ============================
# 🔬 MICROSTRUCTURE ENGINE
# ============================

def fetch_open_interest(symbol):
    """Ritorna l'Open Interest attuale da Binance Futures."""
    try:
        resp = requests.get(
            "https://fapi.binance.com/fapi/v1/openInterest",
            params={"symbol": symbol}, timeout=5
        )
        return float(resp.json().get("openInterest", 0))
    except Exception:
        return 0.0

def fetch_oi_change_pct(symbol):
    """Variazione % OI nelle ultime ore."""
    try:
        resp = requests.get(
            "https://fapi.binance.com/futures/data/openInterestHist",
            params={"symbol": symbol, "period": "1h", "limit": 5}, timeout=5
        )
        data = resp.json()
        if not data or len(data) < 2:
            return 0.0, "neutral"
        oi_now = float(data[-1].get("sumOpenInterest", 0))
        oi_prev = float(data[0].get("sumOpenInterest", 1))
        if oi_prev <= 0:
            return 0.0, "neutral"
        oi_change = (oi_now - oi_prev) / oi_prev
        if oi_change > 0.03:
            return oi_change, "building"
        elif oi_change < -0.03:
            return oi_change, "unwinding"
        return oi_change, "neutral"
    except Exception:
        return 0.0, "neutral"

def fetch_top_trader_ratio(symbol):
    """Long/Short ratio dei top trader."""
    try:
        resp = requests.get(
            "https://fapi.binance.com/futures/data/topLongShortPositionRatio",
            params={"symbol": symbol, "period": "1h", "limit": 1}, timeout=5
        )
        data = resp.json()
        if data:
            return float(data[-1].get("longShortRatio", 1.0))
        return 1.0
    except Exception:
        return 1.0

def microstructure_check(symbol, direction):
    """Combina OI + Funding + Top Trader ratio. Ritorna (pass, details)."""
    try:
        funding = fetch_funding_rate(symbol)
        oi_change, oi_state = fetch_oi_change_pct(symbol)
        ls_ratio = fetch_top_trader_ratio(symbol)

        score = 0
        details = {
            "funding": funding, "oi_change_pct": oi_change,
            "oi_state": oi_state, "ls_ratio": ls_ratio,
        }

        if direction == "long":
            if funding > 0.001: score -= 1
            elif funding < -0.0005: score += 1
        else:
            if funding < -0.001: score -= 1
            elif funding > 0.0005: score += 1

        if oi_state == "building": score += 1
        elif oi_state == "unwinding": score -= 1

        if direction == "long" and ls_ratio > 2.0: score -= 1
        elif direction == "short" and ls_ratio < 0.5: score -= 1

        details["micro_score"] = score
        return score >= -1, details
    except Exception:
        return True, {"micro_score": 0}

# ============================
# ⚙️ RISK ENGINE FUNCTIONS
# ============================

def reset_daily_risk_if_needed():
    today = datetime.datetime.utcnow().date().isoformat()
    if risk_engine_state.get("last_reset_day") != today:
        risk_engine_state["daily_pnl_pct"] = 0.0
        risk_engine_state["trades_today"] = 0
        risk_engine_state["last_reset_day"] = today
        logger.info("🔄 [RISK] Reset giornaliero contatori")

def check_risk_limits():
    """Verifica se il Risk Engine permette nuovi trade."""
    reset_daily_risk_if_needed()
    if risk_engine_state["daily_pnl_pct"] <= -MAX_DAILY_DRAWDOWN:
        return False, f"Max daily DD ({risk_engine_state['daily_pnl_pct']*100:.1f}%)"
    if risk_engine_state["trades_today"] >= MAX_DAILY_TRADES:
        return False, f"Max trade giornalieri ({MAX_DAILY_TRADES})"
    if risk_engine_state["open_heat"] >= MAX_PORTFOLIO_HEAT:
        return False, f"Portfolio heat max ({risk_engine_state['open_heat']*100:.1f}%)"
    cl = risk_engine_state["consecutive_losses"]
    if cl >= 4:
        return False, "4+ perdite consecutive"
    return True, "OK"

def calc_dynamic_position_size(df, direction, entry, sl, signal_probability, balance=None):
    """Position sizing: Kelly + Vol Target + Heat + Loss scaling."""
    try:
        if balance is None: balance = ACCOUNT_BALANCE
        if not entry or not sl or entry <= 0 or sl <= 0:
            return 0.0, 0.0, {"reason": "Livelli mancanti"}

        risk_per_unit = abs(entry - sl)
        if risk_per_unit <= 0:
            return 0.0, 0.0, {"reason": "SL=Entry"}
        risk_pct_per_unit = risk_per_unit / entry

        reward_risk = 2.0
        vol_scale = 1.0
        if df is not None and "atr" in df.columns:
            atr = float(df["atr"].iloc[-1])
            price = float(df["close"].iloc[-1])
            if atr > 0 and risk_per_unit > 0:
                reward_risk = max(1.0, min(4.0, (2.0 * atr) / risk_per_unit))
            if price > 0:
                daily_vol = (atr / price) * np.sqrt(365)
                if daily_vol > 0:
                    vol_scale = min(1.5, VOLATILITY_TARGET_ANNUAL / daily_vol)

        kelly_raw = signal_probability - ((1 - signal_probability) / reward_risk)
        kelly_pct = max(0.0, kelly_raw * 0.4)
        kelly_pct = min(kelly_pct, MAX_SINGLE_TRADE_RISK)
        kelly_pct *= vol_scale

        cl = risk_engine_state.get("consecutive_losses", 0)
        loss_scale = CONSECUTIVE_LOSS_SCALE.get(min(cl, 4), 0.0)
        kelly_pct *= loss_scale

        remaining = MAX_PORTFOLIO_HEAT - risk_engine_state.get("open_heat", 0)
        kelly_pct = min(kelly_pct, max(0, remaining))

        risk_usd = balance * kelly_pct
        size_usd = risk_usd / risk_pct_per_unit if risk_pct_per_unit > 0 else 0

        return size_usd, kelly_pct, {
            "kelly_raw": kelly_raw, "kelly_safe_pct": kelly_pct * 100,
            "vol_scale": vol_scale, "loss_scale": loss_scale,
            "risk_usd": risk_usd, "size_usd": size_usd, "reward_risk": reward_risk,
        }
    except Exception:
        return 0.0, 0.0, {"reason": "Errore calcolo"}

def update_risk_after_signal(risk_pct):
    risk_engine_state["open_heat"] += risk_pct
    risk_engine_state["trades_today"] += 1

def is_strategy_allowed_for_regime(signal_kind, regime):
    """Verifica se il tipo di segnale è permesso nel regime attuale."""
    try:
        if regime == "SHOCK":
            return False
        config = REGIME_STRATEGY_MAP.get(regime)
        if not config:
            return True
        for forbidden in config.get("forbidden", []):
            if forbidden == "all": return False
            if forbidden.lower() in signal_kind.lower(): return False
        allowed = config.get("allowed", [])
        if not allowed: return True
        for pattern in allowed:
            if pattern.lower() in signal_kind.lower(): return True
        return False
    except Exception:
        return True

def calc_weighted_confluence(df, direction):
    """Punteggio confluenza pesato 0-100."""
    try:
        if df is None or len(df) < 50: return 30
        score = 0.0
        max_score = 0.0

        max_score += CONFLUENCE_WEIGHTS["rsi_alignment"]
        if "rsi" in df.columns:
            rsi = float(df["rsi"].iloc[-1])
            if direction == "long" and 40 < rsi < 70: score += CONFLUENCE_WEIGHTS["rsi_alignment"]
            elif direction == "short" and 30 < rsi < 60: score += CONFLUENCE_WEIGHTS["rsi_alignment"]

        max_score += CONFLUENCE_WEIGHTS["ema_stack"]
        try:
            e20 = df["close"].ewm(span=20).mean().iloc[-1]
            e50 = df["close"].ewm(span=50).mean().iloc[-1]
            e200 = df["close"].ewm(span=200).mean().iloc[-1]
            if direction == "long" and e20 > e50 > e200: score += CONFLUENCE_WEIGHTS["ema_stack"]
            elif direction == "short" and e20 < e50 < e200: score += CONFLUENCE_WEIGHTS["ema_stack"]
        except: pass

        max_score += CONFLUENCE_WEIGHTS["macd_alignment"]
        if "macd" in df.columns and "macd_signal" in df.columns:
            m = float(df["macd"].iloc[-1])
            s = float(df["macd_signal"].iloc[-1])
            if direction == "long" and m > s: score += CONFLUENCE_WEIGHTS["macd_alignment"]
            elif direction == "short" and m < s: score += CONFLUENCE_WEIGHTS["macd_alignment"]

        max_score += CONFLUENCE_WEIGHTS["volume_strong"]
        _, rvol = get_rvol_state(df)
        if rvol >= 1.5: score += CONFLUENCE_WEIGHTS["volume_strong"]
        elif rvol >= 1.2: score += CONFLUENCE_WEIGHTS["volume_strong"] * 0.5

        max_score += CONFLUENCE_WEIGHTS["cmf_concorde"]
        if "cmf" in df.columns:
            cmf = float(df["cmf"].iloc[-1])
            if (direction == "long" and cmf > 0.03) or (direction == "short" and cmf < -0.03):
                score += CONFLUENCE_WEIGHTS["cmf_concorde"]

        max_score += CONFLUENCE_WEIGHTS["stoch_zone_ok"]
        if "stoch_k" in df.columns:
            sk = float(df["stoch_k"].iloc[-1])
            if direction == "long" and sk < 80: score += CONFLUENCE_WEIGHTS["stoch_zone_ok"]
            elif direction == "short" and sk > 20: score += CONFLUENCE_WEIGHTS["stoch_zone_ok"]

        max_score += CONFLUENCE_WEIGHTS["tsi_concorde"]
        if "tsi" in df.columns:
            tsi = float(df["tsi"].iloc[-1])
            if (direction == "long" and tsi > 0) or (direction == "short" and tsi < 0):
                score += CONFLUENCE_WEIGHTS["tsi_concorde"]

        max_score += CONFLUENCE_WEIGHTS["wavetrend_concorde"]
        if "wt1" in df.columns and "wt2" in df.columns:
            w1 = float(df["wt1"].iloc[-1])
            w2 = float(df["wt2"].iloc[-1])
            if (direction == "long" and w1 > w2) or (direction == "short" and w1 < w2):
                score += CONFLUENCE_WEIGHTS["wavetrend_concorde"]

        max_score += CONFLUENCE_WEIGHTS["structure_ok"]
        try:
            h = df["high"].tail(10)
            l = df["low"].tail(10)
            if direction == "long" and l.iloc[-1] > l.iloc[-5] and h.iloc[-1] > h.iloc[-5]:
                score += CONFLUENCE_WEIGHTS["structure_ok"]
            elif direction == "short" and h.iloc[-1] < h.iloc[-5] and l.iloc[-1] < l.iloc[-5]:
                score += CONFLUENCE_WEIGHTS["structure_ok"]
        except: pass

        max_score += CONFLUENCE_WEIGHTS["rvol_ok"]
        if rvol >= 2.0: score += CONFLUENCE_WEIGHTS["rvol_ok"]
        elif rvol >= 1.3: score += CONFLUENCE_WEIGHTS["rvol_ok"] * 0.5

        if max_score <= 0: return 30
        return int(min(100, (score / max_score) * 100))
    except Exception:
        return 30

def calculate_signal_probability(df, direction, features, regime, tf):
    """P(successo) bayesiano: Regime + Trend + Volume + Indicatori + Micro."""
    try:
        if df is None or len(df) < 50: return 0.30
        p = 0.50

        # 1. Regime (±15%)
        if regime in ("LONG_TREND", "SHORT_TREND"):
            if (regime == "LONG_TREND" and direction == "long") or (regime == "SHORT_TREND" and direction == "short"):
                p += 0.15
            else:
                p -= 0.20
        elif regime == "MEAN_REVERSION":
            kind = features.get("kind", "") if isinstance(features, dict) else ""
            if "Squeeze" in kind or "NR7" in kind: p -= 0.10
            else: p += 0.10
        elif regime == "SHOCK":
            p -= 0.15

        # 2. Trend (±10%)
        trend = get_trend(df)
        if (direction == "long" and trend == "rialzista") or (direction == "short" and trend == "ribassista"):
            p += 0.10
        elif (direction == "long" and trend == "ribassista") or (direction == "short" and trend == "rialzista"):
            p -= 0.10

        # 3. Volume (±12%)
        rvol = float(features.get("rvol", 1.0)) if isinstance(features, dict) else 1.0
        if rvol >= 4.0: p += 0.12
        elif rvol >= 2.5: p += 0.08
        elif rvol >= 1.5: p += 0.04
        elif rvol < 0.8: p -= 0.08

        # 4. Confluence (±15%)
        confluence = calc_weighted_confluence(df, direction)
        if confluence >= 70: p += 0.15
        elif confluence >= 50: p += 0.08
        elif confluence >= 30: p += 0.03
        elif confluence < 20: p -= 0.12

        # 5. CMF micro (±5%)
        try:
            if "cmf" in df.columns:
                cmf = float(df["cmf"].iloc[-1])
                if (direction == "long" and cmf > 0.08) or (direction == "short" and cmf < -0.08): p += 0.03
                elif (direction == "long" and cmf < -0.10) or (direction == "short" and cmf > 0.10): p -= 0.05
        except: pass

        # 6. Body ratio (±5%)
        if isinstance(features, dict):
            body = float(features.get("body_ratio", 0.5))
            if body >= 0.80: p += 0.05
            elif body < 0.40: p -= 0.05

        # 7. Time of day (±3%)
        ora = datetime.datetime.utcnow().hour
        if ora in ORARI_MIGLIORI_UTC: p += 0.03
        elif ora in ORARI_MEDIOCRI_UTC: p -= 0.03

        return max(0.05, min(0.95, p))
    except Exception:
        return 0.40

def log_trade_to_journal(symbol, tf, direction, kind, entry, sl, tp1, tp2, tp3,
                          probability, confluence_score, regime, micro_score,
                          ai_forza, ai_successo, position_size_usd, risk_pct):
    try:
        with DB_LOCK:
            conn = get_db_connection()
            conn.execute("""
                INSERT INTO trade_journal
                (timestamp, symbol, tf, direction, kind, entry, sl, tp1, tp2, tp3,
                 probability, confluence_score, regime, micro_score,
                 ai_forza, ai_successo, position_size_usd, risk_pct)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                datetime.datetime.utcnow().isoformat(), symbol, tf, direction, kind,
                entry, sl, tp1, tp2, tp3, probability, confluence_score, regime,
                micro_score, ai_forza, ai_successo, position_size_usd, risk_pct
            ))
            conn.commit()
            conn.close()
    except Exception as e:
        logger.error(f"[JOURNAL] Errore: {e}")

def get_rolling_stats(lookback_trades=50):
    try:
        with DB_LOCK:
            conn = get_db_connection()
            rows = conn.execute(
                "SELECT outcome, pnl_pct FROM trade_journal WHERE outcome != 'pending' ORDER BY id DESC LIMIT ?",
                (lookback_trades,)
            ).fetchall()
            conn.close()
        if not rows:
            return {"win_rate": 0.55, "avg_pnl": 0.0, "total_trades": 0, "max_dd": 0.0}
        wins = sum(1 for r in rows if r[0] == "win")
        total = len(rows)
        pnls = [r[1] for r in rows]
        cumulative = 0.0
        peak = 0.0
        max_dd = 0.0
        for pnl in reversed(pnls):
            cumulative += pnl
            peak = max(peak, cumulative)
            max_dd = max(max_dd, peak - cumulative)
        return {"win_rate": wins / total if total > 0 else 0.55, "avg_pnl": sum(pnls) / total if total > 0 else 0.0, "total_trades": total, "max_dd": max_dd}
    except Exception:
        return {"win_rate": 0.55, "avg_pnl": 0.0, "total_trades": 0, "max_dd": 0.0}

# ============================
# AI CALL SAFE (DUAL ROLE: SCOUT + ANALYST)
# ============================
def call_ai_safe(symbol, tf, pre_score, direction, context, role="analyst"):
    try:
        # Selezione URL e Modello in base al ruolo
        if role == "scout":
            url = AI_URL_SCOUT
            model = AI_MODEL_SCOUT
            sys_msg = "Sei uno Scout HFT. Analizza il trend. Rispondi JSON."
            temp = 0.1
        else:
            url = AI_URL_ANALYST
            model = AI_MODEL_ANALYST
            sys_msg = "Sei un Analista Quant Senior. Valuta rischi. Rispondi JSON."
            temp = 0.2

        # Costruzione Prompt - V16 STRICT JSON FORCING
        prompt = (
            f"Analizza {symbol} ({direction}) su {tf}.\n"
            "REGOLE TASSATIVE (PENA IL FALLIMENTO DEL SISTEMA):\n"
            "1. Devi rispondere ESCLUSIVAMENTE con un oggetto JSON valido e nient'altro.\n"
            "2. NON USARE formattazione markdown (niente ```json e niente ```).\n"
            "3. Nessun saluto, nessuna spiegazione prima o dopo le parentesi { }.\n\n"
            "FORMATO ESATTO RICHIESTO:\n"
            "{\"forza\": \"debole\" o \"media\" o \"forte\", \"successo\": <numero_0_100>, \"commento\": \"<max_5_parole>\", \"score\": <numero_0_100>}\n\n"
            f"Contesto Dati:\n{context}\n"
        )

        payload = {
            "model": model,
            "messages": [
                {"role": "system", "content": sys_msg},
                {"role": "user", "content": prompt}
            ],
            "temperature": temp,
            "max_tokens": 200
        }
        
        response = requests.post(url, json=payload, timeout=AI_TIMEOUT)
        response.raise_for_status()
        
        # Parsing Risposta
        data_json = response.json()
        raw = data_json.get("choices", [{}])[0].get("message", {}).get("content", "").strip()
        
        import re
        
        # Pulizia JSON (rimuove markdown ```json ... ```)
        try:
            raw = raw.replace("```json", "").replace("```", "").strip()
            clean = re.search(r'\{.*\}', raw, re.DOTALL).group()
            data = json.loads(clean)
        except:
            return {"successo": 55, "forza": "media", "commento": "AI Parsing Error", "score": 50}

        # FUNZIONE ANTI-PROIETTILE PER NUMERI (Pulisce %, lettere e spazi)
        def safe_int(value, default_val):
            try:
                if isinstance(value, int): return value
                if isinstance(value, float): return int(value)
                # Estrae solo i numeri dalla stringa (es: "65%" -> "65")
                cleaned = re.sub(r'[^\d.]', '', str(value))
                if not cleaned: return default_val
                return int(float(cleaned))
            except:
                return default_val

        # Normalizzazione Output
        forza = str(data.get("forza", "media")).lower()
        if forza not in ["debole", "media", "forte"]: forza = "media"
        
        return {
            "forza": forza,
            "commento": str(data.get("commento", "Analisi completata.")),
            "successo": safe_int(data.get("successo"), 55),
            "score": safe_int(data.get("score"), 50)
        }

    except Exception as e:
        logger.error(f"[AI-{role.upper()}] Errore {symbol}: {e}")
        return {
            "forza": "media",
            "commento": "Fallback: AI non disponibile.",
            "successo": 55,
            "score": 50
        }



# ============================
# INIT HISTORICAL
# ============================


def klines_to_df(klines):
    df = pd.DataFrame(
        klines,
        columns=[
            "open_time",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "close_time",
            "quote_volume",
            "trades",
            "taker_base",
            "taker_quote",
            "ignore",
        ],
    )
    df["open"] = df["open"].astype(float)
    df["high"] = df["high"].astype(float)
    df["low"] = df["low"].astype(float)
    df["close"] = df["close"].astype(float)
    df["volume"] = df["volume"].astype(float)
    df["taker_base"] = df["taker_base"].astype(float)  # Conversione aggiunta
    df["open_time"] = pd.to_datetime(df["open_time"], unit="ms")
    df.set_index("open_time", inplace=True)
    return df


def init_historical_for_symbol(symbol):
    try:
        historical_data.setdefault(symbol, {})
        klines_1h = client.futures_klines(symbol=symbol, interval="1h", limit=300)
        df_1h = klines_to_df(klines_1h)
        df_1h = compute_indicators(df_1h)
        historical_data[symbol]["1h"] = df_1h

        klines_4h = client.futures_klines(symbol=symbol, interval="4h", limit=300)
        df_4h = klines_to_df(klines_4h)
        df_4h = compute_indicators(df_4h)
        historical_data[symbol]["4h"] = df_4h

        klines_15m = client.futures_klines(symbol=symbol, interval="15m", limit=300)
        df_15m = klines_to_df(klines_15m)
        df_15m = compute_indicators(df_15m)
        historical_data[symbol]["15m"] = df_15m

        logger.debug(f"[INIT] {symbol} caricato")
    except Exception as e:
        logger.error(f"[INIT-ERROR] {symbol}: {e}")


# ============================
# DIVERGENZE - SWING DETECTION
# ============================


def find_swings(series, lookback=5):
    if series is None or len(series) < lookback * 2 + 3:
        return [], []
    series = series.dropna()
    highs = []
    lows = []
    for i in range(lookback, len(series) - lookback):
        try:
            window = series.iloc[i - lookback : i + lookback + 1]
            val = series.iloc[i]
            if window.isna().any():
                continue
            if window.max() == window.min():
                continue
            if val == window.max():
                highs.append((series.index[i], float(val)))
            if val == window.min():
                lows.append((series.index[i], float(val)))
        except Exception:
            continue
    return highs, lows


def divergence_rsi_v12_fixed(df, rsi_col="rsi", price_col="close", lookback=80):
    try:
        if df is None or len(df) < 50:
            return {
                "rsi_advanced": "none",
                "div_index": None,
                "breakline_price": None,
                "div_early": "none",
                "div_early_quality": 0,
                "points": {},
                "rsi_quality": 0,
            }
        df = df.copy()
        if rsi_col not in df.columns or price_col not in df.columns:
            return {
                "rsi_advanced": "none",
                "div_index": None,
                "breakline_price": None,
                "div_early": "none",
                "div_early_quality": 0,
                "points": {},
                "rsi_quality": 0,
            }
        df = df[[price_col, rsi_col]].dropna()
        if len(df) < 50:
            return {
                "rsi_advanced": "none",
                "div_index": None,
                "breakline_price": None,
                "div_early": "none",
                "div_early_quality": 0,
                "points": {},
                "rsi_quality": 0,
            }
        df = df.iloc[-lookback:].copy()
        prices = df[price_col]
        rsi = df[rsi_col]

        # --- NUOVA RICERCA PICCHI VETTORIZZATA (SCIPY) ---
        def find_swings_local(series, window=1, mode="high"):
            data = series.values
            if mode == "high":
                # argrelextrema trova tutti i picchi massimi in un'unica operazione C veloce
                extrema_indices = argrelextrema(data, np.greater_equal, order=window)[0]
            else:
                # trova tutti i picchi minimi
                extrema_indices = argrelextrema(data, np.less_equal, order=window)[0]
            
            # Mappa gli indici numerici ai timestamp del DataFrame
            idxs = series.index[extrema_indices].tolist()
            vals = series.iloc[extrema_indices].tolist()
            return idxs, vals

        high_idx, high_vals = find_swings_local(prices, window=1, mode="high")
        low_idx, low_vals = find_swings_local(prices, window=1, mode="low")

        if len(high_idx) < 2 and len(low_idx) < 2:
            return {
                "rsi_advanced": "none",
                "div_index": None,
                "breakline_price": None,
                "div_early": "none",
                "div_early_quality": 0,
                "points": {},
                "rsi_quality": 0,
            }

        div_type = "none"
        div_index = None
        breakline_price = None
        points = {}
        div_early = "none"
        div_early_quality = 0
        rsi_quality = 0

        if len(low_idx) >= 2:
            p1_t, p2_t = low_idx[-2], low_idx[-1]
            p1_v, p2_v = prices.loc[p1_t], prices.loc[p2_t]
            r1_v, r2_v = rsi.loc[p1_t], rsi.loc[p2_t]

            if p2_v < p1_v and r2_v > r1_v:
                div_type = "bullish_classic"
            elif p2_v > p1_v and r2_v < r1_v:
                div_type = "bullish_hidden"

            if div_type.startswith("bullish"):
                div_index = p2_t
                local_high = prices.loc[p1_t:p2_t].max()
                breakline_price = float(local_high)
                points = {
                    "p1": (p1_t, float(p1_v)),
                    "p2": (p2_t, float(p2_v)),
                    "r1": (p1_t, float(r1_v)),
                    "r2": (p2_t, float(r2_v)),
                }

        if div_type == "none" and len(high_idx) >= 2:
            p1_t, p2_t = high_idx[-2], high_idx[-1]
            p1_v, p2_v = prices.loc[p1_t], prices.loc[p2_t]
            r1_v, r2_v = rsi.loc[p1_t], rsi.loc[p2_t]

            if p2_v > p1_v and r2_v < r1_v:
                div_type = "bearish_classic"
            elif p2_v < p1_v and r2_v > r1_v:
                div_type = "bearish_hidden"

            if div_type.startswith("bearish"):
                div_index = p2_t
                local_low = prices.loc[p1_t:p2_t].min()
                breakline_price = float(local_low)
                points = {
                    "p1": (p1_t, float(p1_v)),
                    "p2": (p2_t, float(p2_v)),
                    "r1": (p1_t, float(r1_v)),
                    "r2": (p2_t, float(r2_v)),
                }

        if div_type != "none" and points:
            p1_v = points["p1"][1]
            p2_v = points["p2"][1]
            r1_v = points["r1"][1]
            r2_v = points["r2"][1]

            rsi_delta = abs(r2_v - r1_v)

            # ✅ Filtro divergenza troppo debole
            if rsi_delta < 2.5:
                return {
                    "rsi_advanced": "none",
                    "div_index": None,
                    "breakline_price": None,
                    "div_early": "none",
                    "div_early_quality": 0,
                    "points": {},
                    "rsi_quality": 0,
                }

            price_delta = abs(p2_v - p1_v) / max(1e-9, prices.iloc[-1])
            raw_q = rsi_delta * 1.5 + price_delta * 1000.0
            div_early_quality = int(max(0, min(100, raw_q)))

            if "bullish" in div_type:
                div_early = "bullish_early"
            elif "bearish" in div_type:
                div_early = "bearish_early"

            try:
                p1_t, p2_t = points["p1"][0], points["p2"][0]
                i1 = df.index.get_loc(p1_t)
                i2 = df.index.get_loc(p2_t)
                sep_candles = max(1, abs(i2 - i1))
                rsi_score = min(rsi_delta, 30) / 30.0
                price_score = min(price_delta, 0.02) / 0.02
                time_score = min(sep_candles, 12) / 12.0
                rsi_quality = int(
                    max(
                        0, min(100, 40 * rsi_score + 40 * price_score + 20 * time_score)
                    )
                )
            except Exception:
                rsi_quality = 0

        return {
            "rsi_advanced": div_type,
            "div_index": div_index,
            "breakline_price": breakline_price,
            "div_early": div_early,
            "div_early_quality": div_early_quality,
            "points": points,
            "rsi_quality": rsi_quality,
        }
    except Exception:
        return {
            "rsi_advanced": "none",
            "div_index": None,
            "breakline_price": None,
            "div_early": "none",
            "div_early_quality": 0,
            "points": {},
            "rsi_quality": 0,
        }


# ============================
# UPDATE DIVERGENZE
# ============================


def update_divergences(symbol, tf):
    try:
        df = historical_data.get(symbol, {}).get(tf)
        if df is None or len(df) < 80:
            return
        div = divergence_rsi_v12_fixed(df)
        if symbol not in divergence_state:
            divergence_state[symbol] = {}
        divergence_state[symbol][tf] = div
    except Exception as e:
        logger.error(f"Errore update_divergences {symbol} {tf}: {e}")


def scan_divergences_on_startup():
    try:
        logger.info("✅ Storico caricato - Divergenze calcolate solo su nuove candele")
        for symbol in symbols_whitelist:
            divergence_state[symbol] = {
                "1h": {
                    "rsi_advanced": "none",
                    "div_index": None,
                    "breakline_price": None,
                    "div_early": "none",
                    "div_early_quality": 0,
                    "points": {},
                    "rsi_quality": 0,
                },
                "4h": {
                    "rsi_advanced": "none",
                    "div_index": None,
                    "breakline_price": None,
                    "div_early": "none",
                    "div_early_quality": 0,
                    "points": {},
                    "rsi_quality": 0,
                },
                "15m": {
                    "rsi_advanced": "none",
                    "div_index": None,
                    "breakline_price": None,
                    "div_early": "none",
                    "div_early_quality": 0,
                    "points": {},
                    "rsi_quality": 0,
                },
            }
        while not signal_queue.empty():
            try:
                signal_queue.get_nowait()
            except:
                break
        logger.info("🎯 Bot pronto - In attesa divergenze FRESCHE su nuove candele 1H")
    except Exception as e:
        logger.error(f"Errore scan_divergences_on_startup: {e}")


# ============================
# HELPER FUNZIONI MERCATO
# ============================


def get_trend(df):
    try:
        if df is None or len(df) < 50:
            return "laterale"
        close = df["close"].dropna()
        if len(close) < 50:
            return "laterale"
        ema50 = close.ewm(span=50).mean()
        ema200 = close.ewm(span=200).mean()
        if ema50.iloc[-1] > ema200.iloc[-1] * 1.002:
            return "rialzista"
        if ema50.iloc[-1] < ema200.iloc[-1] * 0.998:
            return "ribassista"
        return "laterale"
    except Exception:
        return "laterale"

def get_market_regime(df):
    """
    Classifica il mercato in 3 stati fondamentali usando ADX e Volatilità (BB/KC).
    Restituisce:
    - 'RANGING': Mercato laterale/debole (Ideale per Divergenze RSI).
    - 'TRENDING': Mercato direzionale forte (VIETATO tradare contro-trend).
    - 'EXPLOSIVE': Volatilità estrema (Riduci size, pericolo stoppate).
    """
    try:
        if df is None or len(df) < 50:
            return "RANGING" # Default prudente

        # 1. Calcolo ADX (Forza del Trend)
        adx = calc_adx(df) # Usa la tua funzione esistente
        
        # 2. Calcolo Squeeze Ratio (Volatilità)
        # BB Width / KC Width
        m, bbu, bbl, bbw = _bbands(df['close'])
        ke, kcu, kcl, kcw = _keltner(df)
        
        # Evita divisione per zero
        kcw = kcw.replace(0, 0.0001)
        squeeze_ratio = (bbw / kcw).iloc[-1]

        # LOGICA DI CLASSIFICAZIONE
        if adx < 25:
            if squeeze_ratio < 0.8:
                return "SQUEEZE" # Accumulazione silenziosa (Hidden Gems)
            return "RANGING" # Laterale classico (Divergenze OK)
            
        elif adx >= 25:
            if squeeze_ratio > 1.5:
                return "EXPLOSIVE" # Volatilità folle (Pericolo)
            return "TRENDING" # Trend sano (Solo a favore)
            
        return "RANGING"

    except Exception:
        return "RANGING"


def get_rvol_state(df, lookback=20):
    try:
        if df is None or len(df) < lookback + 2:
            return "sconosciuto", 1.0
        vol = df["volume"].fillna(0)
        ma = vol.rolling(lookback).mean()
        if ma.iloc[-1] <= 0:
            return "sconosciuto", 1.0
        rvol = vol.iloc[-1] / ma.iloc[-1]
        if rvol < 0.7:
            return "debole", rvol
        if rvol < 1.2:
            return "normale", rvol
        if rvol < 2.0:
            return "forte", rvol
        if rvol < 3.5:
            return "estremo", rvol
        return "climax", rvol
    except Exception:
        return "sconosciuto", 1.0


def get_symbol_quality(df):
    try:
        if df is None or len(df) < 80:
            return -5
        _, rvol = get_rvol_state(df)
        atr = df["atr"].iloc[-1]
        price = df["close"].iloc[-1]
        if price <= 0 or np.isnan(price) or np.isnan(atr):
            return -5
        atr_ratio = atr / price
        score = 0
        if 1.5 <= rvol <= 4.0:
            score += 5
        elif 1.0 <= rvol < 1.5:
            score += 3
        elif rvol < 0.7:
            score -= 3
        if 0.002 <= atr_ratio <= 0.02:
            score += 5
        elif 0.001 <= atr_ratio < 0.002:
            score += 2
        elif atr_ratio < 0.0008:
            score -= 4
        return max(min(score, 10), -10)
    except Exception:
        return 0


def get_regime_symbol(df):
    try:
        if df is None or len(df) < 50:
            return "neutro"
        close = df["close"].dropna()
        if len(close) < 50:
            return "neutro"
        ema50 = close.ewm(span=50).mean()
        ema200 = close.ewm(span=200).mean()
        if ema50.iloc[-1] > ema200.iloc[-1]:
            return "bull"
        if ema50.iloc[-1] < ema200.iloc[-1]:
            return "bear"
        return "neutro"
    except Exception:
        return "neutro"


def get_global_regime():
    try:
        btc = historical_data.get("BTCUSDT", {}).get("1h")
        eth = historical_data.get("ETHUSDT", {}).get("1h")
        r_btc = get_regime_symbol(btc)
        r_eth = get_regime_symbol(eth)
        if r_btc == "bull" and r_eth == "bull":
            return "bull"
        if r_btc == "bear" and r_eth == "bear":
            return "bear"
        return "neutro"
    except Exception:
        return "neutro"


def get_risk_regime_btc_eth():
    try:
        btc = historical_data.get("BTCUSDT", {}).get("1h")
        eth = historical_data.get("ETHUSDT", {}).get("1h")
        if btc is None or eth is None:
            return "neutral"

        def _score(df):
            try:
                rsi = df["rsi"].iloc[-1]
                obv_slope = (
                    (df["obv"].iloc[-1] - df["obv"].iloc[-5]) if len(df) > 5 else 0
                )
                atr = df["atr"].iloc[-1]
                price = df["close"].iloc[-1]
                atr_ratio = atr / price if price > 0 else 0
                s = 0
                if rsi > 55:
                    s += 1
                if obv_slope > 0:
                    s += 1
                if atr_ratio > 0.01:
                    s += 1
                if rsi < 45:
                    s -= 1
                if obv_slope < 0:
                    s -= 1
                if atr_ratio < 0.004:
                    s -= 1
                return s
            except:
                return 0

        s_btc = _score(btc)
        s_eth = _score(eth)
        total = s_btc + s_eth
        if total >= 3:
            return "risk_on"
        if total <= -3:
            return "risk_off"
        return "neutral"
    except:
        return "neutral"


def fetch_funding_rate(symbol):
    """Ritorna il funding rate attuale da Binance Futures. Positivo = troppi long, Negativo = troppi short."""
    try:
        resp = requests.get(
            "https://fapi.binance.com/fapi/v1/premiumIndex",
            params={"symbol": symbol},
            timeout=5
        )
        data = resp.json()
        return float(data.get("lastFundingRate", 0))
    except Exception:
        return 0.0


def check_btc_correlation_filter(direction):
    """
    Blocca segnali long se BTC sta crollando forte, e short se BTC sta pompando forte.
    Ritorna True se il segnale è OK, False se deve essere bloccato.
    """
    try:
        btc_df = historical_data.get("BTCUSDT", {}).get("1h")
        if btc_df is None or len(btc_df) < 5:
            return True  # Non bloccare se dati mancanti
        btc_now = btc_df["close"].iloc[-1]
        btc_5h_ago = btc_df["close"].iloc[-5]
        if btc_5h_ago <= 0:
            return True
        btc_change_5h = (btc_now - btc_5h_ago) / btc_5h_ago
        # BTC crolla -2% in 5h → blocca long su altcoin
        if direction == "long" and btc_change_5h < -0.02:
            return False
        # BTC pompa +2% in 5h → blocca short su altcoin
        if direction == "short" and btc_change_5h > 0.02:
            return False
        return True
    except Exception:
        return True


def calc_volume_profile_poc(df, bins=50, lookback=50):
    """
    Calcola il Point of Control (POC) = prezzo dove si è concentrato il massimo volume.
    Il POC agisce come supporto/resistenza dinamico.
    """
    try:
        if df is None or len(df) < lookback:
            return None
        recent = df.tail(lookback)
        price_min = recent["low"].min()
        price_max = recent["high"].max()
        if price_max <= price_min:
            return None
        prices = np.linspace(price_min, price_max, bins)
        vol_profile = np.zeros(bins - 1)
        for i in range(len(recent)):
            row_low = recent["low"].iloc[i]
            row_high = recent["high"].iloc[i]
            row_vol = recent["volume"].iloc[i]
            mask = (prices[:-1] >= row_low) & (prices[:-1] <= row_high)
            vol_profile[mask] += row_vol
        poc_idx = np.argmax(vol_profile)
        poc_price = (prices[poc_idx] + prices[poc_idx + 1]) / 2
        return float(poc_price)
    except Exception:
        return None


def detect_macd_divergence(df, lookback=50):
    """
    Rileva divergenze sul MACD Histogram.
    Ritorna: 'bullish', 'bearish', o 'none'
    """
    try:
        if df is None or len(df) < lookback:
            return "none"
        if "macd_hist" not in df.columns:
            return "none"

        hist = df["macd_hist"].dropna().iloc[-lookback:]
        prices = df["close"].iloc[-lookback:]

        if len(hist) < 20:
            return "none"

        # Trova minimi del prezzo e dell'histogram
        price_lows = argrelextrema(prices.values, np.less_equal, order=3)[0]

        if len(price_lows) >= 2:
            i1, i2 = price_lows[-2], price_lows[-1]
            # Bullish: prezzo fa lower low, MACD hist fa higher low
            if prices.iloc[i2] < prices.iloc[i1] and hist.iloc[i2] > hist.iloc[i1]:
                return "bullish"

        # Trova massimi del prezzo
        price_highs = argrelextrema(prices.values, np.greater_equal, order=3)[0]

        if len(price_highs) >= 2:
            i1, i2 = price_highs[-2], price_highs[-1]
            # Bearish: prezzo fa higher high, MACD hist fa lower high
            if prices.iloc[i2] > prices.iloc[i1] and hist.iloc[i2] < hist.iloc[i1]:
                return "bearish"

        return "none"
    except Exception:
        return "none"


def get_divergence_direction(div_type: str) -> str:
    if div_type in ["bullish_classic", "bullish_hidden", "bullish_early"]:
        return "bull"
    if div_type in ["bearish_classic", "bearish_hidden", "bearish_early"]:
        return "bear"
    return "none"


def get_multi_tf_divergence_state(symbol):
    try:
        state = divergence_state.get(symbol, {})
        d1 = state.get("1h", {})
        d4 = state.get("4h", {})
        d15 = state.get("15m", {})
        dir_1h = get_divergence_direction(d1.get("rsi_advanced", "none"))
        dir_4h = get_divergence_direction(d4.get("rsi_advanced", "none"))
        dir_15m = get_divergence_direction(d15.get("rsi_advanced", "none"))
        conferma_4h = dir_1h != "none" and dir_1h == dir_4h
        conferma_15m = dir_1h != "none" and dir_1h == dir_15m
        return {
            "dir_1h": dir_1h,
            "dir_4h": dir_4h,
            "dir_15m": dir_15m,
            "conferma_4h": conferma_4h,
            "conferma_15m": conferma_15m,
        }
    except Exception:
        return {
            "dir_1h": "none",
            "dir_4h": "none",
            "dir_15m": "none",
            "conferma_4h": False,
            "conferma_15m": False,
        }


def momentum_slope(series, lookback=10):
    try:
        if series is None:
            return 0.0
        s = series.dropna().iloc[-lookback:]
        if len(s) < lookback:
            return 0.0
        x = np.arange(len(s))
        y = s.values
        A = np.vstack([x, np.ones(len(x))]).T
        m, c = np.linalg.lstsq(A, y, rcond=None)[0]
        return float(m)
    except Exception:
        return 0.0

def get_btc_momentum(lookback_candles=3) -> float:
    try:
        df_btc = historical_data.get("BTCUSDT", {}).get("1h")
        if df_btc is None or len(df_btc) < lookback_candles + 1:
            return 0.0
        price_now  = df_btc["close"].iloc[-1]
        price_past = df_btc["close"].iloc[-lookback_candles]
        return (price_now - price_past) / price_past * 100
    except Exception:
        return 0.0


# ============================
# HIDDEN GEMS — Helper + Detector
# ============================


def _bbands(series: pd.Series, n=20, k=2.0):
    m = series.rolling(n).mean()
    s = series.rolling(n).std(ddof=0)
    upper, lower = m + k * s, m - k * s
    width = (upper - lower).abs()
    return m, upper, lower, width


def _keltner(df: pd.DataFrame, n=20, m=2.0):
    ema = df["close"].ewm(span=n).mean()
    atr = df["atr"] if "atr" in df.columns else calc_atr(df)
    upper, lower = ema + m * atr, ema - m * atr
    width = (upper - lower).abs()
    return ema, upper, lower, width


def partial_rvol_from_kline(df: pd.DataFrame, k: dict, lookback=20, tf_seconds=3600):
    try:
        v_cur = float(k.get("v", 0.0))
        if v_cur <= 0:
            return 1.0
        open_ts = int(k.get("t", 0)) / 1000.0
        now_ts = time.time()
        frac = max(0.05, min(1.0, (now_ts - open_ts) / tf_seconds))
        vma = float(df["volume"].rolling(lookback).mean().iloc[-1])
        if vma <= 0:
            return 1.0
        return float(v_cur) / float(vma * frac)
    except Exception:
        return 1.0


def _body_ratio_from_bar(o, h, l, c):
    rng = (h - l) + 1e-9
    return abs(c - o) / rng


def detect_squeeze_immediate(
    df: pd.DataFrame, k: dict, rvol_partial_min=1.5, tf_seconds=3600
):
    # 1. Controlli base
    if not (HG_ENABLED and HG_SQZ_ON) or df is None or len(df) < 60:
        return None
    
    # 2. Verifica Squeeze (compressione)
    m, bbu, bbl, bbw = _bbands(df["close"])
    ke, kcu, kcl, kcw = _keltner(df)
    cond_squeeze = (bbw / (kcw.replace(0, np.nan))).fillna(np.inf) < 1.0
    
    # Basta che ci sia stato squeeze nelle ultime 10 barre (più flessibile)
    if not cond_squeeze.tail(HG_SQUEEZE_MIN_BARS).any(): 
        return None

    # 3. Dati candela attuale
    c = float(k.get("c", df["close"].iloc[-1]))
    o = float(k.get("o", df["open"].iloc[-1]))
    h = float(k.get("h", df["high"].iloc[-1]))
    l = float(k.get("l", df["low"].iloc[-1]))
    v = float(k.get("v", 0))
    taker_buy = float(k.get("V", 0)) # Volume aggressivo (Long)

    # 4. Calcolo RVOL Parziale
    rvol_p = partial_rvol_from_kline(df, k, tf_seconds=tf_seconds)
    
    # 5. ORDER FLOW RATIO (Ignition)
    # Se il volume è basso ma composto all'65% da acquisti al meglio, è un segnale.
    buy_ratio = taker_buy / v if v > 0 else 0.5
    
    # Logica Ignition: se c'è molta pressione (65%), accettiamo volume più basso (1.2)
    is_ignition_long = (rvol_p >= 1.2 and buy_ratio > 0.65) or (rvol_p >= rvol_partial_min)
    is_ignition_short = (rvol_p >= 1.2 and buy_ratio < 0.35) or (rvol_p >= rvol_partial_min)
    
    body_ratio = _body_ratio_from_bar(o, h, l, c)
    
    # 6. CALCOLO OMBRE (Wick) - Il filtro "Anti-Fakeout"
    body_size = abs(c - o)
    upper_wick = h - max(c, o)
    lower_wick = min(c, o) - l
    
    # Bande di Bollinger
    upper_band = float(bbu.iloc[-1])
    lower_band = float(bbl.iloc[-1])

    # Accettiamo candele anche in formazione se hanno un corpo decente (40%)
    if body_ratio >= 0.40: 
        
        # --- LONG ---
        if c > upper_band and is_ignition_long:
            # FILTRO WICK LONG: Se l'ombra sopra è > 20% del corpo, è una finta.
            if body_size > 0 and (upper_wick / body_size) > 0.20:
                return None # BLOCCA: Resistenza forte
                
            return {
                "dir": "long",
                "kind": "⚡ Squeeze→IGNITION (Pura)",
                "rvol": rvol_p,
                "body_ratio": body_ratio,
                "buy_pressure": f"{buy_ratio*100:.0f}%",
                "wick_check": "OK"
            }

        # --- SHORT ---
        if c < lower_band and is_ignition_short:
            # FILTRO WICK SHORT: Se l'ombra sotto è > 20% del corpo, è una finta.
            if body_size > 0 and (lower_wick / body_size) > 0.20:
                return None # BLOCCA: Supporto forte
                
            return {
                "dir": "short",
                "kind": "⚡ Squeeze→IGNITION (Pura)",
                "rvol": rvol_p,
                "body_ratio": body_ratio,
                "sell_pressure": f"{(1-buy_ratio)*100:.0f}%",
                "wick_check": "OK"
            }
            
    return None



def detect_squeeze_validated(df: pd.DataFrame, rvol_bar_min=HG_RVOL_BAR_MIN):
    if not (HG_ENABLED and HG_SQZ_ON) or df is None or len(df) < 60:
        return None
    m, bbu, bbl, bbw = _bbands(df["close"])
    ke, kcu, kcl, kcw = _keltner(df)
    cond_squeeze = (bbw / (kcw.replace(0, np.nan))).fillna(np.inf) < 1.0
    if not cond_squeeze.tail(HG_SQUEEZE_MIN_BARS).all():
        return None
    c = df["close"].iloc[-1]
    o = df["open"].iloc[-1]
    h = df["high"].iloc[-1]
    l = df["low"].iloc[-1]
    body_ratio = _body_ratio_from_bar(o, h, l, c)
    _, rvol = get_rvol_state(df)
    if rvol >= rvol_bar_min and body_ratio >= 0.65:
        if c > float(bbu.iloc[-1]):
            return {
                "dir": "long",
                "kind": "Squeeze→Breakout (VALIDATA)",
                "rvol": rvol,
                "body_ratio": body_ratio,
            }
        if c < float(bbl.iloc[-1]):
            return {
                "dir": "short",
                "kind": "Squeeze→Breakdown (VALIDATA)",
                "rvol": rvol,
                "body_ratio": body_ratio,
            }
    return None


def detect_nr7_validated(df: pd.DataFrame, rvol_min=HG_NR7_RVOL_MIN):
    if not (HG_ENABLED and HG_NR7_ON) or df is None or len(df) < 12:
        return None
    tr = (df["high"] - df["low"]).abs()
    is_nr7_prev = tr.iloc[-2] == tr.tail(7).min()
    if not is_nr7_prev:
        return None
    hi = df["high"].iloc[-2]
    lo = df["low"].iloc[-2]
    c = df["close"].iloc[-1]
    _, rvol = get_rvol_state(df)
    if rvol < rvol_min:
        return None
    if c > hi:
        return {"dir": "long", "kind": "NR7→Breakout (VALIDATA)", "rvol": rvol}
    if c < lo:
        return {"dir": "short", "kind": "NR7→Breakdown (VALIDATA)", "rvol": rvol}
    return None


def detect_rs_leader_breakout(symbol: str, df: pd.DataFrame):
    try:
        if not (HG_ENABLED and HG_RS_ON):
            return None
        df_btc = historical_data.get("BTCUSDT", {}).get("1h")
        if df is None or df_btc is None:
            return None
        if (
            len(df) < max(HG_LOOKBACK_RS, HG_LOOKBACK_HIGH) + 5
            or len(df_btc) < HG_LOOKBACK_RS
        ):
            return None
        rs = (df["close"] / df_btc["close"]).dropna().tail(HG_LOOKBACK_RS)
        if len(rs) < HG_LOOKBACK_RS:
            return None
        x = np.arange(len(rs))
        y = rs.values / rs.iloc[0]
        m, _ = np.polyfit(x, y, 1)
        if m < HG_RS_SLOPE_MIN:
            return None
        hi20_prev = df["high"].rolling(HG_LOOKBACK_HIGH).max().iloc[-2]
        c = df["close"].iloc[-1]
        _, rvol = get_rvol_state(df)
        if c > hi20_prev and rvol >= HG_RVOL_BAR_MIN:
            return {
                "dir": "long",
                "kind": "RS Leader + High20 Break (VALIDATA)",
                "rvol": float(rvol),
                "rs_slope": float(m),
            }
        return None
    except Exception:
        return None


def _estimate_hg_score(features: dict, df: pd.DataFrame, direction: str) -> float:
    try:
        rvol = float(features.get("rvol", 1.0))
        body = float(features.get("body_ratio", 0.0))
        rs_slope = float(features.get("rs_slope", 0.0))
        p = 0.0
        if rvol >= 3.0:
            p += 0.35
        elif rvol >= 2.0:
            p += 0.25
        elif rvol >= 1.5:
            p += 0.15
        if body >= 0.75:
            p += 0.25
        elif body >= 0.60:
            p += 0.15
        if get_trend(df) == ("rialzista" if direction == "long" else "ribassista"):
            p += 0.20
        if rs_slope >= HG_RS_SLOPE_MIN:
            p += 0.20

        # --- NUOVI FILTRI INDICATORI REALI ---
        try:
            # CMF: Flusso di denaro concorde con la direzione
            if "cmf" in df.columns:
                cmf_val = float(df["cmf"].iloc[-1])
                if (direction == "long" and cmf_val > 0.05) or (direction == "short" and cmf_val < -0.05):
                    p += 0.10  # Flusso di denaro forte e concorde
                elif (direction == "long" and cmf_val < -0.10) or (direction == "short" and cmf_val > 0.10):
                    p -= 0.10  # Flusso CONTRARIO → penalizza

            # Stochastic: Non entrare in zone estreme contro-trend
            if "stoch_k" in df.columns:
                stoch_val = float(df["stoch_k"].iloc[-1])
                if direction == "long" and stoch_val > 90:
                    p -= 0.08  # Overbought estremo, long rischioso
                elif direction == "short" and stoch_val < 10:
                    p -= 0.08  # Oversold estremo, short rischioso
                elif (direction == "long" and 20 < stoch_val < 70) or (direction == "short" and 30 < stoch_val < 80):
                    p += 0.05  # Zona favorevole

            # TSI: Momentum puro concorde
            if "tsi" in df.columns:
                tsi_val = float(df["tsi"].iloc[-1])
                if (direction == "long" and tsi_val > 5) or (direction == "short" and tsi_val < -5):
                    p += 0.05  # Momentum concorde
                elif (direction == "long" and tsi_val < -15) or (direction == "short" and tsi_val > 15):
                    p -= 0.05  # Momentum fortemente contrario
        except:
            pass

        return max(0.0, min(1.0, p))
    except Exception:
        return 0.0


def detect_hammer_integrated(
    df,
    volume_threshold=0.0,
    vwap_threshold=0.005,  # Ultra-preciso
    order_flow_threshold=0.0,
    min_body_to_shadow=0.28,  # Body più solido
    min_shadow_ratio=2.8,  # Shadow > 2.8x body
    max_opposite_shadow=0.20,  # Opposta molto corta
    lookback=10,
    symbol=None,
    tf="1h",
):
    """
    Rileva pattern Hammer e Inverted Hammer sulle ultime 10 candele,
    confermati da Volume Profile, VWAP e Order Flow.
    Integra divergenza RSI se disponibile.
    Restituisce una lista di pattern trovati con dettagli.
    """
    results = []

    if df is None or len(df) < lookback:
        return results

    vol_sum = df["volume"][-lookback:].sum()
    if vol_sum <= 0:
        return results

    vwap = (df["close"][-lookback:] * df["volume"][-lookback:]).sum() / vol_sum
    price_volume = df["close"][-lookback:] * df["volume"][-lookback:]
    max_volume_idx = price_volume.idxmax()
    max_volume_price = df["close"].loc[max_volume_idx]

    # Divergenza RSI (se disponibile)
    div_info = None
    if symbol and symbol in divergence_state and tf in divergence_state[symbol]:
        div_info = divergence_state[symbol][tf]
        div_dir = get_divergence_direction(div_info.get("rsi_advanced", "none"))
    else:
        div_dir = "none"

    for i in range(-lookback, 0):
        o = df["open"].iloc[i]
        h = df["high"].iloc[i]
        l = df["low"].iloc[i]
        c = df["close"].iloc[i]
        v = df["volume"].iloc[i]

        # ===== ORDER FLOW FIX =====
        if "taker_base" in df.columns:
            buy_volume = df["taker_base"].iloc[i]
            if buy_volume is None or np.isnan(buy_volume):
                buy_volume = 0.0
            sell_volume = v - buy_volume
            order_flow = buy_volume - sell_volume
            strong_order_flow = abs(order_flow) > order_flow_threshold
        else:
            buy_volume = 0.0
            sell_volume = 0.0
            order_flow = 0.0
            strong_order_flow = True

        rng = h - l + 1e-9
        body = abs(c - o)
        upper_shadow = h - max(c, o)
        lower_shadow = min(c, o) - l
        body_ratio = body / rng

        # Hammer
        is_hammer = (
            lower_shadow >= min_shadow_ratio * body
            and upper_shadow <= max_opposite_shadow * body
            and body_ratio >= min_body_to_shadow
        )

        # Inverted Hammer
        is_inverted = (
            upper_shadow >= min_shadow_ratio * body
            and lower_shadow <= max_opposite_shadow * body
            and body_ratio >= min_body_to_shadow
        )

        near_vwap = abs(c - vwap) < vwap_threshold * vwap

        vol_ma = df["volume"].rolling(20).mean().iloc[i] if len(df) >= 20 else np.nan
        if volume_threshold > 0:
            high_volume = v > volume_threshold
        else:
            high_volume = (not np.isnan(vol_ma)) and vol_ma > 0 and v >= 1.2 * vol_ma

        if (is_hammer or is_inverted) and near_vwap and high_volume and strong_order_flow:
            pattern = "hammer" if is_hammer else "inverted_hammer"

            # Direzione suggerita dalla divergenza, se presente
            direction = None
            if div_dir == "bull" and pattern == "hammer":
                direction = "long"
            elif div_dir == "bear" and pattern == "inverted_hammer":
                direction = "short"

            # accetta solo se divergenza coerente OR rvol>=1.6 con trend coerente
            trend_ok = get_trend(df) == (
                "rialzista" if pattern == "hammer" else "ribassista"
            )
            _, rvol_now = get_rvol_state(df)

            if not ((direction is not None) or (rvol_now >= 1.6 and trend_ok)):
                continue

            results.append(
                {
                    "index": df.index[i],
                    "open": o,
                    "high": h,
                    "low": l,
                    "close": c,
                    "volume": v,
                    "buy_volume": buy_volume,
                    "order_flow": order_flow,
                    "pattern": pattern,
                    "near_vwap": near_vwap,
                    "max_volume_price": max_volume_price,
                    "divergence": div_info if div_info else {},
                    "suggested_direction": direction,
                }
            )

    return results
def safe_loc(index, value):
    try:
        if value not in index:
            return None
        loc = index.get_loc(value)
        if isinstance(loc, slice):
            return loc.start
        if isinstance(loc, (list, np.ndarray)):
            return loc[0] if len(loc) > 0 else None
        return int(loc)
    except Exception:
        return None

# ============================
# PLOT
# ============================


def plot_candles_v13(df, symbol, tf, div_type=None, div_points=None):
    try:
        if df is None or len(df) < 20:
            return None
        df = df.copy().dropna(how="all")
        if df.empty:
            return None
        tail_size = 150 
        if div_points and div_type and div_type != "none":
            try:
                p1_t = div_points.get("p1", (None,))[0]
                p2_t = div_points.get("p2", (None,))[0]
                for pt in [p1_t, p2_t]:
                    if pt is not None and pt in df.index:
                        pos = df.index.get_loc(pt)
                        needed = len(df) - pos + 10
                        tail_size = max(tail_size, needed)
            except Exception:
                pass
        df = df.tail(tail_size)
        plt.style.use("default")
        fig = plt.figure(figsize=(16, 10), facecolor="white")
        ax_price = plt.subplot2grid((6, 1), (0, 0), rowspan=3, facecolor="white")
        ax_rsi = plt.subplot2grid(
            (6, 1), (3, 0), rowspan=2, sharex=ax_price, facecolor="white"
        )
        ax_vol = plt.subplot2grid(
            (6, 1), (5, 0), rowspan=1, sharex=ax_price, facecolor="white"
        )
        for idx, (t, row) in enumerate(df.iterrows()):
            o, h, l, c = row["open"], row["high"], row["low"], row["close"]
            color = "#2ca02c" if c >= o else "#d62728"
            from matplotlib.patches import Rectangle

            body_height = abs(c - o)
            body_bottom = min(o, c)
            ax_price.add_patch(
                Rectangle(
                    (idx - 0.3, body_bottom),
                    0.6,
                    body_height,
                    facecolor=color,
                    edgecolor="black",
                    linewidth=0.5,
                    alpha=0.9,
                )
            )
            ax_price.plot([idx, idx], [l, h], color="black", linewidth=0.8)
        ax_price.set_title(
            f"📊 {symbol} — {tf} — Ultimi 150 candele",
            color="black",
            fontsize=16,
            fontweight="bold",
            pad=15,
        )
        ax_price.grid(True, alpha=0.3, color="gray", linestyle="--")
        ax_price.tick_params(colors="black", labelsize=10)
        ax_price.set_ylabel("Prezzo", fontsize=11, fontweight="bold")
        if div_type and div_points and div_type != "none":
            try:
                p1_t, p1_v = div_points.get("p1", (None, None))
                p2_t, p2_v = div_points.get("p2", (None, None))
                if p1_t is None or p2_t is None:
                    raise ValueError("punti divergenza None")
                if p1_t not in df.index or p2_t not in df.index:
                    raise ValueError(f"punti fuori dal df: p1={p1_t}, p2={p2_t}")
                i1 = df.index.get_loc(p1_t)
                i2 = df.index.get_loc(p2_t)
                if isinstance(i1, slice): i1 = i1.start
                if isinstance(i2, slice): i2 = i2.start
                if isinstance(i1, (list, np.ndarray)): i1 = int(i1[0])
                if isinstance(i2, (list, np.ndarray)): i2 = int(i2[0])
                color_div = "#2ca02c" if "bullish" in div_type else "#d62728"
                ax_price.scatter(
                    [i1, i2],
                    [p1_v, p2_v],
                    color=color_div,
                    s=100,
                    zorder=5,
                    edgecolors="black",
                    linewidths=2,
                )
                ax_price.plot(
                    [i1, i2],
                    [p1_v, p2_v],
                    color=color_div,
                    linewidth=3,
                    linestyle="--",
                    alpha=0.8,
                    zorder=4,
                )
                ax_price.text(
                    i2,
                    p2_v,
                    f"  {div_type}",
                    fontsize=10,
                    color=color_div,
                    fontweight="bold",
                    va="center",
                )
            except Exception as e:
                logger.warning(f"[PLOT-DIV-PREZZO] {symbol} {tf}: {e}")
        if "rsi" in df.columns:
            rsi_values = df["rsi"].values
            ax_rsi.plot(
                range(len(df)),
                rsi_values,
                color="#1f77b4",
                linewidth=2,
                label="RSI(14)",
            )
            ax_rsi.axhline(
                70, color="#d62728", linestyle="--", alpha=0.5, linewidth=1.5
            )
            ax_rsi.axhline(
                30, color="#2ca02c", linestyle="--", alpha=0.5, linewidth=1.5
            )
            ax_rsi.fill_between(range(len(df)), 30, 70, alpha=0.05, color="gray")
            ax_rsi.text(
                len(df) - 1, 70, " 70", va="center", fontsize=9, color="#d62728"
            )
            ax_rsi.text(
                len(df) - 1, 30, " 30", va="center", fontsize=9, color="#2ca02c"
            )
            ax_rsi.set_title("RSI", color="black", fontsize=12, fontweight="bold")
            ax_rsi.legend(loc="upper left", fontsize=10)
            ax_rsi.grid(True, alpha=0.3, color="gray", linestyle="--")
            ax_rsi.tick_params(colors="black", labelsize=9)
            ax_rsi.set_ylim(0, 100)
            ax_rsi.set_ylabel("RSI", fontsize=10, fontweight="bold")
            if div_type and div_points:
                try:
                    r1_t, r1_v = div_points["r1"]
                    r2_t, r2_v = div_points["r2"]
                    if r1_t not in df.index or r2_t not in df.index:
                        pass  # salta solo la divergenza RSI, ma il grafico resta
                    else:
                        i1 = safe_loc(df.index, r1_t)
                        i2 = safe_loc(df.index, r2_t)
                        if i1 is None or i2 is None:
                            pass  # salta solo la divergenza RSI
                        else:
                        
                            color_div = "#2ca02c" if "bullish" in div_type else "#d62728"
                            ax_rsi.scatter(
                                [i1, i2],
                                [r1_v, r2_v],
                                color=color_div,
                                s=100,
                                zorder=5,
                                edgecolors="black",
                                linewidths=2,
                            )
                            ax_rsi.plot(
                                [i1, i2],
                                [r1_v, r2_v],
                                color=color_div,
                                linewidth=3,
                                linestyle="--",
                                alpha=0.8,
                                zorder=4,
                            )
                except Exception as e:
                    logger.debug(f"Errore plot divergenza RSI: {e}")
        if "volume" in df.columns:
            volumes = df["volume"].values
            colors_vol = [
                "#2ca02c" if df["close"].iloc[i] >= df["open"].iloc[i] else "#d62728"
                for i in range(len(df))
            ]
            ax_vol.bar(range(len(df)), volumes, color=colors_vol, alpha=0.6, width=0.8)
            vol_ma = df["volume"].rolling(20).mean().values
            ax_vol.plot(
                range(len(df)), vol_ma, color="orange", linewidth=2, label="MA(20)"
            )
            ax_vol.set_title("Volume", color="black", fontsize=12, fontweight="bold")
            ax_vol.legend(loc="upper left", fontsize=10)
            ax_vol.grid(True, alpha=0.3, color="gray", linestyle="--")
            ax_vol.tick_params(colors="black", labelsize=9)
            ax_vol.set_ylabel("Volume", fontsize=10, fontweight="bold")
        plt.tight_layout()
        buf = BytesIO()
        fig.savefig(buf, format="png", dpi=150, facecolor="white", edgecolor="none")
        buf.seek(0)
        plt.close(fig)
        return buf
    except Exception as e:
        try:
            plt.close("all")
        except:
            pass
        logger.error(f"Errore plot_candles_v13 {symbol}: {e}")
        return None


# ============================
# LOGBOOK
# ============================


def log_signal(
    symbol,
    tf,
    direction,
    pre_score,
    score_ai,
    combined_score,
    forza,
    successo,
    div_rsi_adv,
    trend_1h,
    volume_state,
    rvol,
    regime_btc,
    regime_eth,
    entry=None,
    sl=None,
    tp1=None,
    tp2=None,
    tp3=None,
    gate_reason="",
    n_confirms=0,
):
    try:
        row = {
            "time": datetime.datetime.utcnow().isoformat(),
            "symbol": symbol,
            "tf": tf,
            "direction": direction,
            "pre_score": pre_score,
            "score_ai": score_ai,
            "combined_score": combined_score,
            "forza": forza,
            "successo": successo,
            "div_rsi_adv": div_rsi_adv,
            "trend_1h": trend_1h,
            "volume_state": volume_state,
            "rvol": rvol,
            "regime_btc": regime_btc,
            "regime_eth": regime_eth,
            "entry": entry,
            "sl": sl,
            "tp1": tp1,
            "tp2": tp2,
            "tp3": tp3,  
            "gate_reason": gate_reason,
            "n_confirms": n_confirms,
        }
        with LOGBOOK_LOCK:
            file_exists = os.path.isfile(LOGBOOK_FILE)
            with open(LOGBOOK_FILE, "a", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=row.keys())
                if not file_exists:
                    writer.writeheader()
                writer.writerow(row)
    except Exception as e:
        logger.error(f"Errore log_signal {symbol} {tf}: {e}")


# ============================
# VALIDAZIONE DIVERGENZA & BREAKOUT
# ============================

def is_recent_divergence(div_index, df_1h, max_candles=3):
    try:
        if div_index is None:
            return False
        if div_index not in df_1h.index:
            return False
        div_pos = df_1h.index.get_loc(div_index)
        last_pos = len(df_1h) - 1
        distance = last_pos - div_pos
        if distance > max_candles:
            return False
        div_time = div_index
        if isinstance(div_time, pd.Timestamp):
            div_time = div_time.to_pydatetime()
        now = datetime.datetime.utcnow()
        time_elapsed_hours = (now - div_time).total_seconds() / 3600
        if time_elapsed_hours > DIVERGENCE_MAX_AGE_HOURS:
            return False
        return True
    except Exception:
        return False


def is_recent_breakout(div_index, df_1h, breakline_price, div_rsi_adv, max_candles=2, tf="1h"): 
    try:
        if div_index not in df_1h.index:
            return False
        start_pos = df_1h.index.get_loc(div_index)
        end_pos = min(len(df_1h), start_pos + max_candles + 1)
        if start_pos >= end_pos:
            return False
        window = df_1h.iloc[start_pos + 1 : end_pos]
        if window.empty:
            return False

        cfg = BREAKOUT_RULES.get(tf, BREAKOUT_RULES["1h"])
        vol_min = cfg["vol_min"]
        break_mult = cfg["break_mult"]
        min_closes = cfg["min_closes"]
        atr_mult = cfg["atr_mult"]

        # volume breakout (PATCH QUANT: Abbassato a 0.6 per sbloccare i segnali)
        if "volume" in df_1h.columns:
            vma = df_1h["volume"].rolling(20).mean().iloc[-1]
            v_last = df_1h["volume"].iloc[-1]
            # Ignoriamo vol_min e usiamo 0.6 (60% della media) perché ci fidiamo dello Z-Score
            if vma > 0 and (v_last / vma) < 0.6: 
                return False

        if div_rsi_adv.startswith("bullish"):
            if (window["close"] > breakline_price * break_mult).sum() < min_closes:
                return False
        if div_rsi_adv.startswith("bearish"):
            if (window["close"] < breakline_price * (2 - break_mult)).sum() < min_closes:
                return False

        # ATR filter
        if "atr" in df_1h.columns:
            atr = df_1h["atr"].iloc[-1]
            if atr > 0:
                if div_rsi_adv.startswith("bullish"):
                    if (window["close"].max() - breakline_price) < (atr_mult * atr):
                        return False
                if div_rsi_adv.startswith("bearish"):
                    if (breakline_price - window["close"].min()) < (atr_mult * atr):
                        return False

        return True
    except Exception:
        return False

# ============================
# AI — CONTEXT + CALL SAFE
# ============================


def _clamp(x, lo=0.0, hi=1.0):
    try:
        return max(lo, min(hi, float(x)))
    except:
        return lo


def build_ai_context_for_signal(
    symbol,
    tf,
    direction,
    div_state,
    confirms,
    trend_1h,
    volume_state,
    rvol,
    global_regime,
    risk_regime,
    entry=None,
    sl=None,
    tp1=None,
    tp2=None,
    tp3=None,
):
    try:
        ctx = {
            "symbol": symbol,
            "timeframe": tf,
            "direction": direction,
            "divergence": {
                "type": div_state.get("rsi_advanced", "none"),
                "rsi_quality": float(div_state.get("rsi_quality", 0)),
                "early_quality": float(div_state.get("div_early_quality", 0)),
            },
            "confirms": {
                k: bool(confirms.get(k, False))
                for k in [
                    "rsi_over_under_50",
                    "rsi_ma_cross",
                    "ema_stack",
                    "ema_slope",
                    "macd_cross",
                    "macd_hist",
                    "vol_breakout",
                    "atr_expansion",
                    "vwap_reclaim",
                    "momentum_obv",
                    "rvol_ok",
                    "structure_hh_hl",
                ]
            },
            "market": {
                "trend_1h": trend_1h,
                "volume_state": volume_state,
                "rvol": float(rvol),
                "global_regime": global_regime,
                "risk_regime": risk_regime,
            },
            "levels": {
                "entry": float(entry) if entry is not None else None,
                "sl": float(sl) if sl is not None else None,
                "tp1": float(tp1) if tp1 is not None else None,
                "tp2": float(tp2) if tp2 is not None else None,
                "tp3": float(tp3) if tp3 is not None else None,
            },
        }
        return json.dumps(ctx, ensure_ascii=False)
    except Exception as e:
        logger.debug(f"[AI-CTX] Errore: {e}")
        return (
            f"symbol={symbol}, timeframe={tf}, direction={direction}, "
            f"trend_1h={trend_1h}, volume_state={volume_state}, rvol={rvol}, "
            f"global_regime={global_regime}, risk_regime={risk_regime}, "
            f"entry={entry}, sl={sl}, tp1={tp1}, tp2={tp2}, tp3={tp3}"
            
        )
def build_ai_context_simple(
    symbol,
    tf,
    direction,
    pattern_type,
    features,
    df=None,
    entry=None,
    sl=None,
    tp1=None,
    tp2=None,
    tp3=None,
):
    try:
        ctx = {
            "mode": "PATTERN_UPDATE",
            "symbol": symbol,
            "timeframe": tf,
            "direction": direction,
            "pattern": pattern_type,
            "features": features,
            "levels": {
                "entry": float(entry) if entry else None,
                "sl": float(sl) if sl else None,
                "tp1": float(tp1) if tp1 else None,
                "tp3": float(tp3) if tp3 else None,
            },
        }
        
        if df is not None:
            ctx["trend_1h"] = get_trend(df)
            ctx["rvol_state"] = get_rvol_state(df)[0]
            
        return json.dumps(ctx, ensure_ascii=False)
    except Exception as e:
        logger.error(f"[AI-CTX-SIMPLE] Errore: {e}")
        return f"Analisi pattern {pattern_type} per {symbol} {tf}"



def ai_rating(score: int) -> str:
    try:
        s = int(score)
        if s >= 70:
            return "ALTA"
        if s >= 40:
            return "MEDIA"
        return "BASSA"
    except Exception:
        return "MEDIA"


# ============================
# PERLE — INVIO
# ============================

def _async_plot_and_send(msg, df_copy, symbol, tf, div_type, div_points, caption, photo_path):
    """
    Esegue l'invio del messaggio e la generazione del grafico in un thread separato.
    Usa df_copy per evitare conflitti con i dati in aggiornamento realtime.
    """
    try:
        # 1. Invia il testo immediatamente (così arriva subito all'utente)
        send_telegram(msg)
        
        # 2. Genera il grafico (operazione pesante, ora isolata nel thread)
        buf = plot_candles_v13(df_copy, symbol, tf, div_type=div_type, div_points=div_points)
        
        # 3. Salva e invia la foto
        if buf:
            with open(photo_path, "wb") as f:
                f.write(buf.getvalue())
                
            send_telegram_photo(photo_path, caption=caption)
            
            # Pulizia sicura del file
            try:
                os.remove(photo_path)
            except Exception:
                pass
    except Exception as e:
        logger.error(f"Errore thread async plot/send per {symbol}: {e}")



# ============================
# LIVELLI QUANTISTICI (SMART TARGETS)
# ============================

def _levels_from_df(df: pd.DataFrame, direction: str):
    """
    Calcola SL e TP basati sulla volatilità (ATR) e sulla struttura di mercato.
    Usa la volatilità storica per adattare l'ampiezza dei target.
    """
    try:
        if df is None or len(df) < 20: return None, None, None, None, None
        
        price = df["close"].iloc[-1]
        high = df["high"].iloc[-1]
        low = df["low"].iloc[-1]
        
        # 1. Calcolo Volatilità Locale (ATR)
        if "atr" in df.columns:
            atr = df["atr"].iloc[-1]
            atr_prev = df["atr"].iloc[-2]
        else:
            atr = (high - low)
            atr_prev = atr
            
        # 2. Fattore di Espansione (Se la volatilità sale, allarga i target)
        vol_factor = 1.0
        if atr > atr_prev: vol_factor = 1.2
        elif atr < atr_prev: vol_factor = 0.8
        
        # 3. Stop Loss Strutturale (Swing Low/High recenti)
        lookback = 10
        if direction == "long":
            swing_low = df["low"].tail(lookback).min()
            # Stop minimo garantito (0.5 ATR) e massimo (3 ATR)
            sl = min(max(swing_low, price - 3*atr), price - 0.5*atr)
            risk = price - sl
            
            tp1 = price + (risk * 1.0)
            tp2 = price + (risk * 2.0 * vol_factor)
            tp3 = price + (risk * 3.5 * vol_factor)
            
        else: # Short
            swing_high = df["high"].tail(lookback).max()
            sl = max(min(swing_high, price + 3*atr), price + 0.5*atr)
            risk = sl - price
            
            tp1 = price - (risk * 1.0)
            tp2 = price - (risk * 2.0 * vol_factor)
            tp3 = price - (risk * 3.5 * vol_factor)
            
        return price, sl, tp1, tp2, tp3

    except Exception:
        return None, None, None, None, None


def send_hidden_gem(symbol, tf, direction, kind, features, df, phase_label):
    entry, sl, tp1, tp2, tp3 = _levels_from_df(df, direction)
    filters = get_symbol_filters(symbol)
    try:
        if entry: entry = round_to(entry, filters["tickSize"])
        if sl: sl = round_to(sl, filters["tickSize"])
        if tp1: tp1 = round_to(tp1, filters["tickSize"])
        if tp2: tp2 = round_to(tp2, filters["tickSize"])
        if tp3: tp3 = round_to(tp3, filters["tickSize"])
    except: pass

    # NUOVO: Volume Profile POC
    poc = calc_volume_profile_poc(df)
    poc_info = ""
    if poc is not None:
        try:
            poc = round_to(poc, filters["tickSize"])
        except:
            pass
        poc_dist_pct = abs(entry - poc) / entry * 100 if entry and entry > 0 else 0
        poc_info = f"📍 POC (Vol Profile): {poc} ({poc_dist_pct:.1f}% dist)\n"

    score = 0.0
    rvol = float(features.get("rvol", 1.0))
    score += min((rvol - 1.0) / 3.0, 0.35) if rvol > 1 else 0.0
    score += 0.15 if features.get("body_ratio", 0) >= 0.6 else 0.0
    score += 0.20 if features.get("rs_slope", 0) >= HG_RS_SLOPE_MIN else 0.0
    
    ai_forza = "media"
    
    # Filtro AI V16
    if AI_ENABLED and AI_SYNC_ON_SIGNAL:
        try:
            ai_ctx = build_ai_context_simple(symbol, tf, direction, kind, features, df, entry, sl, tp1, tp2, tp3)
            ai_res = call_ai_safe(symbol, tf, score, direction, ai_ctx)
            
            # SOGLIA CECCHINO: Scarta i segnali con successo < 78 o se l'AI dice che è "debole"
            if int(ai_res.get('successo', 0)) < 78 or ai_res.get('forza', 'media').lower() == 'debole':
                logger.info(f"⛔ [HG-VETO] {symbol} scartato da AI Sniper (Segnale debole o sotto 78%).")
                return 

            commento_ai = ai_res.get('commento', '')
            ai_forza = ai_res.get('forza', 'media').lower()
        except: 
            commento_ai = "AI non disponibile"
    else: 
        commento_ai = ""

    # ==========================================
    # --- LOGICA 2 LIVELLI DI FORZA (Forte / Media) ---
    # ==========================================
    if AI_ENABLED and AI_SYNC_ON_SIGNAL and commento_ai != "AI non disponibile":
        if ai_forza == "forte": forza_label = "🟢 FORTE"
        else: forza_label = "🟡 MEDIA"
    else:
        # Fallback matematico se l'AI è spenta
        if score >= 0.60: forza_label = "🟢 FORTE"
        else: forza_label = "🟡 MEDIA"

    emoji_dir = "🚀" if direction == "long" else "🔻"
    
    msg = f"{emoji_dir} *V16 SNIPER GEM — {kind}* — {symbol} {tf}\n\n"
    msg += f"💥 *Fase:* {phase_label}\n"
    msg += f"📦 RVOL: {features.get('rvol',0):.2f} | 📊 Score Tech: {score:.2f}\n"
    msg += f"💪 *Forza Segnale:* {forza_label}\n"
    msg += poc_info
    
    if commento_ai:
        msg += f"🤖 AI Check: {commento_ai}\n"
        
    msg += "\n🎯 *LIVELLI OPERATIVI*\n"
    
    # ==========================================
    # --- STAMPA DI TUTTI I LIVELLI (TP1, TP2, TP3) ---
    # ==========================================
    if entry and sl and tp1 and tp2 and tp3:
        risk = abs(entry - sl) if abs(entry - sl) > 0 else 0.0001
        rr1 = abs(tp1 - entry) / risk
        rr2 = abs(tp2 - entry) / risk
        rr3 = abs(tp3 - entry) / risk
        
        msg += f"▪️ Entry: `{entry}`\n"
        msg += f"▪️ SL: `{sl}`\n"
        msg += f"▪️ TP1: `{tp1}` (R:R 1:{rr1:.1f})\n"
        msg += f"▪️ TP2: `{tp2}` (R:R 1:{rr2:.1f})\n"
        msg += f"▪️ TP3: `{tp3}` (R:R 1:{rr3:.1f})"
    else: 
        msg += f"▪️ Entry: `{entry}`\n⚠️ Volatilità insufficiente per SL/TP completi"

    # QUANT ENGINE DATA
    regime_label, _ = get_probabilistic_regime(df)
    confluence = calc_weighted_confluence(df, direction)
    probability = calculate_signal_probability(df, direction, features, regime_label, tf)
    size_usd, risk_pct, sizing_details = calc_dynamic_position_size(df, direction, entry, sl, probability)

    msg += f"\n📐 QUANT ENGINE:\n"
    msg += f"  🎯 P(successo): {probability:.0%}\n"
    msg += f"  📊 Confluenza: {confluence}/100\n"
    msg += f"  🏛️ Regime: {regime_label}\n"
    msg += f"  💰 Size: ${size_usd:.2f} ({risk_pct*100:.2f}% risk)\n"
    msg += f"  📈 R:R: {sizing_details.get('reward_risk', 2.0):.1f}:1\n"

    # ==========================================
    # --- SISTEMA ASINCRONO VELOCE ---
    # ==========================================
    df_safe_copy = df.copy() 
    phase_str = 'imm' if 'IMMEDIATA' in phase_label else 'val'
    photo_path = f"chart_hg_{symbol}_{tf}_{phase_str}_{int(time.time())}.png"
    caption = f"📊 {symbol} {tf} — Hidden Gem: {kind}"

    t = threading.Thread(
        target=_async_plot_and_send,
        args=(msg, df_safe_copy, symbol, tf, features.get("div_type"), features.get("div_points"), caption, photo_path),
        daemon=True,
        name=f"NOTIFY_HG_{symbol}"
    )
    t.start()

    # LOG TO JOURNAL
    try:
        micro_sc = 0
        ai_f = ai_forza if 'ai_forza' in dir() else "n/a"
        ai_s = int(ai_res.get('successo', 0)) if 'ai_res' in dir() and ai_res else 0
        log_trade_to_journal(
            symbol=symbol, tf=tf, direction=direction, kind=kind,
            entry=entry, sl=sl, tp1=tp1, tp2=tp2, tp3=tp3,
            probability=probability, confluence_score=confluence,
            regime=regime_label, micro_score=micro_sc,
            ai_forza=ai_f, ai_successo=ai_s,
            position_size_usd=size_usd, risk_pct=risk_pct
        )
        update_risk_after_signal(risk_pct)
    except Exception as e:
        logger.debug(f"[JOURNAL] Errore post-signal: {e}")


# ============================
# PROCESS CLOSED CANDLE (V16 SNIPER EDITION)
# ============================
def process_closed_candle(symbol, tf, k):
    try:
        df = historical_data.get(symbol, {}).get(tf)
        if df is None or len(df) < 50: return

        ts = k.get("t")
        if not ts: return

        with LAST_PROCESSED_LOCK:
            if LAST_PROCESSED_CLOSED.get(f"{symbol}_{tf}") == ts: return
            LAST_PROCESSED_CLOSED[f"{symbol}_{tf}"] = ts

        # 1. HAMMER (Pattern Puro)
        hammer_results = detect_hammer_integrated(df, symbol=symbol, tf=tf)
        for res in hammer_results:
            if res.get("index") == df.index[-1]:
                pattern = res.get("pattern", "")
                direction = "long" if pattern == "hammer" else "short"
                msg = f"🔨 Pattern *{pattern.upper().replace('_',' ')}* rilevato su {symbol} {tf}\n"
                # Invio testuale reso asincrono per evitare lag di rete!
                threading.Thread(target=send_telegram, args=(msg,), daemon=True).start()

        # 2. DIVERGENZE V16
        div_state = divergence_state.get(symbol, {}).get(tf, {})
        div_type = div_state.get("rsi_advanced", "none")
        div_index = div_state.get("div_index", None)
        
        if div_type == "none" or div_index is None: return 
        if not is_good_trading_hour(): return
        
        if div_type.startswith("bullish"): direction = "long"
        elif div_type.startswith("bearish"): direction = "short"
        else: return

        max_age = DIVERGENCE_MAX_AGE_BY_TF.get(tf, 3)
        if not is_recent_divergence(div_index, df, max_candles=max_age): return

        # NUOVO: Richiedi conferma MACD divergence
        macd_div = detect_macd_divergence(df)
        rsi_div_dir = div_state.get("rsi_advanced", "none")

        # Se c'è divergenza RSI bullish, verifica che anche MACD confermi (o almeno non contraddica)
        if rsi_div_dir.startswith("bullish") and macd_div == "bearish":
            return  # RSI dice bullish ma MACD dice bearish → contraddizione, scarta
        if rsi_div_dir.startswith("bearish") and macd_div == "bullish":
            return  # RSI dice bearish ma MACD dice bullish → contraddizione, scarta

        # BONUS: Se entrambi concordano, il segnale è molto più forte
        macd_confirms = (rsi_div_dir.startswith("bullish") and macd_div == "bullish") or \
                        (rsi_div_dir.startswith("bearish") and macd_div == "bearish")

        # --- REGIME DETECTION & Z-SCORE ADATTIVO ---
        current_regime, regime_scores = get_probabilistic_regime(df)
        
        # BLOCCO SHOCK: Se c'è panico, stai fermo.
        if current_regime == "SHOCK":
            logger.info(f"⛔ [{symbol}] SNIPER ABORT: Volatilità Shock")
            return

        # FILTRO TREND: Mai contro trend forte
        if current_regime == "LONG_TREND" and direction == "short": return 
        if current_regime == "SHORT_TREND" and direction == "long": return

        # FILTRO Z-SCORE ADATTIVO (Il cuore del Cecchino)
        z_curr = calculate_z_score_series(df["close"]).iloc[-1]
        
        # Se il mercato è laterale, vogliamo estremi ASSOLUTI (>2.0 o <-2.0)
        # Se il mercato è in trend, accettiamo pullback normali (>0.0)
        z_threshold = 2.0 if current_regime == "MEAN_REVERSION" else 0.0
        
        if direction == "long" and z_curr > -z_threshold: return # Prezzo non abbastanza basso
        if direction == "short" and z_curr < z_threshold: return # Prezzo non abbastanza alto

        # --- AI SCOUT (Filtro Rapido) ---
        if AI_ENABLED:
            scout_ctx = f"Regime: {current_regime}, Z: {z_curr:.2f}, Trend: {get_trend(df)}"
            res_scout = call_ai_safe(symbol, tf, 0, direction, scout_ctx, role="scout")
            if int(res_scout.get("successo", 0)) < 70:
                logger.info(f"⛔ SCOUT VETO: {res_scout.get('commento')}")
                return

        # --- EXECUTION & RISK MANAGEMENT ---
        entry, sl, tp1, tp2, tp3 = _levels_from_df(df, direction)
        
        # Calcolo Risk/Reward Base
        risk = abs(entry - sl) if sl and entry != sl else 0.0001
        reward = abs(tp2 - entry) if tp2 else 0
        rr_ratio = reward / risk
        
        # CECCHINO: Scarta se non paga almeno MIN_RR volte il rischio al TP2
        if rr_ratio < MIN_RR:
            logger.info(f"⛔ R:R INSUFFICIENTE ({rr_ratio:.2f}) su {symbol}")
            return
            
        win_rate_est = 0.65 
        kelly_usdt, kelly_pct = calculate_kelly_position(win_rate_est, rr_ratio, ACCOUNT_BALANCE)

        # --- AI ANALYST (Filtro Finale) ---
        ai_comm = ""
        ai_rate = "N/A"
        forza_label = "🟡 MEDIA"
        
        if AI_ENABLED:
            analyst_ctx = f"SNIPER SETUP. Regime: {current_regime}. RR: {rr_ratio:.2f}. Z-Score: {z_curr:.2f}. Kelly: {kelly_pct:.1f}%"
            res_analyst = call_ai_safe(symbol, tf, 0, direction, analyst_ctx, role="analyst")
            
            # SOGLIA CECCHINO ALZATA A 65: Solo segnali di media/alta qualità
            if int(res_analyst.get("successo", 0)) < 65:
                logger.info(f"⛔ ANALYST VETO (<65%): {res_analyst.get('commento')}")
                return
                
            ai_comm = res_analyst.get("commento", "")
            ai_rate = ai_rating(int(res_analyst.get("successo", 0)))
            
            # Assegnazione Forza Segnale da AI
            ai_forza = res_analyst.get("forza", "media").lower()
            if ai_forza == "forte": forza_label = "🟢 FORTE"
            elif ai_forza == "debole": forza_label = "🔴 DEBOLE"
            else: forza_label = "🟡 MEDIA"

        # --- CREAZIONE MESSAGGIO TELEGRAM ---
        emoji = "🎯"
        msg = f"{emoji} *V16 SNIPER SIGNAL* — {symbol} {tf}\n\n"
        msg += f"🏛️ Regime: *{current_regime}*\n"
        msg += f"⚛️ Z-Score: `{z_curr:.2f}` (Adattivo)\n"
        msg += f"💰 Kelly Size: `{kelly_usdt:.0f} USDT` ({kelly_pct:.1f}%)\n"
        msg += f"💪 *Forza Segnale:* {forza_label}\n\n"
        
        msg += "🎯 *LIVELLI OPERATIVI*\n"
        if entry and sl and tp1 and tp2 and tp3:
            rr1 = abs(tp1 - entry) / risk
            rr2 = abs(tp2 - entry) / risk
            rr3 = abs(tp3 - entry) / risk
            
            msg += f"▪️ Entry: `{entry:.5f}`\n"
            msg += f"▪️ SL: `{sl:.5f}`\n"
            msg += f"▪️ TP1: `{tp1:.5f}` (R:R 1:{rr1:.1f})\n"
            msg += f"▪️ TP2: `{tp2:.5f}` (R:R 1:{rr2:.1f})\n"
            msg += f"▪️ TP3: `{tp3:.5f}` (R:R 1:{rr3:.1f})\n\n"
        else:
            msg += f"▪️ Entry: `{entry:.5f}`\n⚠️ Volatilità insufficiente per SL/TP\n\n"
            
        msg += f"🤖 AI Sniper ({ai_rate}): {ai_comm}"
        
        # ==========================================
        # --- NUOVO SISTEMA ASINCRONO (PUNTO 2) ---
        # ==========================================
        
        # Aggiorna la memoria RAM velocemente
        last_signal_time[symbol] = time.time()
        
        # Salva nel Database in background (Evita I/O Disk block)
        threading.Thread(target=db_save_last_signal_time, daemon=True).start()
        
        # Copia sicura dei dati
        df_safe_copy = df.copy()
        div_points = div_state.get("points")
        photo_path = f"chart_sniper_{symbol}_{tf}_{int(time.time())}.png"
        caption = f"🎯 {symbol} {tf} — V16 Sniper"

        # Lancia il thread pesante e libera istantaneamente il WebSocket
        t = threading.Thread(
            target=_async_plot_and_send,
            args=(msg, df_safe_copy, symbol, tf, div_type, div_points, caption, photo_path),
            daemon=True,
            name=f"NOTIFY_SNIPER_{symbol}"
        )
        t.start()

    except Exception as e:
        logger.error(f"Err {symbol}: {e}")
        traceback.print_exc()


# ============================
# UPDATE REALTIME + HG (V16 OPTIMIZED)
# ============================

def update_realtime(symbol, tf, k):
    lock = get_symbol_lock(symbol)
    with lock:
        try:
            if symbol not in historical_data or tf not in historical_data[symbol]:
                # init on demand for HG 1h
                if tf == "1h" and HG_ENABLED and HG_MONITOR_ALL:
                    try:
                        kl = client.futures_klines(
                            symbol=symbol, interval="1h", limit=300
                        )
                        df_1h = klines_to_df(kl)
                        df_1h = compute_indicators(df_1h)
                        historical_data.setdefault(symbol, {})["1h"] = df_1h
                    except Exception:
                        return
                else:
                    return

            df = historical_data[symbol][tf]
            ts = k.get("t")
            closed = bool(k.get("x"))
            if not ts or ts <= 0:
                return
                
            try:
                open_time = datetime.datetime.fromtimestamp(ts / 1000.0)
            except Exception:
                return
                
            try:
                o = float(k["o"])
                h = float(k["h"])
                l = float(k["l"])
                c = float(k["c"])
                v = float(k["v"])
                tb = float(k.get("V", 0.0))  # taker buy base volume (solo Hammer)
            except Exception:
                return
                
            try:
                # --- PUNTO 3: OTTIMIZZAZIONE PANDAS (ADDIO pd.concat) ---
                # Usiamo .loc per l'aggiornamento E per l'inserimento. È molto più efficiente.
                df.loc[open_time, ["open", "high", "low", "close", "volume", "taker_base"]] = [o, h, l, c, v, tb]
            except Exception as e:
                logger.error(f"[DF-UPDATE] Errore inserimento dati per {symbol}: {e}")
                return
                
            # Mantieni la lunghezza fissa per non far esplodere la RAM, 
            # ma lo facciamo solo se il dataframe eccede il limite di sicurezza (es. 310 righe)
            if len(df) > 310:
                df = df.iloc[-300:].copy()

            # Evita locazioni "Not a Number"
            if df.index.hasnans:
                df = df[~df.index.isna()].copy()

            # Salva la modifica
            historical_data[symbol][tf] = df

            # =========================================================
            # Hidden Gems IMMEDIATA intrabar
            # =========================================================
            if not closed and tf in HG_TF and HG_ENABLED:
                cfg = HG_CFG.get(tf, HG_CFG["1h"])
                ev = detect_squeeze_immediate(
                    df,
                    k,
                    rvol_partial_min=cfg["rvol_partial_min"],
                    tf_seconds=HG_TF_SECONDS.get(tf, 3600),
                )
                if ev:
                    hg_score = _estimate_hg_score(ev, df, ev["dir"])
                    if hg_score >= cfg["min_score"]:
                        trend_ok = True
                        if tf == "15m":
                            trend_ok = get_trend(df) == (
                                "rialzista" if ev["dir"] == "long" else "ribassista"
                            )
                        if trend_ok:
                            # NUOVO: Filtro BTC Correlation
                            if not check_btc_correlation_filter(ev["dir"]):
                                pass  # BTC in direzione opposta, skip
                            else:
                                # NUOVO: Filtro Funding Rate (solo per futures)
                                funding = fetch_funding_rate(symbol)
                                funding_ok = True
                                if ev["dir"] == "long" and funding > 0.001:
                                    funding_ok = False  # Troppi long, rischio squeeze
                                elif ev["dir"] == "short" and funding < -0.001:
                                    funding_ok = False  # Troppi short, rischio squeeze

                                if funding_ok:
                                    # NUOVO: Bonus/Malus orario
                                    ora_utc = datetime.datetime.utcnow().hour
                                    score_adj = hg_score
                                    if ora_utc in ORARI_MIGLIORI_UTC:
                                        score_adj += 0.03  # Bonus ore d'oro
                                    elif ora_utc in ORARI_MEDIOCRI_UTC:
                                        score_adj -= 0.08  # Penalità ore mediocri

                                    if score_adj >= cfg["min_score"]:
                                        last_key = (int(k.get("t", 0)), ev["dir"], tf)
                                        key = f"{symbol}_{tf}"
                                        if last_hg_bar_immediate.get(key) != last_key and (
                                            time.time() - last_hg_immediate_time.get(key, 0)
                                            >= cfg["cooldown"]
                                        ):
                                            # INSTITUTIONAL GATE
                                            risk_ok, risk_reason = check_risk_limits()
                                            if not risk_ok:
                                                logger.info(f"🛑 [RISK] {symbol} bloccato: {risk_reason}")
                                            else:
                                                regime_now, _ = get_probabilistic_regime(df)
                                                if not is_strategy_allowed_for_regime(ev["kind"], regime_now):
                                                    logger.info(f"🏛️ [REGIME] {symbol} {ev['kind']} vietato in {regime_now}")
                                                else:
                                                    micro_ok, micro_det = microstructure_check(symbol, ev["dir"])
                                                    if not micro_ok:
                                                        logger.info(f"🔬 [MICRO] {symbol} bloccato: score={micro_det.get('micro_score')}")
                                                    else:
                                                        prob = calculate_signal_probability(df, ev["dir"], ev, regime_now, tf)
                                                        if prob < 0.75:
                                                            logger.info(f"📊 [PROB] {symbol} P={prob:.0%} < 75%")
                                                        else:
                                                            # ALL GATES PASSED → SEND
                                                            send_hidden_gem(
                                                                symbol,
                                                                tf,
                                                                ev["dir"],
                                                                ev["kind"],
                                                                ev,
                                                                df,
                                                                phase_label=f"IMMEDIATA (P={prob:.0%} C={calc_weighted_confluence(df, ev['dir'])} R={regime_now})",
                                                            )
                                                            last_hg_bar_immediate[key] = last_key
                                                            last_hg_immediate_time[key] = time.time()
                
            # Se la candela non è chiusa, fermati qui
            if not closed:
                return

            # =========================================================
            # CANDELA CHIUSA - Calcolo Indicatori & Validazioni Finali
            # =========================================================
            df = compute_indicators(df)
            historical_data[symbol][tf] = df
            update_divergences(symbol, tf=tf)

            # Hidden Gems VALIDATA (a chiusura)
            if tf in HG_TF and HG_ENABLED:
                cfg = HG_CFG.get(tf, HG_CFG["1h"])
                ev1 = detect_squeeze_validated(df, rvol_bar_min=cfg["rvol_bar_min"])
                ev2 = detect_nr7_validated(
                    df, rvol_min=max(HG_NR7_RVOL_MIN, cfg["rvol_bar_min"])
                )
                ev3 = (
                    detect_rs_leader_breakout(symbol, df) if tf == "1h" else None
                )  # RS solo 1h
                
                for ev in [ev1, ev2, ev3]:
                    if not ev:
                        continue
                    if tf == "15m":
                        if get_trend(df) != ("rialzista" if ev["dir"] == "long" else "ribassista"):
                            continue
                    # NUOVO: Filtro BTC Correlation per VALIDATA
                    if not check_btc_correlation_filter(ev["dir"]):
                        continue  # BTC in direzione opposta

                    # NUOVO: Filtro Funding Rate per VALIDATA
                    funding = fetch_funding_rate(symbol)
                    if ev["dir"] == "long" and funding > 0.001:
                        continue  # Troppi long
                    if ev["dir"] == "short" and funding < -0.001:
                        continue  # Troppi short

                    key = f"{symbol}_{tf}"
                    if (time.time() - last_hg_validated_time.get(key, 0) < cfg["cooldown"]):
                        continue

                    # INSTITUTIONAL GATE
                    risk_ok, risk_reason = check_risk_limits()
                    if not risk_ok:
                        logger.info(f"🛑 [RISK] {symbol} bloccato: {risk_reason}")
                        continue
                    regime_now, _ = get_probabilistic_regime(df)
                    if not is_strategy_allowed_for_regime(ev["kind"], regime_now):
                        logger.info(f"🏛️ [REGIME] {symbol} {ev['kind']} vietato in {regime_now}")
                        continue
                    micro_ok, micro_det = microstructure_check(symbol, ev["dir"])
                    if not micro_ok:
                        logger.info(f"🔬 [MICRO] {symbol} bloccato: score={micro_det.get('micro_score')}")
                        continue
                    prob = calculate_signal_probability(df, ev["dir"], ev, regime_now, tf)
                    if prob < 0.75:
                        logger.info(f"📊 [PROB] {symbol} P={prob:.0%} < 75%")
                        continue

                    # ALL GATES PASSED → SEND
                    send_hidden_gem(
                        symbol,
                        tf,
                        ev["dir"],
                        ev["kind"],
                        ev,
                        df,
                        phase_label=f"VALIDATA (P={prob:.0%} C={calc_weighted_confluence(df, ev['dir'])} R={regime_now})",
                    )
                    last_hg_validated_time[key] = time.time()

        except Exception as e:
            logger.error(f"Errore update_realtime {symbol} {tf}: {e}")
            traceback.print_exc()

# ============================
# FALLBACK REST — CHIUSURA CANDELE (V16 SMART RATE-LIMIT)
# ============================

POLL_CLOSED_ENABLE = True
POLL_CLOSED_INTERVAL = 60  # Secondi tra ogni check

def _get_tf_seconds(interval_str):
    """Converte le stringhe '15m', '1h', '4h' in secondi per calcolare i ritardi."""
    if interval_str == "15m": return 900
    if interval_str == "1h": return 3600
    if interval_str == "4h": return 14400
    return 3600

def _poll_closed_candles(interval: str, symbols_list):
    try:
        tf_seconds = _get_tf_seconds(interval)
        current_time = time.time()
        
        for symbol in symbols_list:
            try:
                # --- CONTROLLO SMART PRE-CHIAMATA REST ---
                # Evitiamo di chiamare Binance se il WebSocket ha già aggiornato questo simbolo.
                df = historical_data.get(symbol, {}).get(interval)
                if df is not None and not df.empty:
                    last_candle_time = df.index[-1].timestamp()
                    # Se l'ultima candela registrata ha meno "età" della durata del timeframe + tolleranza (60s)
                    # Significa che il WebSocket sta funzionando regolarmente. SALTIAMO la chiamata.
                    if (current_time - last_candle_time) < (tf_seconds + 60):
                        continue  # <--- RISPARMIO DEL 99% DEL RATE LIMIT QUI!

                # Se arriviamo qui, il WebSocket è in ritardo. Facciamo la chiamata REST.
                kl = client.futures_klines(symbol=symbol, interval=interval, limit=2)
                if not kl or len(kl) < 2:
                    continue
                    
                last = kl[-1]
                close_time = int(last[6])
                
                # Se la candela REST non è ancora chiusa matematicamente, skippa
                if close_time > int(current_time * 1000):
                    continue  

                open_time = int(last[0])
                key = f"{symbol}_{interval}"
                
                # Evitiamo di processare due volte la stessa candela
                if LAST_KLINE_TIME.get(key) == open_time:
                    continue  

                # Simuliamo il payload WebSocket
                k = {
                    "t": open_time,
                    "o": last[1],
                    "h": last[2],
                    "l": last[3],
                    "c": last[4],
                    "v": last[5],
                    "V": last[10], # taker buy base volume aggiunto per sicurezza
                    "x": True,     # flag di chiusura manuale
                    "s": symbol,
                    "i": interval,
                }
                
                LAST_KLINE_TIME[key] = open_time

                # Chiamiamo le nostre funzioni di update simulando che sia arrivato dal WS
                update_realtime(symbol, interval, k)
                if interval in ("1h", "15m", "4h"):
                    process_closed_candle(symbol, interval, k)
                    
                # Piccola pausa per non spammare troppe richieste ravvicinate anche in caso di down totale
                time.sleep(0.1)

            except Exception as e:
                logger.debug(f"[POLL-{interval}] {symbol} Fallito: {e}")
                
    except Exception as e:
        logger.error(f"[POLL-{interval}] Errore critico nel loop: {e}")


def poll_closed_candles_all():
    """Il thread in background che lancia i controlli ciclicamente."""
    # Pausa iniziale per far allineare i WS al caricamento
    time.sleep(30)
    
    while True:
        try:
            # Controllo solo sulla whitelist per non sovraccaricare con tutte le Hidden Gems
            _poll_closed_candles("15m", symbols_whitelist)
            _poll_closed_candles("1h", symbols_whitelist)
            _poll_closed_candles("4h", symbols_whitelist)
        except Exception as e:
            logger.error(f"[POLL-MASTER] Errore: {e}")
            
        time.sleep(POLL_CLOSED_INTERVAL)


# ============================
# STARTUP HEALTH CHECK
# ============================

STARTUP_TIMEOUT = 25


def startup_health_check(timeout=STARTUP_TIMEOUT):
    start = time.time()
    logger.info("🔍 Controllo salute iniziale WebSocket...")
    while time.time() - start < timeout:
        if not WS_HEALTH:
            time.sleep(1)
            continue
        now = time.time()
        all_ok = True
        for name, data in WS_HEALTH.items():
            last = data.get("last_msg", 0)
            alive = data.get("alive", False)
            if (not alive) or (now - last > 10):
                all_ok = False
        if all_ok:
            logger.info("✅ Tutti i WS attivi")
            return True
        time.sleep(1)
    logger.warning("⚠️ Alcuni WS non rispondono")
    return False
# ============================
# GESTIONE SIMBOLI & WEBSOCKET MANAGER
# ============================

def load_top_symbols(limit=120):
    global symbols_whitelist
    try:
        info = client.futures_exchange_info()
        tickers = client.futures_ticker()
        qvol_map = {}
        for t in tickers:
            sym = t.get("symbol")
            if sym: qvol_map[sym] = float(t.get("quoteVolume", 0.0))
            
        valid = []
        for s in info["symbols"]:
            if s.get("contractType") == "PERPETUAL" and s.get("quoteAsset") == "USDT" and s.get("status") == "TRADING":
                sym = s.get("symbol")
                if sym and qvol_map.get(sym, 0.0) > 0: valid.append(sym)
                
        valid.sort(key=lambda sym: qvol_map.get(sym, 0.0), reverse=True)
        symbols_whitelist = valid[:limit]
        logger.info(f"✅ Divergenze: caricati {len(symbols_whitelist)} simboli top USDT-M")
    except Exception as e:
        logger.error(f"Errore load_top_symbols: {e}")
        symbols_whitelist = ["BTCUSDT", "ETHUSDT", "SOLUSDT"]

def load_all_futures_symbols():
    global symbols_hg_all
    try:
        info = client.futures_exchange_info()
        valid = [s["symbol"] for s in info["symbols"] if s["contractType"]=="PERPETUAL" and s["quoteAsset"]=="USDT" and s["status"]=="TRADING"]
        symbols_hg_all = sorted(valid)
        logger.info(f"💎 HG: Caricati {len(symbols_hg_all)} simboli")
    except:
        symbols_hg_all = symbols_whitelist[:]

def filter_hg_symbols_by_liquidity(min_quote_usdt=70000):
    global symbols_hg_all
    try:
        tickers = client.futures_ticker()
        qvol = {t["symbol"]: float(t.get("quoteVolume", 0)) for t in tickers if "symbol" in t}
        symbols_hg_all = [s for s in symbols_hg_all if qvol.get(s, 0) >= min_quote_usdt]
        logger.info(f"💎 HG: Filtro liquidità > {min_quote_usdt} -> {len(symbols_hg_all)} simboli rimasti")
    except: pass

def get_symbols_for_tf(tf):
    if tf == "1h" and HG_ENABLED and HG_MONITOR_ALL:
         return sorted(set(symbols_whitelist + symbols_hg_all))
    return symbols_whitelist

def split_symbols_v4(tf, group_size=10, symbols=None):
    use_list = symbols if symbols is not None else symbols_whitelist
    groups = []
    current = []
    for sym in use_list:
        current.append(sym)
        if len(current) >= group_size:
            groups.append(current)
            current = []
    if current: groups.append(current)
    return groups

def build_stream_url_v4(symbols_group, tf):
    streams = [f"{s.lower()}@kline_{tf}" for s in symbols_group]
    return "wss://fstream.binance.com/stream?streams=" + "/".join(streams)

# --- WEBSOCKET CALLBACKS ---
def on_message(ws, message):
    try:
        data = json.loads(message)
        k = data.get("data", {}).get("k")
        if not k: return
        symbol, tf = k["s"], k["i"]
        
        # Update Realtime
        update_realtime(symbol, tf, k)
        
        # Heartbeat
        name = getattr(ws, "name", "WS")
        with LAST_MESSAGE_LOCK: LAST_MESSAGE_TIME[name] = time.time()
        WS_HEALTH[name] = {"alive": True, "last_msg": time.time()}
        
        # Process Closed Candle
        if k.get("x"): process_closed_candle(symbol, tf, k)
            
    except Exception as e: logger.error(f"WS Msg Err: {e}")

def on_error(ws, error): logger.error(f"WS Error: {error}")
def on_close(ws, c, m): logger.warning(f"WS Closed: {c} {m}")
def on_open(ws): logger.info(f"WS Open")

# --- WEBSOCKET MANAGER ---

def watchdog(ws, name, stale_timeout=None):
    """Monitora un WebSocket e lo riavvia se si blocca (nessun messaggio per stale_timeout secondi)."""
    if stale_timeout is None:
        stale_timeout = WS_STALE_TIMEOUT
    try:
        time.sleep(WATCHDOG_GRACE_PERIOD)  # Grace period iniziale per permettere la connessione
        while True:
            time.sleep(WATCHDOG_CHECK_INTERVAL)  # Check ogni 15 secondi
            with LAST_MESSAGE_LOCK:
                last = LAST_MESSAGE_TIME.get(name, 0)
            if last == 0:
                continue  # Non ha ancora ricevuto messaggi, aspetta
            elapsed = time.time() - last
            if elapsed > stale_timeout:
                logger.warning(f"🐕 [WATCHDOG] {name} stallo rilevato ({elapsed:.0f}s senza messaggi) → riavvio forzato")
                try:
                    ws.close()
                except Exception:
                    pass
                break  # Il loop in _run_ws ricreerà il WS
    except Exception as e:
        logger.exception(f"🐕 [WATCHDOG] {name} errore: {e}")

def start_ws_for_tf(tf):
    num_ws = {"1h": 10, "4h": 6, "15m": 12}.get(tf, 10)
    symbols_list = get_symbols_for_tf(tf)
    group_size = max(1, math.ceil(max(1, len(symbols_list)) / num_ws))
    groups = split_symbols_v4(tf, group_size=group_size, symbols=symbols_list)
    
    for i, group in enumerate(groups):
        if not group: continue
        name = f"WS_{tf}_{i+1}"
        url = build_stream_url_v4(group, tf)
        logger.info(f"[WS-INIT] {name} -> {len(group)} simboli")
        
        def _run_ws(url=url, name=name):
            while True:
                try:
                    ws = websocket.WebSocketApp(url, on_message=on_message, on_error=on_error, on_close=on_close, on_open=on_open)
                    ws.name = name
                    
                    # 🔥 PUNTO 5: AVVIO DEL WATCHDOG 🔥
                    # Questo thread riavvierà forzatamente il WS se si blocca.
                    threading.Thread(target=watchdog, args=(ws, name), daemon=True, name=f"WATCHDOG_{name}").start()
                    
                    ws.run_forever(ping_interval=0, ping_timeout=None, sslopt={"cert_reqs": ssl.CERT_NONE})
                except Exception as e: 
                    logger.error(f"[{name}] Crash interno WS: {e}")
                    time.sleep(5)
                time.sleep(5) # Pausa prima di tentare la riconnessione

        # Lancio del thread principale per questo gruppo WebSocket
        t = threading.Thread(target=_run_ws, daemon=True, name=name)
        t.start()
        
        # Inizializza lo stato salute
        WS_HEALTH[name] = {"alive": False, "last_msg": time.time()}

def start_multi_websocket_v4():
    # 🔥 PUNTO 5: FIX TF ITERATION 🔥 
    # Usavi `for tf in HG_TF`, ma HG_TF contiene solo 1h e 15m. Ci perdevamo il 4H!
    # Ora usiamo i TF definiti nella configurazione nativa.
    tf_to_monitor = ["1h", "15m", "4h"]
    for tf in tf_to_monitor: 
        start_ws_for_tf(tf)
        
    threading.Thread(target=lambda: (time.sleep(300), ws_health_report()), daemon=True, name="WS_HEALTH_REPORT").start()

def ws_health_report():
    logger.info(f"📊 WS HEALTH: {len(WS_HEALTH)} connessioni attive")



# ============================
# MAIN
# ============================

TELEGRAM_TEST_ON_START = False
# ✅ metti True solo se vuoi il test ad ogni avvio


def load_universes():
    load_top_symbols(120)
    load_all_futures_symbols()
    filter_hg_symbols_by_liquidity(HG_MIN_QUOTE_VOL)

def test_lm_studio():
    try:
        logger.info("[LM-STUDIO-TEST] Avvio test...")
        ctx = '{"test": true, "symbol": "BTCUSDT", "timeframe": "1h"}'
        res = call_ai_safe("BTCUSDT", "1h", 0.5, "long", ctx)
        logger.info(f"[LM-STUDIO-TEST] Risultato: {res}")
        send_telegram(f"🤖 Test LM Studio OK\n{res}")
    except Exception as e:
        logger.error(f"[LM-STUDIO-TEST] Errore: {e}")
        send_telegram(f"🔴 Test LM Studio fallito: {e}")


def main():
    logger.info("=" * 60)
    logger.info(
        "🤖 BOT V14.2 QUANTUM EDITION — Divergenze + Hidden Gems + AI + Z-Score/Kelly"
    )
    logger.info("=" * 60)

    # === LOG DI STATO QUANTISTICO ===
    logger.info("⚛️ MODULI QUANTISTICI ATTIVI:")
    logger.info("   - Z-Score Filter: ON")
    logger.info("   - Kelly Criterion Sizing: ON")
    logger.info("   - Volatility Target: ON")
    logger.info("=" * 60)

    if TELEGRAM_TEST_ON_START:
        test_telegram()

    try:
        gc.set_threshold(700, 10, 10)

        init_database()
        db_load_last_signal_time()
        db_load_last_ai_call_per_symbol()
        db_load_ws_health()
        db_load_last_message_time()

        global client
        client = Client(API_KEY, API_SECRET)
        client.API_URL = "https://fapi.binance.com"

        load_universes()

        if not symbols_whitelist:
            raise ValueError("❌ Nessun simbolo caricato per divergenze!")

        if not symbols_hg_all:
            logger.warning("⚠️ Nessun universo Hidden Gems — uso fallback top list")

        logger.info(
            f"📥 Caricamento storico per {len(symbols_whitelist)} simboli (divergenze)..."
        )
        for idx, sym in enumerate(symbols_whitelist, 1):
            init_historical_for_symbol(sym)
            if idx % 20 == 0 or idx == len(symbols_whitelist):
                logger.info(
                    f"📊 Progresso divergenze: {idx}/{len(symbols_whitelist)} completati"
                )

        logger.info(
            f"📥 Caricamento storico per {len(symbols_hg_all)} simboli (Hidden Gems)..."
        )
        for idx, sym in enumerate(symbols_hg_all, 1):
            init_historical_for_symbol(sym)
            if idx % 20 == 0 or idx == len(symbols_hg_all):
                logger.info(f"📊 Progresso HG: {idx}/{len(symbols_hg_all)} completati")

        scan_divergences_on_startup()
        start_multi_websocket_v4()

        # AVVIO THREAD POLL CHIUSURA CANDELE
        if POLL_CLOSED_ENABLE:
            logger.info("[POLL-CLOSED] Avvio polling REST candele chiuse...")
            threading.Thread(
                target=poll_closed_candles_all,
                daemon=True,
                name="POLL-CLOSED",
            ).start()

        ok = startup_health_check(timeout=STARTUP_TIMEOUT)
        if ok:
            logger.info("✅ Tutti i WS attivi e ricevono dati")
        else:
            logger.warning("⚠️ Alcuni WS non rispondono")

        send_telegram(
            "🤖 Bot V14.2 QUANTUM avviato ⚛️\n\n"
            f"✅ Divergenze: {len(symbols_whitelist)} simboli\n"
            f"💎 Perle (HG): {len(symbols_hg_all)} simboli\n"
            "🧠 Moduli Quant: Z-Score & Kelly ON\n"
            "⏰ In attesa nuove candele..."
        )
        send_telegram("⏳ In attesa dei segnali...")
        

        logger.info("=" * 60)
        logger.info("🚀 BOT OPERATIVO — Premi Ctrl+C per terminare")
        logger.info("=" * 60)

        while True:
            time.sleep(10)
            gc.collect()

    except KeyboardInterrupt:
        logger.info("")
        logger.info("=" * 60)
        logger.info("⏹️ INTERRUZIONE MANUALE (Ctrl+C)")
        logger.info("=" * 60)

        try:
            db_save_last_signal_time()
            db_save_last_ai_call_per_symbol()
            db_save_ws_health()
            db_save_last_message_time()
            logger.info("💾 Stato salvato su database")
        except Exception as e:
            logger.error(f"Errore salvataggio finale: {e}")

        send_telegram("⏹️ Bot V14.2 terminato")
        logger.info("👋 Bot terminato correttamente")
        sys.exit(0)

    except Exception as e:
        logger.critical("=" * 60)
        logger.critical(f"❌ ERRORE FATALE: {e}")
        logger.critical("=" * 60)
        traceback.print_exc()

        try:
            send_telegram(f"🔴 ERRORE FATALE\n\n{str(e)[:200]}")
        except Exception:
            pass

        sys.exit(1)


if __name__ == "__main__":
    # --- SOLO PER RECUPERARE IL CHAT_ID DEL TUO BOT TELEGRAM ---
    # Sostituisci 'IL_TUO_TOKEN' con il token reale del tuo bot
    # Decommenta la riga sotto SOLO per recuperare il chat_id, poi ricommentala!
    # get_telegram_chat_id('IL_TUO_TOKEN')
    # Dopo averlo eseguito, guarda l'output in console per trovare il chat_id
    # Poi commenta o rimuovi questa riga e lascia solo main()
    main()
