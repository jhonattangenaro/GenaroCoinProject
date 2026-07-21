import json
import threading
import time
import logging
import queue
import math
import datetime
from dataclasses import dataclass
from typing import Optional, Dict, List, Tuple
from flask import Flask, render_template, Response, jsonify, request
from binance.um_futures import UMFutures
import websocket
import numpy as np
import pandas as pd
import ccxt

# ===================================================================
# CONFIGURACIÓN GENERAL Y LOGS
# ===================================================================
_handler_diagnostico = logging.StreamHandler()
_handler_diagnostico.setFormatter(logging.Formatter('%(asctime)s [%(name)s] %(levelname)s: %(message)s'))
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger("synapse_scanner")

for _nombre_logger in ('binance', 'websocket'):
    _logger = logging.getLogger(_nombre_logger)
    _logger.setLevel(logging.DEBUG)
    _logger.addHandler(_handler_diagnostico)

app = Flask(__name__)

# ===================================================================
# MÓDULO 1: ESCÁNER SFP & ORDER BOOK
# ===================================================================
cliente_rest = UMFutures()
datos_mercado = {}
alertas_historial = []
monedas_top_global = []

def obtener_top_20_volumen():
    print("[INFO] Buscando las 20 monedas con mayor liquidez en Binance Futures...")
    try:
        tickers = cliente_rest.ticker_24hr_price_change()
        pares_usdt = [t for t in tickers if t['symbol'].endswith('USDT') and '_' not in t['symbol']]
        pares_usdt.sort(key=lambda x: float(x['quoteVolume']), reverse=True)
        
        global monedas_top_global
        monedas_top_global = [t['symbol'] for t in pares_usdt[:20]]
        print(f"[ÉXITO] Top 20 detectado: {', '.join(monedas_top_global)}")
        return monedas_top_global
    except Exception as e:
        print(f"[ERROR] No se pudo obtener el top de volumen: {e}")
        monedas_top_global = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "XRPUSDT", "ADAUSDT", "AVAXUSDT", "LINKUSDT", "DOTUSDT", "MATICUSDT"]
        return monedas_top_global

def inicializar_contexto_historico(lista_simbolos):
    print(f"[INFO] Descargando estructura de 24 horas para {len(lista_simbolos)} activos...")
    for symbol in lista_simbolos:
        try:
            velas = cliente_rest.klines(symbol=symbol, interval="1h", limit=24)
            maximos = [float(v[2]) for v in velas]
            minimos = [float(v[3]) for v in velas]
            
            datos_mercado[symbol] = {
                "soporte_mayor": min(minimos),
                "resistencia_mayor": max(maximos),
                "alerta_long": "ESPERANDO",
                "alerta_short": "ESPERANDO"
            }
        except Exception as e:
            print(f"[ERROR] Al inicializar el historial de {symbol}: {e}")

def refrescar_contexto_historico_periodicamente(lista_simbolos, intervalo_segundos=900):
    while True:
        time.sleep(intervalo_segundos)
        for symbol in lista_simbolos:
            try:
                velas = cliente_rest.klines(symbol=symbol, interval="1h", limit=24)
                maximos = [float(v[2]) for v in velas]
                minimos = [float(v[3]) for v in velas]
                if symbol in datos_mercado:
                    datos_mercado[symbol]["soporte_mayor"] = min(minimos)
                    datos_mercado[symbol]["resistencia_mayor"] = max(maximos)
            except Exception as e:
                pass

contador_mensajes_ws = 0
ultimo_log_general = 0
ultimo_log_diagnostico = {}

def message_handler(_, message):
    global alertas_historial, contador_mensajes_ws, ultimo_log_general
    contador_mensajes_ws += 1
    
    try:
        raw = json.loads(message)
        data = raw.get("data", raw) if isinstance(raw, dict) else raw
        if "s" in data:
            symbol = data["s"]
            precio_actual = float(data["c"])
            
            if symbol not in datos_mercado:
                return

            soporte = datos_mercado[symbol]["soporte_mayor"]
            resistencia = datos_mercado[symbol]["resistencia_mayor"]
            estado_long = datos_mercado[symbol]["alerta_long"]
            estado_short = datos_mercado[symbol]["alerta_short"]
            
            if precio_actual < soporte and estado_long != "BARRIDO_BAJISTA":
                datos_mercado[symbol]["alerta_long"] = "BARRIDO_BAJISTA"
                
            elif precio_actual > soporte and estado_long == "BARRIDO_BAJISTA":
                datos_mercado[symbol]["alerta_long"] = "ESPERANDO"
                alertas_historial.append({
                    "activo": symbol, "tipo": "LONG", "precio": precio_actual,
                    "nivel_clave": soporte, "color": "text-green-400"
                })

            if precio_actual > resistencia and estado_short != "BARRIDO_ALCISTA":
                datos_mercado[symbol]["alerta_short"] = "BARRIDO_ALCISTA"
                
            elif precio_actual < resistencia and estado_short == "BARRIDO_ALCISTA":
                datos_mercado[symbol]["alerta_short"] = "ESPERANDO"
                alertas_historial.append({
                    "activo": symbol, "tipo": "SHORT", "precio": precio_actual,
                    "nivel_clave": resistencia, "color": "text-red-400"
                })
                
    except Exception as e:
        pass

def iniciar_websocket_binance(lista_simbolos):
    reconectar = threading.Event()
    streams = "/".join([f"{symbol.lower()}@ticker" for symbol in lista_simbolos])
    url = f"wss://fstream.binance.com/stream?streams={streams}"

    def on_close_raw(ws, close_status_code, close_msg):
        reconectar.set()

    while True:
        reconectar.clear()
        try:
            ws_app = websocket.WebSocketApp(
                url, on_message=lambda ws, msg: message_handler(ws, msg),
                on_error=lambda ws, err: None, on_close=on_close_raw
            )
            hilo_socket = threading.Thread(target=ws_app.run_forever, kwargs={"ping_interval": 180, "ping_timeout": 10})
            hilo_socket.daemon = True
            hilo_socket.start()
        except Exception:
            reconectar.set()

        reconectar.wait()
        time.sleep(5)

pools_precio = {}
pools_lock = threading.Lock()
GRACIA_CIERRE_SEGUNDOS = 30

def obtener_o_crear_pool(symbol, intervalo):
    key = f"{symbol}_{intervalo}"
    with pools_lock:
        pool = pools_precio.get(key)
        if pool is not None:
            if pool["timer_cierre"] is not None:
                pool["timer_cierre"].cancel()
                pool["timer_cierre"] = None
            return pool, key
        pool = {"subs": set(), "ultimo": None, "activo": True, "timer_cierre": None, "ws_app": None}
        pools_precio[key] = pool
    _iniciar_conexion_pool(symbol, intervalo, key, pool)
    return pool, key

def _iniciar_conexion_pool(symbol, intervalo, key, pool):
    url = f"wss://fstream.binance.com/stream?streams={symbol.lower()}@kline_{intervalo}/{symbol.lower()}@ticker"

    def on_message(_, message):
        try:
            raw = json.loads(message)
            data = raw.get("data", raw) if isinstance(raw, dict) else raw
            stream_name = raw.get("stream", "")
            payload = None
            if "kline" in stream_name:
                k = data.get("k")
                if k:
                    payload = {"type": "kline", "time": k["t"] // 1000, "open": float(k["o"]), "high": float(k["h"]), "low": float(k["l"]), "close": float(k["c"])}
            elif "ticker" in stream_name:
                payload = {"type": "tick", "price": float(data["c"])}
                
            if payload:
                with pools_lock:
                    if payload["type"] == "kline":
                        pool["ultimo"] = payload
                    destinatarios = list(pool["subs"])
                for cola in destinatarios:
                    cola.put(payload)
        except Exception:
            pass

    def on_close(_, code, msg):
        with pools_lock:
            todavia_existe = pools_precio.get(key) is pool and pool["activo"]
        if todavia_existe:
            time.sleep(2)
            with pools_lock:
                if pools_precio.get(key) is pool and pool["activo"]:
                    _iniciar_conexion_pool(symbol, intervalo, key, pool)

    ws_app = websocket.WebSocketApp(url, on_message=on_message, on_error=lambda w, e: None, on_close=on_close)
    pool["ws_app"] = ws_app
    hilo = threading.Thread(target=ws_app.run_forever, kwargs={"ping_interval": 180, "ping_timeout": 10})
    hilo.daemon = True
    hilo.start()

def liberar_suscriptor(key, mi_cola):
    with pools_lock:
        pool = pools_precio.get(key)
        if pool is None: return
        pool["subs"].discard(mi_cola)
        if not pool["subs"]:
            timer = threading.Timer(GRACIA_CIERRE_SEGUNDOS, _cerrar_pool_si_sigue_vacio, args=(key,))
            timer.daemon = True
            pool["timer_cierre"] = timer
            timer.start()

def _cerrar_pool_si_sigue_vacio(key):
    with pools_lock:
        pool = pools_precio.get(key)
        if pool is None or pool["subs"]: return
        pool["activo"] = False
        ws_app = pool["ws_app"]
        del pools_precio[key]
    if ws_app is not None: ws_app.close()


# ===================================================================
# MÓDULO 2: SYNAPSE TRAIL PRO SCANNER
# ===================================================================
@dataclass
class IndicatorParams:
    atr_len: int = 13
    trail_len: int = 21
    base_mult: float = 1.618
    use_adaptive_mult: bool = False
    use_ratchet: bool = True
    adx_len: int = 14
    chop_len: int = 14
    regime_len: int = 50
    regime_trending: float = 60.0
    regime_choppy: float = 35.0
    use_htf_filter: bool = True
    use_volume_filter: bool = False
    vol_mult: float = 1.3
    min_quality: int = 0
    skip_choppy: bool = False
    sl_mult: float = 1.5
    tp1_mult: float = 1.0
    tp2_mult: float = 2.0
    tp3_mult: float = 3.0
    grade_a: float = 75.0
    grade_b: float = 55.0

PARAMS = IndicatorParams()
SIGNAL_TFS = ["3m", "5m", "15m", "30m", "1h"]
CONFLUENCE_TFS = SIGNAL_TFS
TOP_N_SYMBOLS = 50
CANDLE_LIMIT = 300
SCAN_INTERVAL = 300  

TF_MINUTES = {
    "1m": 1, "3m": 3, "5m": 5, "15m": 15, "30m": 30,
    "1h": 60, "2h": 120, "4h": 240, "6h": 360, "8h": 480,
    "12h": 720, "1d": 1440, "3d": 4320, "1w": 10080,
}

def true_range(df):
    hl = df["high"] - df["low"]
    hc = (df["high"] - df["close"].shift()).abs()
    lc = (df["low"] - df["close"].shift()).abs()
    return pd.concat([hl, hc, lc], axis=1).max(axis=1)

def rma(series, length): return series.ewm(alpha=1.0/length, adjust=False, min_periods=length).mean()
def ema(series, length): return series.ewm(span=length, adjust=False, min_periods=length).mean()

def rsi(series, length=14):
    delta = series.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    avg_gain = rma(gain, length)
    avg_loss = rma(loss, length)
    rs = avg_gain / avg_loss.replace(0, np.nan)
    return (100 - (100 / (1 + rs))).fillna(50.0)

def dmi(df, length):
    up_move = df["high"].diff()
    down_move = -df["low"].diff()
    plus_dm = np.where((up_move > down_move) & (up_move > 0), up_move, 0.0)
    minus_dm = np.where((down_move > up_move) & (down_move > 0), down_move, 0.0)
    tr = true_range(df)
    atr_ = rma(tr, length)
    plus_di = 100 * rma(pd.Series(plus_dm, index=df.index), length) / atr_.replace(0, np.nan)
    minus_di = 100 * rma(pd.Series(minus_dm, index=df.index), length) / atr_.replace(0, np.nan)
    dx = 100 * (plus_di - minus_di).abs() / (plus_di + minus_di).replace(0, np.nan)
    adx = rma(dx.fillna(0.0), length)
    return plus_di.fillna(0.0), minus_di.fillna(0.0), adx.fillna(0.0)

def choppiness_index(df, length):
    tr = true_range(df)
    atr_sum = tr.rolling(length).sum()
    hh = df["high"].rolling(length).max()
    ll = df["low"].rolling(length).min()
    rng = hh - ll
    log_n = math.log10(max(length, 2))
    chop = np.where(rng <= 0, 100.0, np.where(atr_sum > 0, 100.0 * np.log10(atr_sum / rng.replace(0, np.nan)) / log_n, 50.0))
    return pd.Series(chop, index=df.index).clip(0, 100)

def r_squared(series, length):
    idx = pd.Series(np.arange(len(series)), index=series.index, dtype=float)
    corr = series.rolling(length).corr(idx)
    return (corr ** 2).fillna(0.0)

def atr(df, length): return rma(true_range(df), length)

def compute_regime(df, p):
    _, _, adx_val = dmi(df, p.adx_len)
    adx_score = (adx_val / 50.0 * 100.0).clip(upper=100.0)
    chop_raw = choppiness_index(df, p.chop_len)
    chop_score = (100.0 - chop_raw).clip(lower=0.0, upper=100.0)
    r2_score = r_squared(df["close"], p.regime_len) * 100.0
    return adx_score * 0.40 + chop_score * 0.35 + r2_score * 0.25

def compute_trail(df, p):
    df = df.copy()
    atr_val = atr(df, p.atr_len).fillna(0.0)
    trail_center = ema(df["close"], p.trail_len)
    mult_adjust = np.ones(len(df))
    if p.use_adaptive_mult:
        vol_rank = atr_val.rolling(100).apply(lambda x: pd.Series(x).rank(pct=True).iloc[-1] * 100, raw=False).fillna(50.0)
        mult_adjust = np.where(vol_rank < 30, 0.8, np.where(vol_rank > 70, 1.25, 1.0))
    
    eff_mult = p.base_mult * mult_adjust
    raw_upper = (trail_center + atr_val * eff_mult).values
    raw_lower = (trail_center - atr_val * eff_mult).values
    close = df["close"].values
    n = len(df)
    upper, lower, direction = np.full(n, np.nan), np.full(n, np.nan), np.zeros(n, dtype=int)
    warmup = max(p.atr_len, p.trail_len, p.regime_len) + 5

    for i in range(n):
        if i < warmup or np.isnan(raw_upper[i]) or np.isnan(raw_lower[i]):
            upper[i], lower[i], direction[i] = raw_upper[i], raw_lower[i], 0
            continue
        prev_upper = upper[i-1] if not np.isnan(upper[i-1]) else close[i]
        prev_lower = lower[i-1] if not np.isnan(lower[i-1]) else close[i]
        prev_dir = direction[i-1]

        d = prev_dir
        if close[i] > prev_upper: d = 1
        elif close[i] < prev_lower: d = -1
        flipped = d != prev_dir

        if p.use_ratchet:
            if d == 1:
                lower[i] = raw_lower[i] if flipped else max(raw_lower[i], prev_lower)
                upper[i] = raw_upper[i]
            elif d == -1:
                upper[i] = raw_upper[i] if flipped else min(raw_upper[i], prev_upper)
                lower[i] = raw_lower[i]
            else:
                upper[i], lower[i] = raw_upper[i], raw_lower[i]
        else:
            upper[i], lower[i] = raw_upper[i], raw_lower[i]
        direction[i] = d

    df["atr"], df["upper"], df["lower"], df["dir"] = atr_val, upper, lower, direction
    df["trail"] = np.where(direction == 1, lower, np.where(direction == -1, upper, np.nan))
    df["raw_buy"] = (df["dir"] == 1) & (df["dir"].shift(1) == -1)
    df["raw_sell"] = (df["dir"] == -1) & (df["dir"].shift(1) == 1)
    df["raw_buy"] = df["raw_buy"].fillna(False)
    df["raw_sell"] = df["raw_sell"].fillna(False)
    return df

def auto_htf(tf):
    cur = TF_MINUTES.get(tf, 60)
    target = cur * 4
    if target <= 5: return "5m"
    if target <= 15: return "15m"
    if target <= 30: return "30m"
    if target <= 60: return "1h"
    if target <= 240: return "4h"
    if target <= 1440: return "1d"
    return "1w"

def grade_from_score(score):
    if score is None or np.isnan(score): return "—"
    if score >= PARAMS.grade_a: return "A"
    if score >= PARAMS.grade_b: return "B"
    return "C"

def fetch_ohlcv_df(exchange, symbol, timeframe, limit=CANDLE_LIMIT):
    raw = exchange.fetch_ohlcv(symbol, timeframe=timeframe, limit=limit)
    df = pd.DataFrame(raw, columns=["ts", "open", "high", "low", "close", "volume"])
    df["ts"] = pd.to_datetime(df["ts"], unit="ms")
    df.set_index("ts", inplace=True)
    return df

def get_top_symbols(exchange, n=TOP_N_SYMBOLS):
    markets = exchange.load_markets()
    tickers = exchange.fetch_tickers()
    rows = []
    for sym, m in markets.items():
        if not m.get("swap") or not m.get("linear"): continue
        if m.get("quote") != "USDT": continue
        if not m.get("active", True): continue
        t = tickers.get(sym)
        if not t: continue
        qv = t.get("quoteVolume") or 0.0
        rows.append((sym, qv))
    rows.sort(key=lambda x: x[1], reverse=True)
    return [s for s, _ in rows[:n]]

def compute_signal_info(exchange, symbol, timeframe, p):
    df = fetch_ohlcv_df(exchange, symbol, timeframe)
    min_bars = max(p.regime_len, p.trail_len, p.atr_len) + 15
    if len(df) < min_bars: return None

    df = compute_trail(df, p)
    df["regime_score"] = compute_regime(df, p)
    df["is_trending"] = df["regime_score"] >= p.regime_trending
    df["is_choppy"] = df["regime_score"] < p.regime_choppy
    df["rsi"] = rsi(df["close"], 14)

    htf_tf = auto_htf(timeframe)
    htf_bull = htf_bear = False
    try:
        htf_df = fetch_ohlcv_df(exchange, symbol, htf_tf, limit=150)
        htf_ema = ema(htf_df["close"], 50)
        htf_close_last = htf_df["close"].iloc[-2]
        htf_ema_last = htf_ema.iloc[-2]
        if p.use_htf_filter and not np.isnan(htf_ema_last):
            htf_bull = htf_close_last > htf_ema_last
            htf_bear = htf_close_last < htf_ema_last
    except Exception:
        pass

    vol_sma = df["volume"].rolling(20).mean()
    has_volume = df["volume"].tail(20).sum() > 0
    vol_confirm_series = (df["volume"] > vol_sma * p.vol_mult) if has_volume else pd.Series(False, index=df.index)

    last = df.iloc[-2]
    prev_upper = df["upper"].iloc[-3] if len(df) > 2 else np.nan
    prev_lower = df["lower"].iloc[-3] if len(df) > 2 else np.nan

    is_buy_signal = bool(last["raw_buy"])
    is_sell_signal = bool(last["raw_sell"])

    quality = None
    grade = "—"
    if is_buy_signal or is_sell_signal:
        is_buy = is_buy_signal
        htf_matches = (is_buy and htf_bull) or ((not is_buy) and htf_bear)
        htf_against = (is_buy and htf_bear) or ((not is_buy) and htf_bull)
        htf_part = 30.0 if htf_matches else (0.0 if htf_against else 15.0)

        vol_part = 20.0 if (not p.use_volume_filter or not has_volume) else (20.0 if bool(vol_confirm_series.iloc[-2]) else 0.0)
        rsi_bull_ok = last["rsi"] > 50
        rsi_part = 20.0 if ((is_buy and rsi_bull_ok) or ((not is_buy) and not rsi_bull_ok)) else 0.0
        regime_part = last["regime_score"] * 0.20

        break_dist = (last["close"] - prev_upper) if is_buy else (prev_lower - last["close"])
        break_dist = 0.0 if np.isnan(break_dist) else break_dist
        atr_v = last["atr"] if last["atr"] not in (0, None) and not np.isnan(last["atr"]) else 1e-9
        break_strength = min(abs(break_dist) / atr_v, 3.0) / 3.0 * 100.0
        break_part = break_strength * 0.10

        quality = htf_part + vol_part + rsi_part + regime_part + break_part
        grade = grade_from_score(quality)

    is_choppy = bool(last["is_choppy"])
    passes_filters = True
    if quality is not None and quality < p.min_quality: passes_filters = False
    if p.skip_choppy and is_choppy: passes_filters = False

    sl_dist = last["atr"] * p.sl_mult
    entry = float(last["close"])
    sl = tp1 = tp2 = tp3 = None
    if is_buy_signal:
        sl = entry - sl_dist
        tp1, tp2, tp3 = entry + sl_dist * p.tp1_mult, entry + sl_dist * p.tp2_mult, entry + sl_dist * p.tp3_mult
    elif is_sell_signal:
        sl = entry + sl_dist
        tp1, tp2, tp3 = entry - sl_dist * p.tp1_mult, entry - sl_dist * p.tp2_mult, entry - sl_dist * p.tp3_mult

    regime_label = "Trending" if last["is_trending"] else ("Choppy" if last["is_choppy"] else "Mixed")
    now_local = datetime.datetime.now(datetime.timezone.utc).astimezone()

    return {
        "symbol": symbol, "timeframe": timeframe, "htf": htf_tf,
        "direction": int(last["dir"]), "is_buy_signal": is_buy_signal,
        "is_sell_signal": is_sell_signal, "passes_filters": passes_filters,
        "quality": quality, "grade": grade, "regime_label": regime_label,
        "regime_score": float(last["regime_score"]), "is_choppy": is_choppy,
        "rsi": float(last["rsi"]), "entry": entry if (is_buy_signal or is_sell_signal) else None,
        "sl": sl, "tp1": tp1, "tp2": tp2, "tp3": tp3,
        "htf_bull": bool(htf_bull), "htf_bear": bool(htf_bear),
        "atr": float(last["atr"]), "close_time": str(now_local.strftime('%Y-%m-%d %H:%M:%S')),
        "current_price": float(df["close"].iloc[-1])
    }

def compute_trend_direction(exchange, symbol, timeframe, p):
    limit = max(p.regime_len, p.trail_len, p.atr_len) + 60
    df = fetch_ohlcv_df(exchange, symbol, timeframe, limit=limit)
    if len(df) < limit - 10: return 0
    df = compute_trail(df, p)
    return int(df["dir"].iloc[-2])

def compute_directions_cache(exchange, symbol, tfs, p):
    cache = {}
    for tf in tfs:
        try: cache[tf] = compute_trend_direction(exchange, symbol, tf, p)
        except Exception: cache[tf] = None
    return cache

def confluence_from_cache(primary_dir, cache):
    agree, total = 0, 0
    for tf, d in cache.items():
        total += 1
        if d is not None and d == primary_dir and d != 0: agree += 1
    return agree, total, cache

active_alerts = {}

def is_alert_closed(info):
    price = info["current_price"]
    if info["is_buy_signal"]:
        if price >= info["tp3"] or price <= info["sl"]: return True
    else:
        if price <= info["tp3"] or price >= info["sl"]: return True
    return False

def should_emit_alert(symbol, timeframe, info):
    key = f"{symbol}_{timeframe}"
    if key not in active_alerts: return True
    if is_alert_closed(active_alerts[key]):
        del active_alerts[key]
        return True
    return False

def _fmt(v): return "n/a" if v is None or (isinstance(v, float) and np.isnan(v)) else f"{v:.6g}"

def tf_to_tv_interval(tf):
    if tf.endswith('m'): return tf[:-1]
    elif tf.endswith('h'): return str(int(tf[:-1]) * 60)
    elif tf.endswith('d'): return str(int(tf[:-1]) * 1440)
    elif tf.endswith('w'): return str(int(tf[:-1]) * 10080)
    return '60'

def alert_html(info, agree, total, details):
    direction = "LONG" if info["is_buy_signal"] else "SHORT"
    css_class = "long" if info["is_buy_signal"] else "short"
    dir_emoji = "🟢" if info["is_buy_signal"] else "🔴"
    chop_flag = " ⚠️" if info["is_choppy"] else ""
    conf_symbols = {1: "↑", -1: "↓", 0: "·", None: "?"}
    conf_str = " ".join(f"{tf}:{conf_symbols[details[tf]]}" for tf in CONFLUENCE_TFS)
    htf_bias_txt = "Alcista" if info["htf_bull"] else ("Bajista" if info["htf_bear"] else "N/A")
    rr = abs(info["tp1"] - info["entry"]) / abs(info["entry"] - info["sl"]) if info["entry"] and info["sl"] and info["entry"] != info["sl"] else None
    
    base = info["symbol"].split('/')[0]
    symbol_tv = f"{base}USDT"
    tv_interval = tf_to_tv_interval(info["timeframe"])
    tv_url = f"https://www.tradingview.com/chart/?symbol=BINANCE:{symbol_tv}.P&interval={tv_interval}"

    html = f'''
    <div class="alert {css_class}">
        <div class="alert-header">
            <span class="symbol"><a href="{tv_url}" target="_blank">{info["symbol"]}</a></span>
            <span class="direction {css_class}">{dir_emoji} {direction}</span>
            <span class="grade">Grado {info["grade"]} ({_fmt(info["quality"])}/100)</span>
        </div>
        <div class="alert-details">
            <div><span class="label">TF señal:</span> {info["timeframe"]} | <span class="label">HTF bias ({info["htf"]}):</span> {htf_bias_txt}</div>
            <div><span class="label">Confluencia:</span> {agree}/{total} [{conf_str}]</div>
            <div><span class="label">Régimen:</span> {info["regime_label"]} (score {info["regime_score"]:.0f}){chop_flag}</div>
            <div><span class="label">RSI(14):</span> {info["rsi"]:.1f} | <span class="label">ATR:</span> {_fmt(info["atr"])}</div>
            <div><span class="label">Entrada:</span> {_fmt(info["entry"])} | <span class="label">SL:</span> {_fmt(info["sl"])}</div>
            <div><span class="label">TP1:</span> {_fmt(info["tp1"])} | <span class="label">TP2:</span> {_fmt(info["tp2"])} | <span class="label">TP3:</span> {_fmt(info["tp3"])}{f' (R:R TP1 = {rr:.2f})' if rr else ''}</div>
            <div class="timestamp">{info["close_time"]}</div>
        </div>
    </div>
    '''
    return ' '.join(html.split())

def scan_generator():
    exchange = ccxt.binanceusdm({"enableRateLimit": True})
    symbols = get_top_symbols(exchange, TOP_N_SYMBOLS)
    yield f"event: progress\ndata: Escaneo continuo activo. Próximo escaneo en {SCAN_INTERVAL}s...\n\n"

    while True:
        start_time = time.time()
        total_alerts = 0
        for symbol in symbols:
            try: dir_cache = compute_directions_cache(exchange, symbol, CONFLUENCE_TFS, PARAMS)
            except Exception: continue

            for signal_tf in SIGNAL_TFS:
                try:
                    info = compute_signal_info(exchange, symbol, signal_tf, PARAMS)
                    if info is None or not (info["is_buy_signal"] or info["is_sell_signal"]) or not info["passes_filters"]: continue
                    if not should_emit_alert(symbol, signal_tf, info): continue

                    agree, total, details = confluence_from_cache(info["direction"], dir_cache)
                    html = alert_html(info, agree, total, details)
                    yield f"event: alert\ndata: {html}\n\n"
                    total_alerts += 1
                    active_alerts[f"{symbol}_{signal_tf}"] = info
                except Exception:
                    pass

        elapsed = time.time() - start_time
        yield f"event: progress\ndata: Escaneo completado en {elapsed:.1f}s. {total_alerts} alertas. Próximo en {SCAN_INTERVAL}s.\n\n"
        time.sleep(SCAN_INTERVAL)


# ===================================================================
# RUTAS DE FLASK UNIFICADAS
# ===================================================================

@app.route('/')
def index():
    return render_template('menu.html')

@app.route('/sfp')
def sfp_view():
    return render_template('sfp.html')

@app.route('/synapse')
def synapse_view():
    return render_template('synapse.html')

# Endpoints SFP
@app.route('/api/top20')
def api_top20():
    return jsonify(monedas_top_global)

@app.route('/api/historia/<symbol>')
def api_historia(symbol):
    intervalo = request.args.get('interval', '15m')
    try:
        return jsonify(cliente_rest.klines(symbol=symbol.upper(), interval=intervalo, limit=500))
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/stream-alertas')
def stream_alertas():
    def event_stream():
        global alertas_historial
        while True:
            if alertas_historial:
                yield f"data: {json.dumps(alertas_historial.pop(0))}\n\n"
            else:
                time.sleep(0.5)
    return Response(event_stream(), mimetype='text/event-stream', headers={'Cache-Control': 'no-cache', 'X-Accel-Buffering': 'no'})

@app.route('/stream-precio/<symbol>/<intervalo>')
def stream_precio(symbol, intervalo):
    pool, key = obtener_o_crear_pool(symbol.upper(), intervalo)
    mi_cola = queue.Queue()
    with pools_lock:
        pool["subs"].add(mi_cola)
        if pool["ultimo"] is not None: mi_cola.put(pool["ultimo"])

    def event_stream():
        try:
            while True:
                try: yield f"data: {json.dumps(mi_cola.get(timeout=15))}\n\n"
                except queue.Empty: yield ": keep-alive\n\n"
        finally:
            liberar_suscriptor(key, mi_cola)
    return Response(event_stream(), mimetype='text/event-stream', headers={'Cache-Control': 'no-cache', 'X-Accel-Buffering': 'no'})

# Endpoints Synapse
@app.route('/synapse-stream')
def stream_synapse():
    return Response(scan_generator(), mimetype='text/event-stream', headers={'Cache-Control': 'no-cache', 'X-Accel-Buffering': 'no'})

# ===================================================================
# INICIALIZACIÓN GLOBAL (FUNCIONA TANTO EN LOCAL COMO EN GUNICORN/RENDER)
# ===================================================================
lista_activos = obtener_top_20_volumen()
inicializar_contexto_historico(lista_activos)

threading.Thread(target=iniciar_websocket_binance, args=(lista_activos,), daemon=True).start()
threading.Thread(target=refrescar_contexto_historico_periodicamente, args=(lista_activos,), daemon=True).start()

if __name__ == '__main__':
    app.run(debug=False, host='0.0.0.0', port=5050, threaded=True)
