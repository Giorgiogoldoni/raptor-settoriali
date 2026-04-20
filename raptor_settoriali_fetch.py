#!/usr/bin/env python3
"""
🦅 RAPTOR SETTORIALI — Autonomous Sector Portfolio Manager
BUY1 / BUY2 / BUY3 / EXIT1 / EXIT2 / EXIT3
VIX fallback: se non disponibile → regime NORMALE
"""

import json, os
from datetime import datetime, date, timedelta
import pytz
import yfinance as yf
import pandas as pd

ROME_TZ = pytz.timezone("Europe/Rome")

PORTFOLIOS = {
    "europa": {
        "name": "🇪🇺 EUROPA",
        "benchmark": {"ticker": "600X.MI", "label": "600X"},
        "sectors": [
            {"ticker": "DFSV.DE", "label": "DFSV", "name": "SPDR S&P Eu Defence"},
            {"ticker": "STKX.MI", "label": "STKX", "name": "SPDR MSCI Eu Technology"},
            {"ticker": "STNX.MI", "label": "STNX", "name": "SPDR MSCI Eu Energy"},
            {"ticker": "STPX.MI", "label": "STPX", "name": "SPDR MSCI Eu Materials"},
            {"ticker": "STQX.MI", "label": "STQX", "name": "SPDR MSCI Eu Industrials"},
            {"ticker": "STRX.MI", "label": "STRX", "name": "SPDR MSCI Eu Cons Disc"},
            {"ticker": "STSX.MI", "label": "STSX", "name": "SPDR MSCI Eu Cons Staples"},
            {"ticker": "STTX.MI", "label": "STTX", "name": "SPDR MSCI Eu Comm Services"},
            {"ticker": "STUX.MI", "label": "STUX", "name": "SPDR MSCI Eu Utilities"},
            {"ticker": "STWX.MI", "label": "STWX", "name": "SPDR MSCI Eu Health Care"},
            {"ticker": "STZX.MI", "label": "STZX", "name": "SPDR MSCI Eu Financials"},
        ]
    },
    "usa": {
        "name": "🇺🇸 USA",
        "benchmark": {"ticker": "SP5A.MI", "label": "SP5A"},
        "sectors": [
            {"ticker": "SXLB.MI", "label": "SXLB", "name": "SPDR S&P US Materials"},
            {"ticker": "SXLC.MI", "label": "SXLC", "name": "SPDR S&P US Comm Services"},
            {"ticker": "SXLE.MI", "label": "SXLE", "name": "SPDR S&P US Energy"},
            {"ticker": "SXLF.MI", "label": "SXLF", "name": "SPDR S&P US Financials"},
            {"ticker": "SXLI.MI", "label": "SXLI", "name": "SPDR S&P US Industrials"},
            {"ticker": "SXLK.MI", "label": "SXLK", "name": "SPDR S&P US Technology"},
            {"ticker": "SXLP.MI", "label": "SXLP", "name": "SPDR S&P US Cons Staples"},
            {"ticker": "SXLU.MI", "label": "SXLU", "name": "SPDR S&P US Utilities"},
            {"ticker": "SXLV.MI", "label": "SXLV", "name": "SPDR S&P US Health Care"},
            {"ticker": "SXLY.MI", "label": "SXLY", "name": "SPDR S&P US Cons Disc"},
            {"ticker": "MGIN.MI", "label": "MGIN", "name": "SPDR Morningstar Multi-Asset Global Infra"},
        ]
    },
    "mondo": {
        "name": "🌍 MONDO",
        "benchmark": {"ticker": "SWRD.MI", "label": "SWRD"},
        "sectors": [
            {"ticker": "WCOD.MI", "label": "WCOD", "name": "SPDR MSCI Wld Cons Disc"},
            {"ticker": "WCOS.MI", "label": "WCOS", "name": "SPDR MSCI Wld Cons Staples"},
            {"ticker": "WFIN.MI", "label": "WFIN", "name": "SPDR MSCI Wld Financials"},
            {"ticker": "WHEA.MI", "label": "WHEA", "name": "SPDR MSCI Wld Health Care"},
            {"ticker": "WIND.MI", "label": "WIND", "name": "SPDR MSCI Wld Industrials"},
            {"ticker": "WMAT.MI", "label": "WMAT", "name": "SPDR MSCI Wld Materials"},
            {"ticker": "WNRG.MI", "label": "WNRG", "name": "SPDR MSCI Wld Energy"},
            {"ticker": "WTEC.MI", "label": "WTEC", "name": "SPDR MSCI Wld Technology"},
            {"ticker": "WTEL.MI", "label": "WTEL", "name": "SPDR MSCI Wld Comm Services"},
            {"ticker": "WUTI.MI", "label": "WUTI", "name": "SPDR MSCI Wld Utilities"},
        ]
    }
}

XEON_TICKER   = "XEON.MI"
COOLDOWN_DAYS = 2

# MAX posizioni per regime
MAX_POSITIONS = {
    'CALMA':      5,
    'NORMALE':    5,
    'ATTENZIONE': 3,
    'STRESS':     0,
    'PAURA':      0,
}

# ─────────────────────────────────────────────────────────────────────────────
# DATA FETCH
# ─────────────────────────────────────────────────────────────────────────────

def get_ohlcv(ticker: str):
    try:
        df = yf.download(ticker, period="1y", interval="1d",
                         progress=False, auto_adjust=True)
        if df is None or df.empty:
            return None
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)
        df = df.loc[:, ~df.columns.duplicated()]
        df = df.dropna(subset=['Close', 'High', 'Low'])
        if len(df) < 55:
            return None
        return {
            'close': [float(x) for x in df['Close'].tolist()],
            'high':  [float(x) for x in df['High'].tolist()],
            'low':   [float(x) for x in df['Low'].tolist()],
            'open':  [float(x) for x in df['Open'].tolist()] if 'Open' in df.columns else [float(x) for x in df['Close'].tolist()],
        }
    except Exception as e:
        print(f"    fetch error {ticker}: {e}")
        return None

def fetch_vix():
    vix = vstoxx = None
    for sym, key in [('^VIX','vix'), ('^V2TX','vstoxx')]:
        try:
            df = yf.download(sym, period='5d', interval='1d', progress=False)
            if not df.empty:
                val = float(df['Close'].iloc[-1])
                if key == 'vix': vix = round(val, 2)
                else: vstoxx = round(val, 2)
        except: pass
    return vix, vstoxx

def get_regime(vix, vstoxx):
    # Fallback: se VIX non disponibile → NORMALE
    if vix is None and vstoxx is None:
        return 'NORMALE'
    avg = ((vix or vstoxx or 20) + (vstoxx or vix or 20)) / 2
    if avg < 15:  return 'CALMA'
    if avg < 20:  return 'NORMALE'
    if avg < 25:  return 'ATTENZIONE'
    if avg < 30:  return 'STRESS'
    return 'PAURA'

# ─────────────────────────────────────────────────────────────────────────────
# INDICATORS
# ─────────────────────────────────────────────────────────────────────────────

def ema_arr(values, period):
    k = 2 / (period + 1)
    r = [values[0]]
    for v in values[1:]:
        r.append(v * k + r[-1] * (1 - k))
    return r

def calc_kama(prices, n=10, fast=2, slow=30):
    fsc = 2/(fast+1); ssc = 2/(slow+1)
    result = list(prices[:n])
    for i in range(n, len(prices)):
        d = abs(prices[i] - prices[i-n])
        v = sum(abs(prices[j]-prices[j-1]) for j in range(i-n+1, i+1))
        er = d/v if v > 0 else 0
        sc = (er*(fsc-ssc)+ssc)**2
        result.append(result[-1] + sc*(prices[i]-result[-1]))
    return result

def calc_er(p, n=10):
    if len(p) < n+1: return 0.0
    d = abs(p[-1]-p[-n-1])
    v = sum(abs(p[i]-p[i-1]) for i in range(-n, 0))
    return round(d/v if v > 0 else 0, 4)

def calc_ao(values):
    if len(values) < 34: return 0.0, 0
    s5  = [sum(values[i-4:i+1])/5  for i in range(4, len(values))]
    s34 = [sum(values[i-33:i+1])/34 for i in range(33, len(values))]
    n = min(len(s5), len(s34))
    series = [s5[-(n-i)] - s34[-(n-i)] for i in range(n)]
    ao_val = series[-1]
    baff = 0
    for i in range(len(series)-1, 0, -1):
        if series[i] > series[i-1]: baff += 1
        else: break
    # AO miglioramento: crescente nelle ultime 2 barre
    ao_improving = len(series) >= 2 and series[-1] > series[-2]
    return round(ao_val, 6), baff, ao_improving

def calc_rsi(p, n=14):
    if len(p) < n+1: return 50.0
    d = [p[i]-p[i-1] for i in range(1, len(p))]
    g = [max(x,0) for x in d]; l = [max(-x,0) for x in d]
    ag = sum(g[:n])/n; al = sum(l[:n])/n
    for i in range(n, len(g)):
        ag=(ag*(n-1)+g[i])/n; al=(al*(n-1)+l[i])/n
    rs = ag/al if al > 0 else 100
    return round(100-100/(1+rs), 1)

def trendycator(values):
    if len(values) < 55: return 'GRIGIO'
    e21 = ema_arr(values, 21)
    e55 = ema_arr(values, 55)
    if values[-1] > e21[-1] > e55[-1]: return 'VERDE'
    if values[-1] < e21[-1] < e55[-1]: return 'ROSSO'
    return 'GRIGIO'

def calc_cross_days(values, kama):
    above_now = values[-1] > kama[-1]
    for i in range(len(values)-2, max(0, len(values)-60), -1):
        if (values[i] > kama[i]) != above_now:
            return len(values)-1-i
    return 999

def calc_kama_cross_up(values, kama, lookback=3):
    """True se prezzo ha tagliato KAMA verso l'alto negli ultimi N giorni"""
    for i in range(len(values)-1, max(0, len(values)-1-lookback), -1):
        if values[i] > kama[i] and values[i-1] <= kama[i-1]:
            return True
    return False

def calc_sar(high, low, af0=0.02, af_max=0.2):
    if len(high)<5: return low[-1], True
    sar,ep,af,bull=low[0],high[0],af0,True; sars=[sar]
    for i in range(1,len(high)):
        prev=sars[-1]
        if bull:
            new=prev+af*(ep-prev)
            cands=low[max(0,i-2):i]; new=min(new,min(cands)) if cands else new
            if low[i]<new: bull,new,ep,af=False,ep,low[i],af0
            else:
                if high[i]>ep: ep=high[i]; af=min(af+af0,af_max)
        else:
            new=prev+af*(ep-prev)
            cands=high[max(0,i-2):i]; new=max(new,max(cands)) if cands else new
            if high[i]>new: bull,new,ep,af=True,ep,high[i],af0
            else:
                if low[i]<ep: ep=low[i]; af=min(af+af0,af_max)
        sars.append(new)
    return round(sars[-1],5), bull

def calc_vortex(high, low, close, n=14):
    if len(close)<n+1: return 1.0,1.0,False
    vm_p=[abs(high[i]-low[i-1]) for i in range(1,len(close))]
    vm_m=[abs(low[i]-high[i-1]) for i in range(1,len(close))]
    tr=[max(high[i]-low[i],abs(high[i]-close[i-1]),abs(low[i]-close[i-1]))
        for i in range(1,len(close))]
    ts=sum(tr[-n:])
    vip=round(sum(vm_p[-n:])/ts if ts>0 else 1,4)
    vim=round(sum(vm_m[-n:])/ts if ts>0 else 1,4)
    return vip,vim,vip>vim

def calc_rvi(close, open_, high, low, n=10):
    if len(close)<n+4: return 0.0,0.0,False
    num,den=[],[]
    for i in range(3,len(close)):
        nv=(close[i]-open_[i]+2*(close[i-1]-open_[i-1])+2*(close[i-2]-open_[i-2])+(close[i-3]-open_[i-3]))/6
        dv=(high[i]-low[i]+2*(high[i-1]-low[i-1])+2*(high[i-2]-low[i-2])+(high[i-3]-low[i-3]))/6
        num.append(nv); den.append(dv)
    if len(num)<n: return 0.0,0.0,False
    rs=[sum(num[i-n+1:i+1])/(sum(den[i-n+1:i+1]) or 1) for i in range(n-1,len(num))]
    if len(rs)<4: return 0.0,0.0,False
    ss=[(rs[i]+2*rs[i-1]+2*rs[i-2]+rs[i-3])/6 for i in range(3,len(rs))]
    if not ss: return 0.0,0.0,False
    return round(rs[-1],6),round(ss[-1],6),rs[-1]>ss[-1]

def calc_score_rs(er, baff, k_pct, p7, p30, ao_pos, trend):
    s = (er*30 + min(baff,10)*5 + min(abs(k_pct),5)*3
         + max(-10,min(5,p7))*4
         + max(-20,min(10,p30))*2
         + (5 if ao_pos else 0))
    if trend == 'ROSSO': s *= 0.6
    return round(s, 1)

# ─────────────────────────────────────────────────────────────────────────────
# CALCOLA SEGNALE BUY1/BUY2/BUY3/EXIT
# ─────────────────────────────────────────────────────────────────────────────

def calc_signal_buy(d, regime):
    """
    Ritorna (signal, qualifies, size)
    signal: BUY1 / BUY2 / BUY3 / EXIT1 / EXIT2 / EXIT3 / WATCH / STOP
    qualifies: True se va aperta una nuova posizione
    """
    sec_er   = d.get('sec_er', 0)
    rs_er    = d.get('rs_er', 0)
    score    = d.get('score', 0)
    rs_baff  = d.get('rs_baff', 0)
    sar_bull = d.get('sar_bullish', False)
    vortex   = d.get('vortex_bullish', False)
    sec_above= d.get('sec_above', False)
    rs_above = d.get('rs_above', False)
    rs_trend = d.get('rs_trend', 'GRIGIO')
    rs_ao_pos= d.get('rs_ao_pos', False)
    rs_ao_imp= d.get('rs_ao_improving', False)
    kama_cross=d.get('kama_cross_up', False)
    rs_kpct  = d.get('rs_kpct', 0)

    blocked = regime in ('STRESS', 'PAURA')

    # ── EXIT3 — uscita totale ──────────────────────────────────────
    neg = sum([
        not sec_above,
        not rs_ao_pos,
        sec_er < 0.3,
        not rs_above and rs_kpct < -5,
    ])
    if score < 35 or neg >= 2:
        return 'EXIT3', False, '0%'

    # ── EXIT2 — uscita forte ───────────────────────────────────────
    if score < 50 and sec_er < 0.5:
        return 'EXIT2', False, '20-30%'

    # ── EXIT1 — uscita parziale ────────────────────────────────────
    if not sar_bull or (not rs_ao_pos and score < 65):
        return 'EXIT1', False, '50-70%'

    # ── BUY3 — full entry ──────────────────────────────────────────
    if (score >= 80 and sec_er > 0.6 and rs_baff >= 3
            and rs_above and rs_trend == 'VERDE'):
        qualifies = not blocked and regime in ('CALMA', 'NORMALE', 'ATTENZIONE')
        return 'BUY3', qualifies, '100%'

    # ── BUY2 — confirmation ────────────────────────────────────────
    if (sar_bull and rs_ao_pos and sec_above and score >= 65 and sec_er >= 0.5):
        qualifies = not blocked and regime in ('CALMA', 'NORMALE', 'ATTENZIONE')
        return 'BUY2', qualifies, '70%'

    # ── BUY1 — early entry ─────────────────────────────────────────
    if (sec_above and sec_er > 0.4 and sar_bull
            and (kama_cross or rs_ao_imp)):
        qualifies = not blocked and regime in ('CALMA', 'NORMALE')
        return 'BUY1', qualifies, '30-40%'

    # WATCH / STOP
    if sec_above or sar_bull:
        return 'WATCH', False, '—'
    return 'STOP', False, '—'

# ─────────────────────────────────────────────────────────────────────────────
# ANALYZE ONE SECTOR
# ─────────────────────────────────────────────────────────────────────────────

def analyze_sector(sector, bench_close, regime):
    ticker = sector['ticker']
    label  = sector['label']
    name   = sector['name']

    ohlcv = get_ohlcv(ticker)
    if ohlcv is None:
        return None

    sec_close = ohlcv['close']; sec_high=ohlcv['high']
    sec_low=ohlcv['low'];       sec_open=ohlcv['open']
    n = min(len(sec_close), len(bench_close))
    if n < 55:
        return None

    sec_c=sec_close[-n:]; sec_h=sec_high[-n:]
    sec_l=sec_low[-n:];   sec_o=sec_open[-n:]
    ben_c=bench_close[-n:]

    # RS Line
    rs = [s/b if b > 0 else 1.0 for s, b in zip(sec_c, ben_c)]

    # Indicators on RS Line
    rs_kama   = calc_kama(rs)
    rs_er     = calc_er(rs)
    rs_trend  = trendycator(rs)
    rs_ao_val, rs_baff, rs_ao_imp = calc_ao(rs)
    rs_cross  = calc_cross_days(rs, rs_kama)
    rs_kpct   = round((rs[-1]/rs_kama[-1]-1)*100 if rs_kama[-1] else 0, 2)
    rs_p7     = round((rs[-1]/rs[-8]-1)*100  if len(rs)>=8  else 0, 2)
    rs_p30    = round((rs[-1]/rs[-31]-1)*100 if len(rs)>=31 else 0, 2)
    rs_above  = rs[-1] > rs_kama[-1]
    rs_rsi    = calc_rsi(rs)
    rs_ao_pos = rs_ao_val > 0

    # Indicators on ETF price
    sec_kama   = calc_kama(sec_c)
    sec_er     = calc_er(sec_c)
    sec_above  = sec_c[-1] > sec_kama[-1]
    sec_trend  = trendycator(sec_c)
    kama_cross = calc_kama_cross_up(sec_c, sec_kama, lookback=3)

    # SAR, Vortex, RVI
    sar_val, sar_bull           = calc_sar(sec_h, sec_l)
    vi_plus, vi_minus, vortex_bull = calc_vortex(sec_h, sec_l, sec_c)
    rvi_val, rvi_sig, rvi_bull  = calc_rvi(sec_c, sec_o, sec_h, sec_l)

    score = calc_score_rs(rs_er, rs_baff, rs_kpct, rs_p7, rs_p30, rs_ao_pos, rs_trend)

    d = {
        'ticker': ticker, 'label': label, 'name': name,
        'sec_er': sec_er, 'rs_er': rs_er, 'score': score,
        'rs_baff': rs_baff, 'sar_bullish': sar_bull, 'vortex_bullish': vortex_bull,
        'sec_above': sec_above, 'rs_above': rs_above, 'rs_trend': rs_trend,
        'rs_ao_pos': rs_ao_pos, 'rs_ao_improving': rs_ao_imp,
        'kama_cross_up': kama_cross, 'rs_kpct': rs_kpct,
    }
    signal, qualifies, size = calc_signal_buy(d, regime)

    return {
        'ticker': ticker, 'label': label, 'name': name,
        'price': round(sec_c[-1], 4),
        'sec_kama': round(sec_kama[-1], 4),
        'sec_above': sec_above,
        'sar': sar_val, 'sar_bullish': sar_bull,
        'vi_plus': vi_plus, 'vi_minus': vi_minus, 'vortex_bullish': vortex_bull,
        'rvi': rvi_val, 'rvi_signal': rvi_sig, 'rvi_bullish': rvi_bull,
        'rs': round(rs[-1], 6),
        'rs_kama': round(rs_kama[-1], 6),
        'rs_above': rs_above,
        'rs_kpct': rs_kpct,
        'rs_er': rs_er,
        'rs_baff': rs_baff,
        'rs_trend': rs_trend,
        'rs_cross': rs_cross,
        'rs_rsi': rs_rsi,
        'rs_p7': rs_p7,
        'rs_p30': rs_p30,
        'rs_ao_pos': rs_ao_pos,
        'rs_ao_improving': rs_ao_imp,
        'kama_cross_up': kama_cross,
        'sec_er': sec_er,
        'sec_trend': sec_trend,
        'score': score,
        'signal': signal,
        'size': size,
        'qualifies': qualifies,
    }

# ─────────────────────────────────────────────────────────────────────────────
# PORTFOLIO MANAGEMENT
# ─────────────────────────────────────────────────────────────────────────────

STATE_FILE = 'portfolio_state.json'

def load_state():
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE) as f: return json.load(f)
    return {k: {'positions': [], 'history': [], 'cooldown': {}} for k in PORTFOLIOS}

def save_state(state):
    with open(STATE_FILE, 'w') as f: json.dump(state, f, indent=2, default=str)

def update_portfolio(port_key, existing, candidates, cooldowns, today_str, regime):
    today    = date.fromisoformat(today_str)
    kept     = []
    exited   = []
    cand_map = {c['ticker']: c for c in candidates}

    # Aggiorna cooldown — rimuove scaduti
    active_cd = {}
    for t, exit_date_str in cooldowns.items():
        exit_date = date.fromisoformat(exit_date_str)
        if (today - exit_date).days < COOLDOWN_DAYS:
            active_cd[t] = exit_date_str

    # ── GESTIONE POSIZIONI APERTE ─────────────────────────────────
    for pos in existing:
        cur = cand_map.get(pos['ticker'])
        if cur is None:
            pos['warning'] = 'Dati non disponibili'
            kept.append(pos)
            continue

        entry_date = date.fromisoformat(pos['entry_date'])
        days_held  = (today - entry_date).days
        cur_gain   = round((cur['price'] / pos['entry_price'] - 1) * 100, 2)
        signal     = cur['signal']

        exit_reason = None

        # EXIT3 — uscita totale
        if signal == 'EXIT3':
            exit_reason = 'EXIT3 — Score < 35 o 2+ condizioni negative'
        # EXIT2 — uscita forte (score cade molto)
        elif signal == 'EXIT2':
            prev_score = pos.get('score', 100)
            if cur['score'] < prev_score - 10:
                exit_reason = 'EXIT2 — Score -10pt e ER < 0.5'
        # EXIT1b — time stop: dopo 7gg senza conferma BUY2/BUY3
        elif days_held >= 7 and signal == 'BUY1':
            exit_reason = f'EXIT1b — Time Stop: {days_held} giorni senza conferma'
        # Time stop assoluto a 10 giorni
        elif days_held >= 10:
            exit_reason = f'Time Stop — {days_held} giorni'

        if exit_reason:
            exited.append({**pos,
                'exit_reason': exit_reason,
                'exit_price': cur['price'],
                'final_gain_pct': cur_gain,
                'exit_date': today_str})
            active_cd[pos['ticker']] = today_str
            continue

        # Aggiorna dati posizione
        pre_alert  = days_held >= 7 and cur_gain < 5.0
        target_hit = cur_gain >= 5.0

        pos.update({
            'current_price':    cur['price'],
            'current_gain_pct': cur_gain,
            'days_held':        days_held,
            'pre_alert':        pre_alert,
            'target_hit':       target_hit,
            'pre_rs_alert':     cur['rs_trend'] == 'ROSSO',
            'signal':           signal,
            'size':             cur.get('size', '—'),
            'score':            cur['score'],
            'rs_er':            cur['rs_er'],
            'rs_trend':         cur['rs_trend'],
            'rs_baff':          cur['rs_baff'],
            'rs_kpct':          cur['rs_kpct'],
            'rs_above':         cur['rs_above'],
            'sec_above':        cur['sec_above'],
            'sec_er':           cur['sec_er'],
            'sar_bullish':      cur.get('sar_bullish', True),
            'vortex_bullish':   cur.get('vortex_bullish', True),
            'rvi_bullish':      cur.get('rvi_bullish', True),
            'warning':          None,
        })
        kept.append(pos)

    # ── APERTURA NUOVE POSIZIONI ──────────────────────────────────
    max_pos = MAX_POSITIONS.get(regime, 0)
    slots   = max_pos - len(kept)
    existing_tickers = {p['ticker'] for p in kept}
    blocked_tickers  = set(active_cd.keys())

    # Candidati ordinati: prima BUY3, poi BUY2, poi BUY1 — per score desc
    priority = {'BUY3': 0, 'BUY2': 1, 'BUY1': 2}
    new_q = sorted(
        [c for c in candidates
         if c['qualifies']
         and c['ticker'] not in existing_tickers
         and c['ticker'] not in blocked_tickers],
        key=lambda x: (priority.get(x['signal'], 9), -x['score'])
    )

    for c in new_q[:slots]:
        ep = c['price']
        kept.append({
            'ticker':           c['ticker'],
            'label':            c['label'],
            'name':             c['name'],
            'entry_date':       today_str,
            'entry_price':      ep,
            'current_price':    ep,
            'target_price':     round(ep * 1.05, 4),
            'current_gain_pct': 0.0,
            'days_held':        0,
            'pre_alert':        False,
            'target_hit':       False,
            'pre_rs_alert':     False,
            'signal':           c['signal'],
            'size':             c.get('size', '—'),
            'score':            c['score'],
            'rs_er':            c['rs_er'],
            'rs_trend':         c['rs_trend'],
            'rs_baff':          c['rs_baff'],
            'rs_kpct':          c['rs_kpct'],
            'rs_above':         c['rs_above'],
            'sec_above':        c['sec_above'],
            'sec_er':           c['sec_er'],
            'sar_bullish':      c.get('sar_bullish', True),
            'vortex_bullish':   c.get('vortex_bullish', True),
            'rvi_bullish':      c.get('rvi_bullish', True),
            'weight_pct':       0,
            'warning':          None,
        })

    # Pesi per score
    total_score = sum(max(p['score'], 1) for p in kept)
    for p in kept:
        p['weight_pct'] = round(max(p['score'], 1) / total_score * 100, 1) if total_score else 0

    return kept, exited, active_cd

# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────

def main():
    now       = datetime.now(ROME_TZ)
    today_str = now.date().isoformat()
    print(f"🦅 RAPTOR SETTORIALI — {now.strftime('%d/%m/%Y %H:%M')} CET")

    print("\n🌡️ VIX/VSTOXX...")
    vix, vstoxx = fetch_vix()
    regime = get_regime(vix, vstoxx)
    print(f"   VIX={vix} VSTOXX={vstoxx} Regime={regime} (fallback NORMALE se null)")

    state = load_state()
    output_ports = {}

    for port_key, port_cfg in PORTFOLIOS.items():
        print(f"\n📊 {port_cfg['name']}...")

        print(f"  Benchmark {port_cfg['benchmark']['label']}...", end=' ', flush=True)
        bench = get_ohlcv(port_cfg['benchmark']['ticker'])
        if bench is None:
            print("✗ SKIP"); continue
        bench_close = bench['close']
        print(f"✓ ({len(bench_close)} bars)")

        sector_data = []
        for sec in port_cfg['sectors']:
            print(f"  {sec['label']}...", end=' ', flush=True)
            r = analyze_sector(sec, bench_close, regime)
            if r:
                sector_data.append(r)
                print(f"✓ score={r['score']} er={r['sec_er']} signal={r['signal']} qualifies={r['qualifies']}")
            else:
                print("✗")

        if port_key not in state:
            state[port_key] = {'positions': [], 'history': [], 'cooldown': {}}

        positions, exited, cooldowns = update_portfolio(
            port_key,
            state[port_key].get('positions', []),
            sector_data,
            state[port_key].get('cooldown', {}),
            today_str,
            regime
        )

        state[port_key]['positions'] = positions
        state[port_key]['cooldown']  = cooldowns
        state[port_key].setdefault('history', []).extend(exited)

        bench_price = bench_close[-1]
        bench_p30   = round((bench_close[-1]/bench_close[-31]-1)*100 if len(bench_close)>=31 else 0, 2)

        buy3 = [d for d in sector_data if d['signal'] == 'BUY3']
        buy2 = [d for d in sector_data if d['signal'] == 'BUY2']
        buy1 = [d for d in sector_data if d['signal'] == 'BUY1']

        output_ports[port_key] = {
            'name':            port_cfg['name'],
            'benchmark_label': port_cfg['benchmark']['label'],
            'benchmark_price': round(bench_price, 4),
            'benchmark_p30':   bench_p30,
            'regime':          regime,
            'positions':       positions,
            'use_xeon':        len(positions) == 0,
            'cooldown':        cooldowns,
            'watchlist':       sorted([d for d in sector_data if not d['qualifies']], key=lambda x: x['score'], reverse=True)[:8],
            'qualified':       [d for d in sector_data if d['qualifies']],
            'all':             sorted(sector_data, key=lambda x: x['score'], reverse=True),
            'buy3_count':      len(buy3),
            'buy2_count':      len(buy2),
            'buy1_count':      len(buy1),
        }

        print(f"   → {len(positions)} pos | BUY3={len(buy3)} BUY2={len(buy2)} BUY1={len(buy1)} | {len(cooldowns)} CD")

    print("\n💰 XEON...")
    xeon = get_ohlcv(XEON_TICKER)
    xeon_price = round(xeon['close'][-1], 4) if xeon else None
    print(f"   XEON={xeon_price}")

    save_state(state)

    output = {
        'updated_at':      now.isoformat(),
        'updated_display': now.strftime('%d/%m/%Y %H:%M CET'),
        'vix':             vix,
        'vstoxx':          vstoxx,
        'regime':          regime,
        'xeon_price':      xeon_price,
        'portfolios':      output_ports,
    }

    with open('settoriali.json', 'w') as f:
        json.dump(output, f, indent=2, default=str)

    print(f"\n✅ settoriali.json — {now.strftime('%d/%m/%Y %H:%M CET')}")
    for k, p in output_ports.items():
        print(f"   {p['name']}: {len(p['positions'])} pos {'| XEON' if p['use_xeon'] else ''}")

if __name__ == '__main__':
    main()
