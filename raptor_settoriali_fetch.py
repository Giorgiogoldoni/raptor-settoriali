#!/usr/bin/env python3
"""
🦅 RAPTOR SETTORIALI — Autonomous Sector Portfolio Manager
3 portfolios: EUROPA / USA / MONDO (SPDR ETFs)
RS Line = Sector Price / Benchmark Price
Signals based on RS Line KAMA + Trendycator + Baffetti
VIX filter, cool-down 2 days, same sector allowed in multiple portfolios
Runs hourly 09:00-19:00 CET Mon-Fri via GitHub Actions
"""

import json, os
from datetime import datetime, date, timedelta
import pytz
import yfinance as yf
import pandas as pd

ROME_TZ = pytz.timezone("Europe/Rome")

# ─────────────────────────────────────────────────────────────────────────────
# UNIVERSE
# ─────────────────────────────────────────────────────────────────────────────

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

XEON_TICKER = "XEON.MI"
VIX_BLOCK   = 25.0   # above this → no new entries
VIX_CAUTION = 20.0   # between this and VIX_BLOCK → only LONG_CONF
COOLDOWN_DAYS = 2

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
            'close':  [float(x) for x in df['Close'].tolist()],
            'high':   [float(x) for x in df['High'].tolist()],
            'low':    [float(x) for x in df['Low'].tolist()],
            'dates':  [str(d.date()) for d in df.index.tolist()],
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
    avg = ((vix or 20) + (vstoxx or 20)) / 2
    if avg < 15:   return 'CALMA'
    if avg < 20:   return 'NORMALE'
    if avg < 25:   return 'ATTENZIONE'
    if avg < 30:   return 'STRESS'
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
    return round(ao_val, 6), baff

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

def calc_score_rs(er, baff, k_pct, p7, p30, ao_pos, trend):
    s = (er*30 + min(baff,10)*5 + min(abs(k_pct),5)*3
         + max(-10,min(5,p7))*4
         + max(-20,min(10,p30))*2
         + (5 if ao_pos else 0))
    if trend == 'ROSSO': s *= 0.6
    return round(s, 1)

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

    sec_close = ohlcv['close']
    n = min(len(sec_close), len(bench_close))
    if n < 55:
        return None

    sec_c  = sec_close[-n:]
    ben_c  = bench_close[-n:]

    # RS Line
    rs = [s/b if b > 0 else 1.0 for s, b in zip(sec_c, ben_c)]

    # Indicators on RS Line
    rs_kama  = calc_kama(rs)
    rs_er    = calc_er(rs)
    rs_trend = trendycator(rs)
    rs_ao, rs_baff = calc_ao(rs)
    rs_cross = calc_cross_days(rs, rs_kama)
    rs_kpct  = round((rs[-1]/rs_kama[-1]-1)*100 if rs_kama[-1] else 0, 2)
    rs_p7    = round((rs[-1]/rs[-8]-1)*100  if len(rs)>=8  else 0, 2)
    rs_p30   = round((rs[-1]/rs[-31]-1)*100 if len(rs)>=31 else 0, 2)
    rs_above = rs[-1] > rs_kama[-1]
    rs_rsi   = calc_rsi(rs)

    # Indicators on ETF price (absolute)
    sec_kama  = calc_kama(sec_c)
    sec_er    = calc_er(sec_c)
    sec_above = sec_c[-1] > sec_kama[-1]
    sec_trend = trendycator(sec_c)

    score = calc_score_rs(rs_er, rs_baff, rs_kpct, rs_p7, rs_p30, rs_ao > 0, rs_trend)

    # Signal
    signal = 'NEUTRO'
    if rs_above and rs_trend == 'VERDE' and rs_baff >= 3 and sec_above and rs_cross <= 5:
        if regime in ('CALMA', 'NORMALE'):
            signal = 'LONG'
        elif regime == 'ATTENZIONE':
            signal = 'LONG_CONF'  # more selective
        # STRESS/PAURA → no new entries
    elif rs_above and rs_trend in ('VERDE','GRIGIO') and rs_baff >= 1 and sec_above:
        signal = 'WATCH'
    elif not rs_above or not sec_above:
        signal = 'STOP' if (rs_trend == 'ROSSO' or sec_trend == 'ROSSO') else 'USCITA'

    qualifies = signal in ('LONG',) and regime not in ('STRESS','PAURA')
    if regime == 'ATTENZIONE' and signal == 'LONG_CONF':
        qualifies = True

    return {
        'ticker':     ticker,
        'label':      label,
        'name':       name,
        'price':      round(sec_c[-1], 4),
        'sec_kama':   round(sec_kama[-1], 4),
        'sec_above':  sec_above,
        'rs':         round(rs[-1], 6),
        'rs_kama':    round(rs_kama[-1], 6),
        'rs_above':   rs_above,
        'rs_kpct':    rs_kpct,
        'rs_er':      rs_er,
        'rs_baff':    rs_baff,
        'rs_trend':   rs_trend,
        'rs_cross':   rs_cross,
        'rs_rsi':     rs_rsi,
        'rs_p7':      rs_p7,
        'rs_p30':     rs_p30,
        'rs_ao_pos':  rs_ao > 0,
        'sec_er':     sec_er,
        'sec_trend':  sec_trend,
        'score':      score,
        'signal':     signal,
        'qualifies':  qualifies,
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
    today  = date.fromisoformat(today_str)
    kept   = []
    exited = []

    # Update cooldowns — remove expired
    active_cd = {}
    for t, exit_date_str in cooldowns.items():
        exit_date = date.fromisoformat(exit_date_str)
        if (today - exit_date).days < COOLDOWN_DAYS:
            active_cd[t] = exit_date_str

    for pos in existing:
        cur = next((c for c in candidates if c['ticker'] == pos['ticker']), None)
        if cur is None:
            pos['warning'] = 'Dati non disponibili'
            kept.append(pos)
            continue

        entry_date = date.fromisoformat(pos['entry_date'])
        days_held  = (today - entry_date).days
        cur_gain   = round((cur['price'] / pos['entry_price'] - 1) * 100, 2)

        exit_reason = None
        if not cur['rs_above']:
            exit_reason = 'RS Line sotto KAMA RS'
        elif not cur['sec_above']:
            exit_reason = 'Prezzo ETF sotto KAMA'
        elif days_held >= 10:
            exit_reason = f'Time Stop — {days_held} giorni'
        elif cur['score'] < 15:
            exit_reason = 'Score RS < 15'

        if exit_reason:
            exited.append({**pos, 'exit_reason': exit_reason,
                           'exit_price': cur['price'],
                           'final_gain_pct': cur_gain,
                           'exit_date': today_str})
            # Start cooldown
            active_cd[pos['ticker']] = today_str
            continue

        pre_alert  = days_held >= 7 and cur_gain < 5.0
        target_hit = cur_gain >= 5.0
        pre_rs     = cur['rs_trend'] == 'ROSSO'

        pos.update({
            'current_price':    cur['price'],
            'current_gain_pct': cur_gain,
            'days_held':        days_held,
            'pre_alert':        pre_alert,
            'target_hit':       target_hit,
            'pre_rs_alert':     pre_rs,
            'signal':           cur['signal'],
            'score':            cur['score'],
            'rs_er':            cur['rs_er'],
            'rs_trend':         cur['rs_trend'],
            'rs_baff':          cur['rs_baff'],
            'rs_kpct':          cur['rs_kpct'],
            'rs_above':         cur['rs_above'],
            'sec_above':        cur['sec_above'],
            'warning':          None,
        })
        kept.append(pos)

    # Fill empty slots — ranked by RS ER desc
    slots = 5 - len(kept)
    existing_tickers = {p['ticker'] for p in kept}
    blocked_tickers  = set(active_cd.keys())

    new_q = sorted(
        [c for c in candidates
         if c['qualifies']
         and c['ticker'] not in existing_tickers
         and c['ticker'] not in blocked_tickers
         and regime not in ('STRESS', 'PAURA')],
        key=lambda x: x['rs_er'], reverse=True
    )

    for c in new_q[:slots]:
        ep   = c['price']
        kept.append({
            'ticker':        c['ticker'],
            'label':         c['label'],
            'name':          c['name'],
            'entry_date':    today_str,
            'entry_price':   ep,
            'current_price': ep,
            'target_price':  round(ep * 1.05, 4),
            'current_gain_pct': 0.0,
            'days_held':     0,
            'pre_alert':     False,
            'target_hit':    False,
            'pre_rs_alert':  False,
            'signal':        c['signal'],
            'score':         c['score'],
            'rs_er':         c['rs_er'],
            'rs_trend':      c['rs_trend'],
            'rs_baff':       c['rs_baff'],
            'rs_kpct':       c['rs_kpct'],
            'rs_above':      c['rs_above'],
            'sec_above':     c['sec_above'],
            'weight_pct':    0,
            'warning':       None,
        })

    # Weights by score
    total_score = sum(p['score'] for p in kept)
    for p in kept:
        p['weight_pct'] = round(p['score']/total_score*100, 1) if total_score else 0

    return kept, exited, active_cd

# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────

def main():
    now       = datetime.now(ROME_TZ)
    today_str = now.date().isoformat()
    print(f"🦅 RAPTOR SETTORIALI — {now.strftime('%d/%m/%Y %H:%M')} CET")

    # VIX
    print("\n🌡️ VIX/VSTOXX...")
    vix, vstoxx = fetch_vix()
    regime = get_regime(vix, vstoxx)
    print(f"   VIX={vix} VSTOXX={vstoxx} Regime={regime}")

    state = load_state()
    output_ports = {}

    for port_key, port_cfg in PORTFOLIOS.items():
        print(f"\n📊 {port_cfg['name']}...")

        # Fetch benchmark
        print(f"  Benchmark {port_cfg['benchmark']['label']}...", end=' ', flush=True)
        bench = get_ohlcv(port_cfg['benchmark']['ticker'])
        if bench is None:
            print("✗ SKIP")
            continue
        bench_close = bench['close']
        print(f"✓ ({len(bench_close)} bars)")

        # Analyze sectors
        sector_data = []
        for sec in port_cfg['sectors']:
            print(f"  {sec['label']}...", end=' ', flush=True)
            r = analyze_sector(sec, bench_close, regime)
            if r:
                sector_data.append(r)
                print(f"✓ score={r['score']} rs_er={r['rs_er']} signal={r['signal']}")
            else:
                print("✗")

        # Ensure state key exists
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

        # Benchmark info
        bench_price = bench_close[-1]
        bench_p30   = round((bench_close[-1]/bench_close[-31]-1)*100 if len(bench_close)>=31 else 0, 2)

        output_ports[port_key] = {
            'name':           port_cfg['name'],
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
        }

        print(f"   → {len(positions)} posizioni {'| XEON' if not positions else ''} | {len(cooldowns)} in cooldown")

    # XEON
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

    print(f"\n✅ settoriali.json aggiornato — {now.strftime('%d/%m/%Y %H:%M CET')}")
    for k, p in output_ports.items():
        print(f"   {p['name']}: {len(p['positions'])} pos {'| XEON' if p['use_xeon'] else ''}")

if __name__ == '__main__':
    main()
