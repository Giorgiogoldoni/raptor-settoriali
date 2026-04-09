#!/usr/bin/env python3
"""
RAPTOR SETTORIALI — Chart Fetch
Downloads OHLCV data for all sector ETFs + benchmarks
Runs twice daily: 11:00 CET + 17:00 CET (Mon-Fri)
"""

import json, time, datetime
import yfinance as yf
import pandas as pd

TICKERS = [
    # Benchmarks
    {"y": "600X.MI",  "t": "600X"},
    {"y": "SP5A.MI",  "t": "SP5A"},
    {"y": "SWRD.MI",  "t": "SWRD"},
    # Europa
    {"y": "DFSV.DE",  "t": "DFSV"},
    {"y": "STKX.MI",  "t": "STKX"},
    {"y": "STNX.MI",  "t": "STNX"},
    {"y": "STPX.MI",  "t": "STPX"},
    {"y": "STQX.MI",  "t": "STQX"},
    {"y": "STRX.MI",  "t": "STRX"},
    {"y": "STSX.MI",  "t": "STSX"},
    {"y": "STTX.MI",  "t": "STTX"},
    {"y": "STUX.MI",  "t": "STUX"},
    {"y": "STWX.MI",  "t": "STWX"},
    {"y": "STZX.MI",  "t": "STZX"},
    # USA
    {"y": "SXLB.MI",  "t": "SXLB"},
    {"y": "SXLC.MI",  "t": "SXLC"},
    {"y": "SXLE.MI",  "t": "SXLE"},
    {"y": "SXLF.MI",  "t": "SXLF"},
    {"y": "SXLI.MI",  "t": "SXLI"},
    {"y": "SXLK.MI",  "t": "SXLK"},
    {"y": "SXLP.MI",  "t": "SXLP"},
    {"y": "SXLU.MI",  "t": "SXLU"},
    {"y": "SXLV.MI",  "t": "SXLV"},
    {"y": "SXLY.MI",  "t": "SXLY"},
    {"y": "MGIN.MI",  "t": "MGIN"},
    # Mondo
    {"y": "WCOD.MI",  "t": "WCOD"},
    {"y": "WCOS.MI",  "t": "WCOS"},
    {"y": "WFIN.MI",  "t": "WFIN"},
    {"y": "WHEA.MI",  "t": "WHEA"},
    {"y": "WIND.MI",  "t": "WIND"},
    {"y": "WMAT.MI",  "t": "WMAT"},
    {"y": "WNRG.MI",  "t": "WNRG"},
    {"y": "WTEC.MI",  "t": "WTEC"},
    {"y": "WTEL.MI",  "t": "WTEL"},
    {"y": "WUTI.MI",  "t": "WUTI"},
    # XEON
    {"y": "XEON.MI",  "t": "XEON"},
]

def parse_hist(hist):
    bars = []
    for ts, row in hist.iterrows():
        try:
            o = float(row['Open']); h = float(row['High'])
            l = float(row['Low']);  c = float(row['Close'])
            v = int(row['Volume'])
            if any(x != x for x in [o,h,l,c]): continue
            bars.append([int(ts.timestamp()), round(o,4), round(h,4), round(l,4), round(c,4), v])
        except: pass
    return bars

def fetch_ticker(symbol):
    tk = yf.Ticker(symbol)
    daily = []; hourly = []
    try:
        hist_d = tk.history(period='1y', interval='1d', timeout=20)
        if isinstance(hist_d.columns, pd.MultiIndex):
            hist_d.columns = hist_d.columns.get_level_values(0)
        if not hist_d.empty:
            daily = parse_hist(hist_d)
    except: pass
    time.sleep(0.3)
    try:
        hist_h = tk.history(period='5d', interval='1h', timeout=20)
        if isinstance(hist_h.columns, pd.MultiIndex):
            hist_h.columns = hist_h.columns.get_level_values(0)
        if not hist_h.empty:
            hourly = parse_hist(hist_h)
    except: pass
    return daily, hourly

def main():
    now = datetime.datetime.now()
    print(f"🦅 RAPTOR SETTORIALI Chart Fetch — {now.strftime('%Y-%m-%d %H:%M')}")
    print(f"Ticker: {len(TICKERS)}")

    chart_data = {}
    ok = 0; errors = 0

    for i, info in enumerate(TICKERS):
        sym = info['y']
        try:
            daily, hourly = fetch_ticker(sym)
            if daily:
                chart_data[sym] = {'d': daily, 'h': hourly}
                ok += 1
            else:
                errors += 1
        except:
            errors += 1
        if (i+1) % 10 == 0:
            print(f"  {i+1}/{len(TICKERS)} — ok:{ok} err:{errors}")
        time.sleep(0.2)

    output = {
        'timestamp':    now.isoformat(),
        'timestamp_it': now.strftime('%d/%m/%Y %H:%M'),
        'total':  len(TICKERS),
        'ok':     ok,
        'errors': errors,
        'data':   chart_data
    }

    with open('settoriali_chart.json', 'w') as f:
        json.dump(output, f, ensure_ascii=False, separators=(',',':'))

    import os
    size_mb = os.path.getsize('settoriali_chart.json') / 1024 / 1024
    print(f"\n✅ settoriali_chart.json — {ok} OK, {errors} errori — {size_mb:.1f} MB")

if __name__ == '__main__':
    main()
