import yfinance as yf
import numpy as np
import pandas as pd

DURATION_MAP = {
    "1 Day":      ("5d",  "30m"),
    "5 Days":     ("1mo", "1h"),
    "1 Month":    ("1mo", "1d"),
    "3 Months":   ("3mo", "1d"),
    "6 Months":   ("6mo", "1d"),
    "1 Year":     ("1y",  "1d"),
    "3 Years":    ("3y",  "1wk"),
    "5 Years":    ("5y",  "1wk"),
    "Max":        ("max", "1mo"),
}

def price_snapshot(symbols):
    out = []
    for s in symbols:
        try:
            t = yf.Ticker(s)
            hist = t.history(period="5d", interval="1d", auto_adjust=False)
            close = hist["Close"].dropna()
            last = float(close.iloc[-1]) if len(close) else None
            prev = float(close.iloc[-2]) if len(close) > 1 else None
            chg = last - prev if (last is not None and prev is not None) else None
            chg_pct = ((last/prev)-1.0)*100 if (last is not None and prev and prev!=0) else None
            out.append({"symbol": s, "last": last, "prev": prev, "chg": chg, "chg_pct": chg_pct})
        except Exception:
            out.append({"symbol": s, "last": None, "prev": None, "chg": None, "chg_pct": None})
    return pd.DataFrame(out)

def history(symbol, duration_label):
    period, interval = DURATION_MAP.get(duration_label, ("6mo","1d"))
    hist = yf.Ticker(symbol).history(period=period, interval=interval, auto_adjust=False)
    if hist is None or hist.empty:
        return pd.DataFrame()
    return hist.reset_index()
