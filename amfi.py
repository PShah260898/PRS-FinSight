import io
import time
import requests
import pandas as pd
from functools import lru_cache

AMFI_URL = "https://www.amfiindia.com/spages/NAVAll.txt"

# The AMFI file is ~10–15MB. Cache in memory for 15 minutes to be gentle.
_CACHE = {"df": None, "ts": 0, "ttl": 900}

def _load_from_amfi() -> pd.DataFrame:
    r = requests.get(AMFI_URL, timeout=30)
    r.raise_for_status()
    # AMFI is pipe- or semicolon-separated depending on snapshot; normalize.
    raw = r.text.strip()
    # Fast sniff of delimiter: try ';' first (common), then ',' then '|'
    delim = ';' if raw.splitlines()[0].count(';') >= 3 else ('|' if raw.splitlines()[0].count('|') >= 3 else ',')
    df = pd.read_csv(io.StringIO(raw), sep=delim, engine="python")
    # Standardize column names we care about
    cols = {c.lower().strip(): c for c in df.columns}
    # Heuristics: AMFI header variants
    rename_map = {}
    for c in df.columns:
        lc = c.lower().strip()
        if "scheme code" in lc or lc == "schemecode" or lc == "scheme code":
            rename_map[c] = "scheme_code"
        elif "scheme name" in lc:
            rename_map[c] = "scheme_name"
        elif lc.startswith("isin") and "payout" in lc:
            rename_map[c] = "isin_div_payout"
        elif lc.startswith("isin") and "reinvestment" in lc:
            rename_map[c] = "isin_div_reinvest"
        elif lc.startswith("isin") and "growth" in lc:
            rename_map[c] = "isin_growth"
        elif lc.startswith("net asset") or lc == "nav" or "net asset value" in lc:
            rename_map[c] = "nav"
        elif lc == "date" or "date" in lc:
            rename_map[c] = "date"
        elif "amc" in lc or "fund house" in lc:
            rename_map[c] = "amc"
        elif "category" in lc:
            rename_map[c] = "category"
        elif "plan" in lc:
            rename_map[c] = "plan"
        elif "option" in lc:
            rename_map[c] = "option"
    df = df.rename(columns=rename_map)
    keep = [c for c in ["scheme_code","scheme_name","amc","category","plan","option",
                        "isin_growth","isin_div_payout","isin_div_reinvest","nav","date"] if c in df.columns]
    df = df[keep].copy()
    # Types
    if "nav" in df.columns:
        df["nav"] = pd.to_numeric(df["nav"], errors="coerce")
    if "scheme_code" in df.columns:
        df["scheme_code"] = pd.to_numeric(df["scheme_code"], errors="coerce").astype("Int64")
    # Some rows may be empty
    df = df.dropna(subset=["scheme_name"])
    return df

def get_amfi_df(force_refresh=False) -> pd.DataFrame:
    now = time.time()
    if (not force_refresh) and _CACHE["df"] is not None and (now - _CACHE["ts"] < _CACHE["ttl"]):
        return _CACHE["df"]
    df = _load_from_amfi()
    _CACHE["df"] = df
    _CACHE["ts"] = now
    return df

def search_amfi(q: str = "", amc: str | None = None, category: str | None = None, limit: int = 100) -> pd.DataFrame:
    df = get_amfi_df()
    mask = pd.Series([True]*len(df))
    if q:
        q = q.strip()
        mask &= (df["scheme_name"].str.contains(q, case=False, na=False) |
                 (df["scheme_code"].astype(str).str.contains(q, case=False, na=False)))
    if amc and "amc" in df.columns:
        mask &= df["amc"].str.contains(amc, case=False, na=False)
    if category and "category" in df.columns:
        mask &= df["category"].str.contains(category, case=False, na=False)
    out = df[mask].head(limit).copy()
    return out

def get_schemes_by_codes(codes: list[int]) -> pd.DataFrame:
    df = get_amfi_df()
    if "scheme_code" not in df.columns: return pd.DataFrame(columns=df.columns)
    return df[df["scheme_code"].astype("Int64").isin(pd.Series(codes, dtype="Int64"))].copy()
