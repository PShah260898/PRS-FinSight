import os, hashlib
from datetime import datetime
import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
import feedparser
from pathlib import Path

from db import (
    init_db, create_user, get_user_by_username,
    insert_tx, fetch_tx,
    add_watch, del_watch, get_watchlist,
    add_post, update_post_status, fetch_posts,
    add_message, fetch_messages, unread_count, mark_all_seen,
    add_inquiry, fetch_inquiries
)
from pricing import price_snapshot, history, DURATION_MAP

# ---------- App config ----------
st.set_page_config(page_title="PRS FinSight", page_icon="📈", layout="wide")
if "user" not in st.session_state: st.session_state.user = None
if "settings" not in st.session_state:
    st.session_state.settings = {
        "auto_refresh": False,
        "refresh_secs": 60,
        "default_duration": "6 Months",
    }
if "bot" not in st.session_state:
    st.session_state.bot = {"mode": "idle", "step": 0, "data": {}}

# ---------- Helpers ----------
def hash_pw(password: str, salt: str) -> str:
    return hashlib.sha256((salt + password).encode("utf-8")).hexdigest()

@st.cache_data
def load_catalog():
    p = Path("data/symbols.csv")
    if not p.exists():
        return pd.DataFrame(columns=["symbol","name","type","country","sector"])
    df = pd.read_csv(p)
    for c in ["symbol","name","type","country","sector"]:
        if c not in df.columns: df[c]=""
    df["symbol"]=df["symbol"].astype(str)
    return df[["symbol","name","type","country","sector"]]

def login_view():
    st.header("Welcome to PRS FinSight")
    t1, t2 = st.tabs(["🔐 Login", "🆕 Register"])

    with t1:
        with st.form("login"):
            username = st.text_input("Username").strip()
            password = st.text_input("Password", type="password")
            ok = st.form_submit_button("Login")
        if ok:
            row = get_user_by_username(username)
            if not row:
                st.error("User not found.")
            else:
                user_id, full_name, uname, email, phone, pw_hash, salt, created_at = row
                if hash_pw(password, salt) == pw_hash:
                    st.session_state.user = {"id": user_id, "name": full_name or uname, "username": uname, "email": email, "phone": phone}
                    st.success(f"Welcome, {st.session_state.user['name']}!")
                    st.rerun()
                else:
                    st.error("Invalid password.")

    with t2:
        with st.form("register", clear_on_submit=False):
            full_name = st.text_input("Full Name")
            username  = st.text_input("Username").strip()
            email     = st.text_input("Email")
            phone     = st.text_input("Mobile Number")
            pw        = st.text_input("New Password", type="password")
            pw2       = st.text_input("Confirm Password", type="password")
            ok2 = st.form_submit_button("Create Account")
        if ok2:
            if not full_name or not username or not pw:
                st.error("Full Name, Username and Password are required.")
            elif pw != pw2:
                st.error("Passwords do not match.")
            else:
                salt = os.urandom(8).hex()
                pw_hash = hash_pw(pw, salt)
                try:
                    create_user(full_name, username, email, phone, pw_hash, salt)
                    st.success("Account created. Please login.")
                except Exception as e:
                    st.error(f"Could not create user: {e}")

def sidebar_nav():
    with st.sidebar:
        st.image("https://dummyimage.com/600x120/0b3d5c/ffffff&text=PRS+FinSight", use_column_width=True)
        if st.session_state.user:
            st.markdown(f"**User:** {st.session_state.user['name']}")
            # Unread badge for admin only (user_id==1 is owner)
            if st.session_state.user["id"]==1:
                uc = unread_count()
                if uc>0: st.warning(f"🔔 {uc} new client messages")
            auto = st.toggle("Auto-refresh", value=st.session_state.settings["auto_refresh"])
            st.session_state.settings["auto_refresh"] = auto
            secs = st.slider("Refresh every (secs)", 15, 300, st.session_state.settings["refresh_secs"], step=15)
            st.session_state.settings["refresh_secs"] = secs
            st.session_state.settings["default_duration"] = st.selectbox(
                "Default chart range", list(DURATION_MAP.keys()),
                index=list(DURATION_MAP.keys()).index(st.session_state.settings["default_duration"])
            )
            st.divider()
            if st.button("Log out"):
                st.session_state.user = None
                st.rerun()
        page = st.radio("Navigate", [
            "🏠 Dashboard","🧾 Transactions","📊 Portfolio",
            "🌍 Screener","🔎 Discover","⭐ Watchlist",
            "🧠 My Analysis","🤖 Assistant","💬 Q&A","📰 News+",
            "📨 Inquiry","⚙️ Settings"
        ])
        return page

def compute_holdings(tx_df: pd.DataFrame) -> pd.DataFrame:
    if tx_df.empty:
        return pd.DataFrame(columns=["symbol","asset_type","units","avg_cost","invested","last","value","pnl","pnl_pct"])
    df = tx_df.copy()
    df["symbol"] = df["symbol"].str.upper()
    df.loc[df["txn_type"]=="SIP","txn_type"]="BUY"
    df["signed_units"] = np.where(df["txn_type"]=="SELL",-df["units"], np.where(df["txn_type"]=="BUY", df["units"], 0.0))
    buys = df[df["txn_type"]=="BUY"].copy()
    buys["cost"] = buys["units"]*buys["price"] + buys["fees"]
    if buys.empty:
        net = df.groupby(["symbol","asset_type"], as_index=False).agg(units=("signed_units","sum"))
        net["avg_cost"]=0.0; net["invested"]=0.0
    else:
        cost = buys.groupby(["symbol","asset_type"], as_index=False).agg(total_units=("units","sum"), total_cost=("cost","sum"))
        net  = df.groupby(["symbol","asset_type"], as_index=False).agg(units=("signed_units","sum"))
        net  = net.merge(cost, on=["symbol","asset_type"], how="left").fillna({"total_units":0.0,"total_cost":0.0})
        net["avg_cost"] = np.where(net["total_units"]>0, net["total_cost"]/net["total_units"], 0.0)
        net["invested"] = net["total_cost"]
    snaps = price_snapshot(net["symbol"].tolist())
    out = net.merge(snaps[["symbol","last"]], on="symbol", how="left")
    out["value"] = out["units"]*out["last"]
    out["pnl"] = (out["last"]-out["avg_cost"])*out["units"]
    out["pnl_pct"] = np.where(out["avg_cost"]>0, (out["last"]/out["avg_cost"]-1.0)*100, np.nan)
    return out[["symbol","asset_type","units","avg_cost","invested","last","value","pnl","pnl_pct"]].sort_values("symbol")

# ---------- News helpers ----------
NEWS_FEEDS = {
    "Markets": [
        "https://feeds.finance.yahoo.com/rss/2.0/headline?s=%5EGSPC&region=US&lang=en-US",
        "https://www.cnbc.com/id/100003114/device/rss/rss.html"
    ],
    "Crypto": [
        "https://www.coindesk.com/arc/outboundfeeds/rss/"
    ],
    "Business & Politics": [
        "https://www.cnbc.com/id/10000664/device/rss/rss.html"
    ],
}

def read_rss(url, limit=12):
    try:
        feed = feedparser.parse(url)
        src = feed.feed.get("title","RSS")
        rows = []
        for e in feed.entries[:limit]:
            rows.append({
                "source": src,
                "title": e.get("title",""),
                "link": e.get("link",""),
                "published": e.get("published","")
            })
        return pd.DataFrame(rows)
    except Exception:
        return pd.DataFrame(columns=["source","title","link","published"])

def aggregate_news():
    dfs=[]
    for cat, urls in NEWS_FEEDS.items():
        for u in urls:
            df = read_rss(u)
            if not df.empty:
                df["category"]=cat
                dfs.append(df)
    if not dfs: return pd.DataFrame(columns=["category","source","title","link","published"])
    out = pd.concat(dfs).drop_duplicates(subset=["title"]).reset_index(drop=True)
    return out

# ---------- App ----------
init_db()
if not st.session_state.user:
    login_view()
    st.stop()

page = sidebar_nav()
user = st.session_state.user

# 1) Dashboard
if page == "🏠 Dashboard":
    st.header("Dashboard")
    tx = fetch_tx(user["id"])
    holdings = compute_holdings(tx)
    total_value = float(np.nansum(holdings["value"])) if not holdings.empty else 0.0
    invested    = float(np.nansum(holdings["invested"])) if not holdings.empty else 0.0
    pnl         = float(np.nansum(holdings["pnl"])) if not holdings.empty else 0.0
    pnl_pct     = (total_value/invested-1.0)*100 if invested>0 else 0.0
    c1,c2,c3,c4 = st.columns(4)
    c1.metric("Portfolio Value", f"${total_value:,.2f}")
    c2.metric("Invested (est.)", f"${invested:,.2f}")
    c3.metric("Total P/L", f"${pnl:,.2f}")
    c4.metric("Return %", f"{pnl_pct:,.2f}%")
    st.divider()
    dur = st.session_state.settings["default_duration"]
    sym = st.text_input("Chart a symbol", value=holdings["symbol"].iloc[0] if not holdings.empty else "AAPL").strip()
    if sym:
        h = history(sym, dur)
        if h.empty:
            st.info("No history found.")
        else:
            colA, colB = st.columns(2)
            with colA:
                st.subheader(f"{sym} — Close ({dur})")
                fig = px.line(h, x=h.columns[0], y="Close")
                st.plotly_chart(fig, use_container_width=True)
            with colB:
                if set(["Open","High","Low","Close"]).issubset(h.columns):
                    st.subheader(f"{sym} — Candlestick ({dur})")
                    fig2 = go.Figure(data=[go.Candlestick(
                        x=h[h.columns[0]], open=h["Open"], high=h["High"], low=h["Low"], close=h["Close"]
                    )])
                    fig2.update_layout(xaxis_rangeslider_visible=False, height=420)
                    st.plotly_chart(fig2, use_container_width=True)

# 2) Transactions
elif page == "🧾 Transactions":
    st.header("Transactions")
    with st.form("add_tx", clear_on_submit=True):
        c1,c2,c3 = st.columns(3)
        date = c1.date_input("Date", value=pd.Timestamp.today())
        symbol = c2.text_input("Symbol (e.g., AAPL, BTC-USD)").strip()
        account = c3.text_input("Account", value="Default").strip()
        c4,c5,c6 = st.columns(3)
        asset = c4.selectbox("Asset Type", ["stock","mutual_fund","etf","crypto"])
        ttype = c5.selectbox("Txn Type", ["BUY","SELL","DIV","SIP"])
        units = c6.number_input("Units", value=1.0, step=0.1)
        c7,c8 = st.columns(2)
        price = c7.number_input("Price per Unit", value=100.0, step=0.01)
        fees  = c8.number_input("Fees", value=0.0, step=0.01)
        ok = st.form_submit_button("Add")
    if ok:
        if not symbol: st.error("Symbol is required.")
        else:
            insert_tx(user["id"], date, symbol, asset, ttype, units, price, fees, account)
            st.success("Transaction added.")
    st.subheader("All Transactions")
    st.dataframe(fetch_tx(user["id"]), use_container_width=True)

# 3) Portfolio
elif page == "📊 Portfolio":
    st.header("Portfolio")
    tx = fetch_tx(user["id"])
    holdings = compute_holdings(tx)
    if holdings.empty:
        st.info("No holdings yet. Add transactions first.")
    else:
        # Join with catalog for country/sector filters
        cat = load_catalog()
        df = holdings.merge(cat, on="symbol", how="left")
        c1,c2,c3 = st.columns(3)
        country = c1.multiselect("Country", sorted(cat["country"].dropna().unique().tolist()))
        a_type  = c2.multiselect("Type", sorted(cat["type"].dropna().unique().tolist()))
        sector  = c3.multiselect("Sector", sorted(cat["sector"].dropna().unique().tolist()))
        filt = pd.Series([True]*len(df))
        if country: filt &= df["country"].isin(country)
        if a_type:  filt &= df["asset_type"].isin(a_type) | df["type"].isin(a_type)
        if sector:  filt &= df["sector"].isin(sector)
        st.dataframe(df[filt], use_container_width=True)
        st.divider()
        st.subheader("Allocation by Asset Type")
        if df["value"].notna().any():
            alloc = df.groupby("asset_type", as_index=False).agg(value=("value","sum"))
            fig = px.pie(alloc, names="asset_type", values="value")
            st.plotly_chart(fig, use_container_width=True)

# 4) Screener (country/type/sector + compare)
elif page == "🌍 Screener":
    st.header("Screener")
    cat = load_catalog()
    c1,c2,c3,c4 = st.columns([2,2,2,3])
    q = c1.text_input("Search").strip()
    countries = c2.multiselect("Country", sorted(cat["country"].unique()))
    types     = c3.multiselect("Type", sorted(cat["type"].unique()))
    sectors   = c4.multiselect("Sector", sorted(cat["sector"].dropna().unique()))
    df = cat.copy()
    if q:
        df = df[df["symbol"].str.contains(q, case=False, na=False) | df["name"].str.contains(q, case=False, na=False)]
    if countries: df = df[df["country"].isin(countries)]
    if types:     df = df[df["type"].isin(types)]
    if sectors:   df = df[df["sector"].isin(sectors)]
    if df.empty:
        st.info("No matches.")
    else:
        # show live price under each
        syms = df["symbol"].tolist()[:50]
        snaps = price_snapshot(syms)
        df = df.merge(snaps, on="symbol", how="left")
        st.dataframe(df, use_container_width=True)
        st.caption("Tip: add more rows to data/symbols.csv for a bigger universe.")
        cA,cB,cC = st.columns([1,1,2])
        add_one = cA.text_input("Add to watchlist (symbol)").strip()
        if cA.button("Add"):
            if add_one:
                add_watch(user["id"], add_one)
                st.success(f"Added {add_one}.")
        # Compare
        comp = cB.text_input("Compare (comma symbols, up to 3)", placeholder="AAPL, MSFT, NVDA").strip()
        if cB.button("Compare"):
            picks = [s.strip() for s in comp.split(",") if s.strip()][:3]
            if not picks: st.warning("Add up to 3 symbols.")
            else:
                dur = st.session_state.settings["default_duration"]
                merged=None
                for s in picks:
                    h = history(s, dur)
                    if h.empty: continue
                    sub = h[[h.columns[0],"Close"]].rename(columns={"Close":s})
                    merged = sub if merged is None else merged.merge(sub, on=h.columns[0], how="outer")
                if merged is not None:
                    fig = px.line(merged, x=merged.columns[0], y=[c for c in merged.columns[1:]])
                    st.plotly_chart(fig, use_container_width=True)
                else:
                    st.info("No data for these symbols.")

# 5) Discover
elif page == "🔎 Discover":
    st.header("Discover symbols & add")
    cat = load_catalog()
    q = st.text_input("Search by symbol or name").strip()
    df = cat
    if q:
        df = df[df["symbol"].str.contains(q, case=False, na=False) | df["name"].str.contains(q, case=False, na=False)]
    if df.empty:
        st.info("No matches.")
    else:
        for r in df.head(60).itertuples(index=False):
            c1,c2,c3,c4,c5 = st.columns([3,2,2,2,1])
            c1.markdown(f"**{r.symbol}** — {r.name}")
            c2.write(r.type); c3.write(r.country)
            if c4.button("Mini Chart", key=f"mc_{r.symbol}"):
                with st.expander(f"Chart: {r.symbol}", expanded=True):
                    h = history(r.symbol, st.session_state.settings["default_duration"])
                    if h.empty: st.caption("No data")
                    else: st.plotly_chart(px.line(h, x=h.columns[0], y="Close"), use_container_width=True)
            if c5.button("➕", key=f"add_{r.symbol}"):
                add_watch(user["id"], r.symbol)
                st.success(f"Added {r.symbol} to watchlist.")
                st.rerun()

# 6) Watchlist
elif page == "⭐ Watchlist":
    st.header("Watchlist")
    cA,cB = st.columns([2,1])
    with cA:
        sym = st.text_input("Add symbol", placeholder="AAPL, BTC-USD, TCS.NS").strip()
        alias = st.text_input("Alias (optional)").strip()
        if st.button("Add"):
            if sym:
                add_watch(user["id"], sym, alias or None)
                st.success("Added.")
                st.rerun()
    with cB:
        rm = st.text_input("Remove symbol").strip()
        if st.button("Remove"):
            if rm:
                del_watch(user["id"], rm)
                st.success("Removed.")
                st.rerun()
    wl = get_watchlist(user["id"])
    if wl.empty:
        st.info("Empty watchlist. Try Discover tab.")
    else:
        st.dataframe(wl, use_container_width=True)
        snaps = price_snapshot(wl["symbol"].tolist())
        st.subheader("Snapshots")
        st.dataframe(snaps, use_container_width=True)
        st.subheader("Mini charts")
        cols = st.columns(3)
        for i, s in enumerate(wl["symbol"].tolist()[:9]):
            with cols[i%3]:
                h = history(s, st.session_state.settings["default_duration"])
                st.write(f"**{s}**")
                if h.empty: st.caption("No data")
                else: st.plotly_chart(px.line(h, x=h.columns[0], y="Close"), use_container_width=True)

# 7) My Analysis (auto charts by tags)
elif page == "🧠 My Analysis":
    st.header("My Analysis")
    with st.form("post_form", clear_on_submit=True):
        title = st.text_input("Title").strip()
        symbols = st.text_input("Symbols (comma-separated) e.g. AAPL, NVDA, BTC-USD").strip()
        content = st.text_area("Content", height=200)
        status = st.selectbox("Status", ["draft","published"], index=0)
        ok = st.form_submit_button("Save")
    if ok:
        if not title or not content:
            st.error("Title and content are required.")
        else:
            add_post(user["id"], title, content, symbols, status)
            st.success("Saved.")
    posts = fetch_posts(user_id=user["id"])
    st.subheader("My Posts")
    if posts.empty:
        st.info("No posts yet.")
    else:
        for _, r in posts.iterrows():
            with st.expander(f"{r['title']}  —  {r['status']}  (updated {r['updated_at']})", expanded=False):
                st.write(r["content"])
                if r["symbols"]:
                    tags = [s.strip() for s in r["symbols"].split(",") if s.strip()]
                    if tags:
                        st.caption("Auto charts from symbols:")
                        cc = st.columns(min(3, len(tags)))
                        for i, s in enumerate(tags[:6]):
                            h = history(s, st.session_state.settings["default_duration"])
                            if not h.empty:
                                with cc[i%len(cc)]:
                                    st.write(f"**{s}**")
                                    st.plotly_chart(px.line(h, x=h.columns[0], y="Close"), use_container_width=True)
                if r["status"]=="draft":
                    if st.button(f"Publish #{r['id']}"):
                        update_post_status(int(r["id"]), user["id"], "published")
                        st.success("Published.")
                        st.rerun()

# 8) Assistant (rule-based bot for appointments + quick Q&A)
elif page == "🤖 Assistant":
    st.header("Assistant")
    mode = st.radio("How can I help?", ["Book an appointment","Ask a quick question"])
    if mode=="Ask a quick question":
        msgs = fetch_messages(user["id"], include_admin=True)
        if not msgs.empty:
            for _, m in msgs.sort_values("created_at").iterrows():
                speaker = "You" if m["role"]=="user" else "Support"
                st.markdown(f"**{speaker}** — _{m['created_at']}_")
                st.write(m["text"])
        with st.form("ask_quick"):
            q = st.text_area("Your question")
            ok = st.form_submit_button("Send")
        if ok and q.strip():
            add_message(user["id"], "user", q.strip())
            add_message(user["id"], "admin", "Thanks! We received your question and will get back soon.")
            st.success("Sent.")
            st.rerun()
    else:
        st.caption("I’ll collect the basics and file an appointment request for you.")
        step = st.session_state.bot["step"]
        data = st.session_state.bot["data"]
        if step==0:
            data["topic"] = st.text_input("What would you like to discuss? (e.g., Portfolio review, SIP setup)")
            if st.button("Next"): 
                if not data.get("topic"): st.warning("Please enter a topic.")
                else: st.session_state.bot["step"]=1; st.rerun()
        elif step==1:
            c1,c2 = st.columns(2)
            data["preferred_date"] = c1.date_input("Preferred date")
            data["preferred_time"] = c2.time_input("Preferred time")
            if st.button("Next"): st.session_state.bot["step"]=2; st.rerun()
        elif step==2:
            data["method"] = st.selectbox("Meeting method", ["Phone","WhatsApp","Google Meet","Zoom","In-person"])
            data["notes"]  = st.text_area("Anything else?")
            if st.button("Submit request"):
                # Store as an inquiry + notify via message
                msg = f"APPOINTMENT REQUEST\nTopic: {data['topic']}\nWhen: {data['preferred_date']} {data['preferred_time']}\nMethod: {data['method']}\nNotes: {data.get('notes','')}"
                add_inquiry(user["id"], user["name"], user.get("email",""), user.get("phone",""), msg)
                add_message(user["id"], "user", f"Appointment requested: {data['topic']}")
                add_message(user["id"], "admin", "Thanks! We have your request and will confirm a slot soon.")
                st.success("Request submitted. We’ll confirm availability.")
                st.session_state.bot = {"mode":"idle","step":0,"data":{}}
        st.info("You’ll also see updates in **Q&A** once we reply.")

# 9) Q&A (simple inbox thread)
elif page == "💬 Q&A":
    st.header("Questions & Answers")
    msgs = fetch_messages(user["id"], include_admin=True)
    if msgs.empty:
        st.info("No messages yet.")
    else:
        for _, m in msgs.sort_values("created_at").iterrows():
            speaker = "You" if m["role"]=="user" else "Support"
            st.markdown(f"**{speaker}** — _{m['created_at']}_")
            st.write(m["text"])
            st.divider()
    with st.form("ask"):
        txt = st.text_area("Your message")
        ok = st.form_submit_button("Send")
    if ok and txt.strip():
        add_message(user["id"], "user", txt.strip())
        add_message(user["id"], "admin", "Thanks! We received your message and will get back soon.")
        st.success("Sent.")
        st.rerun()

# 10) News+
elif page == "📰 News+":
    st.header("Latest News")
    # General feeds
    news = aggregate_news()
    if news.empty:
        st.info("No headlines from RSS right now.")
    else:
        st.subheader("Top stories")
        st.dataframe(news[["category","source","title","published","link"]], use_container_width=True)
    # Ticker-based news (from your watchlist)
    st.subheader("From your Watchlist")
    wl = get_watchlist(user["id"])
    if wl.empty:
        st.caption("Add tickers to your Watchlist to see symbol headlines here.")
    else:
        import yfinance as yf
        rows=[]
        for s in wl["symbol"].tolist()[:15]:
            try:
                items = yf.Ticker(s).news or []
                for it in items[:5]:
                    rows.append({"symbol":s,"source":it.get("publisher",""),"title":it.get("title",""),"link":it.get("link","")})
            except Exception:
                pass
        if rows:
            st.dataframe(pd.DataFrame(rows).drop_duplicates(subset=["title"]), use_container_width=True)
        else:
            st.caption("No symbol headlines available right now.")

# 11) Inquiry page
elif page == "📨 Inquiry":
    st.header("Contact / Inquiry")
    with st.form("inq", clear_on_submit=True):
        name  = st.text_input("Your name", value=user["name"])
        email = st.text_input("Email", value=user.get("email") or "")
        phone = st.text_input("Phone", value=user.get("phone") or "")
        msg   = st.text_area("Message")
        ok = st.form_submit_button("Send")
    if ok:
        if not name.strip():
            st.error("Name required.")
        else:
            add_inquiry(user["id"], name.strip(), email.strip(), phone.strip(), msg.strip())
            st.success("Thanks! We’ll reach out.")

# 12) Settings
elif page == "⚙️ Settings":
    st.header("Settings")
    st.write("Use the sidebar to toggle auto-refresh, select refresh interval, and default chart range.")
