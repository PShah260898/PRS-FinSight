import sqlite3
from pathlib import Path
from datetime import datetime, timezone
import pandas as pd

DB_PATH = Path("portfolio.db")

def get_conn():
    return sqlite3.connect(DB_PATH)

def now_iso():
    return datetime.now(timezone.utc).isoformat()

def init_db():
    with get_conn() as con:
        cur = con.cursor()
        # users
        cur.execute("""
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            full_name TEXT,
            username TEXT UNIQUE NOT NULL,
            email TEXT,
            phone TEXT,
            password_hash TEXT NOT NULL,
            salt TEXT NOT NULL,
            created_at TEXT NOT NULL
        );
        """)
        # transactions
        cur.execute("""
        CREATE TABLE IF NOT EXISTS transactions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            date TEXT NOT NULL,
            symbol TEXT NOT NULL,
            asset_type TEXT NOT NULL,
            txn_type TEXT NOT NULL,
            units REAL NOT NULL,
            price REAL NOT NULL,
            fees REAL NOT NULL DEFAULT 0,
            account TEXT NOT NULL DEFAULT 'Default',
            FOREIGN KEY(user_id) REFERENCES users(id)
        );
        """)
        # watchlist (tickers for yfinance)
        cur.execute("""
        CREATE TABLE IF NOT EXISTS watchlist (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            symbol TEXT NOT NULL,
            alias TEXT,
            created_at TEXT NOT NULL,
            UNIQUE(user_id, symbol),
            FOREIGN KEY(user_id) REFERENCES users(id)
        );
        """)
        # posts
        cur.execute("""
        CREATE TABLE IF NOT EXISTS posts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            title TEXT NOT NULL,
            content TEXT NOT NULL,
            symbols TEXT,
            status TEXT NOT NULL DEFAULT 'draft',
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            FOREIGN KEY(user_id) REFERENCES users(id)
        );
        """)
        # messages (Q&A)
        cur.execute("""
        CREATE TABLE IF NOT EXISTS messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            role TEXT NOT NULL, -- 'user' or 'admin'
            text TEXT NOT NULL,
            created_at TEXT NOT NULL,
            seen INTEGER NOT NULL DEFAULT 0,
            FOREIGN KEY(user_id) REFERENCES users(id)
        );
        """)
        # inquiries (assistant & contact page)
        cur.execute("""
        CREATE TABLE IF NOT EXISTS inquiries (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER,
            name TEXT,
            email TEXT,
            phone TEXT,
            message TEXT,
            created_at TEXT NOT NULL,
            FOREIGN KEY(user_id) REFERENCES users(id)
        );
        """)
        # AMFI MF watchlist (India)
        cur.execute("""
        CREATE TABLE IF NOT EXISTS amfi_watchlist (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            scheme_code INTEGER NOT NULL,
            scheme_name TEXT NOT NULL,
            created_at TEXT NOT NULL,
            UNIQUE(user_id, scheme_code),
            FOREIGN KEY(user_id) REFERENCES users(id)
        );
        """)
        con.commit()

# ---------- users ----------
def create_user(full_name, username, email, phone, password_hash, salt):
    with get_conn() as con:
        con.execute("""
        INSERT INTO users (full_name, username, email, phone, password_hash, salt, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (full_name, username, email, phone, password_hash, salt, now_iso()))
        con.commit()

def get_user_by_username(username):
    with get_conn() as con:
        cur = con.execute("SELECT id, full_name, username, email, phone, password_hash, salt, created_at FROM users WHERE username=?",(username,))
        return cur.fetchone()

# ---------- transactions ----------
def insert_tx(user_id, date, symbol, asset_type, txn_type, units, price, fees, account):
    with get_conn() as con:
        con.execute("""
        INSERT INTO transactions (user_id, date, symbol, asset_type, txn_type, units, price, fees, account)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (user_id, str(date), symbol.upper(), asset_type, txn_type, float(units), float(price), float(fees), account))
        con.commit()

def fetch_tx(user_id):
    with get_conn() as con:
        return pd.read_sql_query("SELECT * FROM transactions WHERE user_id=? ORDER BY date, id", con, params=(user_id,))

# ---------- watchlist ----------
def add_watch(user_id, symbol, alias=None):
    with get_conn() as con:
        con.execute("""
        INSERT OR IGNORE INTO watchlist (user_id, symbol, alias, created_at)
        VALUES (?, ?, ?, ?)
        """, (user_id, symbol.upper(), alias, now_iso()))
        con.commit()

def del_watch(user_id, symbol):
    with get_conn() as con:
        con.execute("DELETE FROM watchlist WHERE user_id=? AND symbol=?", (user_id, symbol.upper()))
        con.commit()

def get_watchlist(user_id):
    with get_conn() as con:
        return pd.read_sql_query("SELECT symbol, alias, created_at FROM watchlist WHERE user_id=? ORDER BY symbol", con, params=(user_id,))

# ---------- posts ----------
def add_post(user_id, title, content, symbols, status="draft"):
    with get_conn() as con:
        ts = now_iso()
        con.execute("""
        INSERT INTO posts (user_id, title, content, symbols, status, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (user_id, title, content, symbols, status, ts, ts))
        con.commit()

def update_post_status(post_id, user_id, status):
    with get_conn() as con:
        con.execute("UPDATE posts SET status=?, updated_at=? WHERE id=? AND user_id=?", (status, now_iso(), post_id, user_id))
        con.commit()

def fetch_posts(user_id=None, only_published=False):
    q = "SELECT id, user_id, title, content, symbols, status, created_at, updated_at FROM posts"
    params = []
    where=[]
    if user_id: where.append("user_id=?"); params.append(user_id)
    if only_published: where.append("status='published'")
    if where: q += " WHERE " + " AND ".join(where)
    q += " ORDER BY updated_at DESC"
    with get_conn() as con:
        return pd.read_sql_query(q, con, params=params)

# ---------- messages ----------
def add_message(user_id, role, text):
    with get_conn() as con:
        con.execute("INSERT INTO messages (user_id, role, text, created_at) VALUES (?, ?, ?, ?)", (user_id, role, text, now_iso()))
        con.commit()

def fetch_messages(user_id, include_admin=False):
    q = "SELECT id, user_id, role, text, created_at, seen FROM messages WHERE user_id=?"
    params = (user_id,)
    with get_conn() as con:
        return pd.read_sql_query(q + " ORDER BY id ASC", con, params=params)

def unread_count():
    with get_conn() as con:
        cur = con.execute("SELECT COUNT(*) FROM messages WHERE role='user' AND seen=0")
        n = cur.fetchone()[0]
        return int(n or 0)

def mark_all_seen():
    with get_conn() as con:
        con.execute("UPDATE messages SET seen=1 WHERE role='user' AND seen=0")
        con.commit()

# ---------- inquiries ----------
def add_inquiry(user_id, name, email, phone, message):
    with get_conn() as con:
        con.execute("""
        INSERT INTO inquiries (user_id, name, email, phone, message, created_at)
        VALUES (?, ?, ?, ?, ?, ?)
        """, (user_id, name, email, phone, message, now_iso()))
        con.commit()

def fetch_inquiries():
    with get_conn() as con:
        return pd.read_sql_query("SELECT * FROM inquiries ORDER BY id DESC", con)

# ---------- AMFI MF watchlist ----------
def add_amfi_watch(user_id: int, scheme_code: int, scheme_name: str):
    with get_conn() as con:
        con.execute("""
        INSERT OR IGNORE INTO amfi_watchlist (user_id, scheme_code, scheme_name, created_at)
        VALUES (?, ?, ?, ?)
        """, (user_id, int(scheme_code), scheme_name, now_iso()))
        con.commit()

def del_amfi_watch(user_id: int, scheme_code: int):
    with get_conn() as con:
        con.execute("DELETE FROM amfi_watchlist WHERE user_id=? AND scheme_code=?", (user_id, int(scheme_code)))
        con.commit()

def get_amfi_watchlist(user_id: int):
    with get_conn() as con:
        return pd.read_sql_query("SELECT scheme_code, scheme_name, created_at FROM amfi_watchlist WHERE user_id=? ORDER BY scheme_name", con, params=(user_id,))
