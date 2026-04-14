"""
データベース処理（グローバル版）
通貨・市場情報も保存できるようにしています
"""
import sqlite3
from datetime import datetime, date
from typing import List, Dict, Optional

DB_PATH = "global_investment.db"
INITIAL_CAPITAL = 2_000_000   # 仮想資金 200万円


def get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    conn = get_conn()
    cur  = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS account (
            id INTEGER PRIMARY KEY CHECK (id = 1),
            cash REAL NOT NULL,
            initial_capital REAL NOT NULL,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS portfolio (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ticker TEXT NOT NULL UNIQUE,
            name TEXT NOT NULL,
            market TEXT NOT NULL,
            currency TEXT NOT NULL,
            flag TEXT NOT NULL,
            shares REAL NOT NULL,
            avg_cost_local REAL NOT NULL,   -- 現地通貨建ての取得単価
            avg_cost_jpy REAL NOT NULL,     -- 円建ての取得単価（為替レート込み）
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS trades (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ticker TEXT NOT NULL,
            name TEXT NOT NULL,
            market TEXT NOT NULL,
            currency TEXT NOT NULL,
            flag TEXT NOT NULL,
            action TEXT NOT NULL,
            shares REAL NOT NULL,
            price_local REAL NOT NULL,      -- 現地通貨の取引単価
            price_jpy REAL NOT NULL,        -- 円換算の取引単価
            total_jpy REAL NOT NULL,        -- 円換算の取引総額
            commission_jpy REAL NOT NULL,   -- 円換算の手数料
            fx_rate REAL NOT NULL,          -- 使用した為替レート
            reason TEXT,
            executed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS asset_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            total_assets REAL NOT NULL,
            cash REAL NOT NULL,
            stock_value REAL NOT NULL,
            recorded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    cur.execute("SELECT id FROM account WHERE id = 1")
    if not cur.fetchone():
        cur.execute(
            "INSERT INTO account (id, cash, initial_capital) VALUES (1, ?, ?)",
            (INITIAL_CAPITAL, INITIAL_CAPITAL)
        )
        print(f"✅ 初期資金 {INITIAL_CAPITAL:,}円 でアカウントを作成しました")

    conn.commit()
    conn.close()
    print("✅ データベースの初期化が完了しました")


def get_account() -> Dict:
    conn = get_conn()
    row  = conn.execute("SELECT * FROM account WHERE id = 1").fetchone()
    conn.close()
    return dict(row)


def update_cash(new_cash: float):
    conn = get_conn()
    conn.execute(
        "UPDATE account SET cash = ?, updated_at = ? WHERE id = 1",
        (new_cash, datetime.now().isoformat())
    )
    conn.commit()
    conn.close()


def get_portfolio() -> List[Dict]:
    conn = get_conn()
    rows = conn.execute("SELECT * FROM portfolio ORDER BY market, ticker").fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_holding(ticker: str) -> Optional[Dict]:
    conn = get_conn()
    row  = conn.execute("SELECT * FROM portfolio WHERE ticker = ?", (ticker,)).fetchone()
    conn.close()
    return dict(row) if row else None


def upsert_holding(ticker: str, name: str, market: str, currency: str, flag: str,
                   shares: float, avg_cost_local: float, avg_cost_jpy: float):
    conn = get_conn()
    conn.execute("""
        INSERT INTO portfolio
            (ticker, name, market, currency, flag, shares, avg_cost_local, avg_cost_jpy)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(ticker) DO UPDATE SET
            shares = ?,
            avg_cost_local = ?,
            avg_cost_jpy = ?,
            name = ?,
            flag = ?
    """, (ticker, name, market, currency, flag, shares, avg_cost_local, avg_cost_jpy,
          shares, avg_cost_local, avg_cost_jpy, name, flag))
    conn.commit()
    conn.close()


def delete_holding(ticker: str):
    conn = get_conn()
    conn.execute("DELETE FROM portfolio WHERE ticker = ?", (ticker,))
    conn.commit()
    conn.close()


def save_trade(ticker: str, name: str, market: str, currency: str, flag: str,
               action: str, shares: float, price_local: float, price_jpy: float,
               total_jpy: float, commission_jpy: float, fx_rate: float, reason: str):
    conn = get_conn()
    conn.execute("""
        INSERT INTO trades
            (ticker, name, market, currency, flag, action, shares,
             price_local, price_jpy, total_jpy, commission_jpy, fx_rate, reason)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (ticker, name, market, currency, flag, action, shares,
          price_local, price_jpy, total_jpy, commission_jpy, fx_rate, reason))
    conn.commit()
    conn.close()


def get_trades(limit: int = 50) -> List[Dict]:
    conn = get_conn()
    rows = conn.execute(
        "SELECT * FROM trades ORDER BY executed_at DESC LIMIT ?", (limit,)
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def save_asset_snapshot(total: float, cash: float, stock_value: float):
    conn = get_conn()
    conn.execute(
        "INSERT INTO asset_history (total_assets, cash, stock_value) VALUES (?, ?, ?)",
        (total, cash, stock_value)
    )
    conn.commit()
    conn.close()


def get_asset_history(days: int = 30) -> List[Dict]:
    conn = get_conn()
    rows = conn.execute("""
        SELECT DATE(recorded_at) as date,
               AVG(total_assets) as total_assets,
               AVG(cash) as cash,
               AVG(stock_value) as stock_value
        FROM asset_history
        WHERE recorded_at >= datetime('now', ?)
        GROUP BY DATE(recorded_at)
        ORDER BY date
    """, (f"-{days} days",)).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def reset_all():
    conn = get_conn()
    conn.execute("DELETE FROM portfolio")
    conn.execute("DELETE FROM trades")
    conn.execute("DELETE FROM asset_history")
    conn.execute(
        "UPDATE account SET cash = ?, updated_at = ? WHERE id = 1",
        (INITIAL_CAPITAL, datetime.now().isoformat())
    )
    conn.commit()
    conn.close()
    print("🔄 データをリセットしました")
