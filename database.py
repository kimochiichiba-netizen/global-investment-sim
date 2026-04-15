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


def _add_column_if_not_exists(cur, table: str, column: str, definition: str):
    """既存テーブルに列が存在しなければ追加する"""
    cur.execute(f"PRAGMA table_info({table})")
    cols = [row[1] for row in cur.fetchall()]
    if column not in cols:
        cur.execute(f"ALTER TABLE {table} ADD COLUMN {column} {definition}")


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

    # グローバル戦略専用の口座テーブル（清原式と完全分離）
    cur.execute("""
        CREATE TABLE IF NOT EXISTS global_account (
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
            avg_cost_local REAL NOT NULL,
            avg_cost_jpy REAL NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # トレーリングストップ・部分利確・購入時指標の列を追加
    _add_column_if_not_exists(cur, "portfolio", "peak_price",    "REAL")
    _add_column_if_not_exists(cur, "portfolio", "trailing_stop", "REAL")
    _add_column_if_not_exists(cur, "portfolio", "partial_taken", "INTEGER DEFAULT 0")
    _add_column_if_not_exists(cur, "portfolio", "buy_per",       "REAL")
    _add_column_if_not_exists(cur, "portfolio", "buy_pbr",       "REAL")
    _add_column_if_not_exists(cur, "portfolio", "buy_nc_ratio",  "REAL")
    # 2ページ化: 戦略種別（kiyohara / global）
    _add_column_if_not_exists(cur, "portfolio", "strategy",      "TEXT DEFAULT 'kiyohara'")

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
            price_local REAL NOT NULL,
            price_jpy REAL NOT NULL,
            total_jpy REAL NOT NULL,
            commission_jpy REAL NOT NULL,
            fx_rate REAL NOT NULL,
            reason TEXT,
            strategy TEXT DEFAULT 'kiyohara',
            executed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    _add_column_if_not_exists(cur, "trades", "strategy", "TEXT DEFAULT 'kiyohara'")

    cur.execute("""
        CREATE TABLE IF NOT EXISTS asset_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            total_assets REAL NOT NULL,
            cash REAL NOT NULL,
            stock_value REAL NOT NULL,
            recorded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # ファンダメンタル指標キャッシュ（1日1回取得）
    cur.execute("""
        CREATE TABLE IF NOT EXISTS fundamental_cache (
            ticker TEXT PRIMARY KEY,
            name TEXT,
            market_cap REAL,
            market_cap_usd REAL,
            per REAL,
            pbr REAL,
            current_assets REAL,
            total_debt REAL,
            cash_and_equiv REAL,
            net_cash REAL,
            net_cash_ratio REAL,
            dividend_yield REAL,
            sector TEXT,
            currency TEXT,
            last_updated DATE
        )
    """)
    # バフェット・リンチ用ファンダメンタル指標列を追加
    _add_column_if_not_exists(cur, "fundamental_cache", "roe",             "REAL")
    _add_column_if_not_exists(cur, "fundamental_cache", "debt_to_equity",  "REAL")
    _add_column_if_not_exists(cur, "fundamental_cache", "peg_ratio",       "REAL")
    _add_column_if_not_exists(cur, "fundamental_cache", "earnings_growth",  "REAL")
    _add_column_if_not_exists(cur, "fundamental_cache", "operating_margin", "REAL")

    # スクリーニング通過銘柄（スコア降順で保存）
    cur.execute("""
        CREATE TABLE IF NOT EXISTS screened_stocks (
            ticker TEXT PRIMARY KEY,
            name TEXT,
            market TEXT,
            currency TEXT,
            flag TEXT,
            market_cap REAL,
            per REAL,
            pbr REAL,
            net_cash_ratio REAL,
            composite_score REAL,
            minervini_pass INTEGER DEFAULT 0,
            canslim_pass INTEGER DEFAULT 0,
            current_price REAL,
            rsi14 REAL,
            ma50 REAL,
            ma150 REAL,
            ma200 REAL,
            screened_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    # Lynch / Buffett / MACD 戦略フラグを追加（旧互換）
    _add_column_if_not_exists(cur, "screened_stocks", "lynch_pass",   "INTEGER DEFAULT 0")
    _add_column_if_not_exists(cur, "screened_stocks", "buffett_pass", "INTEGER DEFAULT 0")
    _add_column_if_not_exists(cur, "screened_stocks", "macd_bullish", "INTEGER DEFAULT 0")
    _add_column_if_not_exists(cur, "screened_stocks", "roc20",        "REAL")
    _add_column_if_not_exists(cur, "screened_stocks", "bb_width",     "REAL")
    _add_column_if_not_exists(cur, "screened_stocks", "roe",          "REAL")
    _add_column_if_not_exists(cur, "screened_stocks", "peg_ratio",    "REAL")
    # 清原式追加フィールド
    _add_column_if_not_exists(cur, "screened_stocks", "catalyst_flag",          "INTEGER DEFAULT 0")
    _add_column_if_not_exists(cur, "screened_stocks", "dividend_payout_ratio",  "REAL")
    # 2ページ化: 戦略種別
    _add_column_if_not_exists(cur, "screened_stocks", "strategy",               "TEXT DEFAULT 'kiyohara'")

    cur.execute("SELECT id FROM account WHERE id = 1")
    if not cur.fetchone():
        cur.execute(
            "INSERT INTO account (id, cash, initial_capital) VALUES (1, ?, ?)",
            (INITIAL_CAPITAL, INITIAL_CAPITAL)
        )
        print(f"✅ 初期資金 {INITIAL_CAPITAL:,}円 でアカウントを作成しました（清原式）")

    cur.execute("SELECT id FROM global_account WHERE id = 1")
    if not cur.fetchone():
        cur.execute(
            "INSERT INTO global_account (id, cash, initial_capital) VALUES (1, ?, ?)",
            (INITIAL_CAPITAL, INITIAL_CAPITAL)
        )
        print(f"✅ 初期資金 {INITIAL_CAPITAL:,}円 でアカウントを作成しました（グローバル）")

    conn.commit()
    conn.close()
    print("✅ データベースの初期化が完了しました")


# ==================== account ====================

def get_account(strategy: str = 'kiyohara') -> Dict:
    conn = get_conn()
    table = 'global_account' if strategy == 'global' else 'account'
    row  = conn.execute(f"SELECT * FROM {table} WHERE id = 1").fetchone()
    conn.close()
    return dict(row)


def update_cash(new_cash: float, strategy: str = 'kiyohara'):
    conn = get_conn()
    table = 'global_account' if strategy == 'global' else 'account'
    conn.execute(
        f"UPDATE {table} SET cash = ?, updated_at = ? WHERE id = 1",
        (new_cash, datetime.now().isoformat())
    )
    conn.commit()
    conn.close()


# ==================== portfolio ====================

def get_portfolio(strategy: str = 'kiyohara') -> List[Dict]:
    conn = get_conn()
    rows = conn.execute(
        "SELECT * FROM portfolio WHERE strategy = ? ORDER BY market, ticker",
        (strategy,)
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_holding(ticker: str, strategy: str = 'kiyohara') -> Optional[Dict]:
    conn = get_conn()
    row = conn.execute(
        "SELECT * FROM portfolio WHERE ticker = ? AND strategy = ?",
        (ticker, strategy)
    ).fetchone()
    conn.close()
    return dict(row) if row else None


def upsert_holding(ticker: str, name: str, market: str, currency: str, flag: str,
                   shares: float, avg_cost_local: float, avg_cost_jpy: float,
                   peak_price: Optional[float] = None,
                   trailing_stop: Optional[float] = None,
                   partial_taken: int = 0,
                   buy_per: Optional[float] = None,
                   buy_pbr: Optional[float] = None,
                   buy_nc_ratio: Optional[float] = None,
                   strategy: str = 'kiyohara'):
    conn = get_conn()
    conn.execute("""
        INSERT INTO portfolio
            (ticker, name, market, currency, flag, shares, avg_cost_local, avg_cost_jpy,
             peak_price, trailing_stop, partial_taken, buy_per, buy_pbr, buy_nc_ratio, strategy)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(ticker) DO UPDATE SET
            shares = ?,
            avg_cost_local = ?,
            avg_cost_jpy = ?,
            name = ?,
            flag = ?,
            peak_price    = COALESCE(?, peak_price),
            trailing_stop = COALESCE(?, trailing_stop),
            partial_taken = ?,
            buy_per       = COALESCE(?, buy_per),
            buy_pbr       = COALESCE(?, buy_pbr),
            buy_nc_ratio  = COALESCE(?, buy_nc_ratio),
            strategy      = ?
    """, (
        ticker, name, market, currency, flag, shares, avg_cost_local, avg_cost_jpy,
        peak_price, trailing_stop, partial_taken, buy_per, buy_pbr, buy_nc_ratio, strategy,
        # UPDATE部分
        shares, avg_cost_local, avg_cost_jpy, name, flag,
        peak_price, trailing_stop, partial_taken,
        buy_per, buy_pbr, buy_nc_ratio, strategy,
    ))
    conn.commit()
    conn.close()


def update_trailing_stop(ticker: str, peak_price: float, trailing_stop: float,
                          strategy: str = 'kiyohara'):
    conn = get_conn()
    conn.execute(
        "UPDATE portfolio SET peak_price = ?, trailing_stop = ? WHERE ticker = ? AND strategy = ?",
        (peak_price, trailing_stop, ticker, strategy)
    )
    conn.commit()
    conn.close()


def mark_partial_taken(ticker: str, strategy: str = 'kiyohara'):
    conn = get_conn()
    conn.execute(
        "UPDATE portfolio SET partial_taken = 1 WHERE ticker = ? AND strategy = ?",
        (ticker, strategy)
    )
    conn.commit()
    conn.close()


def delete_holding(ticker: str, strategy: str = 'kiyohara'):
    conn = get_conn()
    conn.execute("DELETE FROM portfolio WHERE ticker = ? AND strategy = ?", (ticker, strategy))
    conn.commit()
    conn.close()


# ==================== trades ====================

def save_trade(ticker: str, name: str, market: str, currency: str, flag: str,
               action: str, shares: float, price_local: float, price_jpy: float,
               total_jpy: float, commission_jpy: float, fx_rate: float, reason: str,
               strategy: str = 'kiyohara'):
    conn = get_conn()
    conn.execute("""
        INSERT INTO trades
            (ticker, name, market, currency, flag, action, shares,
             price_local, price_jpy, total_jpy, commission_jpy, fx_rate, reason, strategy)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (ticker, name, market, currency, flag, action, shares,
          price_local, price_jpy, total_jpy, commission_jpy, fx_rate, reason, strategy))
    conn.commit()
    conn.close()


def get_trades(limit: int = 50, strategy: str = 'kiyohara') -> List[Dict]:
    conn = get_conn()
    rows = conn.execute(
        "SELECT * FROM trades WHERE strategy = ? ORDER BY executed_at DESC LIMIT ?",
        (strategy, limit)
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def recently_sold(ticker: str, days: int = 3, strategy: str = 'kiyohara') -> bool:
    """直近N日以内に売却したか確認（売買コスト節約）"""
    conn = get_conn()
    row = conn.execute("""
        SELECT id FROM trades
        WHERE ticker = ? AND strategy = ?
        AND action IN ('sell', '損切り', '利確', '部分利確')
        AND executed_at >= datetime('now', ?)
        ORDER BY executed_at DESC LIMIT 1
    """, (ticker, strategy, f"-{days} days")).fetchone()
    conn.close()
    return row is not None


def get_last_sell_action(ticker: str, strategy: str = 'kiyohara') -> Optional[Dict]:
    """直近の売却情報（action と executed_at）を返す。なければ None"""
    conn = get_conn()
    row = conn.execute("""
        SELECT action, executed_at FROM trades
        WHERE ticker = ? AND strategy = ?
        AND action IN ('sell', '損切り', '利確', '部分利確')
        ORDER BY executed_at DESC LIMIT 1
    """, (ticker, strategy)).fetchone()
    conn.close()
    return dict(row) if row else None


# ==================== asset_history ====================

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


# ==================== fundamental_cache ====================

def get_fundamental_cache(ticker: str) -> Optional[Dict]:
    """7日以内のキャッシュがあれば返す（毎日取得せず負荷を下げる）"""
    from datetime import timedelta
    cutoff = (date.today() - timedelta(days=7)).isoformat()
    conn = get_conn()
    row = conn.execute(
        "SELECT * FROM fundamental_cache WHERE ticker = ? AND last_updated >= ?",
        (ticker, cutoff)
    ).fetchone()
    conn.close()
    return dict(row) if row else None


def save_fundamental_cache(data: Dict):
    conn = get_conn()
    conn.execute("""
        INSERT INTO fundamental_cache
            (ticker, name, market_cap, market_cap_usd, per, pbr,
             current_assets, total_debt, cash_and_equiv, net_cash,
             net_cash_ratio, dividend_yield, sector, currency, last_updated,
             roe, debt_to_equity, peg_ratio, earnings_growth, operating_margin)
        VALUES (:ticker, :name, :market_cap, :market_cap_usd, :per, :pbr,
                :current_assets, :total_debt, :cash_and_equiv, :net_cash,
                :net_cash_ratio, :dividend_yield, :sector, :currency, :last_updated,
                :roe, :debt_to_equity, :peg_ratio, :earnings_growth, :operating_margin)
        ON CONFLICT(ticker) DO UPDATE SET
            name=excluded.name, market_cap=excluded.market_cap,
            market_cap_usd=excluded.market_cap_usd, per=excluded.per,
            pbr=excluded.pbr, current_assets=excluded.current_assets,
            total_debt=excluded.total_debt, cash_and_equiv=excluded.cash_and_equiv,
            net_cash=excluded.net_cash, net_cash_ratio=excluded.net_cash_ratio,
            dividend_yield=excluded.dividend_yield, sector=excluded.sector,
            currency=excluded.currency, last_updated=excluded.last_updated,
            roe=excluded.roe, debt_to_equity=excluded.debt_to_equity,
            peg_ratio=excluded.peg_ratio, earnings_growth=excluded.earnings_growth,
            operating_margin=excluded.operating_margin
    """, data)
    conn.commit()
    conn.close()


# ==================== screened_stocks ====================

def clear_screened_stocks():
    conn = get_conn()
    conn.execute("DELETE FROM screened_stocks")
    conn.commit()
    conn.close()


def save_screened_stock(data: Dict):
    conn = get_conn()
    conn.execute("""
        INSERT INTO screened_stocks
            (ticker, name, market, currency, flag, market_cap, per, pbr,
             net_cash_ratio, composite_score, minervini_pass, canslim_pass,
             current_price, rsi14, ma50, ma150, ma200,
             lynch_pass, buffett_pass, macd_bullish, roc20, bb_width, roe, peg_ratio,
             catalyst_flag, dividend_payout_ratio)
        VALUES (:ticker, :name, :market, :currency, :flag, :market_cap, :per, :pbr,
                :net_cash_ratio, :composite_score, :minervini_pass, :canslim_pass,
                :current_price, :rsi14, :ma50, :ma150, :ma200,
                :lynch_pass, :buffett_pass, :macd_bullish, :roc20, :bb_width, :roe, :peg_ratio,
                :catalyst_flag, :dividend_payout_ratio)
        ON CONFLICT(ticker) DO UPDATE SET
            composite_score=excluded.composite_score,
            minervini_pass=excluded.minervini_pass,
            canslim_pass=excluded.canslim_pass,
            current_price=excluded.current_price,
            rsi14=excluded.rsi14, ma50=excluded.ma50,
            ma150=excluded.ma150, ma200=excluded.ma200,
            per=excluded.per, pbr=excluded.pbr,
            net_cash_ratio=excluded.net_cash_ratio,
            lynch_pass=excluded.lynch_pass,
            buffett_pass=excluded.buffett_pass,
            macd_bullish=excluded.macd_bullish,
            roc20=excluded.roc20,
            bb_width=excluded.bb_width,
            roe=excluded.roe,
            peg_ratio=excluded.peg_ratio,
            catalyst_flag=excluded.catalyst_flag,
            dividend_payout_ratio=excluded.dividend_payout_ratio,
            screened_at=CURRENT_TIMESTAMP
    """, data)
    conn.commit()
    conn.close()


def get_screened_stocks(strategy: str = 'kiyohara') -> List[Dict]:
    conn = get_conn()
    rows = conn.execute(
        "SELECT * FROM screened_stocks WHERE strategy = ? ORDER BY composite_score DESC",
        (strategy,)
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def is_screened(ticker: str, strategy: str = 'kiyohara') -> bool:
    conn = get_conn()
    row = conn.execute(
        "SELECT ticker FROM screened_stocks WHERE ticker = ? AND strategy = ?",
        (ticker, strategy)
    ).fetchone()
    conn.close()
    return row is not None


def clear_screened_stocks_by_strategy(strategy: str):
    """指定戦略のスクリーニング結果のみ削除"""
    conn = get_conn()
    conn.execute("DELETE FROM screened_stocks WHERE strategy = ?", (strategy,))
    conn.commit()
    conn.close()


# ==================== reset ====================

def reset_all(strategy: Optional[str] = None):
    """strategy 指定なし → 全データリセット。指定あり → その戦略のみリセット"""
    conn = get_conn()
    now = datetime.now().isoformat()
    if strategy:
        conn.execute("DELETE FROM portfolio WHERE strategy = ?", (strategy,))
        conn.execute("DELETE FROM trades WHERE strategy = ?", (strategy,))
        conn.execute("DELETE FROM screened_stocks WHERE strategy = ?", (strategy,))
        # 戦略専用の口座残高もリセット
        table = 'global_account' if strategy == 'global' else 'account'
        conn.execute(
            f"UPDATE {table} SET cash = ?, updated_at = ? WHERE id = 1",
            (INITIAL_CAPITAL, now)
        )
    else:
        conn.execute("DELETE FROM portfolio")
        conn.execute("DELETE FROM trades")
        conn.execute("DELETE FROM asset_history")
        conn.execute("DELETE FROM screened_stocks")
        conn.execute(
            "UPDATE account SET cash = ?, updated_at = ? WHERE id = 1",
            (INITIAL_CAPITAL, now)
        )
        conn.execute(
            "UPDATE global_account SET cash = ?, updated_at = ? WHERE id = 1",
            (INITIAL_CAPITAL, now)
        )
    conn.commit()
    conn.close()
    print(f"🔄 データをリセットしました（strategy={strategy or 'all'}）")
