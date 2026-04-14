"""
グローバル監視銘柄リストと株価データ取得
5市場・15銘柄をカバーします
ファンダメンタル指標（PER/PBR/NC比率）も取得します
"""
import yfinance as yf
import pandas as pd
from datetime import date
from typing import Dict, List, Optional

from database import get_fundamental_cache, save_fundamental_cache

# 通貨→円換算ティッカー（yfinance）
FX_TICKERS = {
    "USD": "USDJPY=X",
    "GBP": "GBPJPY=X",
    "EUR": "EURJPY=X",
    "HKD": "HKDJPY=X",
    "JPY": None,
}

# 監視銘柄リスト
GLOBAL_WATCHLIST = {
    "7203.T": {"name": "トヨタ自動車",   "market": "TSE",  "currency": "JPY", "flag": "🇯🇵"},
    "6758.T": {"name": "ソニーグループ", "market": "TSE",  "currency": "JPY", "flag": "🇯🇵"},
    "9984.T": {"name": "ソフトバンクG",  "market": "TSE",  "currency": "JPY", "flag": "🇯🇵"},
    "AAPL":   {"name": "Apple",          "market": "NYSE", "currency": "USD", "flag": "🇺🇸"},
    "MSFT":   {"name": "Microsoft",      "market": "NYSE", "currency": "USD", "flag": "🇺🇸"},
    "NVDA":   {"name": "NVIDIA",         "market": "NYSE", "currency": "USD", "flag": "🇺🇸"},
    "TSLA":   {"name": "Tesla",          "market": "NYSE", "currency": "USD", "flag": "🇺🇸"},
    "AMZN":   {"name": "Amazon",         "market": "NYSE", "currency": "USD", "flag": "🇺🇸"},
    "JPM":    {"name": "JPモルガン",     "market": "NYSE", "currency": "USD", "flag": "🇺🇸"},
    "SHEL.L": {"name": "Shell",          "market": "LSE",  "currency": "GBP", "flag": "🇬🇧"},
    "AZN.L":  {"name": "AstraZeneca",    "market": "LSE",  "currency": "GBP", "flag": "🇬🇧"},
    "SAP.DE": {"name": "SAP",            "market": "DAX",  "currency": "EUR", "flag": "🇩🇪"},
    "SIE.DE": {"name": "Siemens",        "market": "DAX",  "currency": "EUR", "flag": "🇩🇪"},
    "0700.HK":{"name": "テンセント",     "market": "HKEX", "currency": "HKD", "flag": "🇭🇰"},
    "9988.HK":{"name": "アリババ",       "market": "HKEX", "currency": "HKD", "flag": "🇭🇰"},
}


def get_fx_rates() -> Dict[str, float]:
    """主要通貨→円の為替レートを取得"""
    rates = {"JPY": 1.0}
    tickers = [v for v in FX_TICKERS.values() if v]
    try:
        data = yf.download(tickers, period="2d", progress=False, auto_adjust=True)
        close = data["Close"] if not data.empty else pd.DataFrame()
        for currency, fx_ticker in FX_TICKERS.items():
            if not fx_ticker:
                continue
            try:
                col = fx_ticker if len(tickers) > 1 else "Close"
                series = (close[col] if len(tickers) > 1 else close).dropna()
                if not series.empty:
                    rates[currency] = float(series.iloc[-1])
            except Exception:
                pass
    except Exception as e:
        print(f"⚠️  為替レート取得エラー: {e}")
    defaults = {"USD": 150.0, "GBP": 190.0, "EUR": 162.0, "HKD": 19.0}
    for c, v in defaults.items():
        rates.setdefault(c, v)
    return rates


def get_stock_history(ticker: str, days: int = 250) -> Optional[pd.DataFrame]:
    """
    株価履歴を取得してテクニカル指標を計算する
    MA5/25（短期）+ MA50/150/200（Minervini用）+ RSI14 + 出来高MA + 52週高低
    """
    try:
        hist = yf.Ticker(ticker).history(period=f"{days}d", auto_adjust=True)
        if hist.empty or len(hist) < 10:
            return None
        hist.index = hist.index.tz_localize(None)

        close = hist["Close"]

        # 短期指標（既存）
        hist["MA5"]   = close.rolling(5).mean()
        hist["MA25"]  = close.rolling(25).mean()

        # Minervini SEPA 用中長期移動平均
        hist["MA50"]  = close.rolling(50).mean()
        hist["MA150"] = close.rolling(150).mean()
        hist["MA200"] = close.rolling(200).mean()

        # CAN-SLIM 出来高急増判定用
        hist["VolAvg20"] = hist["Volume"].rolling(20).mean()

        # リバモア ブレイクアウト用 52週高値・安値
        hist["52wHigh"] = close.rolling(min(252, len(close))).max()
        hist["52wLow"]  = close.rolling(min(252, len(close))).min()

        # RSI14
        delta = close.diff()
        gain  = delta.clip(lower=0).rolling(14).mean()
        loss  = (-delta.clip(upper=0)).rolling(14).mean()
        rs    = gain / loss.replace(0, float("nan"))
        hist["RSI14"] = (100 - 100 / (1 + rs)).round(2)

        return hist.round(4)
    except Exception as e:
        print(f"⚠️  {ticker} 履歴取得エラー: {e}")
        return None


def get_fundamental_info(ticker: str) -> Optional[Dict]:
    """
    yfinance .info + balance_sheet からファンダメンタル指標を取得。
    1日1回だけAPIを叩き、あとはDBキャッシュを使う。
    """
    # キャッシュ確認
    cached = get_fundamental_cache(ticker)
    if cached:
        return cached

    info_meta = GLOBAL_WATCHLIST.get(ticker)
    if not info_meta:
        return None

    try:
        tk = yf.Ticker(ticker)
        info = tk.info or {}

        # 時価総額（現地通貨）
        market_cap = info.get("marketCap")
        market_cap_usd = info.get("marketCap")  # yfinanceはUSD建て

        # バリュエーション
        per = info.get("forwardPE") or info.get("trailingPE")
        pbr = info.get("priceToBook")
        dividend_yield = info.get("dividendYield")
        sector = info.get("sector", "")

        # 貸借対照表からNC比率を計算
        cash_and_equiv = None
        total_debt = None
        current_assets = None
        try:
            bs = tk.balance_sheet
            if bs is not None and not bs.empty:
                def _get_bs(keys):
                    for k in keys:
                        if k in bs.index:
                            val = bs.loc[k].iloc[0]
                            if pd.notna(val):
                                return float(val)
                    return None

                cash_and_equiv = _get_bs([
                    "Cash And Cash Equivalents",
                    "CashAndCashEquivalents",
                    "Cash Cash Equivalents And Short Term Investments",
                ])
                total_debt = _get_bs([
                    "Total Debt",
                    "TotalDebt",
                    "Long Term Debt",
                ])
                current_assets = _get_bs([
                    "Current Assets",
                    "Total Current Assets",
                ])
        except Exception:
            pass

        net_cash = None
        net_cash_ratio = None
        if cash_and_equiv is not None and market_cap and market_cap > 0:
            debt = total_debt or 0
            net_cash = cash_and_equiv - debt
            net_cash_ratio = round(net_cash / market_cap, 4)

        data = {
            "ticker":         ticker,
            "name":           info_meta["name"],
            "market_cap":     market_cap,
            "market_cap_usd": market_cap_usd,
            "per":            round(float(per), 2) if per and per > 0 else None,
            "pbr":            round(float(pbr), 2) if pbr and pbr > 0 else None,
            "current_assets": current_assets,
            "total_debt":     total_debt,
            "cash_and_equiv": cash_and_equiv,
            "net_cash":       net_cash,
            "net_cash_ratio": net_cash_ratio,
            "dividend_yield": round(float(dividend_yield) * 100, 2) if dividend_yield else None,
            "sector":         sector,
            "currency":       info_meta["currency"],
            "last_updated":   date.today().isoformat(),
        }

        save_fundamental_cache(data)
        return data

    except Exception as e:
        print(f"⚠️  {ticker} ファンダメンタル取得エラー: {e}")
        return None


def get_stock_summary(ticker: str) -> Optional[Dict]:
    """
    指定銘柄の最新サマリー（価格・テクニカル指標・ファンダメンタル・チャートデータ）を返す
    """
    info = GLOBAL_WATCHLIST.get(ticker)
    if not info:
        return None

    df = get_stock_history(ticker)
    if df is None:
        return None

    latest = df.iloc[-1]
    prev   = df.iloc[-2] if len(df) >= 2 else latest
    recent = df.tail(30)

    price  = float(latest["Close"])

    # ---------- テクニカル指標 ----------
    def _f(key):
        v = latest.get(key)
        return round(float(v), 4) if v is not None and not pd.isna(v) else None

    ma5   = _f("MA5")
    ma25  = _f("MA25")
    ma50  = _f("MA50")
    ma150 = _f("MA150")
    ma200 = _f("MA200")
    rsi14 = _f("RSI14")
    vol_avg20 = _f("VolAvg20")
    w52h  = _f("52wHigh")
    w52l  = _f("52wLow")

    # Minerviniスコア（0〜4段階）
    minervini_score = 0
    if all(x is not None for x in [ma50, ma150, ma200]):
        if ma200 > 0:
            minervini_score += 1  # MA200 存在
        if ma50 > ma150:
            minervini_score += 1
        if ma150 > ma200:
            minervini_score += 1
        if price > ma50:
            minervini_score += 1

    # CAN-SLIM 出来高急増
    vol_surge = False
    if vol_avg20 and vol_avg20 > 0:
        vol_surge = float(latest.get("Volume", 0)) > vol_avg20 * 1.5

    # リバモア ブレイクアウト
    near_52w_high = False
    if w52h and w52h > 0:
        near_52w_high = price >= w52h * 0.90

    # ---------- ファンダメンタル（キャッシュから） ----------
    fund = get_fundamental_info(ticker)

    return {
        "ticker":       ticker,
        "name":         info["name"],
        "market":       info["market"],
        "currency":     info["currency"],
        "flag":         info["flag"],
        "current_price": round(price, 4),
        "prev_close":    round(float(prev["Close"]), 4),
        "change_pct":    round((price / float(prev["Close"]) - 1) * 100, 2),
        # 短期MA
        "ma5":  ma5,
        "ma25": ma25,
        # Minervini用
        "ma50":  ma50,
        "ma150": ma150,
        "ma200": ma200,
        "minervini_score": minervini_score,
        # RSI
        "rsi14": round(float(latest["RSI14"]), 2) if not pd.isna(latest["RSI14"]) else None,
        # 出来高
        "volume":    int(latest["Volume"]),
        "vol_avg20": int(vol_avg20) if vol_avg20 else None,
        "vol_surge": vol_surge,
        # 52週高値安値
        "week52_high": w52h,
        "week52_low":  w52l,
        "near_52w_high": near_52w_high,
        # ファンダメンタル
        "per":           fund.get("per")           if fund else None,
        "pbr":           fund.get("pbr")           if fund else None,
        "net_cash_ratio": fund.get("net_cash_ratio") if fund else None,
        "market_cap":    fund.get("market_cap")    if fund else None,
        "dividend_yield": fund.get("dividend_yield") if fund else None,
        # チャートデータ
        "price_history": [
            {
                "date":  str(idx.date()),
                "close": round(float(row["Close"]), 4),
                "ma5":   round(float(row["MA5"]),  4) if not pd.isna(row["MA5"])  else None,
                "ma25":  round(float(row["MA25"]), 4) if not pd.isna(row["MA25"]) else None,
                "ma50":  round(float(row["MA50"]), 4) if not pd.isna(row["MA50"]) else None,
            }
            for idx, row in recent.iterrows()
        ],
    }


def get_summaries_for_open_markets(open_markets: List[str]) -> List[Dict]:
    """現在開いている市場の銘柄だけサマリーを取得する"""
    summaries = []
    for ticker, info in GLOBAL_WATCHLIST.items():
        if info["market"] not in open_markets:
            continue
        s = get_stock_summary(ticker)
        if s:
            summaries.append(s)
        else:
            print(f"⚠️  {ticker} のデータ取得をスキップ")
    return summaries


def get_all_summaries() -> List[Dict]:
    """全銘柄のサマリーを取得（スクリーニング・手動取引用）"""
    summaries = []
    for ticker in GLOBAL_WATCHLIST:
        s = get_stock_summary(ticker)
        if s:
            summaries.append(s)
    return summaries
