"""
グローバル監視銘柄リストと株価データ取得
5市場・15銘柄をカバーします
"""
import yfinance as yf
import pandas as pd
from typing import Dict, List, Optional
from zoneinfo import ZoneInfo

# 通貨→円換算ティッカー（yfinance）
FX_TICKERS = {
    "USD": "USDJPY=X",
    "GBP": "GBPJPY=X",
    "EUR": "EURJPY=X",
    "HKD": "HKDJPY=X",
    "JPY": None,   # 換算不要
}

# 監視銘柄リスト（市場 / 通貨 / 表示名）
GLOBAL_WATCHLIST = {
    # 🇯🇵 日本（東証）
    "7203.T": {"name": "トヨタ自動車",   "market": "TSE",  "currency": "JPY", "flag": "🇯🇵"},
    "6758.T": {"name": "ソニーグループ", "market": "TSE",  "currency": "JPY", "flag": "🇯🇵"},
    "9984.T": {"name": "ソフトバンクG",  "market": "TSE",  "currency": "JPY", "flag": "🇯🇵"},
    # 🇺🇸 米国（NYSE/NASDAQ）
    "AAPL":   {"name": "Apple",          "market": "NYSE", "currency": "USD", "flag": "🇺🇸"},
    "MSFT":   {"name": "Microsoft",      "market": "NYSE", "currency": "USD", "flag": "🇺🇸"},
    "NVDA":   {"name": "NVIDIA",         "market": "NYSE", "currency": "USD", "flag": "🇺🇸"},
    "TSLA":   {"name": "Tesla",          "market": "NYSE", "currency": "USD", "flag": "🇺🇸"},
    "AMZN":   {"name": "Amazon",         "market": "NYSE", "currency": "USD", "flag": "🇺🇸"},
    "JPM":    {"name": "JPモルガン",     "market": "NYSE", "currency": "USD", "flag": "🇺🇸"},
    # 🇬🇧 英国（LSE）
    "SHEL.L": {"name": "Shell",          "market": "LSE",  "currency": "GBP", "flag": "🇬🇧"},
    "AZN.L":  {"name": "AstraZeneca",    "market": "LSE",  "currency": "GBP", "flag": "🇬🇧"},
    # 🇩🇪 ドイツ（DAX）
    "SAP.DE": {"name": "SAP",            "market": "DAX",  "currency": "EUR", "flag": "🇩🇪"},
    "SIE.DE": {"name": "Siemens",        "market": "DAX",  "currency": "EUR", "flag": "🇩🇪"},
    # 🇭🇰 香港（HKEX）
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
    # フォールバック（取得できなかった通貨はデフォルト値）
    defaults = {"USD": 150.0, "GBP": 190.0, "EUR": 162.0, "HKD": 19.0}
    for c, v in defaults.items():
        rates.setdefault(c, v)
    return rates


def get_stock_history(ticker: str, days: int = 60) -> Optional[pd.DataFrame]:
    """株価履歴を取得してテクニカル指標を計算する"""
    try:
        hist = yf.Ticker(ticker).history(period=f"{days}d", auto_adjust=True)
        if hist.empty or len(hist) < 10:
            return None
        hist.index = hist.index.tz_localize(None)

        close = hist["Close"]
        hist["MA5"]   = close.rolling(5).mean()
        hist["MA25"]  = close.rolling(25).mean()

        delta = close.diff()
        gain  = delta.clip(lower=0).rolling(14).mean()
        loss  = (-delta.clip(upper=0)).rolling(14).mean()
        rs    = gain / loss.replace(0, float("nan"))
        hist["RSI14"] = (100 - 100 / (1 + rs)).round(2)

        return hist.round(4)
    except Exception as e:
        print(f"⚠️  {ticker} 履歴取得エラー: {e}")
        return None


def get_stock_summary(ticker: str) -> Optional[Dict]:
    """指定銘柄の最新サマリー（価格・指標・チャートデータ）を返す"""
    info = GLOBAL_WATCHLIST.get(ticker)
    if not info:
        return None

    df = get_stock_history(ticker)
    if df is None:
        return None

    latest = df.iloc[-1]
    prev   = df.iloc[-2] if len(df) >= 2 else latest
    recent = df.tail(30)

    return {
        "ticker":       ticker,
        "name":         info["name"],
        "market":       info["market"],
        "currency":     info["currency"],
        "flag":         info["flag"],
        "current_price": round(float(latest["Close"]), 4),
        "prev_close":    round(float(prev["Close"]), 4),
        "change_pct":    round((float(latest["Close"]) / float(prev["Close"]) - 1) * 100, 2),
        "ma5":           round(float(latest["MA5"]),   4) if not pd.isna(latest["MA5"])   else None,
        "ma25":          round(float(latest["MA25"]),  4) if not pd.isna(latest["MA25"])  else None,
        "rsi14":         round(float(latest["RSI14"]), 2) if not pd.isna(latest["RSI14"]) else None,
        "volume":        int(latest["Volume"]),
        "price_history": [
            {
                "date":  str(idx.date()),
                "close": round(float(row["Close"]), 4),
                "ma5":   round(float(row["MA5"]),  4) if not pd.isna(row["MA5"])  else None,
                "ma25":  round(float(row["MA25"]), 4) if not pd.isna(row["MA25"]) else None,
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
    """全銘柄のサマリーを取得（手動取引・ポートフォリオ評価用）"""
    summaries = []
    for ticker in GLOBAL_WATCHLIST:
        s = get_stock_summary(ticker)
        if s:
            summaries.append(s)
    return summaries
