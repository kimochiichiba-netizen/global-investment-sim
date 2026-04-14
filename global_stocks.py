"""
日本小型株監視銘柄リストと株価データ取得（清原式対応版）

【テクニカル指標】
  MA5/25/50/150/200, RSI14, Volume/ATR14, MACD(12/26/9)
  Bollinger Bands(20/2), ROC20/60 (モメンタム), 52週高低

【ファンダメンタル指標】
  PER, PBR, NC比率（清原式）, 配当性向, 配当利回り

【最適化】
  バッチダウンロード（全銘柄を1回のAPIコールで取得・2分キャッシュ）
"""
import threading
import yfinance as yf
import pandas as pd
from datetime import datetime, date
from typing import Dict, List, Optional, Any

from database import get_fundamental_cache, save_fundamental_cache

# ── 為替レート取得 ──────────────────────────────────────────

def get_fx_rates() -> Dict[str, float]:
    """日本株専用: JPY=1.0 のみ返す"""
    return {"JPY": 1.0}


def get_fx_rates_global() -> Dict[str, float]:
    """
    グローバル投資用: yfinanceで主要通貨→JPYレートを取得。
    失敗時はフォールバック値を使用。
    """
    FALLBACK = {"JPY": 1.0, "USD": 150.0, "GBP": 190.0, "EUR": 160.0,
                "HKD": 19.0, "KRW": 0.11, "AUD": 97.0}
    pairs = ["USDJPY=X", "GBPJPY=X", "EURJPY=X", "HKDJPY=X", "KRWJPY=X", "AUDJPY=X"]
    try:
        raw = yf.download(pairs, period="2d", progress=False, auto_adjust=True)
        result = {"JPY": 1.0}
        for pair in pairs:
            currency = pair[:3]
            try:
                if isinstance(raw.columns, pd.MultiIndex):
                    price = float(raw["Close"][pair].dropna().iloc[-1])
                else:
                    price = float(raw["Close"].dropna().iloc[-1])
                result[currency] = round(price, 4)
            except Exception:
                result[currency] = FALLBACK.get(currency, 1.0)
        return result
    except Exception:
        return FALLBACK

# ── 監視銘柄リスト（東証・日本小型株 50銘柄）───────────────
GLOBAL_WATCHLIST: Dict[str, Dict] = {
    # ── IT・ソフトウェア (10銘柄) ──
    "2193.T": {"name": "クックパッド",        "market": "TSE", "currency": "JPY", "flag": "🇯🇵", "sector": "IT"},
    "3054.T": {"name": "ハイパー",             "market": "TSE", "currency": "JPY", "flag": "🇯🇵", "sector": "IT"},
    "3922.T": {"name": "PR TIMES",            "market": "TSE", "currency": "JPY", "flag": "🇯🇵", "sector": "IT"},
    "4776.T": {"name": "サイボウズ",           "market": "TSE", "currency": "JPY", "flag": "🇯🇵", "sector": "IT"},
    "3676.T": {"name": "デジタルハーツHD",     "market": "TSE", "currency": "JPY", "flag": "🇯🇵", "sector": "IT"},
    "3844.T": {"name": "コムチュア",           "market": "TSE", "currency": "JPY", "flag": "🇯🇵", "sector": "IT"},
    "2307.T": {"name": "クロスキャット",       "market": "TSE", "currency": "JPY", "flag": "🇯🇵", "sector": "IT"},
    "4722.T": {"name": "フューチャー",         "market": "TSE", "currency": "JPY", "flag": "🇯🇵", "sector": "IT"},
    "9687.T": {"name": "KSK",                 "market": "TSE", "currency": "JPY", "flag": "🇯🇵", "sector": "IT"},
    "2175.T": {"name": "エス・エム・エス",     "market": "TSE", "currency": "JPY", "flag": "🇯🇵", "sector": "IT"},
    # ── 製造・工業 (10銘柄) ──
    "5803.T": {"name": "フジクラ",             "market": "TSE", "currency": "JPY", "flag": "🇯🇵", "sector": "製造"},
    "7995.T": {"name": "バルカー",             "market": "TSE", "currency": "JPY", "flag": "🇯🇵", "sector": "製造"},
    "6418.T": {"name": "日本金銭機械",         "market": "TSE", "currency": "JPY", "flag": "🇯🇵", "sector": "製造"},
    "5218.T": {"name": "オハラ",               "market": "TSE", "currency": "JPY", "flag": "🇯🇵", "sector": "製造"},
    "6489.T": {"name": "前澤工業",             "market": "TSE", "currency": "JPY", "flag": "🇯🇵", "sector": "製造"},
    "5943.T": {"name": "ノーリツ",             "market": "TSE", "currency": "JPY", "flag": "🇯🇵", "sector": "製造"},
    "6440.T": {"name": "JUKI",                "market": "TSE", "currency": "JPY", "flag": "🇯🇵", "sector": "製造"},
    "7966.T": {"name": "リンテック",           "market": "TSE", "currency": "JPY", "flag": "🇯🇵", "sector": "製造"},
    "5953.T": {"name": "昭和鉄工",             "market": "TSE", "currency": "JPY", "flag": "🇯🇵", "sector": "製造"},
    "3436.T": {"name": "SUMCO",               "market": "TSE", "currency": "JPY", "flag": "🇯🇵", "sector": "製造"},
    # ── 小売・外食 (10銘柄) ──
    "3028.T": {"name": "アルペン",             "market": "TSE", "currency": "JPY", "flag": "🇯🇵", "sector": "小売"},
    "7611.T": {"name": "ハイデイ日高",         "market": "TSE", "currency": "JPY", "flag": "🇯🇵", "sector": "外食"},
    "9994.T": {"name": "やまや",               "market": "TSE", "currency": "JPY", "flag": "🇯🇵", "sector": "小売"},
    "3097.T": {"name": "物語コーポレーション", "market": "TSE", "currency": "JPY", "flag": "🇯🇵", "sector": "外食"},
    "9942.T": {"name": "ジョイフル",           "market": "TSE", "currency": "JPY", "flag": "🇯🇵", "sector": "外食"},
    "2670.T": {"name": "エービーシー・マート", "market": "TSE", "currency": "JPY", "flag": "🇯🇵", "sector": "小売"},
    "3048.T": {"name": "ビックカメラ",         "market": "TSE", "currency": "JPY", "flag": "🇯🇵", "sector": "小売"},
    "2884.T": {"name": "ヨシムラ・フード",     "market": "TSE", "currency": "JPY", "flag": "🇯🇵", "sector": "食品"},
    "9872.T": {"name": "北恵",                "market": "TSE", "currency": "JPY", "flag": "🇯🇵", "sector": "卸売"},
    "2590.T": {"name": "ダイドーグループHD",   "market": "TSE", "currency": "JPY", "flag": "🇯🇵", "sector": "飲料"},
    # ── 食品・飲料 (8銘柄) ──
    "2003.T": {"name": "日東富士製粉",         "market": "TSE", "currency": "JPY", "flag": "🇯🇵", "sector": "食品"},
    "2108.T": {"name": "日本甜菜製糖",         "market": "TSE", "currency": "JPY", "flag": "🇯🇵", "sector": "食品"},
    "2226.T": {"name": "湖池屋",               "market": "TSE", "currency": "JPY", "flag": "🇯🇵", "sector": "食品"},
    "2114.T": {"name": "フジ日本精糖",         "market": "TSE", "currency": "JPY", "flag": "🇯🇵", "sector": "食品"},
    "2894.T": {"name": "石井食品",             "market": "TSE", "currency": "JPY", "flag": "🇯🇵", "sector": "食品"},
    "2221.T": {"name": "岩塚製菓",             "market": "TSE", "currency": "JPY", "flag": "🇯🇵", "sector": "食品"},
    "2264.T": {"name": "森永乳業",             "market": "TSE", "currency": "JPY", "flag": "🇯🇵", "sector": "食品"},
    "2579.T": {"name": "コカ・コーラ ボトラーズジャパン", "market": "TSE", "currency": "JPY", "flag": "🇯🇵", "sector": "飲料"},
    # ── 建設・不動産 (7銘柄) ──
    "1840.T": {"name": "土屋ホールディングス", "market": "TSE", "currency": "JPY", "flag": "🇯🇵", "sector": "建設"},
    "1835.T": {"name": "東鉄工業",             "market": "TSE", "currency": "JPY", "flag": "🇯🇵", "sector": "建設"},
    "8917.T": {"name": "ファースト住建",       "market": "TSE", "currency": "JPY", "flag": "🇯🇵", "sector": "不動産"},
    "1814.T": {"name": "大末建設",             "market": "TSE", "currency": "JPY", "flag": "🇯🇵", "sector": "建設"},
    "1890.T": {"name": "東洋建設",             "market": "TSE", "currency": "JPY", "flag": "🇯🇵", "sector": "建設"},
    "8934.T": {"name": "サンフロンティア不動産","market": "TSE", "currency": "JPY", "flag": "🇯🇵", "sector": "不動産"},
    "3244.T": {"name": "サムティ",             "market": "TSE", "currency": "JPY", "flag": "🇯🇵", "sector": "不動産"},
    # ── 医療・ヘルスケア (5銘柄) ──
    "6730.T": {"name": "アクセル",             "market": "TSE", "currency": "JPY", "flag": "🇯🇵", "sector": "医療"},
    "4538.T": {"name": "扶桑薬品工業",         "market": "TSE", "currency": "JPY", "flag": "🇯🇵", "sector": "医薬"},
    "7840.T": {"name": "フランスベッド",       "market": "TSE", "currency": "JPY", "flag": "🇯🇵", "sector": "医療"},
    "3360.T": {"name": "シップヘルスケアHD",   "market": "TSE", "currency": "JPY", "flag": "🇯🇵", "sector": "医療"},
    "6034.T": {"name": "MRT",                 "market": "TSE", "currency": "JPY", "flag": "🇯🇵", "sector": "医療"},
}

# ── グローバル複合スコア用ウォッチリスト（7市場・50銘柄）───────
GLOBAL_WATCHLIST_7MKT: Dict[str, Dict] = {
    # 米国 NYSE/NASDAQ (8銘柄)
    "AAPL":      {"name": "Apple",            "market": "NYSE",  "currency": "USD", "flag": "🇺🇸", "sector": "IT"},
    "MSFT":      {"name": "Microsoft",        "market": "NYSE",  "currency": "USD", "flag": "🇺🇸", "sector": "IT"},
    "GOOGL":     {"name": "Alphabet",         "market": "NYSE",  "currency": "USD", "flag": "🇺🇸", "sector": "IT"},
    "AMZN":      {"name": "Amazon",           "market": "NYSE",  "currency": "USD", "flag": "🇺🇸", "sector": "消費"},
    "NVDA":      {"name": "NVIDIA",           "market": "NYSE",  "currency": "USD", "flag": "🇺🇸", "sector": "IT"},
    "META":      {"name": "Meta",             "market": "NYSE",  "currency": "USD", "flag": "🇺🇸", "sector": "IT"},
    "JPM":       {"name": "JPMorgan",         "market": "NYSE",  "currency": "USD", "flag": "🇺🇸", "sector": "金融"},
    "JNJ":       {"name": "J&J",             "market": "NYSE",  "currency": "USD", "flag": "🇺🇸", "sector": "医療"},
    # 日本 TSE 大型株 (8銘柄)
    "7203.T":    {"name": "トヨタ",           "market": "TSE",   "currency": "JPY", "flag": "🇯🇵", "sector": "自動車"},
    "6758.T":    {"name": "ソニーG",          "market": "TSE",   "currency": "JPY", "flag": "🇯🇵", "sector": "IT"},
    "6861.T":    {"name": "キーエンス",       "market": "TSE",   "currency": "JPY", "flag": "🇯🇵", "sector": "IT"},
    "9984.T":    {"name": "SoftBank G",       "market": "TSE",   "currency": "JPY", "flag": "🇯🇵", "sector": "IT"},
    "6367.T":    {"name": "ダイキン",         "market": "TSE",   "currency": "JPY", "flag": "🇯🇵", "sector": "製造"},
    "4063.T":    {"name": "信越化学",         "market": "TSE",   "currency": "JPY", "flag": "🇯🇵", "sector": "化学"},
    "8035.T":    {"name": "東京エレクトロン", "market": "TSE",   "currency": "JPY", "flag": "🇯🇵", "sector": "IT"},
    "9432.T":    {"name": "NTT",             "market": "TSE",   "currency": "JPY", "flag": "🇯🇵", "sector": "通信"},
    # 英国 LSE (7銘柄)
    "SHEL.L":    {"name": "Shell",            "market": "LSE",   "currency": "GBP", "flag": "🇬🇧", "sector": "エネルギー"},
    "AZN.L":     {"name": "AstraZeneca",      "market": "LSE",   "currency": "GBP", "flag": "🇬🇧", "sector": "医療"},
    "HSBA.L":    {"name": "HSBC",             "market": "LSE",   "currency": "GBP", "flag": "🇬🇧", "sector": "金融"},
    "ULVR.L":    {"name": "Unilever",         "market": "LSE",   "currency": "GBP", "flag": "🇬🇧", "sector": "消費"},
    "RIO.L":     {"name": "Rio Tinto",        "market": "LSE",   "currency": "GBP", "flag": "🇬🇧", "sector": "素材"},
    "BP.L":      {"name": "BP",              "market": "LSE",   "currency": "GBP", "flag": "🇬🇧", "sector": "エネルギー"},
    "VOD.L":     {"name": "Vodafone",         "market": "LSE",   "currency": "GBP", "flag": "🇬🇧", "sector": "通信"},
    # 欧州 DAX (7銘柄)
    "SAP.DE":    {"name": "SAP",             "market": "DAX",   "currency": "EUR", "flag": "🇩🇪", "sector": "IT"},
    "SIE.DE":    {"name": "Siemens",          "market": "DAX",   "currency": "EUR", "flag": "🇩🇪", "sector": "製造"},
    "BAYN.DE":   {"name": "Bayer",            "market": "DAX",   "currency": "EUR", "flag": "🇩🇪", "sector": "医療"},
    "VOW3.DE":   {"name": "Volkswagen",       "market": "DAX",   "currency": "EUR", "flag": "🇩🇪", "sector": "自動車"},
    "DTE.DE":    {"name": "Deutsche Telekom", "market": "DAX",   "currency": "EUR", "flag": "🇩🇪", "sector": "通信"},
    "ALV.DE":    {"name": "Allianz",          "market": "DAX",   "currency": "EUR", "flag": "🇩🇪", "sector": "金融"},
    "MBG.DE":    {"name": "Mercedes",         "market": "DAX",   "currency": "EUR", "flag": "🇩🇪", "sector": "自動車"},
    # 香港 HKEX (7銘柄)
    "9988.HK":   {"name": "Alibaba",          "market": "HKEX",  "currency": "HKD", "flag": "🇭🇰", "sector": "IT"},
    "0700.HK":   {"name": "Tencent",          "market": "HKEX",  "currency": "HKD", "flag": "🇭🇰", "sector": "IT"},
    "1299.HK":   {"name": "AIA",             "market": "HKEX",  "currency": "HKD", "flag": "🇭🇰", "sector": "金融"},
    "0941.HK":   {"name": "China Mobile",     "market": "HKEX",  "currency": "HKD", "flag": "🇭🇰", "sector": "通信"},
    "2318.HK":   {"name": "Ping An",          "market": "HKEX",  "currency": "HKD", "flag": "🇭🇰", "sector": "金融"},
    "3690.HK":   {"name": "Meituan",          "market": "HKEX",  "currency": "HKD", "flag": "🇭🇰", "sector": "IT"},
    "0005.HK":   {"name": "HSBC HK",          "market": "HKEX",  "currency": "HKD", "flag": "🇭🇰", "sector": "金融"},
    # 韓国 KOSPI (6銘柄)
    "005930.KS": {"name": "Samsung",          "market": "KOSPI", "currency": "KRW", "flag": "🇰🇷", "sector": "IT"},
    "000660.KS": {"name": "SK Hynix",         "market": "KOSPI", "currency": "KRW", "flag": "🇰🇷", "sector": "IT"},
    "207940.KS": {"name": "Samsung Biologics","market": "KOSPI", "currency": "KRW", "flag": "🇰🇷", "sector": "医療"},
    "005380.KS": {"name": "Hyundai Motor",    "market": "KOSPI", "currency": "KRW", "flag": "🇰🇷", "sector": "自動車"},
    "051910.KS": {"name": "LG Chem",          "market": "KOSPI", "currency": "KRW", "flag": "🇰🇷", "sector": "化学"},
    "035420.KS": {"name": "NAVER",            "market": "KOSPI", "currency": "KRW", "flag": "🇰🇷", "sector": "IT"},
    # 豪州 ASX (7銘柄)
    "BHP.AX":    {"name": "BHP",             "market": "ASX",   "currency": "AUD", "flag": "🇦🇺", "sector": "素材"},
    "CBA.AX":    {"name": "Commonwealth Bank","market": "ASX",   "currency": "AUD", "flag": "🇦🇺", "sector": "金融"},
    "CSL.AX":    {"name": "CSL",             "market": "ASX",   "currency": "AUD", "flag": "🇦🇺", "sector": "医療"},
    "WBC.AX":    {"name": "Westpac",          "market": "ASX",   "currency": "AUD", "flag": "🇦🇺", "sector": "金融"},
    "ANZ.AX":    {"name": "ANZ",             "market": "ASX",   "currency": "AUD", "flag": "🇦🇺", "sector": "金融"},
    "RIO.AX":    {"name": "Rio Tinto AX",     "market": "ASX",   "currency": "AUD", "flag": "🇦🇺", "sector": "素材"},
    "WES.AX":    {"name": "Wesfarmers",       "market": "ASX",   "currency": "AUD", "flag": "🇦🇺", "sector": "消費"},
}

# ── バッチダウンロードキャッシュ（2分間有効）─────────────
_batch_lock  = threading.Lock()
_batch_cache: Dict[str, Any] = {"raw": None, "time": None}
_BATCH_TTL   = 120  # 秒


# ============================================================
# 価格データ取得・指標計算
# ============================================================

def _compute_indicators(df: pd.DataFrame) -> pd.DataFrame:
    """
    OHLCVデータフレームにテクニカル指標を計算して追加する。

    計算する指標:
      MA5/25/50/150/200, VolAvg20, 52wHigh/Low
      RSI14
      MACD (12/26/9): macd, macd_signal, macd_hist
      Bollinger Bands (20/2): bb_upper, bb_lower, bb_width
      ATR14 (Average True Range)
      ROC20 / ROC60 (Rate of Change)
    """
    if df.empty or len(df) < 5:
        return df

    close = df["Close"]

    # ── 移動平均 ──
    df["MA5"]   = close.rolling(5).mean()
    df["MA25"]  = close.rolling(25).mean()
    df["MA50"]  = close.rolling(50).mean()
    df["MA150"] = close.rolling(150).mean()
    df["MA200"] = close.rolling(200).mean()

    # ── 出来高移動平均 ──
    if "Volume" in df.columns:
        df["VolAvg20"] = df["Volume"].rolling(20).mean()

    # ── 52週高値・安値 ──
    n52 = min(252, len(close))
    df["52wHigh"] = close.rolling(n52).max()
    df["52wLow"]  = close.rolling(n52).min()

    # ── RSI 14 ──
    delta = close.diff()
    gain  = delta.clip(lower=0).rolling(14).mean()
    loss  = (-delta.clip(upper=0)).rolling(14).mean()
    rs    = gain / loss.replace(0, float("nan"))
    df["RSI14"] = (100 - 100 / (1 + rs)).round(2)

    # ── MACD (12, 26, 9) ──
    ema12 = close.ewm(span=12, adjust=False).mean()
    ema26 = close.ewm(span=26, adjust=False).mean()
    df["MACD"]        = ema12 - ema26
    df["MACD_Signal"] = df["MACD"].ewm(span=9, adjust=False).mean()
    df["MACD_Hist"]   = df["MACD"] - df["MACD_Signal"]

    # ── Bollinger Bands (20, 2σ) ──
    sma20  = close.rolling(20).mean()
    std20  = close.rolling(20).std()
    df["BB_Upper"] = sma20 + 2 * std20
    df["BB_Lower"] = sma20 - 2 * std20
    df["BB_Width"] = ((df["BB_Upper"] - df["BB_Lower"]) / sma20).round(4)

    # ── ATR 14 (Average True Range) ──
    if "High" in df.columns and "Low" in df.columns:
        prev_close = close.shift(1)
        tr = pd.concat([
            df["High"] - df["Low"],
            (df["High"] - prev_close).abs(),
            (df["Low"]  - prev_close).abs(),
        ], axis=1).max(axis=1)
        df["ATR14"] = tr.ewm(span=14, adjust=False).mean()

    # ── ROC (Rate of Change) ─ モメンタム ──
    df["ROC20"] = close.pct_change(20) * 100
    df["ROC60"] = close.pct_change(60) * 100

    # ── 20日高値（前日まで）ブレイクアウト判定用 ──
    df["High20d"] = close.rolling(20).max().shift(1)

    return df.round(4)


def _get_batch_raw() -> Optional[Any]:
    """
    全銘柄を一括ダウンロード（2分間キャッシュ付き）。
    """
    with _batch_lock:
        now = datetime.now()
        if _batch_cache["raw"] is not None and _batch_cache["time"]:
            elapsed = (now - _batch_cache["time"]).total_seconds()
            if elapsed < _BATCH_TTL:
                return _batch_cache["raw"]

        tickers  = list(GLOBAL_WATCHLIST.keys())
        print(f"  📥 全{len(tickers)}銘柄を一括ダウンロード中...")
        try:
            raw = yf.download(
                tickers,
                period="270d",
                progress=False,
                auto_adjust=True,
            )
            if not raw.empty:
                raw.index = pd.to_datetime(raw.index).tz_localize(None)
                _batch_cache["raw"]  = raw
                _batch_cache["time"] = now
                print(f"  ✅ 一括ダウンロード完了（{len(tickers)}銘柄）")
                return raw
        except Exception as e:
            print(f"⚠️  一括ダウンロードエラー: {e}")
        return None


def _extract_ohlcv(raw: Any, ticker: str) -> Optional[pd.DataFrame]:
    """バッチrawデータから1銘柄のOHLCVを抽出"""
    try:
        if isinstance(raw.columns, pd.MultiIndex):
            level1 = raw.columns.get_level_values(1)
            if ticker not in level1:
                return None
            df = raw.xs(ticker, level=1, axis=1).copy()
        else:
            df = raw.copy()

        df.index = pd.to_datetime(df.index).tz_localize(None)
        df = df.dropna(subset=["Close"])
        if len(df) < 10:
            return None
        return df
    except Exception:
        return None


def get_stock_history(ticker: str, days: int = 260) -> Optional[pd.DataFrame]:
    """個別取得（主にチャートAPI用）"""
    with _batch_lock:
        cached_raw = _batch_cache.get("raw")
    if cached_raw is not None:
        df = _extract_ohlcv(cached_raw, ticker)
        if df is not None:
            return _compute_indicators(df)

    try:
        hist = yf.Ticker(ticker).history(period=f"{days}d", auto_adjust=True)
        if hist.empty or len(hist) < 10:
            return None
        hist.index = hist.index.tz_localize(None)
        return _compute_indicators(hist)
    except Exception as e:
        print(f"⚠️  {ticker} 履歴取得エラー: {e}")
        return None


# ============================================================
# ファンダメンタル指標取得（清原式: NC比率・配当性向中心）
# ============================================================

def get_fundamental_info(ticker: str, watchlist: Optional[Dict] = None) -> Optional[Dict]:
    """
    yfinance .info + balance_sheet からファンダメンタル指標を取得。
    1日1回だけAPIを叩き、あとはDBキャッシュを使う。

    watchlist 未指定 → GLOBAL_WATCHLIST（日本株）を使用
    watchlist 指定   → 指定したウォッチリストを使用（7市場対応）

    取得指標:
      PER, PBR, 時価総額（億円換算）
      NC比率 = (流動資産 - 負債) ÷ 時価総額
      配当性向 = (DPS ÷ EPS) × 100
      配当利回り
    """
    cached = get_fundamental_cache(ticker)
    if cached:
        return cached

    wl = watchlist if watchlist is not None else GLOBAL_WATCHLIST
    info_meta = wl.get(ticker)
    if not info_meta:
        # フォールバック: 両方のウォッチリストを確認
        info_meta = GLOBAL_WATCHLIST.get(ticker) or GLOBAL_WATCHLIST_7MKT.get(ticker)
    if not info_meta:
        return None

    try:
        tk   = yf.Ticker(ticker)
        info = tk.info or {}

        market_cap = info.get("marketCap")
        per        = info.get("forwardPE") or info.get("trailingPE")
        pbr        = info.get("priceToBook")

        # バランスシート → NC比率
        cash_and_equiv = total_debt = current_assets = None
        try:
            bs = tk.balance_sheet
            if bs is not None and not bs.empty:
                def _get_bs(keys):
                    for k in keys:
                        if k in bs.index:
                            v = bs.loc[k].iloc[0]
                            if pd.notna(v):
                                return float(v)
                    return None
                cash_and_equiv = _get_bs([
                    "Cash And Cash Equivalents",
                    "CashAndCashEquivalents",
                    "Cash Cash Equivalents And Short Term Investments",
                ])
                total_debt = _get_bs(["Total Debt", "TotalDebt", "Total Liabilities Net Minority Interest"])
                current_assets = _get_bs(["Current Assets", "Total Current Assets"])
        except Exception:
            pass

        net_cash = net_cash_ratio = None
        if current_assets is not None and total_debt is not None and market_cap and market_cap > 0:
            # 清原式: NC = 流動資産 - 負債（固定資産は含めない）
            net_cash = current_assets - total_debt
            net_cash_ratio = round(net_cash / market_cap, 4)
        elif cash_and_equiv is not None and market_cap and market_cap > 0:
            # フォールバック: 現金のみ
            debt = total_debt or 0
            net_cash = cash_and_equiv - debt
            net_cash_ratio = round(net_cash / market_cap, 4)

        # 時価総額を億円換算（JPYなのでそのまま ÷ 1億）
        market_cap_oku = round(market_cap / 1e8, 1) if market_cap else None

        # 配当性向 = (DPS ÷ EPS) × 100
        eps = info.get("trailingEps")
        dps = info.get("dividendRate")
        dividend_payout_ratio = None
        if eps and eps > 0 and dps and dps > 0:
            dividend_payout_ratio = round((dps / eps) * 100, 1)

        dividend_yield = info.get("dividendYield")

        # グローバル複合スコア用指標（Minervini/Lynch/Buffett）
        roe              = info.get("returnOnEquity")   # 例: 0.32 → 32%
        debt_to_equity   = info.get("debtToEquity")
        peg_ratio        = info.get("trailingPegRatio") or info.get("pegRatio")
        earnings_growth  = info.get("earningsGrowth") or info.get("revenueGrowth")
        operating_margin = info.get("operatingMargins")

        currency = info_meta.get("currency", "JPY")

        data = {
            "ticker":               ticker,
            "name":                 info_meta["name"],
            "market_cap":           market_cap,
            "market_cap_oku":       market_cap_oku,
            "per":                  round(float(per), 2) if per and float(per) > 0 else None,
            "pbr":                  round(float(pbr), 2) if pbr and float(pbr) > 0 else None,
            "current_assets":       current_assets,
            "total_debt":           total_debt,
            "cash_and_equiv":       cash_and_equiv,
            "net_cash":             net_cash,
            "net_cash_ratio":       net_cash_ratio,
            "dividend_payout_ratio": dividend_payout_ratio,
            "dividend_yield":       round(float(dividend_yield) * 100, 2) if dividend_yield else None,
            "sector":               info.get("sector", info_meta.get("sector", "")),
            "currency":             currency,
            # グローバル複合スコア指標
            "roe":              round(float(roe), 4) if roe is not None else None,
            "debt_to_equity":   round(float(debt_to_equity), 2) if debt_to_equity is not None else None,
            "peg_ratio":        round(float(peg_ratio), 2) if peg_ratio is not None else None,
            "earnings_growth":  round(float(earnings_growth) * 100, 2) if earnings_growth is not None else None,
            "operating_margin": round(float(operating_margin) * 100, 2) if operating_margin is not None else None,
            "last_updated":     date.today().isoformat(),
        }
        save_fundamental_cache(data)
        return data

    except Exception as e:
        print(f"⚠️  {ticker} ファンダメンタル取得エラー: {e}")
        return None


# ============================================================
# サマリー生成
# ============================================================

def _build_summary(ticker: str, df: pd.DataFrame,
                   watchlist: Optional[Dict] = None) -> Optional[Dict]:
    """指標計算済みDataFrameから銘柄サマリーを生成する内部関数"""
    wl   = watchlist if watchlist is not None else GLOBAL_WATCHLIST
    info = wl.get(ticker)
    if not info or df is None or len(df) < 2:
        return None

    latest = df.iloc[-1]
    prev   = df.iloc[-2]
    recent = df.tail(30)

    price = float(latest["Close"])
    if price <= 0:
        return None

    def _f(key):
        v = latest.get(key)
        if v is None:
            return None
        try:
            fv = float(v)
            return None if pd.isna(fv) else round(fv, 4)
        except Exception:
            return None

    # ── テクニカル指標 ──
    ma5  = _f("MA5");  ma25 = _f("MA25"); ma50 = _f("MA50")
    ma150= _f("MA150"); ma200= _f("MA200")
    rsi14= _f("RSI14")
    vol_avg20 = _f("VolAvg20")
    high20d = _f("High20d")
    w52h = _f("52wHigh"); w52l = _f("52wLow")
    atr14 = _f("ATR14")
    roc20 = _f("ROC20"); roc60 = _f("ROC60")
    bb_width = _f("BB_Width")
    bb_upper = _f("BB_Upper"); bb_lower = _f("BB_Lower")

    # ── MACD クロス状態 ──
    macd_hist_curr = _f("MACD_Hist")
    macd_hist_prev = None
    if len(df) >= 2:
        v = df["MACD_Hist"].iloc[-2]
        try:
            macd_hist_prev = float(v) if not pd.isna(v) else None
        except Exception:
            pass

    macd_cross = "unknown"
    if macd_hist_curr is not None and macd_hist_prev is not None:
        if macd_hist_prev <= 0 and macd_hist_curr > 0:
            macd_cross = "golden"
        elif macd_hist_curr > 0:
            macd_cross = "positive"
        elif macd_hist_prev >= 0 and macd_hist_curr < 0:
            macd_cross = "dead"
        else:
            macd_cross = "negative"

    # ── 出来高急増 ──
    vol_surge = False
    if vol_avg20 and vol_avg20 > 0 and "Volume" in latest.index:
        vol_surge = float(latest["Volume"]) > vol_avg20 * 1.5

    # ── ファンダメンタル ──
    fund = get_fundamental_info(ticker, watchlist=wl)

    return {
        "ticker":        ticker,
        "name":          info["name"],
        "market":        info["market"],
        "currency":      info["currency"],
        "flag":          info["flag"],
        "sector":        info.get("sector", "その他"),
        "current_price": round(price, 4),
        "prev_close":    round(float(prev["Close"]), 4),
        "change_pct":    round((price / float(prev["Close"]) - 1) * 100, 2),
        # 移動平均
        "ma5": ma5, "ma25": ma25, "ma50": ma50, "ma150": ma150, "ma200": ma200,
        "minervini_score": 0,  # 使わないが後方互換のため残す
        # RSI
        "rsi14": rsi14,
        # 出来高
        "volume":     int(latest["Volume"]) if "Volume" in latest.index else 0,
        "vol_avg20":  int(vol_avg20) if vol_avg20 else None,
        "vol_surge":  vol_surge,
        "high_20d":   high20d,
        # 52週
        "week52_high":   w52h,
        "week52_low":    w52l,
        "near_52w_high": False,
        # MACD
        "macd":        _f("MACD"),
        "macd_signal": _f("MACD_Signal"),
        "macd_cross":  macd_cross,
        # Bollinger
        "bb_upper": bb_upper,
        "bb_lower": bb_lower,
        "bb_width": bb_width,
        # ATR
        "atr14": atr14,
        # Momentum
        "roc20": round(roc20, 2) if roc20 else None,
        "roc60": round(roc60, 2) if roc60 else None,
        # ファンダメンタル（清原式）
        "per":                   fund.get("per")                   if fund else None,
        "pbr":                   fund.get("pbr")                   if fund else None,
        "net_cash_ratio":        fund.get("net_cash_ratio")        if fund else None,
        "market_cap":            fund.get("market_cap")            if fund else None,
        "market_cap_oku":        fund.get("market_cap_oku")        if fund else None,
        "dividend_yield":        fund.get("dividend_yield")        if fund else None,
        "dividend_payout_ratio": fund.get("dividend_payout_ratio") if fund else None,
        # 旧フィールド後方互換
        "roe":             None,
        "debt_to_equity":  None,
        "peg_ratio":       None,
        "earnings_growth": None,
        "operating_margin":None,
        # チャートデータ（30日分）
        "price_history": [
            {
                "date":  str(idx.date()),
                "close": round(float(row["Close"]), 4),
                "ma5":   round(float(row["MA5"]),  4) if "MA5" in row and not pd.isna(row["MA5"])  else None,
                "ma25":  round(float(row["MA25"]), 4) if "MA25" in row and not pd.isna(row["MA25"]) else None,
                "ma50":  round(float(row["MA50"]), 4) if "MA50" in row and not pd.isna(row["MA50"]) else None,
                "macd":  round(float(row["MACD"]), 4) if "MACD" in row and not pd.isna(row["MACD"]) else None,
                "macd_signal": round(float(row["MACD_Signal"]), 4)
                                if "MACD_Signal" in row and not pd.isna(row["MACD_Signal"]) else None,
            }
            for idx, row in recent.iterrows()
        ],
    }


def get_stock_summary(ticker: str, watchlist: Optional[Dict] = None) -> Optional[Dict]:
    """指定銘柄の最新サマリーを返す（チャートAPI用）"""
    wl   = watchlist if watchlist is not None else GLOBAL_WATCHLIST
    info = wl.get(ticker)
    if not info:
        # 両方のウォッチリストを確認
        for wl2 in [GLOBAL_WATCHLIST, GLOBAL_WATCHLIST_7MKT]:
            if ticker in wl2:
                info = wl2[ticker]
                wl   = wl2
                break
    if not info:
        return None
    df = get_stock_history(ticker)
    if df is None:
        return None
    return _build_summary(ticker, df, watchlist=wl)


def get_all_summaries() -> List[Dict]:
    """全銘柄のサマリーを一括ダウンロードで高速生成する"""
    raw = _get_batch_raw()
    summaries = []

    for ticker in GLOBAL_WATCHLIST:
        df = None
        if raw is not None:
            ohlcv = _extract_ohlcv(raw, ticker)
            if ohlcv is not None:
                df = _compute_indicators(ohlcv)

        if df is None:
            df = get_stock_history(ticker)

        if df is not None:
            s = _build_summary(ticker, df)
            if s:
                summaries.append(s)
        else:
            print(f"⚠️  {ticker} データ取得スキップ")

    return summaries


def get_summaries_for_open_markets(open_markets: List[str]) -> List[Dict]:
    """現在開いている市場の銘柄だけサマリーを取得する（日本株のみなのでTSEのみ）"""
    if "TSE" not in open_markets:
        return []
    return get_all_summaries()


# ============================================================
# 7市場グローバル版: バッチキャッシュ・サマリー取得
# ============================================================

_batch_lock_7mkt  = threading.Lock()
_batch_cache_7mkt: Dict[str, Any] = {"raw": None, "time": None}


def _get_batch_raw_7mkt() -> Optional[Any]:
    """7市場全銘柄を一括ダウンロード（2分間キャッシュ付き）"""
    with _batch_lock_7mkt:
        now = datetime.now()
        if _batch_cache_7mkt["raw"] is not None and _batch_cache_7mkt["time"]:
            elapsed = (now - _batch_cache_7mkt["time"]).total_seconds()
            if elapsed < _BATCH_TTL:
                return _batch_cache_7mkt["raw"]

        tickers = list(GLOBAL_WATCHLIST_7MKT.keys())
        print(f"  📥 グローバル{len(tickers)}銘柄を一括ダウンロード中（7市場）...")
        try:
            raw = yf.download(
                tickers,
                period="270d",
                progress=False,
                auto_adjust=True,
            )
            if not raw.empty:
                raw.index = pd.to_datetime(raw.index).tz_localize(None)
                _batch_cache_7mkt["raw"]  = raw
                _batch_cache_7mkt["time"] = now
                print(f"  ✅ グローバル一括ダウンロード完了（{len(tickers)}銘柄）")
                return raw
        except Exception as e:
            print(f"⚠️  グローバル一括ダウンロードエラー: {e}")
        return None


def get_global_7mkt_summaries() -> List[Dict]:
    """7市場全銘柄のサマリーを返す（グローバル複合スコア用）"""
    raw = _get_batch_raw_7mkt()
    summaries = []

    for ticker in GLOBAL_WATCHLIST_7MKT:
        df = None
        if raw is not None:
            ohlcv = _extract_ohlcv(raw, ticker)
            if ohlcv is not None:
                df = _compute_indicators(ohlcv)

        if df is None:
            try:
                hist = yf.Ticker(ticker).history(period="270d", auto_adjust=True)
                if not hist.empty and len(hist) >= 10:
                    hist.index = hist.index.tz_localize(None)
                    df = _compute_indicators(hist)
            except Exception:
                pass

        if df is not None:
            s = _build_summary(ticker, df, watchlist=GLOBAL_WATCHLIST_7MKT)
            if s:
                summaries.append(s)
        else:
            print(f"⚠️  {ticker} グローバルデータ取得スキップ")

    return summaries


def get_global_7mkt_summaries_for_open_markets(open_markets: List[str]) -> List[Dict]:
    """開いている市場の7市場銘柄だけサマリーを取得する"""
    if not open_markets:
        return []
    summaries = get_global_7mkt_summaries()
    return [s for s in summaries if s["market"] in open_markets]
