"""
グローバル監視銘柄リストと株価データ取得
7市場・50銘柄をカバーします

【テクニカル指標】
  MA5/25/50/150/200, RSI14, Volume/ATR14, MACD(12/26/9)
  Bollinger Bands(20/2), ROC20/60 (モメンタム), 52週高低

【ファンダメンタル指標】
  PER, PBR, NC比率（清原式）, ROE（バフェット）,
  D/E比率（バフェット）, PEG比率（リンチ）, 配当利回り

【最適化】
  バッチダウンロード（全銘柄を1回のAPIコールで取得・2分キャッシュ）
"""
import threading
import yfinance as yf
import pandas as pd
from datetime import datetime, date
from typing import Dict, List, Optional, Any

from database import get_fundamental_cache, save_fundamental_cache

# ── 通貨→円換算ティッカー ──────────────────────────────
FX_TICKERS = {
    "USD": "USDJPY=X",
    "GBP": "GBPJPY=X",
    "EUR": "EURJPY=X",
    "HKD": "HKDJPY=X",
    "KRW": "KRWJPY=X",
    "AUD": "AUDJPY=X",
    "JPY": None,
}

FX_DEFAULTS = {
    "USD": 150.0,
    "GBP": 190.0,
    "EUR": 162.0,
    "HKD": 19.0,
    "KRW": 0.109,
    "AUD": 96.0,
}

# ── 監視銘柄リスト（7市場・50銘柄）─────────────────────
GLOBAL_WATCHLIST: Dict[str, Dict] = {
    # ── 東京証券取引所 (TSE) ── 10銘柄
    "7203.T": {"name": "トヨタ自動車",    "market": "TSE",   "currency": "JPY", "flag": "🇯🇵", "sector": "自動車"},
    "6758.T": {"name": "ソニーグループ",  "market": "TSE",   "currency": "JPY", "flag": "🇯🇵", "sector": "テック"},
    "9984.T": {"name": "ソフトバンクG",   "market": "TSE",   "currency": "JPY", "flag": "🇯🇵", "sector": "テック"},
    "8306.T": {"name": "三菱UFJ銀行",     "market": "TSE",   "currency": "JPY", "flag": "🇯🇵", "sector": "金融"},
    "6861.T": {"name": "キーエンス",      "market": "TSE",   "currency": "JPY", "flag": "🇯🇵", "sector": "テック"},
    "4661.T": {"name": "オリエンタルL",   "market": "TSE",   "currency": "JPY", "flag": "🇯🇵", "sector": "エンタメ"},
    "8035.T": {"name": "東京エレクトロン","market": "TSE",   "currency": "JPY", "flag": "🇯🇵", "sector": "テック"},
    "6367.T": {"name": "ダイキン工業",    "market": "TSE",   "currency": "JPY", "flag": "🇯🇵", "sector": "工業"},
    "4519.T": {"name": "中外製薬",        "market": "TSE",   "currency": "JPY", "flag": "🇯🇵", "sector": "医薬品"},
    "3382.T": {"name": "セブン＆アイ",    "market": "TSE",   "currency": "JPY", "flag": "🇯🇵", "sector": "消費財"},
    # ── NYSE/NASDAQ (米国) ── 15銘柄
    "AAPL":   {"name": "Apple",           "market": "NYSE",  "currency": "USD", "flag": "🇺🇸", "sector": "テック"},
    "MSFT":   {"name": "Microsoft",       "market": "NYSE",  "currency": "USD", "flag": "🇺🇸", "sector": "テック"},
    "NVDA":   {"name": "NVIDIA",          "market": "NYSE",  "currency": "USD", "flag": "🇺🇸", "sector": "テック"},
    "TSLA":   {"name": "Tesla",           "market": "NYSE",  "currency": "USD", "flag": "🇺🇸", "sector": "自動車"},
    "AMZN":   {"name": "Amazon",          "market": "NYSE",  "currency": "USD", "flag": "🇺🇸", "sector": "テック"},
    "JPM":    {"name": "JPモルガン",      "market": "NYSE",  "currency": "USD", "flag": "🇺🇸", "sector": "金融"},
    "GOOGL":  {"name": "Alphabet",        "market": "NYSE",  "currency": "USD", "flag": "🇺🇸", "sector": "テック"},
    "META":   {"name": "Meta",            "market": "NYSE",  "currency": "USD", "flag": "🇺🇸", "sector": "テック"},
    "V":      {"name": "Visa",            "market": "NYSE",  "currency": "USD", "flag": "🇺🇸", "sector": "金融"},
    "LLY":    {"name": "Eli Lilly",       "market": "NYSE",  "currency": "USD", "flag": "🇺🇸", "sector": "医薬品"},
    "MA":     {"name": "Mastercard",      "market": "NYSE",  "currency": "USD", "flag": "🇺🇸", "sector": "金融"},
    "COST":   {"name": "Costco",          "market": "NYSE",  "currency": "USD", "flag": "🇺🇸", "sector": "消費財"},
    "AMD":    {"name": "AMD",             "market": "NYSE",  "currency": "USD", "flag": "🇺🇸", "sector": "テック"},
    "NFLX":   {"name": "Netflix",         "market": "NYSE",  "currency": "USD", "flag": "🇺🇸", "sector": "エンタメ"},
    "PG":     {"name": "P&G",             "market": "NYSE",  "currency": "USD", "flag": "🇺🇸", "sector": "消費財"},
    # ── ロンドン証券取引所 (LSE) ── 7銘柄
    "SHEL.L": {"name": "Shell",           "market": "LSE",   "currency": "GBP", "flag": "🇬🇧", "sector": "エネルギー"},
    "AZN.L":  {"name": "AstraZeneca",     "market": "LSE",   "currency": "GBP", "flag": "🇬🇧", "sector": "医薬品"},
    "HSBA.L": {"name": "HSBC",            "market": "LSE",   "currency": "GBP", "flag": "🇬🇧", "sector": "金融"},
    "GSK.L":  {"name": "GSK",             "market": "LSE",   "currency": "GBP", "flag": "🇬🇧", "sector": "医薬品"},
    "RIO.L":  {"name": "Rio Tinto",       "market": "LSE",   "currency": "GBP", "flag": "🇬🇧", "sector": "素材"},
    "BP.L":   {"name": "BP",              "market": "LSE",   "currency": "GBP", "flag": "🇬🇧", "sector": "エネルギー"},
    "ULVR.L": {"name": "Unilever",        "market": "LSE",   "currency": "GBP", "flag": "🇬🇧", "sector": "消費財"},
    # ── フランクフルト証券取引所 (DAX) ── 7銘柄
    "SAP.DE": {"name": "SAP",             "market": "DAX",   "currency": "EUR", "flag": "🇩🇪", "sector": "テック"},
    "SIE.DE": {"name": "Siemens",         "market": "DAX",   "currency": "EUR", "flag": "🇩🇪", "sector": "工業"},
    "BMW.DE": {"name": "BMW",             "market": "DAX",   "currency": "EUR", "flag": "🇩🇪", "sector": "自動車"},
    "BAS.DE": {"name": "BASF",            "market": "DAX",   "currency": "EUR", "flag": "🇩🇪", "sector": "素材"},
    "ALV.DE": {"name": "Allianz",         "market": "DAX",   "currency": "EUR", "flag": "🇩🇪", "sector": "金融"},
    "MRK.DE": {"name": "Merck KGaA",      "market": "DAX",   "currency": "EUR", "flag": "🇩🇪", "sector": "医薬品"},
    "MBG.DE": {"name": "Mercedes-Benz",   "market": "DAX",   "currency": "EUR", "flag": "🇩🇪", "sector": "自動車"},
    # ── 香港証券取引所 (HKEX) ── 5銘柄
    "0700.HK": {"name": "テンセント",     "market": "HKEX",  "currency": "HKD", "flag": "🇭🇰", "sector": "テック"},
    "9988.HK": {"name": "アリババ",       "market": "HKEX",  "currency": "HKD", "flag": "🇭🇰", "sector": "テック"},
    "2318.HK": {"name": "中国平安保険",   "market": "HKEX",  "currency": "HKD", "flag": "🇭🇰", "sector": "金融"},
    "1299.HK": {"name": "AIAグループ",    "market": "HKEX",  "currency": "HKD", "flag": "🇭🇰", "sector": "金融"},
    "9618.HK": {"name": "JD.com",         "market": "HKEX",  "currency": "HKD", "flag": "🇭🇰", "sector": "テック"},
    # ── 韓国証券取引所 (KOSPI) ── 3銘柄
    "005930.KS": {"name": "サムスン電子", "market": "KOSPI", "currency": "KRW", "flag": "🇰🇷", "sector": "テック"},
    "000660.KS": {"name": "SKハイニックス","market": "KOSPI","currency": "KRW", "flag": "🇰🇷", "sector": "テック"},
    "035420.KS": {"name": "NAVER",        "market": "KOSPI", "currency": "KRW", "flag": "🇰🇷", "sector": "テック"},
    # ── オーストラリア証券取引所 (ASX) ── 3銘柄
    "BHP.AX": {"name": "BHPグループ",     "market": "ASX",   "currency": "AUD", "flag": "🇦🇺", "sector": "素材"},
    "CBA.AX": {"name": "コモンウェルスBK","market": "ASX",   "currency": "AUD", "flag": "🇦🇺", "sector": "金融"},
    "CSL.AX": {"name": "CSL",             "market": "ASX",   "currency": "AUD", "flag": "🇦🇺", "sector": "医薬品"},
}

# ── バッチダウンロードキャッシュ（2分間有効）─────────────
_batch_lock  = threading.Lock()
_batch_cache: Dict[str, Any] = {"raw": None, "time": None}
_BATCH_TTL   = 120  # 秒


# ============================================================
# 為替レート取得
# ============================================================

def get_fx_rates() -> Dict[str, float]:
    """主要通貨→円の為替レートを取得"""
    rates = {"JPY": 1.0}
    fx_syms = [v for v in FX_TICKERS.values() if v]
    try:
        data = yf.download(fx_syms, period="2d", progress=False, auto_adjust=True)
        if not data.empty:
            close = data["Close"] if isinstance(data.columns, pd.MultiIndex) else data
            for currency, sym in FX_TICKERS.items():
                if not sym:
                    continue
                try:
                    col = sym if (isinstance(close.columns, pd.Index) and sym in close.columns) else None
                    if col:
                        series = close[col].dropna()
                    elif not isinstance(close.columns, pd.MultiIndex):
                        series = close.dropna()
                    else:
                        continue
                    if not series.empty:
                        rates[currency] = float(series.iloc[-1])
                except Exception:
                    pass
    except Exception as e:
        print(f"⚠️  為替レート取得エラー: {e}")
    for c, v in FX_DEFAULTS.items():
        rates.setdefault(c, v)
    return rates


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
    # バンド幅を中央値で正規化（スクイーズ検出に使用）
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

    return df.round(4)


def _get_batch_raw() -> Optional[Any]:
    """
    全銘柄 + FXを一括ダウンロード（2分間キャッシュ付き）。
    キャッシュが有効な場合は再利用することでAPI呼び出しを最小化する。
    """
    with _batch_lock:
        now = datetime.now()
        if _batch_cache["raw"] is not None and _batch_cache["time"]:
            elapsed = (now - _batch_cache["time"]).total_seconds()
            if elapsed < _BATCH_TTL:
                return _batch_cache["raw"]

        tickers  = list(GLOBAL_WATCHLIST.keys())
        fx_syms  = [v for v in FX_TICKERS.values() if v]
        all_syms = tickers + fx_syms

        print(f"  📥 全{len(tickers)}銘柄+FXを一括ダウンロード中...")
        try:
            raw = yf.download(
                all_syms,
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
    """
    個別取得（主にチャートAPI用）。
    バッチキャッシュがあればそちらを優先して再利用する。
    """
    with _batch_lock:
        cached_raw = _batch_cache.get("raw")
    if cached_raw is not None:
        df = _extract_ohlcv(cached_raw, ticker)
        if df is not None:
            return _compute_indicators(df)

    # キャッシュなし → 個別ダウンロード
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
# ファンダメンタル指標取得
# ============================================================

def get_fundamental_info(ticker: str) -> Optional[Dict]:
    """
    yfinance .info + balance_sheet からファンダメンタル指標を取得。
    1日1回だけAPIを叩き、あとはDBキャッシュを使う。

    取得指標:
      PER, PBR, NC比率（清原式）
      ROE（バフェット）, D/E比率（バフェット）
      PEG比率（リンチ）, 利益成長率
      配当利回り, 営業利益率
    """
    cached = get_fundamental_cache(ticker)
    if cached:
        return cached

    info_meta = GLOBAL_WATCHLIST.get(ticker)
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
                total_debt = _get_bs(["Total Debt", "TotalDebt", "Long Term Debt"])
                current_assets = _get_bs(["Current Assets", "Total Current Assets"])
        except Exception:
            pass

        net_cash = net_cash_ratio = None
        if cash_and_equiv is not None and market_cap and market_cap > 0:
            debt = total_debt or 0
            net_cash = cash_and_equiv - debt
            net_cash_ratio = round(net_cash / market_cap, 4)

        # ── バフェット指標 ──
        roe_raw = info.get("returnOnEquity")
        de_raw  = info.get("debtToEquity")
        roe     = round(float(roe_raw) * 100, 2) if roe_raw else None
        debt_to_equity = round(float(de_raw), 2) if de_raw else None

        # ── リンチ指標（PEG比率）──
        peg_raw        = info.get("trailingPegRatio") or info.get("pegRatio")
        eg_raw         = info.get("earningsGrowth") or info.get("earningsQuarterlyGrowth")
        peg_ratio      = round(float(peg_raw), 2) if peg_raw and float(peg_raw) > 0 else None
        earnings_growth = round(float(eg_raw) * 100, 2) if eg_raw else None

        # ── 収益性 ──
        om_raw         = info.get("operatingMargins")
        operating_margin = round(float(om_raw) * 100, 2) if om_raw else None
        dividend_yield = info.get("dividendYield")
        sector         = info.get("sector", "")

        data = {
            "ticker":         ticker,
            "name":           info_meta["name"],
            "market_cap":     market_cap,
            "market_cap_usd": market_cap,
            "per":            round(float(per), 2) if per and float(per) > 0 else None,
            "pbr":            round(float(pbr), 2) if pbr and float(pbr) > 0 else None,
            "current_assets": current_assets,
            "total_debt":     total_debt,
            "cash_and_equiv": cash_and_equiv,
            "net_cash":       net_cash,
            "net_cash_ratio": net_cash_ratio,
            "dividend_yield": round(float(dividend_yield) * 100, 2) if dividend_yield else None,
            "sector":         sector,
            "currency":       info_meta["currency"],
            # 新規指標
            "roe":             roe,
            "debt_to_equity":  debt_to_equity,
            "peg_ratio":       peg_ratio,
            "earnings_growth": earnings_growth,
            "operating_margin": operating_margin,
            "last_updated":   date.today().isoformat(),
        }
        save_fundamental_cache(data)
        return data

    except Exception as e:
        print(f"⚠️  {ticker} ファンダメンタル取得エラー: {e}")
        return None


# ============================================================
# サマリー生成
# ============================================================

def _build_summary(ticker: str, df: pd.DataFrame) -> Optional[Dict]:
    """
    指標計算済みDataFrameから銘柄サマリーを生成する内部関数。
    get_stock_summary と get_all_summaries の共通ロジック。
    """
    info = GLOBAL_WATCHLIST.get(ticker)
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
    w52h = _f("52wHigh"); w52l = _f("52wLow")
    atr14 = _f("ATR14")
    roc20 = _f("ROC20"); roc60 = _f("ROC60")
    bb_width = _f("BB_Width")
    bb_upper = _f("BB_Upper"); bb_lower = _f("BB_Lower")

    # ── MACD クロス状態 ──
    macd_val = _f("MACD"); signal_val = _f("MACD_Signal")
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
            macd_cross = "golden"    # ゴールデンクロス（直近）
        elif macd_hist_curr > 0:
            macd_cross = "positive"  # MACD優勢継続
        elif macd_hist_prev >= 0 and macd_hist_curr < 0:
            macd_cross = "dead"      # デッドクロス（直近）
        else:
            macd_cross = "negative"

    # ── Minerviniスコア（0〜4） ──
    minervini_score = 0
    if all(x is not None for x in [ma50, ma150, ma200]):
        if ma200 > 0:        minervini_score += 1
        if ma50 > ma150:     minervini_score += 1
        if ma150 > ma200:    minervini_score += 1
        if price > ma50:     minervini_score += 1

    # ── CAN-SLIM 出来高急増 ──
    vol_surge = False
    if vol_avg20 and vol_avg20 > 0 and "Volume" in latest.index:
        vol_surge = float(latest["Volume"]) > vol_avg20 * 1.5

    # ── リバモア 52週高値接近 ──
    near_52w_high = (w52h is not None and w52h > 0 and price >= w52h * 0.90)

    # ── ファンダメンタル ──
    fund = get_fundamental_info(ticker)

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
        "minervini_score": minervini_score,
        # RSI
        "rsi14": rsi14,
        # 出来高
        "volume":     int(latest["Volume"]) if "Volume" in latest.index else 0,
        "vol_avg20":  int(vol_avg20) if vol_avg20 else None,
        "vol_surge":  vol_surge,
        # 52週
        "week52_high":  w52h,
        "week52_low":   w52l,
        "near_52w_high": near_52w_high,
        # MACD
        "macd":        round(macd_val, 4) if macd_val else None,
        "macd_signal": round(signal_val, 4) if signal_val else None,
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
        # ファンダメンタル
        "per":             fund.get("per")             if fund else None,
        "pbr":             fund.get("pbr")             if fund else None,
        "net_cash_ratio":  fund.get("net_cash_ratio")  if fund else None,
        "market_cap":      fund.get("market_cap")      if fund else None,
        "dividend_yield":  fund.get("dividend_yield")  if fund else None,
        "roe":             fund.get("roe")              if fund else None,
        "debt_to_equity":  fund.get("debt_to_equity")  if fund else None,
        "peg_ratio":       fund.get("peg_ratio")       if fund else None,
        "earnings_growth": fund.get("earnings_growth") if fund else None,
        "operating_margin":fund.get("operating_margin")if fund else None,
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


def get_stock_summary(ticker: str) -> Optional[Dict]:
    """指定銘柄の最新サマリーを返す（チャートAPI用）"""
    info = GLOBAL_WATCHLIST.get(ticker)
    if not info:
        return None
    df = get_stock_history(ticker)
    if df is None:
        return None
    return _build_summary(ticker, df)


def get_all_summaries() -> List[Dict]:
    """
    全銘柄のサマリーを一括ダウンロードで高速生成する。
    バッチDLが失敗した場合は個別取得にフォールバック。
    """
    raw = _get_batch_raw()
    summaries = []

    for ticker in GLOBAL_WATCHLIST:
        df = None
        if raw is not None:
            ohlcv = _extract_ohlcv(raw, ticker)
            if ohlcv is not None:
                df = _compute_indicators(ohlcv)

        if df is None:
            # フォールバック: 個別取得
            df = get_stock_history(ticker)

        if df is not None:
            s = _build_summary(ticker, df)
            if s:
                summaries.append(s)
        else:
            print(f"⚠️  {ticker} データ取得スキップ")

    return summaries


def get_summaries_for_open_markets(open_markets: List[str]) -> List[Dict]:
    """現在開いている市場の銘柄だけサマリーを取得する"""
    raw = _get_batch_raw()
    summaries = []

    for ticker, meta in GLOBAL_WATCHLIST.items():
        if meta["market"] not in open_markets:
            continue
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
