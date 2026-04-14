"""
バックテスト - 過去の実際の株価データで投資戦略を検証します

【仕様】
  - テクニカル指標（Minervini SEPA + RSIフィルター + トレーリングストップ）を使用
  - ファンダメンタル指標（PER/PBR/NC比率）は過去データが取得できないため除外
  - 初期資金200万円で指定日数分シミュレーション
  - 結果: 最終資産・損益率・最大ドローダウン・取引回数
"""
import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple

from global_stocks import GLOBAL_WATCHLIST, FX_TICKERS

INITIAL_CAPITAL = 2_000_000
COMMISSION_RATE  = 0.001
MIN_COMMISSION   = 100
MAX_HOLDINGS     = 8
MIN_CASH_RATIO   = 0.25
MIN_POS_RATIO    = 0.05
MAX_POS_RATIO    = 0.15
RSI_MAX          = 65
TRAIL_INITIAL    = 0.92
TRAIL_TIGHT      = 0.94


def run_backtest(days: int = 365) -> Dict:
    """
    過去 days 日分のデータで売買戦略をシミュレーションします。

    テクニカル条件:
      - Minervini SEPA: MA50 > MA150 > MA200 かつ 現在値 > MA50
      - RSI65以下（買われすぎ除外）
      - トレーリングストップ: 最高値の-8%（含み益+15%超は-6%に引き締め）
      - 利確: +30% で全売り、+15% で半分売り
    """
    print(f"\n🧪 バックテスト開始 ({days}日間)")

    end_date   = datetime.now()
    # MA200計算のため余分に250日取得
    fetch_start = end_date - timedelta(days=days + 260)

    tickers    = list(GLOBAL_WATCHLIST.keys())
    fx_list    = [v for v in FX_TICKERS.values() if v]
    all_syms   = tickers + fx_list

    print(f"  📥 {len(tickers)}銘柄のデータを取得中...")
    try:
        raw = yf.download(
            all_syms,
            start=fetch_start.strftime("%Y-%m-%d"),
            end=end_date.strftime("%Y-%m-%d"),
            progress=False,
            auto_adjust=True,
        )
    except Exception as e:
        return {"error": f"データ取得失敗: {e}"}

    if raw.empty:
        return {"error": "株価データが空です"}

    # MultiIndex → Close / Volume
    if isinstance(raw.columns, pd.MultiIndex):
        close  = raw["Close"]
        volume = raw.get("Volume", pd.DataFrame())
    else:
        close  = raw[["Close"]] if "Close" in raw.columns else raw
        volume = pd.DataFrame()

    close.index = pd.to_datetime(close.index).tz_localize(None)

    # シミュレーション開始日
    sim_start   = pd.Timestamp(end_date - timedelta(days=days))
    trade_dates = close.index[close.index >= sim_start]

    if len(trade_dates) == 0:
        return {"error": "取引日がありません"}

    # ── ヘルパー: 指定日時点の為替レート ──
    def get_fx(date: pd.Timestamp) -> Dict[str, float]:
        rates = {"JPY": 1.0}
        for currency, fx_ticker in FX_TICKERS.items():
            if not fx_ticker:
                continue
            try:
                if fx_ticker in close.columns:
                    series = close[fx_ticker].loc[:date].dropna()
                    if not series.empty:
                        rates[currency] = float(series.iloc[-1])
            except Exception:
                pass
        for c, v in {"USD": 150.0, "GBP": 190.0, "EUR": 162.0, "HKD": 19.0}.items():
            rates.setdefault(c, v)
        return rates

    # ── ヘルパー: テクニカル指標計算 ──
    def get_tech(ticker: str, date: pd.Timestamp) -> Optional[Dict]:
        try:
            if ticker not in close.columns:
                return None
            series = close[ticker].loc[:date].dropna()
            if len(series) < 50:
                return None
            price = float(series.iloc[-1])
            if price <= 0:
                return None

            ma50  = float(series.tail(50).mean())
            ma150 = float(series.tail(150).mean()) if len(series) >= 150 else None
            ma200 = float(series.tail(200).mean()) if len(series) >= 200 else None

            # RSI14
            delta = series.diff()
            gain  = delta.clip(lower=0).rolling(14).mean()
            loss  = (-delta.clip(upper=0)).rolling(14).mean()
            loss_val = float(loss.iloc[-1])
            rsi = 50.0
            if loss_val > 0:
                rs  = float(gain.iloc[-1]) / loss_val
                rsi = 100 - 100 / (1 + rs)
            elif float(gain.iloc[-1]) > 0:
                rsi = 100.0

            # 52週高値
            w52h = float(series.tail(252).max()) if len(series) >= 252 else float(series.max())
            near_52w_high = price >= w52h * 0.90

            # Minervini条件（MA200が存在する場合のみ全条件、なければ緩和）
            if ma200 is not None and ma150 is not None:
                minervini_ok = (ma50 > ma150 > ma200 and price > ma50)
            elif ma150 is not None:
                minervini_ok = (ma50 > ma150 and price > ma50)
            else:
                minervini_ok = price > ma50

            # テクニカルスコア（高いほど優先購入）
            score = 0.0
            if ma200 is not None and ma150 is not None and ma50 > ma150 > ma200:
                score += 25
            if price > ma50:
                score += 10
            if near_52w_high:
                score += 8
            if rsi < 30:
                score += 5
            elif rsi > 70:
                score -= 10

            return {
                "price": price,
                "ma50": ma50, "ma150": ma150, "ma200": ma200,
                "rsi": rsi,
                "minervini_ok": minervini_ok,
                "near_52w_high": near_52w_high,
                "score": score,
            }
        except Exception:
            return None

    # ── バックテスト状態 ──
    cash: float = float(INITIAL_CAPITAL)
    portfolio: Dict[str, Dict] = {}
    daily: List[Dict] = []
    trades: List[Dict] = []
    # ticker → (action, date) の直近売却記録
    sold_log: Dict[str, Tuple[str, pd.Timestamp]] = {}

    def calc_total(date: pd.Timestamp) -> float:
        fx   = get_fx(date)
        stk  = 0.0
        for t, h in portfolio.items():
            info = GLOBAL_WATCHLIST.get(t, {})
            rate = fx.get(info.get("currency", "JPY"), 1.0)
            try:
                if t in close.columns:
                    val = close[t].loc[:date].dropna()
                    if not val.empty:
                        stk += h["shares"] * float(val.iloc[-1]) * rate
            except Exception:
                pass
        return cash + stk

    # ── メインループ ──
    for date in trade_dates:
        fx = get_fx(date)

        # ▼ 売りチェック
        to_delete = []
        for ticker, h in list(portfolio.items()):
            tech = get_tech(ticker, date)
            if not tech:
                continue
            price_l  = tech["price"]
            info     = GLOBAL_WATCHLIST.get(ticker, {})
            rate     = fx.get(info.get("currency", "JPY"), 1.0)
            price_j  = price_l * rate
            avg_cost = h["avg_cost_local"]
            pnl_pct  = (price_l / avg_cost - 1) * 100 if avg_cost > 0 else 0

            # ピーク価格更新 & ストップ再計算
            new_peak   = max(h.get("peak_price", price_l), price_l)
            trail_pct  = TRAIL_TIGHT if pnl_pct >= 15 else TRAIL_INITIAL
            stop_price = new_peak * trail_pct
            h["peak_price"]     = new_peak
            h["trailing_stop"]  = stop_price

            action = None
            if price_l <= stop_price:
                action = "損切り" if pnl_pct < 0 else "sell"
            elif pnl_pct >= 30:
                action = "利確"
            elif pnl_pct >= 15 and not h.get("partial_taken"):
                action = "部分利確"

            if action in ("損切り", "sell", "利確"):
                sell_shares = h["shares"]
                total_jpy   = price_j * sell_shares
                commission  = max(total_jpy * COMMISSION_RATE, MIN_COMMISSION)
                cash       += total_jpy - commission
                sold_log[ticker] = (action, date)
                trades.append({
                    "date":      str(date.date()),
                    "ticker":    ticker,
                    "flag":      info.get("flag", ""),
                    "name":      info.get("name", ticker),
                    "action":    action,
                    "shares":    sell_shares,
                    "price_jpy": round(price_j, 2),
                    "total_jpy": round(total_jpy, 0),
                    "pnl_pct":   round(pnl_pct, 2),
                })
                to_delete.append(ticker)

            elif action == "部分利確":
                sell_shares = max(1, int(h["shares"] / 2))
                total_jpy   = price_j * sell_shares
                commission  = max(total_jpy * COMMISSION_RATE, MIN_COMMISSION)
                cash       += total_jpy - commission
                h["shares"] -= sell_shares
                h["partial_taken"] = True
                sold_log[ticker] = (action, date)
                trades.append({
                    "date":      str(date.date()),
                    "ticker":    ticker,
                    "flag":      info.get("flag", ""),
                    "name":      info.get("name", ticker),
                    "action":    action,
                    "shares":    sell_shares,
                    "price_jpy": round(price_j, 2),
                    "total_jpy": round(total_jpy, 0),
                    "pnl_pct":   round(pnl_pct, 2),
                })

        for t in to_delete:
            del portfolio[t]

        # ▼ 買いチェック
        total = calc_total(date)

        # 候補をスコア順にソート
        candidates = []
        for ticker in GLOBAL_WATCHLIST:
            if ticker in portfolio:
                continue
            if len(portfolio) >= MAX_HOLDINGS:
                break

            # 再購入禁止チェック（損切り3日, 利確1日）
            if ticker in sold_log:
                last_action, last_date = sold_log[ticker]
                wait = 3 if last_action == "損切り" else 1
                if (date - last_date).days < wait:
                    continue

            tech = get_tech(ticker, date)
            if not tech or not tech["minervini_ok"]:
                continue
            if tech["rsi"] > RSI_MAX:
                continue

            candidates.append((tech["score"], ticker, tech))

        candidates.sort(reverse=True)

        for score, ticker, tech in candidates:
            if len(portfolio) >= MAX_HOLDINGS:
                break

            info     = GLOBAL_WATCHLIST.get(ticker, {})
            currency = info.get("currency", "JPY")
            rate     = fx.get(currency, 1.0)
            price_l  = tech["price"]
            price_j  = price_l * rate

            if price_j <= 0:
                continue

            pos_ratio   = MIN_POS_RATIO + (MAX_POS_RATIO - MIN_POS_RATIO) * min(1.0, score / 50.0)
            max_jpy     = total * pos_ratio
            shares      = max(1, int(max_jpy / price_j))
            trade_jpy   = price_j * shares
            commission  = max(trade_jpy * COMMISSION_RATE, MIN_COMMISSION)
            total_cost  = trade_jpy + commission

            available = cash - total * MIN_CASH_RATIO
            if available < price_j:
                continue
            if total_cost > available:
                shares = max(1, int(available / (price_j * (1 + COMMISSION_RATE))))
                if shares <= 0:
                    continue
                trade_jpy  = price_j * shares
                commission = max(trade_jpy * COMMISSION_RATE, MIN_COMMISSION)
                total_cost = trade_jpy + commission

            portfolio[ticker] = {
                "shares":        shares,
                "avg_cost_local": price_l,
                "avg_cost_jpy":   price_j,
                "peak_price":     price_l,
                "trailing_stop":  price_l * TRAIL_INITIAL,
                "partial_taken":  False,
            }
            cash -= total_cost
            trades.append({
                "date":      str(date.date()),
                "ticker":    ticker,
                "flag":      info.get("flag", ""),
                "name":      info.get("name", ticker),
                "action":    "buy",
                "shares":    shares,
                "price_jpy": round(price_j, 2),
                "total_jpy": round(trade_jpy, 0),
                "pnl_pct":   0,
            })

        # ▼ 日次スナップショット
        total_assets = calc_total(date)
        daily.append({
            "date":         str(date.date()),
            "total_assets": round(total_assets, 0),
            "cash":         round(cash, 0),
            "stock_value":  round(total_assets - cash, 0),
            "holdings":     len(portfolio),
        })

    # ── 最終集計 ──
    final   = daily[-1]["total_assets"] if daily else INITIAL_CAPITAL
    pnl     = final - INITIAL_CAPITAL
    pnl_pct = pnl / INITIAL_CAPITAL * 100

    # 最大ドローダウン計算
    peak   = float(INITIAL_CAPITAL)
    max_dd = 0.0
    for d in daily:
        if d["total_assets"] > peak:
            peak = d["total_assets"]
        dd = (d["total_assets"] - peak) / peak * 100
        if dd < max_dd:
            max_dd = dd

    # 勝率計算（売り取引のみ）
    sell_trades = [t for t in trades if t["action"] != "buy"]
    win  = len([t for t in sell_trades if t["pnl_pct"] > 0])
    lose = len([t for t in sell_trades if t["pnl_pct"] < 0])

    print(f"✅ バックテスト完了: {final:,.0f}円 ({pnl_pct:+.2f}%) / "
          f"取引{len(trades)}件 / 最大DD{max_dd:.1f}%")

    return {
        "status":           "ok",
        "days":             days,
        "initial_capital":  INITIAL_CAPITAL,
        "final_total":      round(final, 0),
        "total_pnl":        round(pnl, 0),
        "total_pnl_pct":    round(pnl_pct, 2),
        "max_drawdown_pct": round(max_dd, 2),
        "trade_count":      len(trades),
        "win_count":        win,
        "lose_count":       lose,
        "win_rate":         round(win / (win + lose) * 100, 1) if (win + lose) > 0 else 0,
        "daily":            daily,
        "recent_trades":    trades[-30:],
    }
