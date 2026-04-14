"""
グローバル自動売買エンジン（スコアリング方式・API不要）
現在開いている市場の銘柄のみを対象に売買判断します

【スコアリングルール】
■ 買いスコア
  - MA5 > MA25（上昇トレンド）: 最大+30点
  - RSI < 30（売られすぎ）: +35点
  - RSI 30〜45（低め）: +20点
  - 当日+1.5%超の上昇: +15点
  - 現金余裕あり（35%超）: +10点

■ 売りスコア
  - MA5 < MA25（下降トレンド）: 最大+30点
  - RSI > 70（買われすぎ）: +35点
  - RSI 60〜70: +15点
  - 当日-1.5%超の下落: +15点
  - 含み損-10%以下: 強制損切り

■ 判断しきい値
  - 買いスコア >= 55 かつ 買い > 売り → buy
  - 売りスコア >= 45 かつ 売り > 買い かつ 保有中 → sell
"""
from datetime import datetime
from typing import List, Dict, Optional

from database import (
    get_account, get_portfolio, get_holding,
    upsert_holding, delete_holding,
    save_trade, update_cash, save_asset_snapshot,
)
from market_hours import get_open_markets
from global_stocks import (
    get_summaries_for_open_markets, get_all_summaries,
    get_fx_rates, GLOBAL_WATCHLIST,
)

# リスク管理パラメータ
MAX_SINGLE_TRADE_RATIO = 0.05   # 1回の取引は総資産の5%まで
MAX_SINGLE_STOCK_RATIO = 0.20   # 1銘柄は総資産の20%まで
MIN_CASH_RATIO         = 0.30   # 現金は総資産の30%以上
COMMISSION_RATE        = 0.001  # 手数料 0.1%
MIN_COMMISSION_JPY     = 100    # 最低手数料 100円


def calc_commission(amount_jpy: float) -> float:
    return max(amount_jpy * COMMISSION_RATE, MIN_COMMISSION_JPY)


def calc_total_assets(cash: float, portfolio: List[Dict], fx_rates: Dict) -> float:
    """現金 + 保有株の円換算評価額で総資産を計算"""
    stock_value = 0.0
    for h in portfolio:
        rate = fx_rates.get(h["currency"], 1.0)
        # avg_cost_local は現地通貨建て取得単価
        # 現在価格がないため取得単価で代用（評価は/api/portfolioで実施）
        stock_value += h["shares"] * h["avg_cost_local"] * rate
    return cash + stock_value


def _score_stock(summary: Dict, holding: Optional[Dict],
                 cash_ratio: float) -> tuple:
    """テクニカル指標でスコアを計算して売買判断を返す"""
    ma5        = summary.get("ma5")
    ma25       = summary.get("ma25")
    rsi        = summary.get("rsi14")
    change_pct = summary.get("change_pct") or 0.0
    price      = summary["current_price"]

    if ma5 is None or ma25 is None or rsi is None:
        return "hold", 0, 0, "指標データ不足のため様子見"

    buy_score, sell_score = 0, 0
    buy_reasons, sell_reasons = [], []

    # トレンド判断
    ma_diff_pct  = abs(ma5 - ma25) / ma25 * 100 if ma25 else 0
    trend_points = min(30, 10 + ma_diff_pct * 4)
    if ma5 > ma25:
        buy_score  += trend_points
        buy_reasons.append(f"上昇トレンド({ma_diff_pct:.1f}%)")
    else:
        sell_score  += trend_points
        sell_reasons.append(f"下降トレンド({ma_diff_pct:.1f}%)")

    # RSI判断
    if rsi < 30:
        buy_score  += 35;  buy_reasons.append(f"RSI売られすぎ({rsi:.0f})")
    elif rsi < 45:
        buy_score  += 20;  buy_reasons.append(f"RSI低め({rsi:.0f})")
    elif rsi > 70:
        sell_score += 35;  sell_reasons.append(f"RSI買われすぎ({rsi:.0f})")
    elif rsi > 60:
        sell_score += 15;  sell_reasons.append(f"RSIやや高め({rsi:.0f})")

    # 当日の値動き
    if change_pct > 1.5:
        buy_score  += 15;  buy_reasons.append(f"本日+{change_pct:.1f}%上昇")
    elif change_pct < -1.5:
        sell_score += 15;  sell_reasons.append(f"本日{change_pct:.1f}%下落")

    # 現金余裕ボーナス
    if cash_ratio >= 0.35:
        buy_score += 10

    # 判断
    if buy_score >= 55 and buy_score > sell_score:
        reason = "、".join(buy_reasons[:2]) or "総合判断で買い"
        return "buy", buy_score, sell_score, reason
    elif sell_score >= 45 and sell_score > buy_score and holding:
        reason = "、".join(sell_reasons[:2]) or "総合判断で売り"
        return "sell", buy_score, sell_score, reason
    else:
        dominant = buy_reasons[0] if buy_score > sell_score and buy_reasons else \
                   (sell_reasons[0] if sell_reasons else "")
        reason = dominant + "だがサイン弱め" if dominant else "様子見"
        return "hold", buy_score, sell_score, reason


def run_global_trading(force: bool = False) -> Dict:
    """
    グローバル自動売買のメイン関数

    force=True のとき: 市場開閉に関係なく全銘柄を対象にする（手動実行用）
    force=False のとき: 現在開いている市場のみ対象
    """
    print(f"\n{'='*55}")
    print(f"🌍 グローバル取引開始: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    # 開いている市場を確認
    open_markets = get_open_markets()
    if not open_markets and not force:
        msg = "現在開いている市場がありません（早朝5:00〜9:00は全市場クローズ）"
        print(f"⏸ {msg}")
        return {"status": "skipped", "message": msg, "open_markets": []}

    if force:
        print("🔴 強制実行モード: 全銘柄を対象にします")
    else:
        names = [f"{GLOBAL_WATCHLIST.get(t, {}).get('flag', '')} {m}" for m, t in
                 [(m, next((k for k, v in GLOBAL_WATCHLIST.items() if v['market'] == m), ''))
                  for m in open_markets]]
        print(f"📡 開いている市場: {', '.join(open_markets)}")

    # 為替レート取得
    print("💱 為替レートを取得中...")
    fx_rates = get_fx_rates()
    print(f"  USD/JPY={fx_rates.get('USD',0):.1f}  GBP/JPY={fx_rates.get('GBP',0):.1f}  "
          f"EUR/JPY={fx_rates.get('EUR',0):.1f}  HKD/JPY={fx_rates.get('HKD',0):.1f}")

    # 市場データ取得
    print("📊 株価データを取得中...")
    if force:
        summaries = get_all_summaries()
    else:
        summaries = get_summaries_for_open_markets(open_markets)

    if not summaries:
        return {"status": "error", "message": "株価データの取得に失敗しました"}

    # 資産状況
    account      = get_account()
    portfolio    = get_portfolio()
    total_assets = calc_total_assets(account["cash"], portfolio, fx_rates)
    cash_ratio   = account["cash"] / total_assets if total_assets > 0 else 1.0

    print(f"💰 総資産: {total_assets:,.0f}円 / 現金: {account['cash']:,.0f}円 ({cash_ratio*100:.1f}%)")

    executed, skipped = [], []

    for summary in summaries:
        ticker   = summary["ticker"]
        name     = summary["name"]
        currency = summary["currency"]
        flag     = summary["flag"]
        market   = summary["market"]
        price_l  = summary["current_price"]   # 現地通貨
        fx_rate  = fx_rates.get(currency, 1.0)
        price_j  = price_l * fx_rate           # 円換算

        holding = get_holding(ticker)
        current_cash = get_account()["cash"]

        # 損切りチェック
        if holding:
            pnl_pct = (price_l / holding["avg_cost_local"] - 1) * 100
            if pnl_pct <= -10:
                # 全株損切り
                sell_shares = holding["shares"]
                total_jpy   = price_j * sell_shares
                commission  = calc_commission(total_jpy)
                proceeds    = total_jpy - commission
                delete_holding(ticker)
                update_cash(current_cash + proceeds)
                reason = f"損切り（含み損{pnl_pct:.1f}%）"
                save_trade(ticker, name, market, currency, flag, "sell",
                           sell_shares, price_l, price_j, total_jpy, commission, fx_rate, reason)
                print(f"🔴 損切り: {flag}{name} {sell_shares}株 @{price_l:.2f}{currency} ({price_j:,.0f}円)")
                executed.append(f"sell:{ticker}")
                continue

        # スコアリング判断
        action, bs, ss, reason = _score_stock(summary, holding, cash_ratio)
        print(f"  {flag}{name}({ticker}): {action.upper()} [買{bs}/売{ss}] — {reason}")

        if action == "buy":
            max_jpy    = total_assets * MAX_SINGLE_TRADE_RATIO
            buy_shares = max(1, int(max_jpy / price_j)) if price_j > 0 else 0
            if buy_shares == 0:
                skipped.append(f"buy:{ticker}")
                continue

            trade_jpy  = price_j * buy_shares
            commission = calc_commission(trade_jpy)
            total_cost = trade_jpy + commission

            # 現金チェック
            if current_cash - total_cost < total_assets * MIN_CASH_RATIO:
                available = current_cash - total_assets * MIN_CASH_RATIO - MIN_COMMISSION_JPY
                if available < price_j:
                    print(f"⚠️  {ticker} 現金不足のためスキップ")
                    skipped.append(f"buy:{ticker}")
                    continue
                buy_shares = max(1, int(available / (price_j * (1 + COMMISSION_RATE))))
                trade_jpy  = price_j * buy_shares
                commission = calc_commission(trade_jpy)
                total_cost = trade_jpy + commission

            # 1銘柄上限チェック
            cur_val = (holding["shares"] * price_j) if holding else 0
            if cur_val + trade_jpy > total_assets * MAX_SINGLE_STOCK_RATIO:
                print(f"⚠️  {ticker} 1銘柄上限のためスキップ")
                skipped.append(f"buy:{ticker}")
                continue

            # 買い実行
            new_shares = buy_shares + (holding["shares"] if holding else 0)
            old_cost   = holding["avg_cost_local"] * holding["shares"] if holding else 0
            new_avg_l  = (old_cost + price_l * buy_shares) / new_shares
            new_avg_j  = new_avg_l * fx_rate

            upsert_holding(ticker, name, market, currency, flag,
                           new_shares, new_avg_l, new_avg_j)
            update_cash(current_cash - total_cost)
            save_trade(ticker, name, market, currency, flag, "buy",
                       buy_shares, price_l, price_j, trade_jpy, commission, fx_rate, reason)
            print(f"✅ 買い: {flag}{name} {buy_shares}株 @{price_l:.2f}{currency} ≈ {trade_jpy:,.0f}円")
            executed.append(f"buy:{ticker}")

        elif action == "sell" and holding:
            sell_shares = max(1, holding["shares"] // 2)
            trade_jpy   = price_j * sell_shares
            commission  = calc_commission(trade_jpy)
            proceeds    = trade_jpy - commission

            new_shares = holding["shares"] - sell_shares
            if new_shares <= 0:
                delete_holding(ticker)
            else:
                upsert_holding(ticker, name, market, currency, flag,
                               new_shares, holding["avg_cost_local"], holding["avg_cost_jpy"])

            update_cash(current_cash + proceeds)
            save_trade(ticker, name, market, currency, flag, "sell",
                       sell_shares, price_l, price_j, trade_jpy, commission, fx_rate, reason)
            print(f"✅ 売り: {flag}{name} {sell_shares}株 @{price_l:.2f}{currency} ≈ {trade_jpy:,.0f}円")
            executed.append(f"sell:{ticker}")

    # 資産スナップショット保存
    account_after  = get_account()
    portfolio_after = get_portfolio()
    total_after    = calc_total_assets(account_after["cash"], portfolio_after, fx_rates)
    save_asset_snapshot(total_after, account_after["cash"], total_after - account_after["cash"])

    print(f"🏁 完了: {len(executed)}件実行 / {len(skipped)}件スキップ")
    print(f"📈 総資産: {total_assets:,.0f}円 → {total_after:,.0f}円")

    return {
        "status":           "success",
        "executed_at":      datetime.now().isoformat(),
        "open_markets":     open_markets,
        "total_assets_before": round(total_assets, 0),
        "total_assets_after":  round(total_after, 0),
        "executed_trades":  executed,
        "skipped_trades":   skipped,
    }
