"""
グローバル自動売買エンジン（複合スコアリング版）

【組み込んだ億トレーダーの手法】
  清原達郎式   : スクリーニング通過銘柄のみ買い。NC比率に応じてポジションサイズを動的配分
  Minervini   : トレンドテンプレート通過銘柄を優先。RSI65超は買わない
  テスタ/BNF式: 最高値から-8%でトレーリングストップ発動（損失を最小限に）
  清原式 利確  : +15%で半分売り → +30%で全売り の段階的利確

【リスク管理パラメータ】
  TRAIL_INITIAL_PCT     = 0.92  最高値の92%以下でストップ発動
  TRAIL_TIGHT_PCT       = 0.94  含み益+15%超で最高値の94%に引き締め
  TRAIL_TIGHTEN_TRIGGER = 15.0  引き締めトリガー（+15%）
  PROFIT_PARTIAL        = 15.0  部分利確（半分売り）トリガー
  PROFIT_FULL           = 30.0  全売りトリガー（+30%達成）
"""
from datetime import datetime
from typing import List, Dict, Optional, Tuple

from database import (
    get_account, get_portfolio, get_holding,
    upsert_holding, delete_holding,
    save_trade, update_cash, save_asset_snapshot,
    update_trailing_stop, mark_partial_taken, recently_sold,
    get_screened_stocks,
)
from market_hours import get_open_markets
from global_stocks import (
    get_summaries_for_open_markets, get_all_summaries,
    get_fx_rates, GLOBAL_WATCHLIST,
)

# ─── リスク管理パラメータ ─────────────────────────────
TRAIL_INITIAL_PCT     = 0.92   # トレーリングストップ: 最高値の92%
TRAIL_TIGHT_PCT       = 0.94   # 含み益+15%超で最高値の94%に引き締め
TRAIL_TIGHTEN_TRIGGER = 15.0   # 引き締めトリガー（含み益 +15%）

PROFIT_PARTIAL        = 15.0   # +15%達成で保有の半分を利確
PROFIT_FULL           = 30.0   # +30%達成で全株利確

RSI_MAX_FOR_BUY       = 65     # RSIがこれを超える場合は買わない
MAX_HOLDINGS          = 8      # 最大保有銘柄数
MIN_POSITION_RATIO    = 0.05   # 最小投資比率（総資産の5%）
MAX_POSITION_RATIO    = 0.15   # 最大投資比率（総資産の15%）
MIN_CASH_RATIO        = 0.25   # 常に現金25%以上を確保

COMMISSION_RATE       = 0.001  # 手数料 0.1%
MIN_COMMISSION_JPY    = 100    # 最低手数料 100円


def calc_commission(amount_jpy: float) -> float:
    return max(amount_jpy * COMMISSION_RATE, MIN_COMMISSION_JPY)


def calc_total_assets(cash: float, portfolio: List[Dict], fx_rates: Dict) -> float:
    """現金 + 保有株の円換算評価額で総資産を計算"""
    stock_value = 0.0
    for h in portfolio:
        rate = fx_rates.get(h["currency"], 1.0)
        stock_value += h["shares"] * h["avg_cost_local"] * rate
    return cash + stock_value


def _calc_trailing_stop(peak_price_local: float, pnl_pct: float) -> float:
    """
    現地通貨建てのトレーリングストップ価格を返す。
    含み益が TRAIL_TIGHTEN_TRIGGER 以上なら TRAIL_TIGHT_PCT（厳しい）、
    そうでなければ TRAIL_INITIAL_PCT（通常）を使う。
    """
    pct = TRAIL_TIGHT_PCT if pnl_pct >= TRAIL_TIGHTEN_TRIGGER else TRAIL_INITIAL_PCT
    return round(peak_price_local * pct, 6)


def _calc_position_size(total_assets: float, composite_score: float) -> float:
    """
    composite_score（0〜80程度）に応じてポジションサイズを動的配分。
    スコアが高い銘柄ほど大きく投資する（清原式のNC比率配分を応用）。
    """
    # スコア0 → 5%、スコア50以上 → 15%
    ratio = MIN_POSITION_RATIO + (MAX_POSITION_RATIO - MIN_POSITION_RATIO) * min(1.0, composite_score / 50.0)
    return max(MIN_POSITION_RATIO, min(MAX_POSITION_RATIO, ratio))


def _decide_sell(holding: Dict, price_local: float) -> Tuple[Optional[str], str]:
    """
    売り判断（現地通貨ベース）。

    優先順位:
    1. トレーリングストップ（最高値から-8%/-6%）→ "full"
    2. +30%達成 全売り → "full"
    3. +15%達成 半分売り（1回のみ）→ "half"
    4. 様子見 → None

    返り値: (売り種別 "full"/"half"/None, 理由文字列)
    """
    avg_cost   = holding.get("avg_cost_local", 0)
    peak_price = holding.get("peak_price") or price_local
    stop_price = holding.get("trailing_stop") or _calc_trailing_stop(peak_price, 0)
    partial    = holding.get("partial_taken", 0)

    if avg_cost <= 0:
        return None, ""

    pnl_pct = (price_local / avg_cost - 1) * 100

    # 1. トレーリングストップ
    if price_local <= stop_price:
        return "full", f"トレーリングストップ発動（含み損益{pnl_pct:.1f}%、ストップ{stop_price:.4f}）"

    # 2. +30%達成 全売り
    if pnl_pct >= PROFIT_FULL:
        return "full", f"目標達成 +{pnl_pct:.1f}% 全株利確"

    # 3. +15%達成 半分売り（まだ実施していない場合のみ）
    if pnl_pct >= PROFIT_PARTIAL and not partial:
        return "half", f"部分利確 +{pnl_pct:.1f}% 半分売り"

    return None, f"保有継続（含み損益{pnl_pct:.1f}%）"


def run_sell_check(summaries_dict: Dict[str, Dict], fx_rates: Dict) -> List[str]:
    """
    保有銘柄全体を確認し、トレーリングストップ・利確条件をチェックして売却を実行。

    summaries_dict: ticker → summary のマッピング
    返り値: 売却実行したtickerのリスト
    """
    portfolio = get_portfolio()
    executed = []

    for holding in portfolio:
        ticker   = holding["ticker"]
        name     = holding["name"]
        market   = holding["market"]
        currency = holding["currency"]
        flag     = holding["flag"]
        fx_rate  = fx_rates.get(currency, 1.0)

        summary = summaries_dict.get(ticker)
        if not summary:
            continue

        price_l = summary["current_price"]
        price_j = price_l * fx_rate

        # ── トレーリングストップ価格を更新 ──
        avg_cost   = holding.get("avg_cost_local", price_l)
        peak_price = holding.get("peak_price") or price_l
        pnl_pct    = (price_l / avg_cost - 1) * 100 if avg_cost > 0 else 0

        # 最高値を更新
        new_peak = max(peak_price, price_l)
        new_stop = _calc_trailing_stop(new_peak, pnl_pct)
        update_trailing_stop(ticker, new_peak, new_stop)

        # 最新の含み損益を再計算（peak更新後）
        sell_type, reason = _decide_sell(
            {**holding, "peak_price": new_peak, "trailing_stop": new_stop},
            price_l
        )

        if sell_type is None:
            continue

        current_cash = get_account()["cash"]

        if sell_type == "full":
            sell_shares = holding["shares"]
        else:  # half
            sell_shares = max(1, int(holding["shares"] / 2))

        total_jpy  = price_j * sell_shares
        commission = calc_commission(total_jpy)
        proceeds   = total_jpy - commission

        # action 表記
        if "トレーリング" in reason or "損" in reason:
            action = "損切り" if pnl_pct < 0 else "sell"
        elif "部分利確" in reason:
            action = "部分利確"
        else:
            action = "利確"

        # DB更新
        if sell_type == "full":
            delete_holding(ticker)
        else:
            new_shares = holding["shares"] - sell_shares
            upsert_holding(
                ticker, name, market, currency, flag,
                new_shares, holding["avg_cost_local"], holding["avg_cost_jpy"],
                peak_price=new_peak, trailing_stop=new_stop,
                partial_taken=1,
                buy_per=holding.get("buy_per"),
                buy_pbr=holding.get("buy_pbr"),
                buy_nc_ratio=holding.get("buy_nc_ratio"),
            )
            mark_partial_taken(ticker)

        update_cash(current_cash + proceeds)
        save_trade(ticker, name, market, currency, flag, action,
                   sell_shares, price_l, price_j, total_jpy, commission, fx_rate, reason)

        emoji = "🔴" if pnl_pct < 0 else "💰"
        print(f"{emoji} {action}: {flag}{name} {sell_shares}株 @{price_l:.4f} ≈ {total_jpy:,.0f}円 | {reason}")
        executed.append(ticker)

    return executed


def run_buy_execution(summaries_dict: Dict[str, Dict], fx_rates: Dict, total_assets: float) -> List[str]:
    """
    スクリーニング通過銘柄から買い候補を選んで購入実行。

    スクリーニング結果（composite_score降順）をベースに、
    RSIフィルター・現金制約・銘柄上限をチェックして購入する。
    """
    screened = get_screened_stocks()
    if not screened:
        print("⚠️  スクリーニング通過銘柄がありません。スクリーニングを先に実行してください。")
        return []

    portfolio  = get_portfolio()
    holding_cnt = len(portfolio)
    executed   = []

    for candidate in screened:
        ticker = candidate["ticker"]

        # 最大保有銘柄数チェック
        if holding_cnt >= MAX_HOLDINGS:
            print(f"⚠️  最大保有数({MAX_HOLDINGS}銘柄)に達しました")
            break

        # 既に保有済みの銘柄はスキップ
        holding = get_holding(ticker)
        if holding:
            continue

        # 直近3日以内に売却した銘柄はスキップ（往復売買コスト節約）
        if recently_sold(ticker, days=3):
            print(f"  ⏭ {ticker}: 直近3日以内に売却済みのためスキップ")
            continue

        summary = summaries_dict.get(ticker)
        if not summary:
            continue

        # RSIフィルター（買われすぎはスキップ）
        rsi = summary.get("rsi14")
        if rsi and rsi > RSI_MAX_FOR_BUY:
            print(f"  ⏭ {ticker}: RSI{rsi:.0f}が上限({RSI_MAX_FOR_BUY})超過のためスキップ")
            continue

        currency = summary["currency"]
        fx_rate  = fx_rates.get(currency, 1.0)
        price_l  = summary["current_price"]
        price_j  = price_l * fx_rate

        if price_j <= 0:
            continue

        # ポジションサイズ（composite_scoreに応じて動的配分）
        position_ratio = _calc_position_size(total_assets, candidate["composite_score"])
        max_jpy        = total_assets * position_ratio
        buy_shares     = max(1, int(max_jpy / price_j))

        trade_jpy  = price_j * buy_shares
        commission = calc_commission(trade_jpy)
        total_cost = trade_jpy + commission

        # 現金チェック（MIN_CASH_RATIO を下回らないよう）
        current_cash = get_account()["cash"]
        available_cash = current_cash - total_assets * MIN_CASH_RATIO - MIN_COMMISSION_JPY
        if available_cash < price_j:
            print(f"  ⏭ {ticker}: 現金不足のためスキップ（利用可能: {available_cash:,.0f}円）")
            continue

        if total_cost > available_cash:
            buy_shares = max(1, int(available_cash / (price_j * (1 + COMMISSION_RATE))))
            trade_jpy  = price_j * buy_shares
            commission = calc_commission(trade_jpy)
            total_cost = trade_jpy + commission

        # 初期トレーリングストップ設定
        trailing_stop = _calc_trailing_stop(price_l, 0)

        # 購入実行
        name     = summary["name"]
        market   = summary["market"]
        flag     = summary["flag"]
        avg_j    = price_l * fx_rate

        upsert_holding(
            ticker, name, market, currency, flag,
            buy_shares, price_l, avg_j,
            peak_price=price_l,
            trailing_stop=trailing_stop,
            partial_taken=0,
            buy_per=candidate.get("per"),
            buy_pbr=candidate.get("pbr"),
            buy_nc_ratio=candidate.get("net_cash_ratio"),
        )
        update_cash(current_cash - total_cost)

        nc_str = f"NC比率{candidate['net_cash_ratio']:.2f}" if candidate.get("net_cash_ratio") is not None else ""
        reason = (f"スコア{candidate['composite_score']:.1f} {nc_str} "
                  f"RSI{rsi:.0f}" if rsi else f"スコア{candidate['composite_score']:.1f} {nc_str}")

        save_trade(ticker, name, market, currency, flag, "buy",
                   buy_shares, price_l, price_j, trade_jpy, commission, fx_rate, reason)

        print(f"✅ 買い: {flag}{name}({ticker}) {buy_shares}株 @{price_l:.4f}{currency} "
              f"≈{trade_jpy:,.0f}円 | {reason}")

        holding_cnt += 1
        executed.append(ticker)

    return executed


def run_global_trading(force: bool = False) -> Dict:
    """
    グローバル自動売買のメイン関数

    force=True: 市場開閉に関係なく全銘柄を対象（手動実行用）
    force=False: 現在開いている市場のみ対象
    """
    print(f"\n{'='*55}")
    print(f"🌍 グローバル取引開始: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    open_markets = get_open_markets()
    if not open_markets and not force:
        msg = "現在開いている市場がありません"
        print(f"⏸ {msg}")
        return {"status": "skipped", "message": msg, "open_markets": []}

    if force:
        print("🔴 強制実行モード: 全銘柄を対象にします")
    else:
        print(f"📡 開いている市場: {', '.join(open_markets)}")

    # 為替レート取得
    print("💱 為替レートを取得中...")
    fx_rates = get_fx_rates()
    print(f"  USD/JPY={fx_rates.get('USD',0):.1f}  GBP/JPY={fx_rates.get('GBP',0):.1f}  "
          f"EUR/JPY={fx_rates.get('EUR',0):.1f}  HKD/JPY={fx_rates.get('HKD',0):.1f}")

    # 株価データ取得
    print("📊 株価データを取得中...")
    if force:
        summaries_list = get_all_summaries()
    else:
        summaries_list = get_summaries_for_open_markets(open_markets)

    if not summaries_list:
        return {"status": "error", "message": "株価データの取得に失敗しました"}

    summaries_dict = {s["ticker"]: s for s in summaries_list}

    # 資産状況
    account      = get_account()
    portfolio    = get_portfolio()
    total_assets = calc_total_assets(account["cash"], portfolio, fx_rates)
    cash_ratio   = account["cash"] / total_assets if total_assets > 0 else 1.0
    print(f"💰 総資産: {total_assets:,.0f}円 / 現金: {account['cash']:,.0f}円 ({cash_ratio*100:.1f}%)")

    # ── Step 1: 売りチェック（トレーリングストップ・利確）──
    print("\n▼ 売りチェック")
    sold = run_sell_check(summaries_dict, fx_rates)

    # ── Step 2: 買い付け（スクリーニング通過銘柄を対象）──
    # 売り後に総資産を再計算
    account_mid  = get_account()
    portfolio_mid = get_portfolio()
    total_mid    = calc_total_assets(account_mid["cash"], portfolio_mid, fx_rates)

    print("\n▼ 買い付け")
    bought = run_buy_execution(summaries_dict, fx_rates, total_mid)

    # ── Step 3: スナップショット保存 ──
    account_after   = get_account()
    portfolio_after = get_portfolio()
    total_after     = calc_total_assets(account_after["cash"], portfolio_after, fx_rates)
    save_asset_snapshot(total_after, account_after["cash"], total_after - account_after["cash"])

    pnl = total_after - account_after["initial_capital"]
    pnl_pct = pnl / account_after["initial_capital"] * 100
    print(f"\n🏁 完了: 売{len(sold)}件 / 買{len(bought)}件")
    print(f"📈 総資産: {total_assets:,.0f}円 → {total_after:,.0f}円 (損益{pnl:+,.0f}円 / {pnl_pct:+.2f}%)")

    return {
        "status":              "success",
        "executed_at":         datetime.now().isoformat(),
        "open_markets":        open_markets,
        "total_assets_before": round(total_assets, 0),
        "total_assets_after":  round(total_after, 0),
        "sold_tickers":        sold,
        "bought_tickers":      bought,
    }
