"""
日本小型株自動売買エンジン（清原達郎式）

【清原式 売買ルール】
  買い: スクリーニング通過銘柄（NC比率≥1.0）をNC比率降順で均等分散購入
  売り:
    利確: 現在株価 ≥ 買値 × 2倍（+100%達成）
    損切り: 現在株価 ≤ 買値 × 0.85（-15%以下）
  最大10銘柄、均等配分

【清原式の哲学】
  「+30%では売らない。2倍になるまで待つ」
  NC比率が1倍以上の銘柄は理論上タダ同然で手に入る会社。
  大きく下がる理由がなく、中長期で2倍以上になる確率が高い。
"""
from datetime import datetime
from typing import List, Dict, Optional, Tuple

from database import (
    get_account, get_portfolio, get_holding,
    upsert_holding, delete_holding,
    save_trade, update_cash, save_asset_snapshot,
    update_trailing_stop, mark_partial_taken, recently_sold,
    get_last_sell_action, get_screened_stocks,
)
from market_hours import get_open_markets
from global_stocks import (
    get_summaries_for_open_markets, get_all_summaries,
    get_fx_rates, GLOBAL_WATCHLIST,
    get_global_7mkt_summaries, get_global_7mkt_summaries_for_open_markets,
    get_fx_rates_global, GLOBAL_WATCHLIST_7MKT,
)

# ─── 清原式リスク管理パラメータ ──────────────────────────
TARGET_GAIN_PCT  = 100.0   # 目標利確: 買値×2倍（+100%）
STOP_LOSS_PCT    = -15.0   # ストップロス: -15%

MAX_HOLDINGS     = 10      # 最大保有銘柄数（清原式: 分散しすぎず）
MIN_CASH_RATIO   = 0.10    # 常に現金10%以上を確保

COMMISSION_RATE     = 0.001  # 手数料 0.1%
MIN_COMMISSION_JPY  = 100    # 最低手数料 100円


def calc_commission(amount_jpy: float) -> float:
    return max(amount_jpy * COMMISSION_RATE, MIN_COMMISSION_JPY)


def calc_total_assets(cash: float, portfolio: List[Dict], fx_rates: Dict) -> float:
    """現金 + 保有株の円換算評価額で総資産を計算（日本株なのでfx_ratesは常に1.0）"""
    stock_value = 0.0
    for h in portfolio:
        stock_value += h["shares"] * h["avg_cost_local"]
    return cash + stock_value


def _decide_sell(holding: Dict, price_local: float) -> Tuple[Optional[str], str]:
    """
    清原式 売り判断。

    2つのルールのみ:
    1. 目標達成: 現在株価 ≥ 買値 × 2倍（+100%）→ 全株利確
    2. ストップロス: 現在株価 ≤ 買値 × 0.85（-15%）→ 損切り

    清原式哲学: 「+30%では売らない。2倍になるまで待つ」

    返り値: (売り種別 "full"/None, 理由文字列)
    """
    avg_cost = holding.get("avg_cost_local", 0)
    if avg_cost <= 0:
        return None, ""

    pnl_pct = (price_local / avg_cost - 1) * 100

    # 1. 目標達成（+100%）
    if pnl_pct >= TARGET_GAIN_PCT:
        return "full", f"目標達成 +{pnl_pct:.1f}% 全株利確（買値×2倍）"

    # 2. ストップロス（-15%）
    if pnl_pct <= STOP_LOSS_PCT:
        return "full", f"ストップロス {pnl_pct:.1f}% 損切り（-15%）"

    return None, f"保有継続（含み損益{pnl_pct:.1f}%）"


def run_sell_check(summaries_dict: Dict[str, Dict], fx_rates: Dict,
                   strategy: str = 'kiyohara') -> List[str]:
    """
    清原式 売りチェック。
    保有銘柄全体を確認し、目標達成（+100%）またはストップロス（-15%）で売却実行。

    summaries_dict: ticker → summary のマッピング
    返り値: 売却実行したtickerのリスト
    """
    portfolio = get_portfolio(strategy=strategy)
    executed = []

    for holding in portfolio:
        ticker   = holding["ticker"]
        name     = holding["name"]
        market   = holding["market"]
        currency = holding["currency"]
        flag     = holding["flag"]

        summary = summaries_dict.get(ticker)
        if not summary:
            continue

        price_l = summary["current_price"]
        price_j = price_l  # JPYなのでそのまま

        avg_cost = holding.get("avg_cost_local", price_l)
        pnl_pct  = (price_l / avg_cost - 1) * 100 if avg_cost > 0 else 0

        # 最高値を更新（記録用）
        peak_price = holding.get("peak_price") or price_l
        new_peak   = max(peak_price, price_l)
        update_trailing_stop(ticker, new_peak, new_peak * 0.85, strategy=strategy)

        sell_type, reason = _decide_sell(holding, price_l)
        if sell_type is None:
            continue

        sell_shares = holding["shares"]
        total_jpy   = price_j * sell_shares
        commission  = calc_commission(total_jpy)
        proceeds    = total_jpy - commission

        action = "利確" if pnl_pct >= TARGET_GAIN_PCT else "損切り"

        delete_holding(ticker, strategy=strategy)
        update_cash(get_account()["cash"] + proceeds)
        save_trade(ticker, name, market, currency, flag, action,
                   sell_shares, price_l, price_j, total_jpy, commission, 1.0, reason,
                   strategy=strategy)

        emoji = "🔴" if pnl_pct < 0 else "💰"
        print(f"{emoji} {action}: {flag}{name} {sell_shares}株 @{price_l:.0f}円 ≈ {total_jpy:,.0f}円 | {reason}")
        executed.append(ticker)

    return executed


def run_buy_execution(summaries_dict: Dict[str, Dict], fx_rates: Dict, total_assets: float,
                      strategy: str = 'kiyohara') -> List[str]:
    """
    清原式 買い付け実行。

    スクリーニング通過銘柄（NC比率降順）から、空きスロットを埋めるよう
    均等分散（total_assets / MAX_HOLDINGS 相当）で購入する。

    RSIフィルター・セクター集中チェックは行わない（清原式では不要）。
    """
    screened = get_screened_stocks(strategy=strategy)
    if not screened:
        print("⚠️  スクリーニング通過銘柄がありません。スクリーニングを先に実行してください。")
        return []

    portfolio   = get_portfolio(strategy=strategy)
    holding_cnt = len(portfolio)
    executed    = []

    # 清原式均等分散: 1銘柄あたりの目標投資額 = 総資産 ÷ MAX_HOLDINGS
    position_budget = total_assets / MAX_HOLDINGS

    for candidate in screened:
        ticker = candidate["ticker"]

        # 最大保有銘柄数チェック
        if holding_cnt >= MAX_HOLDINGS:
            print(f"⚠️  最大保有数({MAX_HOLDINGS}銘柄)に達しました")
            break

        # 既に保有済みの銘柄はスキップ
        if get_holding(ticker, strategy=strategy):
            continue

        # 売却種別に応じた再購入禁止期間:
        #   損切り → 3日（危険な銘柄なので冷却期間を長く）
        #   利確   → 1日（好調な銘柄なので翌日から再参入OK）
        last_sell = get_last_sell_action(ticker, strategy=strategy)
        if last_sell:
            wait_days = 3 if last_sell["action"] == "損切り" else 1
            if recently_sold(ticker, days=wait_days, strategy=strategy):
                print(f"  ⏭ {ticker}: 直近{wait_days}日以内に{last_sell['action']}済みのためスキップ")
                continue

        summary = summaries_dict.get(ticker)
        if not summary:
            continue

        price_l = summary["current_price"]  # JPYなのでそのまま
        if price_l <= 0:
            continue

        # 均等分散: 目標額 ÷ 株価 = 株数（最低1株）
        buy_shares = max(1, int(position_budget / price_l))

        trade_jpy  = price_l * buy_shares
        commission = calc_commission(trade_jpy)
        total_cost = trade_jpy + commission

        # 現金チェック（MIN_CASH_RATIO を下回らないよう）
        current_cash   = get_account()["cash"]
        available_cash = current_cash - total_assets * MIN_CASH_RATIO - MIN_COMMISSION_JPY
        if available_cash < price_l:
            print(f"  ⏭ {ticker}: 現金不足のためスキップ（利用可能: {available_cash:,.0f}円）")
            continue

        # 現金が足りない場合は買える株数に調整
        if total_cost > available_cash:
            buy_shares = max(1, int(available_cash / (price_l * (1 + COMMISSION_RATE))))
            trade_jpy  = price_l * buy_shares
            commission = calc_commission(trade_jpy)
            total_cost = trade_jpy + commission

        # 購入実行
        name     = summary["name"]
        market   = summary["market"]
        currency = summary["currency"]
        flag     = summary["flag"]

        upsert_holding(
            ticker, name, market, currency, flag,
            buy_shares, price_l, price_l,
            peak_price=price_l,
            trailing_stop=price_l * (1 + STOP_LOSS_PCT / 100),
            partial_taken=0,
            buy_per=candidate.get("per"),
            buy_pbr=candidate.get("pbr"),
            buy_nc_ratio=candidate.get("net_cash_ratio"),
            strategy=strategy,
        )
        update_cash(current_cash - total_cost)

        nc_str = f"NC比率{candidate['net_cash_ratio']:.2f}" if candidate.get("net_cash_ratio") is not None else ""
        reason = f"スコア{candidate['composite_score']:.1f} {nc_str}".strip()

        save_trade(ticker, name, market, currency, flag, "buy",
                   buy_shares, price_l, price_l, trade_jpy, commission, 1.0, reason,
                   strategy=strategy)

        print(f"✅ 買い: {flag}{name}({ticker}) {buy_shares}株 @{price_l:.0f}円 "
              f"≈{trade_jpy:,.0f}円 | {reason}")

        holding_cnt += 1
        executed.append(ticker)

    return executed


def run_kiyohara_trading(force: bool = False) -> Dict:
    """
    清原式 自動売買のメイン関数（東証日本小型株専用）

    force=True: 東証の開閉に関係なく全銘柄を対象（手動実行用）
    force=False: 東証が開いている時間帯のみ実行
    """
    print(f"\n{'='*55}")
    print(f"🇯🇵 清原式取引開始: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    open_markets = get_open_markets()
    if not open_markets and not force:
        msg = "現在、東証は閉まっています"
        print(f"⏸ {msg}")
        return {"status": "skipped", "message": msg, "open_markets": []}

    if force:
        print("🔴 強制実行モード: 全銘柄を対象にします")
    else:
        print(f"📡 開いている市場: {', '.join(open_markets)}")

    # 為替レート（日本株なのでJPY固定）
    fx_rates = get_fx_rates()  # {"JPY": 1.0}

    # 株価データ取得
    print("📊 株価データを取得中（東証50銘柄）...")
    if force:
        summaries_list = get_all_summaries()
    else:
        summaries_list = get_summaries_for_open_markets(open_markets)

    if not summaries_list:
        return {"status": "error", "message": "株価データの取得に失敗しました"}

    summaries_dict = {s["ticker"]: s for s in summaries_list}

    # 資産状況
    account      = get_account()
    portfolio    = get_portfolio(strategy='kiyohara')
    total_assets = calc_total_assets(account["cash"], portfolio, fx_rates)
    cash_ratio   = account["cash"] / total_assets if total_assets > 0 else 1.0
    print(f"💰 総資産: {total_assets:,.0f}円 / 現金: {account['cash']:,.0f}円 ({cash_ratio*100:.1f}%)")

    # ── Step 1: 売りチェック（目標×2達成 or -15%損切り）──
    print("\n▼ 売りチェック（目標+100% / ストップ-15%）")
    sold = run_sell_check(summaries_dict, fx_rates, strategy='kiyohara')

    # ── Step 2: 買い付け（スクリーニング通過銘柄を均等分散で購入）──
    # 売り後に総資産を再計算
    account_mid   = get_account()
    portfolio_mid = get_portfolio(strategy='kiyohara')
    total_mid     = calc_total_assets(account_mid["cash"], portfolio_mid, fx_rates)

    print("\n▼ 買い付け（清原式均等分散）")
    bought = run_buy_execution(summaries_dict, fx_rates, total_mid, strategy='kiyohara')

    # ── Step 3: スナップショット保存 ──
    account_after   = get_account()
    portfolio_after = get_portfolio()
    total_after     = calc_total_assets(account_after["cash"], portfolio_after, fx_rates)
    save_asset_snapshot(total_after, account_after["cash"], total_after - account_after["cash"])

    pnl     = total_after - account_after["initial_capital"]
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


# ============================================================
# ミナービニ型 グローバルブレイクアウト売買エンジン
# 「元手少なくても数年で億にする」攻撃的成長戦略
#
# 【買いルール】
#   直近20日高値を出来高急増で上抜けた銘柄のみ買う
#   + トレンドテンプレート（MA50 > MA150 > MA200）確認
#
# 【売りルール】
#   1. 損切り: 買値から-8%（絶対ルール・例外なし）
#   2. トレーリングストップ: 最高値から-20%
#   3. 急騰利確: 3日で+20%以上の急騰
#
# 【ポジション管理】
#   1銘柄=総資産の10%、最大10銘柄、現金20%以上維持
# ============================================================

G_STOP_LOSS_PCT   = 8.0     # 損切り: -8%（絶対ルール）
G_TRAIL_PCT       = 0.20    # トレーリングストップ: 最高値から-20%
G_QUICK_PROFIT    = 20.0    # 急騰利確: +20%超
G_POSITION_RATIO  = 0.10    # 1銘柄=総資産の10%
G_MAX_POSITIONS   = 10      # 最大10銘柄
G_MIN_CASH_RATIO  = 0.20    # 現金最低20%維持


def calc_total_assets_global(cash: float, portfolio: List[Dict], fx_rates: Dict) -> float:
    """グローバル版: 現金 + 保有株の円換算評価額（為替換算あり）"""
    stock_value = 0.0
    for h in portfolio:
        rate = fx_rates.get(h.get("currency", "JPY"), 1.0)
        stock_value += h["shares"] * h["avg_cost_local"] * rate
    return cash + stock_value


def _is_breakout(summary: Dict) -> Tuple[bool, str]:
    """
    ミナービニ型ブレイクアウト判定。

    条件:
    - 直近20日高値（前日まで）を上抜け
    - 出来高が20日平均の1.5倍以上（出来高急増）
    - MA50 > MA150 > MA200（上昇トレンド確認）
    """
    price     = summary.get("current_price", 0)
    high20d   = summary.get("high_20d")
    vol_surge = summary.get("vol_surge", False)
    ma50      = summary.get("ma50")
    ma150     = summary.get("ma150")
    ma200     = summary.get("ma200")

    if not high20d or high20d <= 0 or price <= 0:
        return False, "20日高値データなし"

    breakout  = price > high20d
    trend_ok  = bool(ma50 and ma150 and ma200 and ma50 > ma150 and ma150 > ma200)

    if breakout and vol_surge and trend_ok:
        return True, f"ブレイクアウト {price:.2f}>{high20d:.2f} + 出来高急増 + 上昇トレンド"
    elif breakout and vol_surge:
        return True, f"ブレイクアウト {price:.2f}>{high20d:.2f} + 出来高急増"
    elif breakout and trend_ok:
        return False, f"高値更新だが出来高不足（出来高急増なし）"
    else:
        return False, f"条件未達（価格{price:.2f} / 20日高値{high20d:.2f}）"


def run_global_sell_check(summaries_dict: Dict[str, Dict], fx_rates: Dict) -> List[str]:
    """ミナービニ型売りチェック: 8%損切り + 20%トレーリングストップ + 急騰利確"""
    portfolio = get_portfolio(strategy='global')
    executed  = []

    for holding in portfolio:
        ticker   = holding["ticker"]
        name     = holding["name"]
        market   = holding["market"]
        currency = holding["currency"]
        flag     = holding["flag"]

        summary = summaries_dict.get(ticker)
        if not summary:
            continue

        fx_rate  = fx_rates.get(currency, 1.0)
        price_l  = summary["current_price"]
        price_j  = price_l * fx_rate
        avg_cost = holding.get("avg_cost_local", price_l)
        pnl_pct  = (price_l / avg_cost - 1) * 100 if avg_cost > 0 else 0

        # 最高値を更新してトレーリングストップを記録
        peak_price = holding.get("peak_price") or price_l
        new_peak   = max(peak_price, price_l)
        trail_stop = new_peak * (1.0 - G_TRAIL_PCT)
        update_trailing_stop(ticker, new_peak, trail_stop, strategy='global')

        # 売り判断
        reason = None
        if pnl_pct <= -G_STOP_LOSS_PCT:
            reason = f"損切り -8%ルール（{pnl_pct:.1f}%）"
        elif price_l <= trail_stop:
            reason = f"トレーリングストップ（最高値比-20%）含み益{pnl_pct:.1f}%"
        elif pnl_pct >= G_QUICK_PROFIT:
            reason = f"急騰利確 +{pnl_pct:.1f}%（+20%超）"

        if reason is None:
            continue

        sell_shares = holding["shares"]
        total_jpy   = price_j * sell_shares
        commission  = calc_commission(total_jpy)
        proceeds    = total_jpy - commission
        action      = "損切り" if pnl_pct < 0 else "利確"

        delete_holding(ticker, strategy='global')
        update_cash(get_account('global')["cash"] + proceeds, strategy='global')
        save_trade(ticker, name, market, currency, flag, action,
                   sell_shares, price_l, price_j, total_jpy, commission, fx_rate, reason,
                   strategy='global')

        emoji = "🔴" if pnl_pct < 0 else "💰"
        print(f"{emoji} {action}: {flag}{name} {sell_shares}株 @{price_l:.2f}{currency} "
              f"≈{total_jpy:,.0f}円 | {reason}")
        executed.append(ticker)

    return executed


def run_global_buy_execution(summaries_dict: Dict[str, Dict], fx_rates: Dict,
                             total_assets: float) -> List[str]:
    """ミナービニ型買い付け: ブレイクアウト + 出来高急増のみ買う"""
    portfolio   = get_portfolio(strategy='global')
    holding_cnt = len(portfolio)
    executed    = []

    if holding_cnt >= G_MAX_POSITIONS:
        print(f"  ⏭ 最大保有数({G_MAX_POSITIONS}銘柄)に達しています")
        return []

    for ticker, summary in summaries_dict.items():
        if holding_cnt + len(executed) >= G_MAX_POSITIONS:
            break

        if get_holding(ticker, strategy='global'):
            continue

        # 損切り直後は3日クールダウン
        last_sell = get_last_sell_action(ticker, strategy='global')
        if last_sell and last_sell.get("action") == "損切り":
            if recently_sold(ticker, days=3, strategy='global'):
                continue

        is_bo, reason = _is_breakout(summary)
        print(f"  {summary.get('flag','')}{summary.get('name', ticker)}({ticker}): "
              f"{'🚀ブレイクアウト' if is_bo else '待機'} — {reason}")
        if not is_bo:
            continue

        currency = summary["currency"]
        fx_rate  = fx_rates.get(currency, 1.0)
        price_l  = summary["current_price"]
        price_j  = price_l * fx_rate

        if price_j <= 0:
            continue

        # ポジションサイズ: 総資産の10%
        position_jpy = total_assets * G_POSITION_RATIO
        buy_shares   = max(1, int(position_jpy / price_j))
        trade_jpy    = price_j * buy_shares
        commission   = calc_commission(trade_jpy)
        total_cost   = trade_jpy + commission

        current_cash = get_account('global')["cash"]
        available    = current_cash - total_assets * G_MIN_CASH_RATIO - MIN_COMMISSION_JPY
        if available < price_j:
            print(f"  ⏭ {ticker}: 現金不足のためスキップ（利用可能: {available:,.0f}円）")
            continue

        if total_cost > available:
            buy_shares = max(1, int(available / (price_j * (1 + COMMISSION_RATE))))
            trade_jpy  = price_j * buy_shares
            commission = calc_commission(trade_jpy)
            total_cost = trade_jpy + commission

        name   = summary["name"]
        market = summary["market"]
        flag   = summary.get("flag", "")

        upsert_holding(
            ticker, name, market, currency, flag,
            buy_shares, price_l, price_j,
            peak_price=price_l,
            trailing_stop=price_l * (1.0 - G_TRAIL_PCT),
            partial_taken=0,
            buy_per=None, buy_pbr=None, buy_nc_ratio=None,
            strategy='global',
        )
        update_cash(current_cash - total_cost, strategy='global')
        save_trade(ticker, name, market, currency, flag, "買い",
                   buy_shares, price_l, price_j, trade_jpy, commission, fx_rate, reason,
                   strategy='global')

        print(f"✅ 買い[G]: {flag}{name}({ticker}) {buy_shares}株 @{price_l:.2f}{currency} "
              f"≈{trade_jpy:,.0f}円 | {reason}")
        executed.append(ticker)

    return executed


def run_global_trading_orig(force: bool = False) -> Dict:
    """
    ミナービニ型グローバル自動売買のメイン関数
    ブレイクアウト戦略: 清原式要素なし、純粋なモメンタム投資
    """
    print(f"\n{'='*55}")
    print(f"🌍 グローバル取引開始（ミナービニ型）: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    open_markets = get_open_markets()
    if not open_markets and not force:
        msg = "現在開いている市場がありません"
        print(f"⏸ {msg}")
        return {"status": "skipped", "message": msg, "open_markets": []}

    if force:
        print("🔴 強制実行モード: 全7市場銘柄を対象にします")
    else:
        print(f"📡 開いている市場: {', '.join(open_markets)}")

    print("💱 為替レートを取得中...")
    fx_rates = get_fx_rates_global()
    print(f"  USD/JPY={fx_rates.get('USD',0):.1f}  GBP/JPY={fx_rates.get('GBP',0):.1f}  "
          f"EUR/JPY={fx_rates.get('EUR',0):.1f}  HKD/JPY={fx_rates.get('HKD',0):.1f}")

    print("📊 株価データを取得中（7市場50銘柄）...")
    if force:
        summaries_list = get_global_7mkt_summaries()
    else:
        summaries_list = get_global_7mkt_summaries_for_open_markets(open_markets)

    if not summaries_list:
        return {"status": "error", "message": "株価データの取得に失敗しました"}

    summaries_dict = {s["ticker"]: s for s in summaries_list}

    account      = get_account('global')
    portfolio    = get_portfolio(strategy='global')
    total_assets = calc_total_assets_global(account["cash"], portfolio, fx_rates)
    cash_ratio   = account["cash"] / total_assets if total_assets > 0 else 1.0
    print(f"💰 総資産: {total_assets:,.0f}円 / 現金: {account['cash']:,.0f}円 ({cash_ratio*100:.1f}%)")

    print("\n▼ 売りチェック（損切り -8% / トレーリングストップ -20% / 急騰利確 +20%）")
    sold = run_global_sell_check(summaries_dict, fx_rates)

    account_mid   = get_account('global')
    portfolio_mid = get_portfolio(strategy='global')
    total_mid     = calc_total_assets_global(account_mid["cash"], portfolio_mid, fx_rates)

    print("\n▼ 買い付け（ブレイクアウト + 出来高急増のみ）")
    bought = run_global_buy_execution(summaries_dict, fx_rates, total_mid)

    account_after   = get_account('global')
    portfolio_after = get_portfolio(strategy='global')
    total_after     = calc_total_assets_global(account_after["cash"], portfolio_after, fx_rates)

    pnl     = total_after - account_after["initial_capital"]
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
