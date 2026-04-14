"""
グローバル投資スクリーニング
複数の億トレーダー手法を組み合わせた複合スコアで候補銘柄を選別します

【組み込んだ手法】
  清原達郎式   : NC比率・PER・PBR によるファンダメンタルスクリーニング
  Minervini   : 50日MA > 150日MA > 200日MA のトレンドテンプレート
  O'Neil/CAN-SLIM: 出来高急増（20日平均の1.5倍超）
  リバモア    : 52週高値の90%超ブレイクアウト
  RSIフィルター: 買われすぎ(>70)はペナルティ、売られすぎ(<30)はボーナス
"""
from typing import List, Dict, Optional
from global_stocks import get_all_summaries, GLOBAL_WATCHLIST
from database import clear_screened_stocks, save_screened_stock

# グローバル向けスクリーニング条件（清原式より緩和: 大型成長株も含むため）
GLOBAL_RULES = {
    "max_per":            25.0,   # PER 25倍以下
    "max_pbr":             2.5,   # PBR 2.5倍以下
    "min_net_cash_ratio":  0.1,   # NC比率 0.1以上（現金が時価総額の10%以上）
}

# テクニカルのみでも通過できる緩和条件（ファンダデータがNoneの場合）
TECHNICAL_ONLY_THRESHOLD = 30  # composite_scoreがこれ以上あればテクニカル合格


def _passes_screening(summary: Dict) -> tuple[bool, str]:
    """
    スクリーニング判定。
    ファンダメンタルデータがある場合は全条件チェック。
    ない場合はテクニカルスコアのみで判定。
    戻り値: (通過したか, 判定理由)
    """
    per = summary.get("per")
    pbr = summary.get("pbr")
    nc  = summary.get("net_cash_ratio")

    has_fundamental = (per is not None or pbr is not None or nc is not None)

    if not has_fundamental:
        # ファンダデータなし → テクニカルスコアで仮通過
        tech_score = _calc_technical_score(summary)
        if tech_score >= TECHNICAL_ONLY_THRESHOLD:
            return True, "テクニカル条件のみ合格（ファンダデータ取得中）"
        return False, "ファンダデータ未取得・テクニカルスコア不足"

    reasons = []

    # PERチェック（データがある場合のみ）
    if per is not None:
        if per > GLOBAL_RULES["max_per"]:
            return False, f"PER {per:.1f}倍が上限({GLOBAL_RULES['max_per']}倍)を超過"
        reasons.append(f"PER{per:.1f}倍OK")

    # PBRチェック（データがある場合のみ）
    if pbr is not None:
        if pbr > GLOBAL_RULES["max_pbr"]:
            return False, f"PBR {pbr:.2f}倍が上限({GLOBAL_RULES['max_pbr']}倍)を超過"
        reasons.append(f"PBR{pbr:.2f}倍OK")

    # NC比率チェック（データがある場合のみ）
    if nc is not None:
        if nc < GLOBAL_RULES["min_net_cash_ratio"]:
            return False, f"NC比率 {nc:.2f} が下限({GLOBAL_RULES['min_net_cash_ratio']})未満"
        reasons.append(f"NC比率{nc:.2f}OK")

    return True, "、".join(reasons) if reasons else "条件クリア"


def _calc_technical_score(summary: Dict) -> float:
    """テクニカル部分のスコアのみ計算（Minervini + 出来高 + ブレイクアウト + RSI）"""
    score = 0.0
    price = summary.get("current_price")
    ma50  = summary.get("ma50")
    ma150 = summary.get("ma150")
    ma200 = summary.get("ma200")
    rsi   = summary.get("rsi14")

    # Minervini SEPA: MA200上昇トレンド＋整列
    if all(x is not None for x in [ma50, ma150, ma200, price]):
        if ma50 > ma150 > ma200:
            score += 15  # 完全整列
        if price > ma50 and ma50 > ma150:
            score += 10  # 現値がMA50超え

    # CAN-SLIM 出来高急増
    if summary.get("vol_surge"):
        score += 10

    # リバモア 52週高値ブレイクアウト
    if summary.get("near_52w_high"):
        score += 8

    # RSIフィルター
    if rsi:
        if rsi < 30:
            score += 5   # 売られすぎ反発期待
        elif rsi > 70:
            score -= 10  # 買われすぎペナルティ

    return score


def _calc_composite_score(summary: Dict) -> float:
    """
    複合スコア（高いほど優先度高）

    ファンダメンタル部分（清原式ベース）:
      NC比率 × 30
      max(0, (2.5 - PBR)) × 10
      max(0, (25 - PER)) × 0.8

    テクニカル部分（Minervini + CAN-SLIM + リバモア + RSI）:
      → _calc_technical_score() を呼ぶ
    """
    score = 0.0

    # ── ファンダメンタル（清原式ベース）──
    nc = summary.get("net_cash_ratio") or 0
    score += nc * 30  # NC比率が最重要（0.1 → +3点, 1.0 → +30点）

    pbr = summary.get("pbr")
    if pbr and pbr > 0:
        score += max(0.0, (2.5 - pbr) * 10)  # 低PBRほど高得点

    per = summary.get("per")
    if per and per > 0:
        score += max(0.0, (25.0 - per) * 0.8)  # 低PERほど高得点

    # ── テクニカル ──
    score += _calc_technical_score(summary)

    return round(score, 2)


def run_screening(verbose: bool = True) -> List[Dict]:
    """
    GLOBAL_WATCHLIST 全15銘柄をスクリーニングし、通過銘柄をDB保存して返す。
    スコア降順ソート。

    verbose=True の場合はコンソールにログを出力。
    """
    if verbose:
        print("\n" + "="*55)
        print("🔍 グローバルスクリーニング開始")
        print("="*55)

    # 全銘柄のサマリーを取得（ファンダメンタル含む）
    if verbose:
        print("📊 全銘柄のデータを取得中（ファンダメンタル含む）...")
    summaries = get_all_summaries()

    if not summaries:
        print("⚠️  株価データの取得に失敗しました")
        return []

    # 既存スクリーニング結果をクリア
    clear_screened_stocks()

    passed = []
    for s in summaries:
        ticker = s["ticker"]
        name   = s["name"]
        flag   = s["flag"]

        ok, reason = _passes_screening(s)
        score = _calc_composite_score(s)

        if verbose:
            status = "✅ 合格" if ok else "❌ 不合格"
            print(f"  {flag}{name}({ticker}): {status} スコア{score:.1f} — {reason}")

        if ok:
            minervini_pass = 1 if (
                s.get("ma50") and s.get("ma150") and s.get("ma200") and
                s["ma50"] > s["ma150"] > s["ma200"]
            ) else 0

            data = {
                "ticker":          ticker,
                "name":            name,
                "market":          s["market"],
                "currency":        s["currency"],
                "flag":            flag,
                "market_cap":      s.get("market_cap"),
                "per":             s.get("per"),
                "pbr":             s.get("pbr"),
                "net_cash_ratio":  s.get("net_cash_ratio"),
                "composite_score": score,
                "minervini_pass":  minervini_pass,
                "canslim_pass":    1 if s.get("vol_surge") else 0,
                "current_price":   s["current_price"],
                "rsi14":           s.get("rsi14"),
                "ma50":            s.get("ma50"),
                "ma150":           s.get("ma150"),
                "ma200":           s.get("ma200"),
            }
            save_screened_stock(data)
            passed.append(data)

    # スコア降順ソート
    passed.sort(key=lambda x: x["composite_score"], reverse=True)

    if verbose:
        print(f"\n{'='*55}")
        print(f"✅ スクリーニング完了: {len(summaries)}銘柄中 {len(passed)}銘柄が通過")
        if passed:
            print("📋 投資候補（スコア順）:")
            for p in passed:
                nc_str  = f"NC比率{p['net_cash_ratio']:.2f}" if p.get("net_cash_ratio") is not None else "NC比率-"
                per_str = f"PER{p['per']:.1f}" if p.get("per") is not None else "PER-"
                pbr_str = f"PBR{p['pbr']:.2f}" if p.get("pbr") is not None else "PBR-"
                mstr    = "✅Minervini" if p["minervini_pass"] else ""
                print(f"  {p['flag']}{p['name']}({p['ticker']}) "
                      f"スコア{p['composite_score']:.1f} {nc_str} {per_str} {pbr_str} {mstr}")

    return passed
