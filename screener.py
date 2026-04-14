"""
グローバル投資スクリーニング
複数の著名トレーダー手法を組み合わせた複合スコアで候補銘柄を選別します

【組み込んだ手法】
  清原達郎式   : NC比率・PER・PBR によるファンダメンタルスクリーニング
  Minervini   : 50日MA > 150日MA > 200日MA のトレンドテンプレート (SEPA)
  O'Neil/CAN-SLIM: 出来高急増（20日平均の1.5倍超）
  リバモア    : 52週高値の90%超ブレイクアウト
  RSIフィルター: 買われすぎ(>70)はペナルティ、売られすぎ(<30)はボーナス
  ピーター・リンチ: PEG比率（PER ÷ 利益成長率）< 1.0 で割安成長株
  ウォーレン・バフェット: ROE（自己資本利益率）高・D/E比率（借金の少なさ）
  MACD戦略   : ゴールデンクロス（短期EMAが長期EMAを上抜け）
  ボリンジャースクイーズ: 価格変動が収縮→大相場の予兆
  ドラッケンミラー式モメンタム: ROC20・ROC60 両方プラス
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
    """
    テクニカル部分のスコア計算
    Minervini + CAN-SLIM + リバモア + RSI + MACD + BBスクイーズ + ROCモメンタム
    """
    score = 0.0
    price = summary.get("current_price")
    ma50  = summary.get("ma50")
    ma150 = summary.get("ma150")
    ma200 = summary.get("ma200")
    rsi   = summary.get("rsi14")

    # ── Minervini SEPA: MA200上昇トレンド＋整列 (+25) ──
    if all(x is not None for x in [ma50, ma150, ma200, price]):
        if ma50 > ma150 > ma200:
            score += 15  # 完全整列
        if price > ma50 and ma50 > ma150:
            score += 10  # 現値がMA50超え

    # ── CAN-SLIM 出来高急増 (+10) ──
    if summary.get("vol_surge"):
        score += 10

    # ── リバモア 52週高値ブレイクアウト (+8) ──
    if summary.get("near_52w_high"):
        score += 8

    # ── RSIフィルター ──
    if rsi:
        if rsi < 30:
            score += 5   # 売られすぎ反発期待
        elif rsi > 70:
            score -= 10  # 買われすぎペナルティ

    # ── MACD ゴールデンクロス (+12) / 上昇継続 (+6) ──
    macd_cross = summary.get("macd_cross")
    if macd_cross == "golden":
        score += 12   # 直前にゴールデンクロス発生
    elif macd_cross == "positive":
        score += 6    # MACDがゼロ超えで上昇中

    # ── ボリンジャースクイーズ（価格収縮→爆発前の予兆） (+8) ──
    bb_width = summary.get("bb_width")
    if bb_width is not None and bb_width < 0.04:
        score += 8

    # ── ドラッケンミラー式モメンタム ROC (+10) ──
    roc20 = summary.get("roc20")
    roc60 = summary.get("roc60")
    if roc20 is not None and roc60 is not None:
        if roc20 > 0 and roc60 > 0:
            score += 10   # 短期・長期ともにプラスモメンタム
        elif roc20 > 0 or roc60 > 0:
            score += 4    # どちらかがプラス

    return score


def _calc_fundamental_score(summary: Dict) -> float:
    """
    ファンダメンタル部分のスコア計算
    清原式 + ピーター・リンチ(PEG) + バフェット(ROE/D/E)
    """
    score = 0.0

    # ── 清原達郎式ベース ──
    nc = summary.get("net_cash_ratio") or 0
    score += nc * 30  # NC比率が最重要（0.1 → +3点, 1.0 → +30点）

    pbr = summary.get("pbr")
    if pbr and pbr > 0:
        score += max(0.0, (2.5 - pbr) * 10)  # 低PBRほど高得点

    per = summary.get("per")
    if per and per > 0:
        score += max(0.0, (25.0 - per) * 0.8)  # 低PERほど高得点

    # ── ピーター・リンチ: PEG比率（PER÷利益成長率）──
    # PEG < 0.5 → 割安な成長株の証（強力買いシグナル）
    # PEG < 1.0 → まだ割安
    peg = summary.get("peg_ratio")
    if peg is not None and peg > 0:
        if peg < 0.5:
            score += 15
        elif peg < 1.0:
            score += 8
        elif peg > 2.0:
            score -= 5   # 成長に比べて高すぎる

    # ── ウォーレン・バフェット: ROE（自己資本利益率）──
    # ROE > 25% → 非常に優秀な経営（バフェットの基準）
    roe = summary.get("roe")
    if roe is not None:
        if roe > 0.25:
            score += 15
        elif roe > 0.20:
            score += 12
        elif roe > 0.15:
            score += 6
        elif roe < 0:
            score -= 8   # 赤字企業はペナルティ

    # ── ウォーレン・バフェット: D/E比率（借金の少なさ）──
    # D/E < 0.3 → 借金がほとんどない優良財務
    de = summary.get("debt_to_equity")
    if de is not None:
        if de < 0.3:
            score += 8
        elif de < 0.8:
            score += 4
        elif de > 2.0:
            score -= 5   # 借金が多すぎる

    return score


def _calc_composite_score(summary: Dict) -> float:
    """
    複合スコア（高いほど優先度高）
    ファンダメンタル（清原式+リンチ+バフェット）+ テクニカル（Minervini+MACD+BB+ROC）
    """
    score = _calc_fundamental_score(summary) + _calc_technical_score(summary)
    return round(score, 2)


def _get_strategy_flags(summary: Dict) -> Dict:
    """各戦略の合否フラグを返す"""
    ma50  = summary.get("ma50")
    ma150 = summary.get("ma150")
    ma200 = summary.get("ma200")
    price = summary.get("current_price")

    # Minervini
    minervini_pass = 1 if (
        all(x is not None for x in [ma50, ma150, ma200, price]) and
        ma50 > ma150 > ma200 and price > ma50
    ) else 0

    # CAN-SLIM出来高
    canslim_pass = 1 if summary.get("vol_surge") else 0

    # リンチ: PEG < 1.0
    peg = summary.get("peg_ratio")
    lynch_pass = 1 if (peg is not None and 0 < peg < 1.0) else 0

    # バフェット: ROE>15% かつ D/E<0.8
    roe = summary.get("roe")
    de  = summary.get("debt_to_equity")
    buffett_pass = 1 if (
        roe is not None and roe > 0.15 and
        (de is None or de < 0.8)
    ) else 0

    # MACD: ゴールデンクロスまたはポジティブ
    macd_cross   = summary.get("macd_cross")
    macd_bullish = 1 if macd_cross in ("golden", "positive") else 0

    return {
        "minervini_pass": minervini_pass,
        "canslim_pass":   canslim_pass,
        "lynch_pass":     lynch_pass,
        "buffett_pass":   buffett_pass,
        "macd_bullish":   macd_bullish,
    }


def run_screening(verbose: bool = True) -> List[Dict]:
    """
    GLOBAL_WATCHLIST 全50銘柄をスクリーニングし、通過銘柄をDB保存して返す。
    スコア降順ソート。

    verbose=True の場合はコンソールにログを出力。
    """
    if verbose:
        print("\n" + "="*60)
        print("🔍 グローバルスクリーニング開始（7市場・50銘柄）")
        print("="*60)

    # 全銘柄のサマリーを取得（ファンダメンタル含む）
    if verbose:
        print("📊 全銘柄のデータを取得中（バッチダウンロード最適化済み）...")
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
        score      = _calc_composite_score(s)
        flags      = _get_strategy_flags(s)

        if verbose:
            status = "✅ 合格" if ok else "❌ 不合格"
            badges = ""
            if flags["minervini_pass"]: badges += "M"
            if flags["lynch_pass"]:     badges += "L"
            if flags["buffett_pass"]:   badges += "B"
            if flags["macd_bullish"]:   badges += "D"
            badge_str = f"[{badges}]" if badges else ""
            print(f"  {flag}{name}({ticker}): {status} スコア{score:.1f}{badge_str} — {reason}")

        if ok:
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
                "minervini_pass":  flags["minervini_pass"],
                "canslim_pass":    flags["canslim_pass"],
                "current_price":   s["current_price"],
                "rsi14":           s.get("rsi14"),
                "ma50":            s.get("ma50"),
                "ma150":           s.get("ma150"),
                "ma200":           s.get("ma200"),
                "lynch_pass":      flags["lynch_pass"],
                "buffett_pass":    flags["buffett_pass"],
                "macd_bullish":    flags["macd_bullish"],
                "roc20":           s.get("roc20"),
                "bb_width":        s.get("bb_width"),
                "roe":             s.get("roe"),
                "peg_ratio":       s.get("peg_ratio"),
            }
            save_screened_stock(data)
            passed.append(data)

    # スコア降順ソート
    passed.sort(key=lambda x: x["composite_score"], reverse=True)

    if verbose:
        print(f"\n{'='*60}")
        print(f"✅ スクリーニング完了: {len(summaries)}銘柄中 {len(passed)}銘柄が通過")
        if passed:
            print("📋 投資候補（スコア順）:")
            for p in passed:
                nc_str  = f"NC比率{p['net_cash_ratio']:.2f}" if p.get("net_cash_ratio") is not None else "NC比率-"
                per_str = f"PER{p['per']:.1f}" if p.get("per") is not None else "PER-"
                badges  = ""
                if p["minervini_pass"]: badges += "✅Minervini "
                if p["lynch_pass"]:     badges += "✅Lynch "
                if p["buffett_pass"]:   badges += "✅Buffett "
                if p["macd_bullish"]:   badges += "✅MACD "
                print(f"  {p['flag']}{p['name']}({p['ticker']}) "
                      f"スコア{p['composite_score']:.1f} {nc_str} {per_str} {badges}")

    return passed
