"""
投資シミュレーター メインサーバー（2戦略版）

  /          → static/index.html  （清原式ページ）
  /global    → static/global.html （グローバル複合スコアページ）

  /api/kiyohara/*  清原式エンドポイント（日本小型株・NC比率）
  /api/global/*    グローバル複合スコアエンドポイント（7市場・Minervini等）
  /api/markets     共通（市場ステータス）

ポート: 8001
"""
from datetime import datetime
from zoneinfo import ZoneInfo
from dotenv import load_dotenv
load_dotenv()

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.triggers.cron import CronTrigger

from database import (
    init_db, get_account, get_portfolio,
    get_trades, get_asset_history, reset_all,
    get_screened_stocks,
)
from market_hours import get_all_market_status, get_open_markets, MARKETS
from global_stocks import (
    get_stock_summary, get_fx_rates, GLOBAL_WATCHLIST,
    get_fx_rates_global, GLOBAL_WATCHLIST_7MKT,
)
from trader import (
    run_kiyohara_trading, run_global_trading_orig, calc_total_assets,
)

JST = ZoneInfo("Asia/Tokyo")

app = FastAPI(title="投資シミュレーター（清原式 + グローバル）", version="3.0.0")
app.mount("/static", StaticFiles(directory="static"), name="static")

init_db()

# ==================== スケジューラー ====================

scheduler = BackgroundScheduler(timezone=JST)


def scheduled_trade():
    """1分ごとに自動売買
    - 清原式: 東京証券取引所（TSE）が開いている時のみ実行（平日9:00〜15:30）
    - グローバル: 開いている市場があれば実行
    """
    open_markets = get_open_markets()

    # 清原式: TSEが開いている時のみ
    if "TSE" in open_markets:
        run_kiyohara_trading(force=False)

    # グローバル: どこかの市場が開いていれば
    if open_markets:
        run_global_trading_orig(force=False)


def scheduled_screening():
    """毎朝8時に清原式スクリーニングを自動実行"""
    print(f"\n⏰ 定期スクリーニング開始: {datetime.now(JST).strftime('%H:%M')}")
    from screener import run_kiyohara_screening
    run_kiyohara_screening(verbose=False)


scheduler.add_job(
    scheduled_trade,
    IntervalTrigger(minutes=5, timezone=JST),
    id="auto_trade",
    replace_existing=True,
)

scheduler.add_job(
    scheduled_screening,
    CronTrigger(hour=8, minute=0, timezone=JST),
    id="screening",
    replace_existing=True,
)

scheduler.start()
print("✅ 自動取引スケジューラーを開始しました（1分ごと）")
print("✅ スクリーニングスケジューラーを開始しました（毎朝8時 JST）")


# ==================== ページルーティング ====================

@app.get("/")
async def root():
    return FileResponse("static/index.html")


@app.get("/global")
async def global_page():
    return FileResponse("static/global.html")


# ==================== 共通エンドポイント ====================

@app.get("/api/markets")
async def get_markets():
    return get_all_market_status()


@app.get("/api/asset-history")
async def get_asset_history_api(days: int = 30):
    return {"history": get_asset_history(days)}


@app.get("/api/chart/{ticker}")
async def get_chart(ticker: str):
    # 両方のウォッチリストを確認
    wl = None
    if ticker in GLOBAL_WATCHLIST:
        wl = GLOBAL_WATCHLIST
    elif ticker in GLOBAL_WATCHLIST_7MKT:
        wl = GLOBAL_WATCHLIST_7MKT
    else:
        raise HTTPException(status_code=400, detail="対象外の銘柄です")
    summary = get_stock_summary(ticker, watchlist=wl)
    if not summary:
        raise HTTPException(status_code=404, detail="データ取得に失敗しました")
    return summary


@app.get("/api/scheduler/status")
async def scheduler_status():
    job = scheduler.get_job("auto_trade")
    next_run = job.next_run_time.astimezone(JST).isoformat() if job and job.next_run_time else None
    return {"running": scheduler.running, "next_run_time": next_run}


# ==================== 清原式エンドポイント ====================

def _build_portfolio_response(portfolio, fx_rates):
    """ポートフォリオデータを整形して返す（共通ヘルパー）"""
    if not portfolio:
        return {"holdings": [], "total_stock_value_jpy": 0}

    holdings  = []
    total_jpy = 0.0

    for h in portfolio:
        wl = GLOBAL_WATCHLIST_7MKT if h.get("strategy") == "global" else GLOBAL_WATCHLIST
        summary = get_stock_summary(h["ticker"], watchlist=wl)
        price_l = summary["current_price"] if summary else h["avg_cost_local"]
        fx_rate = fx_rates.get(h["currency"], 1.0)
        price_j = price_l * fx_rate

        market_val_jpy = price_j * h["shares"]
        cost_jpy       = h["avg_cost_jpy"] * h["shares"]
        unrealized     = market_val_jpy - cost_jpy
        unrealized_pct = unrealized / cost_jpy * 100 if cost_jpy else 0
        total_jpy     += market_val_jpy

        avg_cost   = h.get("avg_cost_local", price_l)
        peak_price = h.get("peak_price") or price_l
        stop_price = h.get("trailing_stop") or (price_l * 0.85)
        stop_pct_from_peak = ((stop_price / peak_price) - 1) * 100 if peak_price > 0 else -8.0
        target_price = avg_cost * 2

        holdings.append({
            "ticker":              h["ticker"],
            "name":                h["name"],
            "market":              h["market"],
            "currency":            h["currency"],
            "flag":                h["flag"],
            "shares":              h["shares"],
            "avg_cost_local":      round(h["avg_cost_local"], 4),
            "avg_cost_jpy":        round(h["avg_cost_jpy"], 1),
            "current_price_local": round(price_l, 4),
            "current_price_jpy":   round(price_j, 1),
            "market_value_jpy":    round(market_val_jpy, 0),
            "unrealized_pnl_jpy":  round(unrealized, 0),
            "unrealized_pct":      round(unrealized_pct, 2),
            "fx_rate":             round(fx_rate, 2),
            "peak_price_local":    round(peak_price, 4),
            "trailing_stop_local": round(stop_price, 4),
            "stop_pct_from_peak":  round(stop_pct_from_peak, 1),
            "partial_taken":       h.get("partial_taken", 0),
            "target_price_local":  round(target_price, 4),
            "buy_per":             h.get("buy_per"),
            "buy_pbr":             h.get("buy_pbr"),
            "buy_nc_ratio":        h.get("buy_nc_ratio"),
            "strategy":            h.get("strategy", "kiyohara"),
        })

    return {"holdings": holdings, "total_stock_value_jpy": round(total_jpy, 0)}


@app.get("/api/kiyohara/status")
async def kiyohara_status():
    account   = get_account()
    portfolio = get_portfolio(strategy='kiyohara')
    fx_rates  = get_fx_rates()
    total     = calc_total_assets(account["cash"], portfolio, fx_rates)
    pnl       = total - account["initial_capital"]
    return {
        "total_assets":    round(total, 0),
        "cash":            round(account["cash"], 0),
        "stock_value":     round(total - account["cash"], 0),
        "initial_capital": account["initial_capital"],
        "pnl":             round(pnl, 0),
        "pnl_pct":         round(pnl / account["initial_capital"] * 100, 2),
        "open_markets":    get_open_markets(),
        "updated_at":      datetime.now(JST).isoformat(),
    }


@app.get("/api/kiyohara/portfolio")
async def kiyohara_portfolio():
    portfolio = get_portfolio(strategy='kiyohara')
    return _build_portfolio_response(portfolio, get_fx_rates())


@app.get("/api/kiyohara/trades")
async def kiyohara_trades(limit: int = 50):
    return {"trades": get_trades(limit=limit, strategy='kiyohara')}


@app.get("/api/kiyohara/screening")
async def kiyohara_screening():
    return {"candidates": get_screened_stocks(strategy='kiyohara')}


@app.post("/api/kiyohara/screening/run")
async def run_kiyohara_screening_api():
    from screener import run_kiyohara_screening
    results = run_kiyohara_screening(verbose=False)
    return {
        "status":      "ok",
        "count":       len(results),
        "candidates":  results,
        "executed_at": datetime.now(JST).isoformat(),
    }


@app.post("/api/kiyohara/trade/run")
async def run_kiyohara_trade_now():
    print("🔴 清原式: 手動トリガーによる取引を開始")
    result = run_kiyohara_trading(force=True)
    return result


@app.post("/api/kiyohara/reset")
async def kiyohara_reset():
    reset_all(strategy='kiyohara')
    return {"status": "ok", "message": "清原式データをリセットしました"}


@app.get("/api/kiyohara/watchlist")
async def kiyohara_watchlist():
    fx_rates = get_fx_rates()
    return {
        "watchlist": [
            {
                "ticker":   t,
                "name":     info["name"],
                "market":   info["market"],
                "currency": info["currency"],
                "flag":     info["flag"],
                "fx_rate":  fx_rates.get(info["currency"], 1.0),
            }
            for t, info in GLOBAL_WATCHLIST.items()
        ]
    }


# ==================== グローバル複合スコアエンドポイント ====================

@app.get("/api/global/status")
async def global_status():
    account   = get_account('global')
    portfolio = get_portfolio(strategy='global')
    fx_rates  = get_fx_rates_global()
    from trader import calc_total_assets_global
    total     = calc_total_assets_global(account["cash"], portfolio, fx_rates)
    pnl       = total - account["initial_capital"]
    return {
        "total_assets":    round(total, 0),
        "cash":            round(account["cash"], 0),
        "stock_value":     round(total - account["cash"], 0),
        "initial_capital": account["initial_capital"],
        "pnl":             round(pnl, 0),
        "pnl_pct":         round(pnl / account["initial_capital"] * 100, 2),
        "open_markets":    get_open_markets(),
        "updated_at":      datetime.now(JST).isoformat(),
    }


@app.get("/api/global/portfolio")
async def global_portfolio():
    portfolio = get_portfolio(strategy='global')
    return _build_portfolio_response(portfolio, get_fx_rates_global())


@app.get("/api/global/trades")
async def global_trades(limit: int = 50):
    return {"trades": get_trades(limit=limit, strategy='global')}


@app.get("/api/global/screening")
async def global_screening_api():
    return {"candidates": get_screened_stocks(strategy='global')}


@app.post("/api/global/screening/run")
async def run_global_screening_api():
    from screener import run_global_screening
    results = run_global_screening(verbose=False)
    return {
        "status":      "ok",
        "count":       len(results),
        "candidates":  results,
        "executed_at": datetime.now(JST).isoformat(),
    }


@app.post("/api/global/trade/run")
async def run_global_trade_now():
    print("🔴 グローバル: 手動トリガーによる取引を開始")
    result = run_global_trading_orig(force=True)
    return result


@app.post("/api/global/reset")
async def global_reset():
    reset_all(strategy='global')
    return {"status": "ok", "message": "グローバルデータをリセットしました"}


@app.get("/api/global/watchlist")
async def global_watchlist():
    fx_rates = get_fx_rates_global()
    return {
        "watchlist": [
            {
                "ticker":   t,
                "name":     info["name"],
                "market":   info["market"],
                "currency": info["currency"],
                "flag":     info["flag"],
                "fx_rate":  fx_rates.get(info["currency"], 1.0),
            }
            for t, info in GLOBAL_WATCHLIST_7MKT.items()
        ]
    }


# ==================== 後方互換エンドポイント（旧URL）====================

@app.get("/api/status")
async def get_status():
    return await kiyohara_status()


@app.get("/api/portfolio")
async def get_portfolio_api():
    return await kiyohara_portfolio()


@app.get("/api/trades")
async def get_trades_api(limit: int = 50):
    return await kiyohara_trades(limit=limit)


@app.get("/api/screening")
async def get_screening_api():
    return await kiyohara_screening()


@app.post("/api/screening/run")
async def run_screening_api():
    return await run_kiyohara_screening_api()


@app.post("/api/trade/run")
async def run_trade_now():
    return await run_kiyohara_trade_now()


@app.post("/api/reset")
async def reset():
    reset_all()
    return {"status": "ok", "message": "全データをリセットしました。初期資金200万円からスタートします。"}


@app.get("/api/watchlist")
async def get_watchlist():
    return await kiyohara_watchlist()


@app.get("/api/sector/summary")
async def get_sector_summary():
    portfolio = get_portfolio(strategy='kiyohara')
    fx_rates  = get_fx_rates()
    sector_map: dict = {}
    total_jpy = 0.0

    for h in portfolio:
        info    = GLOBAL_WATCHLIST.get(h["ticker"], {})
        sector  = info.get("sector", "その他")
        val_jpy = h.get("avg_cost_jpy", 0) * h.get("shares", 0)
        total_jpy += val_jpy
        sector_map[sector] = sector_map.get(sector, 0.0) + val_jpy

    result = []
    for sector, val in sorted(sector_map.items(), key=lambda x: -x[1]):
        pct = val / total_jpy * 100 if total_jpy > 0 else 0
        result.append({
            "sector":          sector,
            "value_jpy":       round(val, 0),
            "pct":             round(pct, 1),
            "is_concentrated": pct > 30,
        })

    return {
        "sectors":           result,
        "has_concentration": any(r["is_concentrated"] for r in result),
        "total_stock_jpy":   round(total_jpy, 0),
    }


@app.post("/api/backtest/run")
async def run_backtest_api(days: int = 365):
    import asyncio
    from backtest import run_backtest as _run_backtest
    loop   = asyncio.get_event_loop()
    result = await loop.run_in_executor(None, lambda: _run_backtest(days))
    return result


if __name__ == "__main__":
    import uvicorn
    print("🇯🇵 清原式 & 🌍 グローバル 投資シミュレーター v3.0 を起動します")
    print("📌 清原式: http://localhost:8000/")
    print("📌 グローバル: http://localhost:8000/global")
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=False)
