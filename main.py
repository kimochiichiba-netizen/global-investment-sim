"""
グローバル投資シミュレーター メインサーバー
世界5市場・15銘柄を対象に24時間自動売買を行います
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
)
from trader import run_global_trading, calc_total_assets

JST = ZoneInfo("Asia/Tokyo")

app = FastAPI(title="🌍 グローバル投資シミュレーター", version="2.0.0")
app.mount("/static", StaticFiles(directory="static"), name="static")

init_db()

# ==================== スケジューラー ====================

scheduler = BackgroundScheduler(timezone=JST)


def scheduled_trade():
    """1分ごとに自動実行（市場が開いていれば取引、なければスキップ）"""
    open_markets = get_open_markets()
    if not open_markets:
        return
    run_global_trading(force=False)


def scheduled_screening():
    """毎朝8時にスクリーニングを自動実行"""
    print(f"\n⏰ 定期スクリーニング開始: {datetime.now(JST).strftime('%H:%M')}")
    from screener import run_screening
    run_screening(verbose=True)


scheduler.add_job(
    scheduled_trade,
    IntervalTrigger(minutes=1, timezone=JST),
    id="global_auto_trade",
    replace_existing=True,
)

scheduler.add_job(
    scheduled_screening,
    CronTrigger(hour=8, minute=0, timezone=JST),
    id="global_screening",
    replace_existing=True,
)

scheduler.start()
print("✅ グローバル自動取引スケジューラーを開始しました（1分ごと・市場オープン時のみ）")
print("✅ スクリーニングスケジューラーを開始しました（毎朝8時 JST）")


# ==================== APIエンドポイント ====================

@app.get("/")
async def root():
    return FileResponse("static/index.html")


@app.get("/api/status")
async def get_status():
    account   = get_account()
    portfolio = get_portfolio()
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


@app.get("/api/markets")
async def get_markets():
    return get_all_market_status()


@app.get("/api/portfolio")
async def get_portfolio_api():
    portfolio = get_portfolio()
    if not portfolio:
        return {"holdings": [], "total_stock_value_jpy": 0}

    fx_rates = get_fx_rates()
    holdings = []
    total_jpy = 0.0

    for h in portfolio:
        summary = get_stock_summary(h["ticker"])
        price_l = summary["current_price"] if summary else h["avg_cost_local"]
        fx_rate = fx_rates.get(h["currency"], 1.0)
        price_j = price_l * fx_rate

        market_val_jpy = price_j * h["shares"]
        cost_jpy       = h["avg_cost_jpy"] * h["shares"]
        unrealized     = market_val_jpy - cost_jpy
        unrealized_pct = unrealized / cost_jpy * 100 if cost_jpy else 0
        total_jpy     += market_val_jpy

        # トレーリングストップの状態
        avg_cost   = h.get("avg_cost_local", price_l)
        peak_price = h.get("peak_price") or price_l
        stop_price = h.get("trailing_stop") or (price_l * 0.92)
        stop_pct_from_peak = ((stop_price / peak_price) - 1) * 100 if peak_price > 0 else -8.0
        target_price = avg_cost * 2  # 目標: 2倍

        holdings.append({
            "ticker":          h["ticker"],
            "name":            h["name"],
            "market":          h["market"],
            "currency":        h["currency"],
            "flag":            h["flag"],
            "shares":          h["shares"],
            "avg_cost_local":  round(h["avg_cost_local"], 4),
            "avg_cost_jpy":    round(h["avg_cost_jpy"], 1),
            "current_price_local": round(price_l, 4),
            "current_price_jpy":   round(price_j, 1),
            "market_value_jpy":    round(market_val_jpy, 0),
            "unrealized_pnl_jpy":  round(unrealized, 0),
            "unrealized_pct":      round(unrealized_pct, 2),
            "fx_rate":         round(fx_rate, 2),
            # トレーリングストップ情報
            "peak_price_local":    round(peak_price, 4),
            "trailing_stop_local": round(stop_price, 4),
            "stop_pct_from_peak":  round(stop_pct_from_peak, 1),
            "partial_taken":       h.get("partial_taken", 0),
            "target_price_local":  round(target_price, 4),
            # 購入時指標
            "buy_per":       h.get("buy_per"),
            "buy_pbr":       h.get("buy_pbr"),
            "buy_nc_ratio":  h.get("buy_nc_ratio"),
        })

    return {"holdings": holdings, "total_stock_value_jpy": round(total_jpy, 0)}


@app.get("/api/trades")
async def get_trades_api(limit: int = 50):
    return {"trades": get_trades(limit)}


@app.get("/api/asset-history")
async def get_asset_history_api(days: int = 30):
    return {"history": get_asset_history(days)}


@app.get("/api/chart/{ticker}")
async def get_chart(ticker: str):
    if ticker not in GLOBAL_WATCHLIST:
        raise HTTPException(status_code=400, detail="対象外の銘柄です")
    summary = get_stock_summary(ticker)
    if not summary:
        raise HTTPException(status_code=404, detail="データ取得に失敗しました")
    return summary


@app.get("/api/watchlist")
async def get_watchlist():
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


@app.get("/api/scheduler/status")
async def scheduler_status():
    job = scheduler.get_job("global_auto_trade")
    next_run = job.next_run_time.astimezone(JST).isoformat() if job and job.next_run_time else None
    return {"running": scheduler.running, "next_run_time": next_run}


# ── スクリーニング API ──────────────────────────────────

@app.get("/api/screening")
async def get_screening_api():
    """DB保存済みのスクリーニング結果を高速返却"""
    return {"candidates": get_screened_stocks()}


@app.post("/api/screening/run")
async def run_screening_api():
    """
    スクリーニングを即時実行（yfinanceAPIコールが多く30秒〜2分かかる場合あり）。
    実行後、最新結果を返す。
    """
    from screener import run_screening
    results = run_screening(verbose=False)
    return {
        "status":     "ok",
        "count":      len(results),
        "candidates": results,
        "executed_at": datetime.now(JST).isoformat(),
    }


# ── 取引・リセット API ───────────────────────────────────

@app.post("/api/trade/run")
async def run_trade_now():
    """手動で即時取引を実行（市場開閉に関係なく全銘柄対象）"""
    print("🔴 手動トリガーによる取引を開始")
    result = run_global_trading(force=True)
    return result


@app.post("/api/reset")
async def reset():
    reset_all()
    return {"status": "ok", "message": "リセット完了。初期資金200万円からスタートします。"}


if __name__ == "__main__":
    import uvicorn
    print("🌍 グローバル投資シミュレーター v2.0 を起動します")
    print("📌 ブラウザで http://localhost:8001 を開いてください")
    uvicorn.run("main:app", host="0.0.0.0", port=8001, reload=False)
