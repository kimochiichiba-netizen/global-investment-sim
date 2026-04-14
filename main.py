"""
グローバル投資シミュレーター メインサーバー
世界5市場・15銘柄を対象に24時間自動売買を行います
ポート: 8001（既存の清原式シミュレーターと共存可能）
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

from database import (
    init_db, get_account, get_portfolio,
    get_trades, get_asset_history, reset_all,
)
from market_hours import get_all_market_status, get_open_markets, MARKETS
from global_stocks import (
    get_stock_summary, get_fx_rates, GLOBAL_WATCHLIST,
)
from trader import run_global_trading, calc_total_assets

JST = ZoneInfo("Asia/Tokyo")

app = FastAPI(title="🌍 グローバル投資シミュレーター", version="1.0.0")
app.mount("/static", StaticFiles(directory="static"), name="static")

init_db()

# ==================== スケジューラー ====================

scheduler = BackgroundScheduler(timezone=JST)

def scheduled_trade():
    """1分ごとに自動実行（市場が開いていれば取引、なければスキップ）"""
    open_markets = get_open_markets()
    if not open_markets:
        return   # 全市場クローズ時はスキップ（ログも出さない）
    run_global_trading(force=False)

scheduler.add_job(
    scheduled_trade,
    IntervalTrigger(minutes=1, timezone=JST),
    id="global_auto_trade",
    replace_existing=True,
)
scheduler.start()
print("✅ グローバル自動取引スケジューラーを開始しました（1分ごと・市場オープン時のみ）")


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
    """全市場の開閉状態・現地時刻を返す"""
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
        # 現在価格を取得（取得できない場合は取得単価で代用）
        summary = get_stock_summary(h["ticker"])
        price_l = summary["current_price"] if summary else h["avg_cost_local"]
        fx_rate = fx_rates.get(h["currency"], 1.0)
        price_j = price_l * fx_rate

        market_val_jpy = price_j * h["shares"]
        cost_jpy       = h["avg_cost_jpy"] * h["shares"]
        unrealized     = market_val_jpy - cost_jpy
        unrealized_pct = unrealized / cost_jpy * 100 if cost_jpy else 0
        total_jpy     += market_val_jpy

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
    print("🌍 グローバル投資シミュレーターを起動します")
    print("📌 ブラウザで http://localhost:8001 を開いてください")
    uvicorn.run("main:app", host="0.0.0.0", port=8001, reload=False)
