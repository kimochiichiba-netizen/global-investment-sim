"""
世界の株式市場の営業時間管理
各市場のローカル時刻で開閉を判定します
"""
from datetime import datetime
from zoneinfo import ZoneInfo
from typing import Dict

# 各市場の定義
MARKETS = {
    "TSE": {
        "name":     "東京",
        "tz":       "Asia/Tokyo",
        "open":     (9, 0),
        "close":    (15, 30),
        "flag":     "🇯🇵",
        "currency": "JPY",
        "color":    "#f97316",   # オレンジ
    },
    "HKEX": {
        "name":     "香港",
        "tz":       "Asia/Hong_Kong",
        "open":     (9, 30),
        "close":    (16, 0),
        "flag":     "🇭🇰",
        "currency": "HKD",
        "color":    "#ef4444",   # レッド
    },
    "LSE": {
        "name":     "ロンドン",
        "tz":       "Europe/London",
        "open":     (8, 0),
        "close":    (16, 30),
        "flag":     "🇬🇧",
        "currency": "GBP",
        "color":    "#3b82f6",   # ブルー
    },
    "DAX": {
        "name":     "フランクフルト",
        "tz":       "Europe/Berlin",
        "open":     (9, 0),
        "close":    (17, 30),
        "flag":     "🇩🇪",
        "currency": "EUR",
        "color":    "#f59e0b",   # イエロー
    },
    "NYSE": {
        "name":     "ニューヨーク",
        "tz":       "America/New_York",
        "open":     (9, 30),
        "close":    (16, 0),
        "flag":     "🇺🇸",
        "currency": "USD",
        "color":    "#22c55e",   # グリーン
    },
    "KOSPI": {
        "name":     "ソウル",
        "tz":       "Asia/Seoul",
        "open":     (9, 0),
        "close":    (15, 30),
        "flag":     "🇰🇷",
        "currency": "KRW",
        "color":    "#6366f1",   # インディゴ
    },
    "ASX": {
        "name":     "シドニー",
        "tz":       "Australia/Sydney",
        "open":     (10, 0),
        "close":    (16, 0),
        "flag":     "🇦🇺",
        "currency": "AUD",
        "color":    "#10b981",   # エメラルド
    },
}


def is_market_open(market_key: str) -> bool:
    """
    指定した市場が今開いているか判定する
    土日は全市場クローズ
    """
    market = MARKETS.get(market_key)
    if not market:
        return False

    tz = ZoneInfo(market["tz"])
    now = datetime.now(tz)

    # 土日はクローズ
    if now.weekday() >= 5:
        return False

    open_h,  open_m  = market["open"]
    close_h, close_m = market["close"]

    open_minutes  = open_h  * 60 + open_m
    close_minutes = close_h * 60 + close_m
    now_minutes   = now.hour * 60 + now.minute

    return open_minutes <= now_minutes < close_minutes


def get_all_market_status() -> Dict:
    """全市場の状態（開閉・現地時刻・JST換算）を返す"""
    JST = ZoneInfo("Asia/Tokyo")
    result = {}

    for key, market in MARKETS.items():
        tz  = ZoneInfo(market["tz"])
        now = datetime.now(tz)
        now_jst = datetime.now(JST)

        result[key] = {
            "name":          market["name"],
            "flag":          market["flag"],
            "color":         market["color"],
            "currency":      market["currency"],
            "is_open":       is_market_open(key),
            "local_time":    now.strftime("%H:%M"),
            "local_date":    now.strftime("%m/%d"),
            "open_time":     f"{market['open'][0]:02d}:{market['open'][1]:02d}",
            "close_time":    f"{market['close'][0]:02d}:{market['close'][1]:02d}",
        }

    return result


def get_open_markets() -> list:
    """現在開いている市場のキーリストを返す"""
    return [k for k in MARKETS if is_market_open(k)]
