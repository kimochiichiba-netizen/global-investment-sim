#!/bin/bash
# グローバル投資シミュレーター 起動スクリプト

cd "$(dirname "$0")"

echo "📦 必要なパッケージをインストール中..."
pip3 install -r requirements.txt -q

echo ""
echo "🌍 グローバル投資シミュレーターを起動します..."
echo "📌 ブラウザで http://localhost:8001 を開いてください"
echo ""

# ブラウザを少し遅れて開く（サーバー起動待ち）
sleep 2 && open http://localhost:8001 &

python3 main.py
