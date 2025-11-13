@echo off
chcp 65001 >nul
cd /d "C:\Users\j3210\OneDrive\桌面\ETF網站執行檔"

echo ====== 開始執行 主動ETF爬蟲 (Python) ======
python "C:\Users\j3210\OneDrive\桌面\ETF網站執行檔\981A ETF.py"
echo ====== 爬蟲完成 ======
echo.

echo ====== 開始執行 資料處理 (Python) ======
python "C:\Users\j3210\OneDrive\桌面\ETF網站執行檔\stock_history_data1.py"
echo ====== 資料處理完成 ======
echo.



exit /b 0
