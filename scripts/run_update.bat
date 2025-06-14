REM Update script for local use. Adjust paths as needed.

@echo off
REM Call the Conda environment and run prices import update script
cd "C:\Users\user\Google Drive\Projects\Electricity Prices\scripts"
"C:\Users\user\miniconda3\envs\mc_vic\python.exe" TradingIS_price_current_imp.py >> logs\update_log.txt 2>&1
