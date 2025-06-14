
@echo off

REM Ensure working directory
cd /d "C:\Users\user\Google Drive\Projects\Electricity Prices"

REM Start logging
echo [%date% %time%] Starting update script... >> logs\update_log.txt

REM Activate Conda environment and run script
call conda activate mc_vic
python scripts\TradingIS_price_current_imp.py >> logs\update_log.txt 2>&1

REM Finish logging
echo [%date% %time%] Finished update script. >> logs\update_log.txt
