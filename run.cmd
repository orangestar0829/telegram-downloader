@echo off
cd /d "%~dp0"

if not exist ".venv\Scripts\python.exe" (
    echo Creating virtualenv...
    py -3 -m venv .venv 2>nul || python -m venv .venv || exit /b 1
)

fc /b requirements.txt ".venv\tgdl-requirements.installed" >nul 2>&1
if errorlevel 1 (
    ".venv\Scripts\python.exe" -m pip install -r requirements.txt || exit /b 1
    copy /y requirements.txt ".venv\tgdl-requirements.installed" >nul || exit /b 1
)

if not exist ".env" (
    copy /Y ".env.example" ".env" >nul
    echo Created .env
    echo Fill TELEGRAM_API_ID and TELEGRAM_API_HASH from https://my.telegram.org
    notepad ".env"
    pause
)

".venv\Scripts\python.exe" -m tgdl %*
if errorlevel 1 pause
