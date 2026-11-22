@echo off
REM Double-click launcher for the GUI (no build needed; runs from source).
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
)

start "" ".venv\Scripts\pythonw.exe" -m tgdl gui
