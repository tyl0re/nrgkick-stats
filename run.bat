@echo off
REM NRGkick Logger - manueller Start
setlocal
cd /d "%~dp0"

REM venv anlegen, falls nicht vorhanden
if not exist ".venv\Scripts\python.exe" (
    echo [setup] erstelle virtuelle Umgebung...
    python -m venv .venv || goto :error
    ".venv\Scripts\python.exe" -m pip install --upgrade pip >nul
    ".venv\Scripts\python.exe" -m pip install -r requirements.txt || goto :error
)

".venv\Scripts\python.exe" "%~dp0nrgkick_logger.py"
goto :eof

:error
echo [fehler] Setup fehlgeschlagen.
exit /b 1
