@echo off
REM NRGkick Statistik/HTML-Report per Doppelklick starten
setlocal
cd /d "%~dp0"

REM venv anlegen, falls nicht vorhanden
if not exist ".venv\Scripts\python.exe" (
    echo [setup] erstelle virtuelle Umgebung...
    python -m venv .venv || goto :error
    ".venv\Scripts\python.exe" -m pip install --upgrade pip >nul
    ".venv\Scripts\python.exe" -m pip install -r requirements.txt || goto :error
)

echo [start] Erzeuge HTML-Report...
".venv\Scripts\python.exe" "%~dp0nrgkick_stats.py"
if errorlevel 1 goto :error

echo.
echo [ok] Report-Erstellung abgeschlossen.
pause
goto :eof

:error
echo.
echo [fehler] Statistik-Erstellung fehlgeschlagen.
pause
exit /b 1
