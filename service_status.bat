@echo off
REM Zeigt Status + letzte Log-Zeilen (ohne Admin).
setlocal
cd /d "%~dp0"
powershell.exe -NoProfile -ExecutionPolicy Bypass -File "%~dp0service.ps1" -Action status
pause
