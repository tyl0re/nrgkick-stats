@echo off
REM Dienst neu starten (UAC-Abfrage erscheint). Danach Status anzeigen.
setlocal
cd /d "%~dp0"
powershell.exe -NoProfile -ExecutionPolicy Bypass -Command ^
  "Start-Process powershell -Verb RunAs -ArgumentList '-NoExit','-NoProfile','-ExecutionPolicy','Bypass','-File','%~dp0service.ps1','-Action','restart'"
