@echo off
REM Doppelklick-freundlicher Installer fuer den NRGkick Logger als Windows-Dienst.
REM Startet service.ps1 install in einer Admin-PowerShell (UAC-Abfrage erscheint).
setlocal
cd /d "%~dp0"
powershell.exe -NoProfile -ExecutionPolicy Bypass -Command ^
  "Start-Process powershell -Verb RunAs -ArgumentList '-NoExit','-NoProfile','-ExecutionPolicy','Bypass','-File','%~dp0service.ps1','-Action','install'"
