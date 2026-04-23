<#
.SYNOPSIS
    Steuerung des NRGkick Logger als Windows-Dienst.

.DESCRIPTION
    Ein Einstiegspunkt fuer alle Dienst-Operationen. Nutzt NSSM, um das
    Python-Script als "richtigen" Windows-Dienst zu betreiben (Autostart,
    automatischer Neustart bei Crash, kein Konsolenfenster noetig).

    Aktionen:
        install    - venv anlegen, NSSM holen (falls fehlt), Dienst
                     registrieren und starten                  (Admin)
        uninstall  - Dienst stoppen und entfernen              (Admin)
        start      - Dienst starten                            (Admin)
        stop       - Dienst stoppen                            (Admin)
        restart    - Dienst neu starten                        (Admin)
        status     - aktuellen Lauf-Status, letzte Log-Zeilen,
                     letzte DB-Eintraege anzeigen
        logs       - log-Datei "tailen" (Strg+C zum Beenden)

.PARAMETER Action
    Was gemacht werden soll - siehe oben.

.EXAMPLE
    # (als normaler User) Status anzeigen
    powershell -ExecutionPolicy Bypass -File .\service.ps1 status

.EXAMPLE
    # (als Administrator) Dienst installieren
    powershell -ExecutionPolicy Bypass -File .\service.ps1 install
#>
[CmdletBinding()]
param(
    [Parameter(Position = 0)]
    [ValidateSet('install','uninstall','start','stop','restart','status','logs')]
    [string]$Action = 'status',

    [string]$ServiceName = '',       # aus config.json -> service.service_name (Default: NRGkickLogger)
    [string]$NssmPath    = '',       # optional; sonst wird .\tools\nssm.exe genutzt
    [string]$ConfigFile  = ''
)

$ErrorActionPreference = 'Stop'

# ---------------------------------------------------------------------------
# Pfade
# ---------------------------------------------------------------------------
$ScriptDir  = Split-Path -Parent $MyInvocation.MyCommand.Path
$VenvPython = Join-Path $ScriptDir '.venv\Scripts\python.exe'
$LoggerPy   = Join-Path $ScriptDir 'nrgkick_logger.py'
$ReqFile    = Join-Path $ScriptDir 'requirements.txt'
$ToolsDir   = Join-Path $ScriptDir 'tools'

# Config auslesen fuer Defaults (service_name, data_dir, ...)
function Read-ConfigValues {
    $result = @{
        ServiceName   = 'NRGkickLogger'
        DisplayName   = 'NRGkick Logger'
        Description   = 'Zeichnet NRGkick-Daten periodisch in eine SQLite-DB auf.'
        DataDir       = Join-Path $env:LOCALAPPDATA 'NRGkickLogger'
        DbFile        = $null
        LogFile       = $null
        NssmDownload  = 'https://nssm.cc/release/nssm-2.24.zip'
    }
    if (-not (Test-Path $VenvPython)) {
        return $result
    }
    try {
        $json = & $VenvPython -c @"
import json, sys, nrgkick_config as c
cfg = c.load_config($(if ($ConfigFile) { "'" + $ConfigFile.Replace("\", "/") + "'" } else { 'None' }))
svc = cfg.get('service', {}) or {}
out = {
    'ServiceName':  svc.get('service_name', 'NRGkickLogger'),
    'DisplayName':  svc.get('display_name', 'NRGkick Logger'),
    'Description':  svc.get('description', ''),
    'DataDir':      str(c.data_dir_from(cfg)),
    'DbFile':       str(c.db_path(cfg)),
    'LogFile':      str(c.log_path(cfg)),
    'NssmDownload': svc.get('nssm_download', 'https://nssm.cc/release/nssm-2.24.zip'),
    'NssmPath':     svc.get('nssm_path', ''),
}
print(json.dumps(out))
"@ 2>$null
        if ($json) {
            $parsed = $json | ConvertFrom-Json
            foreach ($key in $parsed.PSObject.Properties.Name) {
                if ($parsed.$key) { $result[$key] = $parsed.$key }
            }
        }
    } catch { }
    return $result
}

$CfgVals = Read-ConfigValues

if (-not $ServiceName -or $ServiceName -eq '') {
    $ServiceName = $CfgVals.ServiceName
}
$DataDir = $CfgVals.DataDir
$LogFile = if ($CfgVals.LogFile) { $CfgVals.LogFile } else { Join-Path $DataDir 'nrgkick.log' }
$DbFile  = if ($CfgVals.DbFile)  { $CfgVals.DbFile }  else { Join-Path $DataDir 'nrgkick.db' }

if (-not $NssmPath -or $NssmPath -eq '') {
    if ($CfgVals.NssmPath) {
        $NssmPath = $CfgVals.NssmPath
    } else {
        $NssmPath = Join-Path $ToolsDir 'nssm.exe'
    }
}

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
function Test-Admin {
    $principal = New-Object Security.Principal.WindowsPrincipal(
        [Security.Principal.WindowsIdentity]::GetCurrent())
    return $principal.IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)
}

function Require-Admin {
    if (Test-Admin) { return }
    Write-Host ""
    Write-Host "Die Aktion '$Action' benoetigt Administrator-Rechte." -ForegroundColor Yellow
    Write-Host "Windows zeigt gleich eine UAC-Abfrage - bitte mit 'Ja' bestaetigen..." -ForegroundColor Yellow
    $argList = @(
        '-NoProfile',
        '-ExecutionPolicy', 'Bypass',
        '-File', "`"$($MyInvocation.MyCommand.Path)`"",
        '-Action', $Action,
        '-ServiceName', $ServiceName
    )
    if ($NssmPath -and $NssmPath -ne '') {
        $argList += @('-NssmPath', "`"$NssmPath`"")
    }
    # Ein neues Konsolenfenster starten, damit Output sichtbar bleibt
    try {
        $p = Start-Process -FilePath 'powershell.exe' `
            -ArgumentList $argList `
            -Verb RunAs `
            -PassThru -Wait
        exit $p.ExitCode
    } catch {
        Write-Err ("Hochstufung abgebrochen oder fehlgeschlagen: {0}" -f $_.Exception.Message)
        Write-Host ""
        Write-Host "Manueller Weg: PowerShell als Administrator oeffnen, dann:" -ForegroundColor Cyan
        Write-Host "  cd `"$ScriptDir`""
        Write-Host "  .\service.ps1 $Action"
        exit 1
    }
}

function Write-Info  ($msg) { Write-Host "[info] $msg"  -ForegroundColor Cyan }
function Write-Ok    ($msg) { Write-Host "[ok]   $msg"  -ForegroundColor Green }
function Write-Warn2 ($msg) { Write-Host "[warn] $msg"  -ForegroundColor Yellow }
function Write-Err   ($msg) { Write-Host "[err]  $msg"  -ForegroundColor Red }

function Ensure-Nssm {
    if (Test-Path $NssmPath) { return $NssmPath }
    Write-Info "NSSM nicht gefunden - lade automatisch nach $NssmPath ..."
    if (-not (Test-Path $ToolsDir)) { New-Item -ItemType Directory -Path $ToolsDir | Out-Null }

    $zipUrl = if ($CfgVals.NssmDownload) { $CfgVals.NssmDownload } else { 'https://nssm.cc/release/nssm-2.24.zip' }
    $zipPath = Join-Path $ToolsDir 'nssm-2.24.zip'
    $extractDir = Join-Path $ToolsDir 'nssm-2.24'

    try {
        # TLS 1.2 erzwingen (aeltere PS-Versionen)
        [System.Net.ServicePointManager]::SecurityProtocol = [System.Net.SecurityProtocolType]::Tls12
        Invoke-WebRequest -Uri $zipUrl -OutFile $zipPath -UseBasicParsing
    } catch {
        throw "Download von NSSM fehlgeschlagen ($zipUrl): $($_.Exception.Message). " +
              "Lade nssm.exe manuell herunter (https://nssm.cc/download) und lege sie unter '$NssmPath' ab."
    }

    if (Test-Path $extractDir) { Remove-Item -Recurse -Force $extractDir }
    Expand-Archive -Path $zipPath -DestinationPath $ToolsDir -Force

    $arch = if ([Environment]::Is64BitOperatingSystem) { 'win64' } else { 'win32' }
    $src = Join-Path $extractDir "$arch\nssm.exe"
    if (-not (Test-Path $src)) {
        throw "nssm.exe nach Entpacken nicht gefunden: $src"
    }
    Copy-Item -Force $src $NssmPath
    Remove-Item -Recurse -Force $extractDir, $zipPath
    Write-Ok "NSSM installiert: $NssmPath"
    return $NssmPath
}

function Ensure-Venv {
    if (Test-Path $VenvPython) {
        Write-Info "venv existiert bereits: $VenvPython"
        return
    }
    Write-Info "lege virtuelle Umgebung an: $ScriptDir\.venv ..."
    $py = (Get-Command python -ErrorAction SilentlyContinue)
    if (-not $py) {
        throw "Python im PATH nicht gefunden. Bitte Python 3.10+ installieren."
    }
    & python -m venv (Join-Path $ScriptDir '.venv')
    & $VenvPython -m pip install --upgrade pip | Out-Null
    & $VenvPython -m pip install -r $ReqFile
    Write-Ok "venv + requirements installiert"
}

function Get-Service-State {
    $svc = Get-Service -Name $ServiceName -ErrorAction SilentlyContinue
    return $svc
}

# ---------------------------------------------------------------------------
# Aktionen
# ---------------------------------------------------------------------------

function Action-Install {
    Require-Admin
    Ensure-Venv
    Ensure-Nssm | Out-Null

    if (-not (Test-Path $DataDir)) {
        New-Item -ItemType Directory -Path $DataDir | Out-Null
        Write-Ok "Datenordner angelegt: $DataDir"
    }

    # alten Dienst ggf. ersetzen
    if (Get-Service-State) {
        Write-Warn2 "bestehender Dienst '$ServiceName' wird ersetzt ..."
        & $NssmPath stop   $ServiceName confirm | Out-Null
        Start-Sleep -Milliseconds 500
        & $NssmPath remove $ServiceName confirm | Out-Null
        Start-Sleep -Seconds 1
    }

    Write-Info "registriere Dienst '$ServiceName' ..."
    & $NssmPath install $ServiceName $VenvPython $LoggerPy | Out-Null
    & $NssmPath set $ServiceName AppDirectory      $ScriptDir         | Out-Null
    & $NssmPath set $ServiceName DisplayName       $CfgVals.DisplayName | Out-Null
    & $NssmPath set $ServiceName Description       $CfgVals.Description | Out-Null
    & $NssmPath set $ServiceName Start             SERVICE_AUTO_START | Out-Null
    & $NssmPath set $ServiceName AppStdout         (Join-Path $DataDir 'service.stdout.log') | Out-Null
    & $NssmPath set $ServiceName AppStderr         (Join-Path $DataDir 'service.stderr.log') | Out-Null
    & $NssmPath set $ServiceName AppRotateFiles    1        | Out-Null
    & $NssmPath set $ServiceName AppRotateOnline   1        | Out-Null
    & $NssmPath set $ServiceName AppRotateBytes    2000000  | Out-Null
    & $NssmPath set $ServiceName AppExit           Default Restart | Out-Null
    & $NssmPath set $ServiceName AppRestartDelay   5000     | Out-Null
    # damit der Prozess bei Systemstart nicht zu frueh losrennt
    & $NssmPath set $ServiceName AppStopMethodSkip 6        | Out-Null
    # WICHTIG: Der Dienst laeuft unter LocalSystem und hat damit ein anderes
    # LOCALAPPDATA. Wir erzwingen per NRGKICK_DATA_DIR denselben User-Pfad wie
    # bei manuellem Start, damit Logger und Stats-Tool auf *eine* DB schauen.
    & $NssmPath set $ServiceName AppEnvironmentExtra "NRGKICK_DATA_DIR=$DataDir" | Out-Null

    Start-Service $ServiceName
    Write-Ok "Dienst '$ServiceName' installiert und gestartet"
    Action-Status
}

function Action-Uninstall {
    Require-Admin
    if (-not (Get-Service-State)) {
        Write-Warn2 "Dienst '$ServiceName' existiert nicht - nichts zu tun."
        return
    }
    if (-not (Test-Path $NssmPath)) {
        Write-Warn2 "NSSM nicht vorhanden, versuche 'sc.exe delete' ..."
        & sc.exe stop   $ServiceName | Out-Null
        & sc.exe delete $ServiceName | Out-Null
    } else {
        & $NssmPath stop   $ServiceName confirm | Out-Null
        Start-Sleep -Milliseconds 500
        & $NssmPath remove $ServiceName confirm | Out-Null
    }
    Write-Ok "Dienst '$ServiceName' entfernt"
}

function Action-Start {
    Require-Admin
    if (-not (Get-Service-State)) {
        throw "Dienst '$ServiceName' existiert nicht. Erst 'install' ausfuehren."
    }
    Start-Service $ServiceName
    Write-Ok "Dienst '$ServiceName' gestartet"
    Action-Status
}

function Action-Stop {
    Require-Admin
    if (-not (Get-Service-State)) {
        Write-Warn2 "Dienst '$ServiceName' existiert nicht."
        return
    }
    Stop-Service $ServiceName -Force
    Write-Ok "Dienst '$ServiceName' gestoppt"
    Action-Status
}

function Action-Restart {
    Require-Admin
    if (-not (Get-Service-State)) {
        throw "Dienst '$ServiceName' existiert nicht. Erst 'install' ausfuehren."
    }
    Restart-Service $ServiceName -Force
    Write-Ok "Dienst '$ServiceName' neu gestartet"
    Action-Status
}

function Action-Status {
    Write-Host ""
    Write-Host "=== NRGkick Logger - Status ===" -ForegroundColor White

    $svc = Get-Service-State
    if (-not $svc) {
        Write-Warn2 "Dienst nicht installiert. Installation: .\service.ps1 install (als Admin)."
    } else {
        $color = switch ($svc.Status) {
            'Running'  { 'Green' }
            'Stopped'  { 'Yellow' }
            default    { 'Red' }
        }
        Write-Host ("Dienst : {0} -> {1}" -f $svc.Name, $svc.Status) -ForegroundColor $color
        $startup = (Get-CimInstance Win32_Service -Filter "Name='$ServiceName'" -ErrorAction SilentlyContinue).StartMode
        if ($startup) { Write-Host ("Startart: {0}" -f $startup) }
    }

    Write-Host ("Datenordner: {0}" -f $DataDir)
    if (Test-Path $DbFile) {
        $sizeKb = [int]((Get-Item $DbFile).Length / 1024)
        Write-Host ("DB         : {0}  ({1} KB)" -f $DbFile, $sizeKb)
        # Letzter DB-Eintrag via SQLite-Query (nutzt venv-Python)
        if (Test-Path $VenvPython) {
            try {
                $py = @"
import sqlite3, sys
try:
    c = sqlite3.connect(r'$DbFile')
    row = c.execute('select ts_local, charging_state, power_w, set_current_a, energy_session_wh from samples order by ts_utc desc limit 1').fetchone()
    n   = c.execute('select count(*) from samples').fetchone()[0]
    if row:
        print(f'{n} samples total; letzter: {row[0]}  state={row[1]}  P={row[2]:.0f}W  Iset={row[3]}  Ekwh_sess={(row[4] or 0)/1000.0:.2f}')
    else:
        print('keine samples in der DB')
except Exception as e:
    print('Fehler beim DB-Lesen:', e)
"@
                & $VenvPython -c $py
            } catch { }
        }
    } else {
        Write-Host ("DB         : {0} (noch nicht vorhanden)" -f $DbFile) -ForegroundColor Yellow
    }

    if (Test-Path $LogFile) {
        Write-Host ""
        Write-Host "--- letzte Log-Zeilen ---" -ForegroundColor White
        Get-Content -Tail 8 $LogFile
    } else {
        Write-Host ("Log-Datei  : {0} (noch nicht vorhanden)" -f $LogFile) -ForegroundColor Yellow
    }
    Write-Host ""
}

function Action-Logs {
    if (-not (Test-Path $LogFile)) {
        throw "Log-Datei existiert noch nicht: $LogFile"
    }
    Write-Info "verfolge $LogFile  (Strg+C zum Beenden)"
    Get-Content -Path $LogFile -Wait -Tail 20
}

# ---------------------------------------------------------------------------
# Dispatcher
# ---------------------------------------------------------------------------
switch ($Action) {
    'install'   { Action-Install }
    'uninstall' { Action-Uninstall }
    'start'     { Action-Start }
    'stop'      { Action-Stop }
    'restart'   { Action-Restart }
    'status'    { Action-Status }
    'logs'      { Action-Logs }
}
