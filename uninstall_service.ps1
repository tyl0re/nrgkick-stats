<#
    Kompatibilitaets-Wrapper. Delegiert an service.ps1.
    Neuer empfohlener Befehl:
        powershell -ExecutionPolicy Bypass -File .\service.ps1 uninstall
#>
[CmdletBinding()]
param(
    [string]$NssmPath    = '',
    [string]$ServiceName = 'NRGkickLogger'
)
$ErrorActionPreference = 'Stop'
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
& (Join-Path $ScriptDir 'service.ps1') `
    -Action uninstall `
    -ServiceName $ServiceName `
    -NssmPath $NssmPath
