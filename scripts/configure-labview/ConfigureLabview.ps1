<#
.SYNOPSIS
  Configure LabVIEW settings by updating LabVIEW.ini file.

.DESCRIPTION
  Updates the LabVIEW.ini file with specified settings. If the ini file does not exist,
  LabVIEW is launched to generate it. Common use cases include enabling TCP/IP server
  and VI scripting operations for automation and testing.

.PARAMETER LabVIEWVersion
  LabVIEW version (e.g., "2025", "2024"). Used to locate the LabVIEW.ini file.

.PARAMETER IniSettings
  INI settings to add to LabVIEW.ini. Can be a multiline string or array of settings.
  Each setting should be in the format: key=value

.PARAMETER LabVIEWWaitSeconds
  Seconds to wait for LabVIEW to generate the ini file if it does not exist.

.EXAMPLE
  .\ConfigureLabview.ps1 -LabVIEWVersion "2025" -IniSettings @("server.tcp.enabled=TRUE")

.NOTES
  [REQ-038] Configure LabVIEW settings by updating LabVIEW.ini
#>

[CmdletBinding()]
param(
    [Parameter(Mandatory = $false)]
    [string]$LabVIEWVersion = "2025",
    
    [Parameter(Mandatory = $false)]
    [string]$IniSettings = @"
server.tcp.enabled=TRUE
server.tcp.access=+127.0.0.1;+localhost;+*
server.viscripting.ShowScriptingOperationsInEditor=TRUE
"@,
    
    [Parameter(Mandatory = $false)]
    [int]$LabVIEWWaitSeconds = 60
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

try {
    Write-Verbose "Starting LabVIEW configuration process..."
    Write-Information "Configuring LabVIEW $LabVIEWVersion settings" -InformationAction Continue
    
    # Determine LabVIEW installation path
    $LabVIEWBasePath = "C:\Program Files (x86)\National Instruments\LabVIEW $LabVIEWVersion"
    $IniPath = Join-Path $LabVIEWBasePath "LabVIEW.ini"
    $LabVIEWExePath = Join-Path $LabVIEWBasePath "LabVIEW.exe"
    
    Write-Verbose "LabVIEW base path: $LabVIEWBasePath"
    Write-Verbose "INI file path: $IniPath"
    Write-Verbose "LabVIEW executable: $LabVIEWExePath"
    
    # Parse settings (handle multiline string or array)
    $SettingsToAppend = @()
    if ($IniSettings -is [string]) {
        $SettingsToAppend = $IniSettings -split "`n" | ForEach-Object { $_.Trim() } | Where-Object { $_ -ne '' }
    } else {
        $SettingsToAppend = $IniSettings
    }
    
    Write-Verbose "Settings to apply: $($SettingsToAppend.Count) entries"
    foreach ($setting in $SettingsToAppend) {
        Write-Verbose "  - $setting"
    }
    
    # Check if INI file exists
    if (-not (Test-Path $IniPath)) {
        Write-Information "LabVIEW.ini not found at $IniPath" -InformationAction Continue
        
        # Verify LabVIEW executable exists
        if (-not (Test-Path $LabVIEWExePath)) {
            throw "LabVIEW executable not found at $LabVIEWExePath. Ensure LabVIEW $LabVIEWVersion is installed."
        }
        
        Write-Information "Launching LabVIEW to generate ini file..." -InformationAction Continue
        
        # Start LabVIEW process
        $lvProcess = Start-Process -FilePath $LabVIEWExePath -PassThru
        Write-Verbose "LabVIEW started with PID: $($lvProcess.Id)"
        
        # Wait for LabVIEW to initialize and create the ini file
        Write-Information "Waiting $LabVIEWWaitSeconds seconds for LabVIEW to generate ini file..." -InformationAction Continue
        Start-Sleep -Seconds $LabVIEWWaitSeconds
        
        # Force close LabVIEW
        if (-not $lvProcess.HasExited) {
            Write-Verbose "Terminating LabVIEW process..."
            Stop-Process -Id $lvProcess.Id -Force -ErrorAction SilentlyContinue
            Write-Information "LabVIEW closed" -InformationAction Continue
        } else {
            Write-Verbose "LabVIEW process already exited"
        }
        
        # Verify ini file was created
        if (-not (Test-Path $IniPath)) {
            throw "INI file not found at $IniPath after launching LabVIEW. Ensure LabVIEW is installed correctly."
        }
        
        Write-Verbose "INI file created successfully"
    } else {
        Write-Verbose "INI file already exists at $IniPath"
    }
    
    # Read current content and determine what needs to be added
    $CurrentContent = Get-Content -Path $IniPath -ErrorAction Stop
    Write-Verbose "Current INI file has $($CurrentContent.Count) lines"
    
    $NewLinesToAdd = @()
    
    foreach ($Setting in $SettingsToAppend) {
        if ($CurrentContent -notcontains $Setting) {
            $NewLinesToAdd += $Setting
            Write-Information "Will add: $Setting" -InformationAction Continue
        } else {
            Write-Verbose "Already exists: $Setting"
        }
    }
    
    # Append new settings if needed
    if ($NewLinesToAdd.Count -gt 0) {
        Write-Information "Adding $($NewLinesToAdd.Count) new settings to $IniPath" -InformationAction Continue
        Add-Content -Path $IniPath -Value $NewLinesToAdd -Encoding ASCII
        Write-Information "Successfully updated LabVIEW.ini" -InformationAction Continue
    } else {
        Write-Information "No changes needed. LabVIEW.ini is already up to date." -InformationAction Continue
    }
    
    exit 0
}
catch {
    Write-Error "ConfigureLabview failed: $_"
    exit 1
}