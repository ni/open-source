<#
.SYNOPSIS
    Build LabVIEW Packed Project Library using LabVIEWCLI

.DESCRIPTION
    [REQ-040] Build LVLIBP using Windows Docker container

.NOTES
    This script is executed inside the Windows Docker container.
#>

[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [string]$LabVIEWPath,

    [Parameter(Mandatory = $true)]
    [string]$ProjectPath,

    [Parameter(Mandatory = $false)]
    [string]$TargetName = "",

    [Parameter(Mandatory = $false)]
    [string]$BuildSpecName = "",

    [Parameter(Mandatory = $false)]
    [string]$Version = ""
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

Write-Host "Building LabVIEW Packed Project Library..."
Write-Host "LabVIEW: $LabVIEWPath"
Write-Host "Project: $ProjectPath"
Write-Host "Target: $(if ($TargetName) { $TargetName } else { '<My Computer>' })"
Write-Host "Build Spec: $(if ($BuildSpecName) { $BuildSpecName } else { '<all>' })"
Write-Host "Version: $(if ($Version) { $Version } else { '<from build spec>' })"

#  Set build version using helper VI if Version is provided
if ($Version) {
    Write-Host "Setting build version to: $Version..."
    $helperVI = "C:\actions\scripts\build-lvlibp-helpers\SetBuildVersionCaller.vi"

    if (-not (Test-Path $helperVI)) {
        throw "Helper VI not found at: $helperVI"
    }

    # SetBuildVersionCaller.vi expects positional arguments:
    # 1. ProjectPath (required)
    # 2. BuildSpecName (optional, empty string to apply to all)
    # 3. TargetName (optional, defaults to "My Computer" in VI)
    # 4. Version (optional, empty string to skip version setting)
    $setVersionArgs = @(
        '-OperationName', 'RunVI'
        '-LabVIEWPath', $LabVIEWPath
        '-VIPath', $helperVI
        $ProjectPath
        $BuildSpecName  
        $TargetName     
        $Version
        '-Headless'
    )

    & LabVIEWCLI @setVersionArgs
    $setVersionExit = $LASTEXITCODE
    
    if ($setVersionExit -ne 0) {
        throw "Failed to set build version (exit code: $setVersionExit)"
    }
    
    Write-Host "Build version set successfully"
} else {
    Write-Host "Skipping version set - using version from build spec(s)"
}

# Construct LabVIEWCLI command
$cliArgs = @(
    '-OperationName', 'ExecuteBuildSpec'
    '-LabVIEWPath', $LabVIEWPath
    '-ProjectPath', $ProjectPath
)

if (-not [string]::IsNullOrWhiteSpace($TargetName)) {
    $cliArgs += '-TargetName', $TargetName
}

if (-not [string]::IsNullOrWhiteSpace($BuildSpecName)) {
    $cliArgs += '-BuildSpecName', $BuildSpecName
}

$cliArgs += '-Headless'

Write-Host "Executing: LabVIEWCLI $($cliArgs -join ' ')"

try {
    & LabVIEWCLI @cliArgs
    $exitCode = $LASTEXITCODE
    Write-Host "Build exit code: $exitCode"

    # Copy LabVIEW logs to workspace for artifact collection
    Write-Host "Copying LabVIEW logs to workspace..."
    $logDir = "C:\workspace\build-logs"
    New-Item -Path $logDir -ItemType Directory -Force | Out-Null
    
    $tempLogs = Get-ChildItem -Path $env:TEMP -Filter "lvtemporary_*.log" -ErrorAction SilentlyContinue
    if ($tempLogs) {
        Copy-Item -Path $tempLogs.FullName -Destination $logDir -Force -ErrorAction SilentlyContinue
        Write-Host "Logs copied to $logDir"
    } else {
        Write-Host "No LabVIEW log files found in $env:TEMP"
    }

    if ($exitCode -ne 0) {
        Write-Host "Build failed"
        exit $exitCode
    }

    Write-Host "Build completed successfully"
    exit 0
}
catch {
    Write-Error "Build failed: $_"
    exit 1
}