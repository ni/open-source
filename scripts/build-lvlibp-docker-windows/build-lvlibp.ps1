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

    [Parameter(Mandatory = $true)]
    [string]$TargetName,

    [Parameter(Mandatory = $false)]
    [string]$BuildSpecName = ""
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

Write-Host "Building LabVIEW Packed Project Library..."
Write-Host "LabVIEW: $LabVIEWPath"
Write-Host "Project: $ProjectPath"
Write-Host "Target: $TargetName"
Write-Host "Build Spec: $(if ($BuildSpecName) { $BuildSpecName } else { '<all>' })"

# Construct LabVIEWCLI command
$cliArgs = @(
    '-OperationName', 'ExecuteBuildSpec'
    '-LabVIEWPath', $LabVIEWPath
    '-ProjectPath', $ProjectPath
    '-TargetName', $TargetName
)

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