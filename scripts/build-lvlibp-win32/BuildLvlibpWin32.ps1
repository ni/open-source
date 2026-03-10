<#
.SYNOPSIS
    Builds the LabVIEW Packed Project Library (.lvlibp) using Windows GitHub-hosted runner.

.DESCRIPTION
    Executes LabVIEW build specification through LabVIEWCLI on a Windows GitHub-hosted runner,
    embedding the provided version information and commit identifier.

.PARAMETER MinimumSupportedLVVersion
    LabVIEW version year used for the build (e.g., "2021", "2023", "2026").

.PARAMETER SupportedBitness
    Bitness of the LabVIEW environment ("32").

.PARAMETER ProjectPath
    Path to the LabVIEW project .lvproj file that contains the build specification.

.PARAMETER TargetName
    Target that contains the build specification.

.PARAMETER BuildSpecName
    Name of the LabVIEW build specification to execute. If empty, builds all specifications in the target.

.PARAMETER Major
    Major version component for the PPL.

.PARAMETER Minor
    Minor version component for the PPL.

.PARAMETER Patch
    Patch version component for the PPL.

.PARAMETER Build
    Build number component for the PPL.

.PARAMETER Commit
    Commit hash or identifier recorded in the build.

.EXAMPLE
    .\BuildLvlibpWin32.ps1 -MinimumSupportedLVVersion "2026" -SupportedBitness "64" -ProjectPath "lv_icon_editor.lvproj" -TargetName "My Computer" -BuildSpecName "Editor Packed Library" -Major 1 -Minor 0 -Patch 0 -Build 0 -Commit "abc1234"

.NOTES
    [REQ-041] Build LabVIEW Packed Project Library using Windows GitHub-hosted runner
#>

[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [string]$MinimumSupportedLVVersion,

    [Parameter(Mandatory = $true)]
    [string]$SupportedBitness,

    [Parameter(Mandatory = $true)]
    [string]$ProjectPath,

    [Parameter(Mandatory = $true)]
    [string]$TargetName,

    [Parameter(Mandatory = $false)]
    [string]$BuildSpecName = "",

    [Parameter(Mandatory = $true)]
    [int]$Major,

    [Parameter(Mandatory = $true)]
    [int]$Minor,

    [Parameter(Mandatory = $true)]
    [int]$Patch,

    [Parameter(Mandatory = $true)]
    [int]$Build,

    [Parameter(Mandatory = $true)]
    [string]$Commit
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

try {
    Write-Verbose "Building PPL with Windows GitHub-hosted runner"
    Write-Information "PPL Version: $Major.$Minor.$Patch.$Build" -InformationAction Continue
    Write-Information "Commit: $Commit" -InformationAction Continue

    # Construct LabVIEWPath for Windows
    $labviewPath = "C:\Program Files (x86)\National Instruments\LabVIEW $MinimumSupportedLVVersion\LabVIEW.exe"
    
    if (-not (Test-Path $labviewPath)) {
        throw "LabVIEW not found at: $labviewPath"
    }

    Write-Verbose "LabVIEWPath: $labviewPath"
    Write-Information "Project: $ProjectPath" -InformationAction Continue
    Write-Information "Target: $TargetName" -InformationAction Continue
    Write-Information "Build Spec: $(if ($BuildSpecName) { $BuildSpecName } else { '<all>' })" -InformationAction Continue

    # Verify project file exists
    if (-not (Test-Path $ProjectPath)) {
        throw "Project file not found: $ProjectPath"
    }

    # Construct LabVIEWCLI command
    $cliArgs = @(
        '-OperationName', 'ExecuteBuildSpec'
        '-LabVIEWPath', $labviewPath
        '-ProjectPath', (Resolve-Path $ProjectPath).Path
        '-TargetName', $TargetName
    )

    if (-not [string]::IsNullOrWhiteSpace($BuildSpecName)) {
        $cliArgs += '-BuildSpecName', $BuildSpecName
    }

    $cliArgs += '-Headless'

    Write-Information "Executing: LabVIEWCLI $($cliArgs -join ' ')" -InformationAction Continue

    # Execute LabVIEWCLI
    & LabVIEWCLI @cliArgs
    $buildExitCode = $LASTEXITCODE
    Write-Information "Build exit code: $buildExitCode" -InformationAction Continue

    # Copy LabVIEW logs to workspace for artifact collection
    Write-Information "Copying LabVIEW logs to workspace..." -InformationAction Continue
    $logDir = "build-logs"
    New-Item -Path $logDir -ItemType Directory -Force | Out-Null

    $tempLogs = Get-ChildItem -Path $env:TEMP -Filter "lvtemporary_*.log" -ErrorAction SilentlyContinue
    if ($tempLogs) {
        Copy-Item -Path $tempLogs.FullName -Destination $logDir -Force -ErrorAction SilentlyContinue
        Write-Information "Logs copied to $logDir" -InformationAction Continue
    } else {
        Write-Warning "No LabVIEW log files found in $env:TEMP"
    }

    if ($buildExitCode -ne 0) {
        throw "Build failed with exit code $buildExitCode"
    }

    Write-Information "Build succeeded" -InformationAction Continue
    exit 0
}
catch {
    Write-Error "BuildLvlibpWin32 failed: $_"
    exit 1
}