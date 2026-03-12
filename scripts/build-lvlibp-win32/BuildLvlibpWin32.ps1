<#
.SYNOPSIS
    Builds the LabVIEW Packed Project Library (.lvlibp) using Windows GitHub-hosted runner.

.DESCRIPTION
    Executes LabVIEW build specification through LabVIEWCLI on a Windows GitHub-hosted runner.

.PARAMETER MinimumSupportedLVVersion
    LabVIEW version year used for the build (e.g., "2021", "2023", "2026").

.PARAMETER SupportedBitness
    Bitness of the LabVIEW environment ("32").

.PARAMETER ProjectPath
    Path to the LabVIEW project .lvproj file that contains the build specification.

.PARAMETER TargetName
    Target that contains the build specification. Defaults to "My Computer".

.PARAMETER BuildSpecName
    Name of the LabVIEW build specification to execute. If empty, builds all specifications in the target.

.PARAMETER Major
    Major version component for the PPL. Optional - if not provided, version setting is skipped.

.PARAMETER Minor
    Minor version component for the PPL. Optional - if not provided, version setting is skipped.

.PARAMETER Patch
    Patch version component for the PPL. Optional - if not provided, version setting is skipped.

.PARAMETER Build
    Build number component for the PPL. Optional - if not provided, version setting is skipped.

.PARAMETER Commit
    Commit hash or identifier recorded in the build.

.EXAMPLE
    .\BuildLvlibpWin32.ps1 -MinimumSupportedLVVersion "2025" -SupportedBitness "32" -ProjectPath "lv_icon_editor.lvproj" -TargetName "My Computer" -BuildSpecName "Editor Packed Library" -Major 1 -Minor 0 -Patch 0 -Build 0 -Commit "abc1234"

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

    [Parameter(Mandatory = $false)]
    [string]$TargetName = "",

    [Parameter(Mandatory = $false)]
    [string]$BuildSpecName = "",

    [Parameter(Mandatory = $false)]
    [int]$Major = -1,

    [Parameter(Mandatory = $false)]
    [int]$Minor = -1,

    [Parameter(Mandatory = $false)]
    [int]$Patch = -1,

    [Parameter(Mandatory = $false)]
    [int]$Build = -1,

    [Parameter(Mandatory = $false)]
    [string]$Commit = ""
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

try {
    Write-Verbose "Building PPL with Windows GitHub-hosted runner"
    $hasVersion = ($Major -ge 0) -and ($Minor -ge 0) -and ($Patch -ge 0) -and ($Build -ge 0)
    
    if ($hasVersion) {
        $versionString = "$Major.$Minor.$Patch.$Build"
        Write-Information "PPL Version: $versionString" -InformationAction Continue
    } else {
        Write-Information "PPL Version setting skipped." -InformationAction Continue
    }

    Write-Information "Commit: $Commit" -InformationAction Continue

    # Construct LabVIEWPath for Windows
    $labviewPath = "C:\Program Files (x86)\National Instruments\LabVIEW $MinimumSupportedLVVersion\LabVIEW.exe"
    $labviewCLI = "C:\Program Files (x86)\National Instruments\Shared\LabVIEW CLI\LabVIEWCLI.exe"
    
    if (-not (Test-Path $labviewPath)) {
        throw "LabVIEW not found at: $labviewPath"
    }

    Write-Verbose "LabVIEWPath: $labviewPath"
    Write-Information "Project: $ProjectPath" -InformationAction Continue
    Write-Information "Target: $(if ($TargetName) { $TargetName } else { '<My Computer>' })" -InformationAction Continue
    Write-Information "Build Spec: $(if ($BuildSpecName) { $BuildSpecName } else { '<all>' })" -InformationAction Continue
    # Verify project file exists
    if (-not (Test-Path $ProjectPath)) {
        throw "Project file not found: $ProjectPath"
    }

    # Only set build version if Version is provided
    if ($hasVersion) {
        Write-Information "Setting build version to: $versionString..." -InformationAction Continue
        $helperVI = Join-Path $PSScriptRoot '..' 'build-lvlibp-helpers' 'SetBuildVersionCaller.vi'
        
        if (-not (Test-Path $helperVI)) {
            throw "Helper VI not found at: $helperVI"
        }

        # SetBuildVersionCaller.vi expects positional arguments:
        # 1. ProjectPath (required)
        # 2. BuildSpecName (empty string to apply to all)
        # 3. TargetName (empty string to use VI default)
        # 4. Version (required when setting version)
        $projectArg = if ($ProjectPath) { $ProjectPath } else { '""' }
        $buildSpecArg = if ($BuildSpecName) { $BuildSpecName } else { '""' }
        $targetArg = if ($TargetName) { $TargetName } else { '""' }
        $versionArg = if ($Version) { $Version } else { '""' }
        
        Write-Host "Executing: LabVIEWCLI -OperationName RunVI -LabVIEWPath `"$LabVIEWPath`" -VIPath `"$helperVI`" `"$projectArg`" `"$buildSpecArg`" `"$targetArg`" `"$versionArg`" -Headless"
        
        & LabVIEWCLI -OperationName RunVI -LabVIEWPath $LabVIEWPath -VIPath $helperVI $projectArg $buildSpecArg $targetArg $versionArg -Headless
            $setVersionExit = $LASTEXITCODE
            
        if ($setVersionExit -ne 0) {
            throw "Failed to set build version (exit code: $setVersionExit)"
        }
        
        Write-Information "Build version set successfully" -InformationAction Continue
    } else {
        Write-Information "Skipping version set " -InformationAction Continue
    }

    # Construct LabVIEWCLI command
    $cliArgs = @(
        '-OperationName', 'ExecuteBuildSpec'
        '-LabVIEWPath', $labviewPath
        '-ProjectPath', (Resolve-Path $ProjectPath).Path
    )

    if (-not [string]::IsNullOrWhiteSpace($TargetName)) {
        $cliArgs += '-TargetName', $TargetName
    }

    if (-not [string]::IsNullOrWhiteSpace($BuildSpecName)) {
        $cliArgs += '-BuildSpecName', $BuildSpecName
    }

    $cliArgs += '-Headless'

    Write-Information "Executing: LabVIEWCLI $($cliArgs -join ' ')" -InformationAction Continue

    # Execute LabVIEWCLI
    & $labviewCLI @cliArgs
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