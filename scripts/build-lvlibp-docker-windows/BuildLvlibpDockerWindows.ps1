<#
.SYNOPSIS
    Builds the LabVIEW Packed Project Library (.lvlibp) using Windows LabVIEW Docker container.

.DESCRIPTION
    Executes LabVIEW build specification through LabVIEWCLI inside a Windows Docker container,
    embedding the provided version information and commit identifier.

.PARAMETER MinimumSupportedLVVersion
    LabVIEW version year used for the build (e.g., "2021", "2023", "2026").

.PARAMETER SupportedBitness
    Bitness of the LabVIEW environment ("32" or "64").

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

.PARAMETER DockerImage
    Docker image name (e.g., "nationalinstruments/labview").

.PARAMETER ImageTag
    Docker image tag (e.g., "2026q1-windows").

.EXAMPLE
    .\BuildLvlibpDockerWindows.ps1 -MinimumSupportedLVVersion "2026" -SupportedBitness "64" -ProjectPath "lv_icon_editor.lvproj" -TargetName "My Computer" -BuildSpecName "Editor Packed Library" -Major 1 -Minor 0 -Patch 0 -Build 0 -Commit "abc1234"

.NOTES
    [REQ-040] Build LabVIEW Packed Project Library using Windows Docker container
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
    [string]$Commit,

    [Parameter(Mandatory = $false)]
    [string]$DockerImage = "nationalinstruments/labview",

    [Parameter(Mandatory = $false)]
    [string]$ImageTag = "2026q1-windows"
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

try {
    Write-Verbose "Building PPL with Windows Docker container"
    Write-Information "PPL Version: $Major.$Minor.$Patch.$Build" -InformationAction Continue
    Write-Information "Commit: $Commit" -InformationAction Continue

    $fullImage = "${DockerImage}:${ImageTag}"
    Write-Information "Docker Image: $fullImage" -InformationAction Continue

    # Pull Docker image
    Write-Information "Pulling Docker image..." -InformationAction Continue
    docker pull $fullImage *>&1 | ForEach-Object { Write-Information $_ -InformationAction Continue }
    if ($LASTEXITCODE -ne 0) {
        throw "Failed to pull Docker image (exit code: $LASTEXITCODE)"
    }

    # Get the path to the PowerShell build script
    $scriptDir = $PSScriptRoot
    $buildScript = Join-Path $scriptDir 'build-lvlibp.ps1'
    
    if (-not (Test-Path $buildScript)) {
        throw "Build script not found: $buildScript"
    }

    # Create temporary directory for script mounting
    $tempDir = Join-Path $env:TEMP "docker-build-$(New-Guid)"
    New-Item -Path $tempDir -ItemType Directory -Force | Out-Null
    $tempScript = Join-Path $tempDir 'build-lvlibp.ps1'
    Copy-Item -Path $buildScript -Destination $tempScript -Force
    Write-Verbose "Copied build script to: $tempScript"

    # Construct LabVIEWPath for Windows container
    $labviewPath = if ($SupportedBitness -eq '32') {
        "C:\Program Files (x86)\National Instruments\LabVIEW $MinimumSupportedLVVersion\LabVIEW.exe"
    } else {
        "C:\Program Files\National Instruments\LabVIEW $MinimumSupportedLVVersion\LabVIEW.exe"
    }

    Write-Verbose "LabVIEWPath: $labviewPath"
    
    # Windows container paths
    $containerProjectPath = "C:\workspace\$ProjectPath"
    $containerScriptPath = "C:\scripts\build-lvlibp.ps1"

    # Construct PowerShell command arguments
    $scriptArgs = @(
        "-LabVIEWPath", "`"$labviewPath`""
        "-ProjectPath", "`"$containerProjectPath`""
        "-TargetName", "`"$TargetName`""
    )
    
    if (-not [string]::IsNullOrWhiteSpace($BuildSpecName)) {
        $scriptArgs += "-BuildSpecName", "`"$BuildSpecName`""
    }

    Write-Information "Executing build script in Windows Docker container..." -InformationAction Continue
    Write-Verbose "Script: $containerScriptPath $($scriptArgs -join ' ')"

    # Run build in Windows container using -File with mounted script
    docker run --rm `
        -v "${PWD}:C:\workspace" `
        -v "${tempDir}:C:\scripts" `
        $fullImage `
        powershell -NoProfile -File $containerScriptPath @scriptArgs `
        *>&1 | ForEach-Object { Write-Information $_ -InformationAction Continue }

    $buildExitCode = $LASTEXITCODE
    Write-Information "Build completed with exit code: $buildExitCode" -InformationAction Continue

    if ($buildExitCode -ne 0) {
        throw "Build failed with exit code $buildExitCode"
    }

    Write-Information "Build succeeded" -InformationAction Continue
    exit 0
}
catch {
    Write-Error "BuildLvlibpDockerWindows failed: $_"
    exit 1
}