<#
.SYNOPSIS
    Builds a LabVIEW build specification using Linux LabVIEW Docker container.

.DESCRIPTION
    Executes LabVIEW build specification through LabVIEWCLI inside a Docker container.

.PARAMETER MinimumSupportedLVVersion
    LabVIEW version year used for the build (e.g., "2021", "2023", "2026").

.PARAMETER SupportedBitness
    Bitness of the LabVIEW environment ("32" or "64").

.PARAMETER ProjectPath
    Path to the LabVIEW project .lvproj file that contains the build specification.

.PARAMETER TargetName
    Target that contains the build specification. Defaults to "My Computer".

.PARAMETER BuildSpecName
    Name of the LabVIEW build specification to execute. If empty, builds all specifications in the target.

.PARAMETER Major
    Major version component for the build specification. Optional - if not provided, version setting is skipped.

.PARAMETER Minor
    Minor version component for the build specification. Optional - if not provided, version setting is skipped.

.PARAMETER Patch
    Patch version component for the build specification. Optional - if not provided, version setting is skipped.

.PARAMETER Build
    Build number component for the build specification. Optional - if not provided, version setting is skipped.

.PARAMETER Commit
    Commit hash or identifier recorded in the build.

.PARAMETER DockerImage
    Docker image name (e.g., "nationalinstruments/labview").

.PARAMETER ImageTag
    Docker image tag (e.g., "2026q1-linux").

.EXAMPLE
    .\BuildSpecDockerLinux.ps1 -MinimumSupportedLVVersion "2026" -SupportedBitness "64" -ProjectPath "lv_icon_editor.lvproj" -TargetName "My Computer" -BuildSpecName "Editor Packed Library" -Major 1 -Minor 0 -Patch 0 -Build 0 -Commit "abc1234"

.NOTES
    [REQ-039] Build LabVIEW build specification using Linux Docker container
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
    [string]$Commit = "",

    [Parameter(Mandatory = $false)]
    [string]$DockerImage = "nationalinstruments/labview",

    [Parameter(Mandatory = $false)]
    [string]$ImageTag = "2026q1-linux"
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

try {
    Write-Verbose "Building build specification with Linux Docker container"
    $hasVersion = ($Major -ge 0) -and ($Minor -ge 0) -and ($Patch -ge 0) -and ($Build -ge 0)
    
    if ($hasVersion) {
        $versionString = "$Major.$Minor.$Patch.$Build"
        Write-Information "Build Specification Version: $versionString" -InformationAction Continue
    } else {
        Write-Information "Build Specification Version setting skipped." -InformationAction Continue
    }
    
    if ($Commit) {
        Write-Information "Commit: $Commit" -InformationAction Continue
    }

    $fullImage = "${DockerImage}:${ImageTag}"
    Write-Information "Docker Image: $fullImage" -InformationAction Continue

    # Pull Docker image
    Write-Information "Pulling Docker image..." -InformationAction Continue
    docker pull $fullImage *>&1 | ForEach-Object { Write-Information $_ -InformationAction Continue }
    if ($LASTEXITCODE -ne 0) {
        throw "Failed to pull Docker image (exit code: $LASTEXITCODE)"
    }

    # Get the path to the bash script
    $scriptDir = $PSScriptRoot
    $buildScript = Join-Path $scriptDir 'build-spec.sh'
    
    if (-not (Test-Path $buildScript)) {
        throw "Build script not found: $buildScript"
    }

    $actionRoot = Split-Path (Split-Path $scriptDir -Parent) -Parent
    Write-Verbose "Calculated action root from script path: $actionRoot"

    $helperDir = Join-Path $actionRoot 'scripts' 'build-spec-helpers'
    
    if (-not (Test-Path $helperDir)) {
        throw "Helper VI directory not found: $helperDir"
    }

    Write-Verbose "Action root: $actionRoot"
    Write-Verbose "Helper directory: $helperDir"
    Write-Verbose "Build script: $buildScript"

    # Construct LabVIEWPath for Linux container
    $labviewPath = "/usr/local/natinst/LabVIEW-${MinimumSupportedLVVersion}-${SupportedBitness}/labview"
    Write-Verbose "LabVIEWPath: $labviewPath"
    
    # Container paths are always Linux-style
    $containerProjectPath = "/workspace/$ProjectPath"
    $containerScriptPath = "/tmp/build-spec.sh"

    # Construct bash command arguments
    $bashArgs = @(
        "--labview-path", "'$labviewPath'"
        "--project-path", "'$containerProjectPath'"
    )
    
    if (-not [string]::IsNullOrWhiteSpace($TargetName)) {
        $bashArgs += "--target-name", "'$TargetName'"
    }
    
    if (-not [string]::IsNullOrWhiteSpace($BuildSpecName)) {
        $bashArgs += "--build-spec-name", "'$BuildSpecName'"
    }
    
    if ($hasVersion) {
        $bashArgs += "--version", "'$versionString'"
    }

    $bashCommand = "chmod +x $containerScriptPath && $containerScriptPath $($bashArgs -join ' ')"
    
    Write-Information "Executing build script in Docker container..." -InformationAction Continue
    Write-Verbose "Command: bash -c `"$bashCommand`""

    # Run build in container
    # Mount user workspace to /workspace
    # Mount folder with helper VIs to /helpers
    # Mount build script to /tmp
    docker run --rm `
        -v "${PWD}:/workspace" `
        -v "${actionRoot}:/helpers" `
        -v "${buildScript}:${containerScriptPath}" `
        $fullImage `
        bash -c $bashCommand `
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
    Write-Error "BuildSpecDockerLinux failed: $_"
    exit 1
}