<#
.SYNOPSIS
    Generates or checks vipm.lock from vipm.toml using the VIPM CLI inside a Linux Docker container.

.DESCRIPTION
    Installs/activates VIPM, runs `vipm refresh` to populate the repository cache for the
    target LabVIEW version/bitness, then runs `vipm lock` (or `vipm lock --check`).
    See docs.vipm.io/cli/command-reference and docs.vipm.io/vipm-toml/lock-files.

.PARAMETER WorkingDirectory
    Path (relative to the repo root) containing vipm.toml.

.PARAMETER LabVIEWVersion
    LabVIEW version (YYYY) matching the [project] section of vipm.toml.

.PARAMETER LabVIEWBitness
    "32" or "64" bitness matching the [project] section of vipm.toml.

.PARAMETER CheckOnly
    If set, checks an existing vipm.lock or generates the initial lock when absent.

.PARAMETER VipmSerialNumber
    VIPM Pro serial number. Omit to skip activation (Free edition).

.PARAMETER VipmFullName
    Name used for VIPM Pro activation.

.PARAMETER VipmEmail
    Email used for VIPM Pro activation.

.PARAMETER VipmDebUrl
    URL for the VIPM Linux .deb package.

.PARAMETER DockerImage
    Docker image name.

.PARAMETER ImageTag
    Docker image tag.

.NOTES
    Leaf script for the 'refresh-vipm-lock' dispatcher action.
#>

[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [string]$WorkingDirectory,

    [Parameter(Mandatory = $true)]
    [string]$LabVIEWVersion,

    [Parameter(Mandatory = $true)]
    [string]$LabVIEWBitness,

    [Parameter(Mandatory = $false)]
    [switch]$CheckOnly,

    [Parameter(Mandatory = $false)]
    [string]$VipmSerialNumber = "",

    [Parameter(Mandatory = $false)]
    [string]$VipmFullName = "",

    [Parameter(Mandatory = $false)]
    [string]$VipmEmail = "",

    [Parameter(Mandatory = $false)]
    [string]$VipmDebUrl = "https://traffic.libsyn.com/secure/jkinc/vipm_26.3.1-4025_amd64.deb",

    [Parameter(Mandatory = $false)]
    [string]$DockerImage = "nationalinstruments/labview",

    [Parameter(Mandatory = $false)]
    [string]$ImageTag = "latest-linux"
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

try {
    $fullImage = "${DockerImage}:${ImageTag}"
    Write-Information "Docker Image: $fullImage" -InformationAction Continue

    $scriptDir = $PSScriptRoot
    $refreshScript = Join-Path $scriptDir 'refresh-vipm-lock.sh'
    if (-not (Test-Path $refreshScript)) {
        throw "Helper script not found: $refreshScript"
    }

    $containerScriptPath = "/tmp/refresh-vipm-lock.sh"
    $checkOnlyValue = if ($CheckOnly) { 'true' } else { 'false' }

    $bashArgs = @(
        "--working-directory", "'$WorkingDirectory'"
        "--labview-version", "'$LabVIEWVersion'"
        "--labview-bitness", "'$LabVIEWBitness'"
        "--check-only", "'$checkOnlyValue'"
        "--vipm-deb-url", "'$VipmDebUrl'"
    )
    $bashCommand = "chmod +x $containerScriptPath && $containerScriptPath $($bashArgs -join ' ')"

    Write-Information "Running vipm refresh + lock in Docker container..." -InformationAction Continue
    Write-Verbose "Command: bash -c `"$bashCommand`""

    docker run --rm `
        -v "${PWD}:/workspace" `
        -v "${refreshScript}:${containerScriptPath}" `
        -w /workspace `
        -e "VIPM_SERIAL_NUMBER=$VipmSerialNumber" `
        -e "VIPM_FULL_NAME=$VipmFullName" `
        -e "VIPM_EMAIL=$VipmEmail" `
        $fullImage `
        bash -c $bashCommand `
        *>&1 | ForEach-Object { Write-Information $_ -InformationAction Continue }

    $exitCode = $LASTEXITCODE
    if ($exitCode -ne 0) {
        throw "vipm lock generation/check failed with exit code $exitCode"
    }

    if (-not $CheckOnly) {
        $lockPath = Join-Path $WorkingDirectory 'vipm.lock'
        if (-not (Test-Path $lockPath)) {
            throw "vipm.lock was not created at $lockPath"
        }
    }

    Write-Information "vipm.lock refresh succeeded" -InformationAction Continue
    exit 0
}
catch {
    Write-Error "RefreshVipmLock failed: $_"
    exit 1
}
