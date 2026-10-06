<#
.SYNOPSIS
    Bumps every dependency in vipm.toml to its latest available version and regenerates vipm.lock.

.DESCRIPTION
    Installs/activates VIPM, runs `vipm refresh`, then runs `vipm add <pkg>` (no version
    suffix, which resolves to the latest available version per
    docs.vipm.io/cli/command-reference#vipm-add) for every package declared in vipm.toml's
    [dependencies] table, and finally regenerates vipm.lock with `vipm lock` (the only
    command that writes it - see docs.vipm.io/vipm-toml/lock-files).

.PARAMETER WorkingDirectory
    Path (relative to the repo root) containing vipm.toml.

.PARAMETER LabVIEWVersion
    LabVIEW version (YYYY) matching the [project] section of vipm.toml.

.PARAMETER LabVIEWBitness
    "32" or "64" bitness matching the [project] section of vipm.toml.

.PARAMETER VipmSerialNumber
    VIPM Pro serial number. `vipm add` requires Community or Professional edition.

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

.PARAMETER ManifestFilename
    Manifest filename (relative to WorkingDirectory) to read/update. Defaults to "vipm.toml".

.PARAMETER LockFilename
    Lock filename (relative to WorkingDirectory) to regenerate. Defaults to "vipm.lock".

.PARAMETER IncludeDevDependencies
    If set, also bumps packages declared under [dev-dependencies] in addition to [dependencies].

.NOTES
    Leaf script for the 'update-vipm-dependencies' dispatcher action.
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
    [string]$ImageTag = "latest-linux",

    [Parameter(Mandatory = $false)]
    [string]$ManifestFilename = "vipm.toml",

    [Parameter(Mandatory = $false)]
    [string]$LockFilename = "vipm.lock",

    [Parameter(Mandatory = $false)]
    [switch]$IncludeDevDependencies
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

try {
    $fullImage = "${DockerImage}:${ImageTag}"
    Write-Information "Docker Image: $fullImage" -InformationAction Continue

    $scriptDir = $PSScriptRoot
    $updateScript = Join-Path $scriptDir 'update-vipm-dependencies.sh'
    if (-not (Test-Path $updateScript)) {
        throw "Helper script not found: $updateScript"
    }

    $containerScriptPath = "/tmp/update-vipm-dependencies.sh"
    $bashArgs = @(
        "--working-directory", "'$WorkingDirectory'"
        "--labview-version", "'$LabVIEWVersion'"
        "--labview-bitness", "'$LabVIEWBitness'"
        "--vipm-deb-url", "'$VipmDebUrl'"
        "--manifest-filename", "'$ManifestFilename'"
        "--lock-filename", "'$LockFilename'"
        "--include-dev-dependencies", "'$(if ($IncludeDevDependencies) { 'true' } else { 'false' })'"
    )
    $bashCommand = "chmod +x $containerScriptPath && $containerScriptPath $($bashArgs -join ' ')"

    Write-Information "Running vipm dependency update in Docker container..." -InformationAction Continue
    Write-Verbose "Command: bash -c `"$bashCommand`""

    docker run --rm `
        -v "${PWD}:/workspace" `
        -v "${updateScript}:${containerScriptPath}" `
        -w /workspace `
        -e "VIPM_SERIAL_NUMBER=$VipmSerialNumber" `
        -e "VIPM_FULL_NAME=$VipmFullName" `
        -e "VIPM_EMAIL=$VipmEmail" `
        $fullImage `
        bash -c $bashCommand `
        *>&1 | ForEach-Object { Write-Information $_ -InformationAction Continue }

    $exitCode = $LASTEXITCODE
    if ($exitCode -ne 0) {
        throw "vipm dependency update failed with exit code $exitCode"
    }

    Write-Information "vipm dependency update succeeded" -InformationAction Continue
    exit 0
}
catch {
    Write-Error "UpdateVipmDependencies failed: $_"
    exit 1
}
