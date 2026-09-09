<#
.SYNOPSIS
    Resolve the VIPM CLI, verify it is new enough for vipm.toml/vipm.lock, and
    install a project's dependencies from a manifest.

.DESCRIPTION
    Shared helper used by the Windows build-spec paths and apply-vipc. When a
    manifest is supplied it resolves the `vipm` CLI (installing it on Windows if
    missing), enforces a minimum version that supports vipm.toml/vipm.lock,
    verifies vipm.lock is in sync (CI never writes it), and installs the
    dependencies. A missing or too-old CLI is a hard error because dependencies
    were explicitly requested; a missing or stale vipm.lock is also a hard
    error — developers generate and commit it locally with `vipm lock`.

.PARAMETER VipmToml
    Path to the vipm.toml manifest (or the directory containing it).

.PARAMETER VipmLock
    Optional path to the vipm.lock file. Defaults to vipm.lock next to
    VipmToml. Must already exist and be committed; verified via
    `vipm lock --check`.

.PARAMETER VipmInstallerUrl
    Windows-only URL used to install VIPM when the CLI is not already present.

.PARAMETER MinimumVipmVersion
    Minimum VIPM CLI version required for vipm.toml/vipm.lock support, using the
    CLI's own dotted MAJOR.MINOR[.PATCH] scheme (e.g. "25.3"), NOT a YYYY.Q year.
    The real CLI reports versions like "26.3.0" (2-digit year, not 4-digit).

.NOTES
    vipm.lock is consumed automatically by `vipm install`; it is verified here,
    not passed as an install source.
#>
[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [string]$VipmToml,

    [Parameter(Mandatory = $false)]
    [string]$VipmLock = "",

    [Parameter(Mandatory = $false)]
    [string]$VipmInstallerUrl = "https://packages.jki.net/vipm/preview/vipm-setup-latest-preview.exe",

    [Parameter(Mandatory = $false)]
    [string]$MinimumVipmVersion = "25.3"
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$onWindows = ($PSVersionTable.PSVersion.Major -lt 6) -or $IsWindows

# Returns the invocable VIPM CLI path, or $null if it cannot be located.
function Resolve-VipmCli {
    $cmd = Get-Command 'vipm' -ErrorAction SilentlyContinue
    if ($cmd) { return $cmd.Source }
    if ($onWindows) {
        $fallback = "C:\Program Files\JKI\VI Package Manager\support\vipm.exe"
        if (Test-Path $fallback) { return $fallback }
    }
    return $null
}

# Parses "vipm 26.3.0" (or `vipm version` output) into [version]. Real VIPM
# versions are plain dotted numbers (2-digit year, not YYYY.Q) with 2-4 parts,
# so capture the whole dotted run and let [version] compare part-by-part.
function Get-VipmVersion {
    param([string]$Vipm)
    $raw = ''
    try { $raw = (& $Vipm --version 2>&1 | Out-String) } catch { $raw = '' }
    if ($raw -notmatch '(\d+(?:\.\d+){1,3})') {
        try { $raw = (& $Vipm version 2>&1 | Out-String) } catch { $raw = '' }
    }
    if ($raw -match '(\d+(?:\.\d+){1,3})') {
        return [version]$Matches[1]
    }
    return $null
}

$minVersion = [version]$MinimumVipmVersion
$vipm = Resolve-VipmCli

if (-not $vipm) {
    if (-not $onWindows) {
        throw "VIPM CLI not found and automatic provisioning is only supported on Windows. Bake VIPM into your container image (FROM the NI LabVIEW image) or install it in the entrypoint, then retry. See https://docs.vipm.io/cli/docker/."
    }
    Write-Information "VIPM CLI not found. Installing from $VipmInstallerUrl..." -InformationAction Continue
    $installer = Join-Path $env:TEMP "vipm-setup.exe"
    Invoke-WebRequest -Uri $VipmInstallerUrl -OutFile $installer
    $proc = Start-Process -FilePath $installer -ArgumentList "/quiet", "/norestart" -Wait -PassThru
    if ($proc.ExitCode -ne 0) {
        throw "VIPM installation failed with exit code $($proc.ExitCode)."
    }
    Remove-Item $installer -Force -ErrorAction SilentlyContinue
    $vipm = Resolve-VipmCli
    if (-not $vipm) {
        throw "VIPM CLI still not found after installation."
    }
}

$version = Get-VipmVersion -Vipm $vipm
if (-not $version) {
    throw "Unable to determine the VIPM CLI version from '$vipm'. A version >= $MinimumVipmVersion is required for vipm.toml/vipm.lock support."
}
if ($version -lt $minVersion) {
    throw "VIPM $version does not support vipm.toml/vipm.lock. Version >= $MinimumVipmVersion is required."
}
Write-Information "Using VIPM $version at $vipm" -InformationAction Continue

# Activation is only attempted when credentials are provided via environment.
if ($env:VIPM_SERIAL_NUMBER) {
    Write-Information "Activating VIPM Pro..." -InformationAction Continue
    & $vipm activate --serial-number $env:VIPM_SERIAL_NUMBER --name $env:VIPM_FULL_NAME --email $env:VIPM_EMAIL
    if ($LASTEXITCODE -ne 0) { throw "VIPM activation failed (exit code $LASTEXITCODE)." }
}

Write-Information "Refreshing VIPM package list..." -InformationAction Continue
& $vipm package-list-refresh
if ($LASTEXITCODE -ne 0) { throw "Failed to refresh VIPM package list (exit code $LASTEXITCODE)." }

if (-not (Test-Path $VipmToml)) { throw "vipm.toml not found at '$VipmToml'." }

# CI never writes vipm.lock; it must be generated and committed locally by a developer.
if (-not $VipmLock) { $VipmLock = Join-Path (Split-Path -Parent $VipmToml) 'vipm.lock' }
if (-not (Test-Path $VipmLock)) {
    throw "vipm.lock not found at '$VipmLock'. Run 'vipm lock' next to vipm.toml locally and commit the result."
}
Write-Information "Verifying vipm.lock is in sync..." -InformationAction Continue
Push-Location (Split-Path -Parent $VipmToml)
try {
    & $vipm lock --check
    if ($LASTEXITCODE -ne 0) { throw "vipm.lock is out of sync with vipm.toml (vipm lock --check exit code $LASTEXITCODE). Run 'vipm lock' locally and commit the update." }
} finally {
    Pop-Location
}
Write-Information "Installing dependencies from $VipmToml..." -InformationAction Continue
& $vipm install $VipmToml --no-dev
if ($LASTEXITCODE -ne 0) { throw "vipm install failed (exit code $LASTEXITCODE)." }


Write-Information "VIPM dependencies installed successfully." -InformationAction Continue
