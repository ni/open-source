<#
.SYNOPSIS
    Generates or refreshes vipm.lock from vipm.toml for CI artifact upload.

.DESCRIPTION
    Requires VIPM Community (public repo) or Pro (`vipm activate`). Never
    commits or pushes; the caller (CI workflow) uploads the resulting
    vipm.lock as a build artifact for a developer to download and commit.

.PARAMETER VipmToml
    Path to the vipm.toml manifest.

.PARAMETER VipmInstallerUrl
    Windows-only URL used to install VIPM when the CLI is not already present.

.PARAMETER MinimumVipmVersion
    Minimum VIPM CLI version required for vipm.toml/vipm.lock support, using the
    CLI's own dotted MAJOR.MINOR[.PATCH] scheme (e.g. "25.3"), NOT a YYYY.Q year.
    The real CLI reports versions like "26.3.0" (2-digit year, not 4-digit).
#>
[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [string]$VipmToml,

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

if (-not (Test-Path $VipmToml)) { throw "vipm.toml not found at '$VipmToml'." }

$minVersion = [version]$MinimumVipmVersion
$vipm = Resolve-VipmCli

if (-not $vipm) {
    if (-not $onWindows) {
        throw "VIPM CLI not found and automatic provisioning is only supported on Windows."
    }
    Write-Information "VIPM CLI not found. Installing from $VipmInstallerUrl..." -InformationAction Continue
    $installer = Join-Path $env:TEMP "vipm-setup.exe"
    Invoke-WebRequest -Uri $VipmInstallerUrl -OutFile $installer
    $proc = Start-Process -FilePath $installer -ArgumentList "/quiet", "/norestart" -Wait -PassThru
    if ($proc.ExitCode -ne 0) { throw "VIPM installation failed with exit code $($proc.ExitCode)." }
    Remove-Item $installer -Force -ErrorAction SilentlyContinue
    $vipm = Resolve-VipmCli
    if (-not $vipm) { throw "VIPM CLI still not found after installation." }
}

$version = Get-VipmVersion -Vipm $vipm
if (-not $version) { throw "Unable to determine the VIPM CLI version from '$vipm'." }
if ($version -lt $minVersion) { throw "VIPM $version does not support vipm.toml/vipm.lock. Version >= $MinimumVipmVersion is required." }
Write-Host "VIPM version: $version ($vipm)"

if ($env:VIPM_SERIAL_NUMBER) {
    Write-Information "Activating VIPM Pro..." -InformationAction Continue
    & $vipm activate --serial-number $env:VIPM_SERIAL_NUMBER --name $env:VIPM_FULL_NAME --email $env:VIPM_EMAIL
    if ($LASTEXITCODE -ne 0) { throw "VIPM activation failed (exit code $LASTEXITCODE)." }
}

Write-Information "Refreshing VIPM package list..." -InformationAction Continue
& $vipm package-list-refresh
if ($LASTEXITCODE -ne 0) { throw "Failed to refresh VIPM package list (exit code $LASTEXITCODE)." }

$tomlDir = Split-Path -Parent (Resolve-Path $VipmToml)
$lockPath = Join-Path $tomlDir 'vipm.lock'

Write-Host "Lock generation command: vipm lock (cwd: $tomlDir)"
Push-Location $tomlDir
try {
    & $vipm lock
    $lockExitCode = $LASTEXITCODE
} finally {
    Pop-Location
}
if ($lockExitCode -ne 0) { throw "vipm lock failed (exit code $lockExitCode)." }

if (-not (Test-Path $lockPath)) { throw "vipm lock reported success but '$lockPath' was not created." }

Write-Host "Final lock file path: $lockPath"
Write-Output $lockPath
