<#
.SYNOPSIS
    Generates a CycloneDX SBOM for a LabVIEW build spec using `vipm sbom`.

.DESCRIPTION
    Minimal wrapper: requires VIPM CLI to be available on the runner and LabVIEW
    to be available for the target project. The reusable workflow installs VIPM
    before invoking this script; callers that invoke the action directly are
    responsible for installing VIPM themselves. Just runs `vipm sbom` with the
    given flags and verifies the output file exists. See
    docs.vipm.io/cli/command-reference#vipm-sbom.

.PARAMETER LvprojPath
    Path to the .lvproj file to scan.

.PARAMETER LabVIEWVersion
    LabVIEW version (YYYY).

.PARAMETER LabVIEWBitness
    "32" or "64".

.PARAMETER BuildSpecName
    Name of the build specification within the .lvproj to scope the SBOM to.

.PARAMETER TargetName
    LabVIEW project target containing the build spec. Defaults to "My Computer".

.PARAMETER ProductName
    Name recorded in the SBOM's metadata.component.

.PARAMETER ProductVersion
    Version recorded in the SBOM's metadata.component.

.PARAMETER OutputPath
    Output file path for the generated SBOM.

.PARAMETER Format
    SBOM output format. Defaults to "cyclonedx".

.PARAMETER SchemaVersion
    CycloneDX schema version. Defaults to "1.5".

.NOTES
    Leaf script for the 'generate-sbom' dispatcher action.
#>

[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [string]$LvprojPath,

    [Parameter(Mandatory = $true)]
    [string]$LabVIEWVersion,

    [Parameter(Mandatory = $true)]
    [string]$LabVIEWBitness,

    [Parameter(Mandatory = $true)]
    [string]$BuildSpecName,

    [Parameter(Mandatory = $false)]
    [string]$TargetName = "My Computer",

    [Parameter(Mandatory = $true)]
    [string]$ProductName,

    [Parameter(Mandatory = $true)]
    [string]$ProductVersion,

    [Parameter(Mandatory = $true)]
    [string]$OutputPath,

    [Parameter(Mandatory = $false)]
    [string]$Format = "cyclonedx",

    [Parameter(Mandatory = $false)]
    [string]$SchemaVersion = "1.5"
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

try {
    if (-not (Get-Command vipm -ErrorAction SilentlyContinue)) {
        throw "vipm CLI not found on PATH. Install and activate VIPM before calling generate-sbom."
    }

    Write-Information "Generating SBOM for '$LvprojPath' (build spec '$BuildSpecName')..." -InformationAction Continue

    vipm sbom $LvprojPath `
        --format $Format `
        --schema-version $SchemaVersion `
        --labview-version $LabVIEWVersion `
        --labview-bitness $LabVIEWBitness `
        --lvproj-build-spec $BuildSpecName `
        --lvproj-target $TargetName `
        --product-name $ProductName `
        --product-version $ProductVersion `
        --output $OutputPath

    if ($LASTEXITCODE -ne 0) {
        throw "vipm sbom failed with exit code $LASTEXITCODE"
    }

    if (-not (Test-Path $OutputPath)) {
        throw "No SBOM file found at $OutputPath. Ensure vipm sbom completed successfully."
    }

    Write-Information "SBOM verified: $OutputPath" -InformationAction Continue
    exit 0
}
catch {
    Write-Error "GenerateSbom failed: $_"
    exit 1
}
