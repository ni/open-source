<#
.SYNOPSIS
  Activate LabVIEW license using NI License Manager utility.

.DESCRIPTION
  Activates a LabVIEW license by calling nilmUtil.exe with the provided serial number
  and package ID. The utility is typically located in the NI License Manager directory.

.PARAMETER SerialNumber
  LabVIEW serial number for activation (required).

.PARAMETER PackageID
  LabVIEW package ID to activate.

.EXAMPLE
  .\ActivateLabview.ps1 -SerialNumber "ABCD-1234-5678-90EF" -PackageID "LabVIEW_COM_PKG 25.0300"

.NOTES
  [REQ-037] Activate LabVIEW license using serial number and package ID
#>

[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [string]$SerialNumber,
    
    [Parameter(Mandatory = $false)]
    [string]$PackageID = "LabVIEW_COM_PKG 25.0300"
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

try {
    Write-Verbose "Starting LabVIEW activation process..."
    Write-Information "Activating $PackageID with provided serial number" -InformationAction Continue
    
    $LicenseManagerPath = "C:\Program Files (x86)\National Instruments\Shared\License Manager\bin"
    $ExeName = "nilmUtil.exe"
    $FullExePath = Join-Path -Path $LicenseManagerPath -ChildPath $ExeName
    
    Write-Verbose "License Manager path: $LicenseManagerPath"
    Write-Verbose "License utility: $FullExePath"
    
    # Check if the license utility exists
    if (-not (Test-Path $FullExePath)) {
        throw "Could not find nilmUtil.exe at $FullExePath. Ensure NI License Manager is installed."
    }
    
    Write-Verbose "Found License Utility at $FullExePath"
    
    # Arguments as a list to avoid string quoting issues
    $ActivationArgs = @(
        "-s", 
        "-activate", "`"$PackageID`"", 
        "-serialnumber", "$SerialNumber"
    )
    
    Write-Verbose "Activation arguments: $($ActivationArgs -join ' ')"
    Write-Information "Executing activation..." -InformationAction Continue
    
    # Run the activation process
    $Process = Start-Process -FilePath $FullExePath `
                             -ArgumentList $ActivationArgs `
                             -WorkingDirectory $LicenseManagerPath `
                             -Wait `
                             -PassThru `
                             -NoNewWindow
    
    $exitCode = $Process.ExitCode
    Write-Verbose "License utility exit code: $exitCode"
    
    if ($exitCode -eq 0) {
        Write-Information "Activation successful!" -InformationAction Continue
        exit 0
    } else {
        throw "Activation failed with exit code: $exitCode"
    }
}
catch {
    Write-Error "ActivateLabview failed: $_"
    exit 1
}