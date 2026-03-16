<#
.SYNOPSIS
  Install and configure NI Package Manager (NIPM) for LabVIEW package management.

.DESCRIPTION
  Downloads and installs NI Package Manager, adds it to the system PATH, and configures
  it for CI/CD environments by disabling package caching.

.PARAMETER NIPMUrl
  URL to download the NI Package Manager installer.

.EXAMPLE
  .\SetupNipm.ps1 -NIPMUrl "https://download.ni.com/support/nipkg/products/ni-package-manager/installers/NIPackageManager25.8.0.exe"

.NOTES
  [REQ-035] Setup NI Package Manager for LabVIEW package management
#>

[CmdletBinding()]
param(
    [Parameter(Mandatory = $false)]
    [string]$NIPMUrl = "https://download.ni.com/support/nipkg/products/ni-package-manager/installers/NIPackageManager25.8.0.exe"
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

try {
    Write-Verbose "Starting NI Package Manager Installation..."
    Write-Information "Installing NI Package Manager from $NIPMUrl" -InformationAction Continue
    
    $InstallerPath = "$env:TEMP\nipm_installer.exe"
    
    # Download the installer
    Write-Verbose "Downloading installer to $InstallerPath..."
    Invoke-WebRequest -Uri $NIPMUrl -OutFile $InstallerPath
    Write-Verbose "Download completed successfully"
    
    # Install NIPM
    Write-Information "Running NIPM installation (this may take a while)..." -InformationAction Continue
    $Process = Start-Process -FilePath $InstallerPath -ArgumentList '--quiet','--accept-eulas','--prevent-reboot' -Wait -PassThru
    
    if ($Process.ExitCode -ne 0) {
        throw "NIPM installer failed with exit code $($Process.ExitCode)"
    }
    Write-Verbose "Installation completed with exit code $($Process.ExitCode)"
    
    # Cleanup the installer file
    if (Test-Path $InstallerPath) {
        Remove-Item $InstallerPath -Force
        Write-Verbose "Installer file removed"
    }
    
    # Update the System PATH
    $NIPM_Path = "C:\Program Files\National Instruments\NI Package Manager"
    $CurrentPath = [Environment]::GetEnvironmentVariable("Path", "Machine")
    
    Write-Verbose "Current machine PATH: $CurrentPath"
    
    # Only add if it's not already there
    if ($CurrentPath -split ';' -notcontains $NIPM_Path) {
        Write-Verbose "Adding NIPM to system PATH..."
        $NewPath = "$CurrentPath;$NIPM_Path"
        [Environment]::SetEnvironmentVariable("Path", $NewPath, "Machine")
        
        # Update the current session so we can use nipkg.exe immediately
        $env:Path = "$env:Path;$NIPM_Path"
        Write-Information "Added NIPM to system PATH" -InformationAction Continue
    } else {
        Write-Verbose "NIPM already in system PATH"
    }
    
    # Prevent package caching
    Write-Information "Disabling package caching..." -InformationAction Continue
    & nipkg.exe set-config nipkg.cachepackages=false
    
    if ($LASTEXITCODE -ne 0) {
        throw "Failed to configure NIPM caching settings (exit code: $LASTEXITCODE)"
    }
    
    Write-Information "NI Package Manager installation complete!" -InformationAction Continue
    exit 0
}
catch {
    Write-Error "SetupNipm failed: $_"
    exit 1
}