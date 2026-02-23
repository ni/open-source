<#
.SYNOPSIS
  Install VI Package Manager (VIPM) and LUnit for G-CLI package.

.DESCRIPTION
  Downloads and installs VIPM if not already installed, refreshes the package list,
  and installs the LUnit for G-CLI package for the specified LabVIEW version and bitness.

.PARAMETER LVVersion
  LabVIEW version (e.g., "2025", "2024").

.PARAMETER LVBitness
  LabVIEW bitness ("32" or "64").

.PARAMETER VipmInstallerUrl
  URL to download the VIPM installer.

.EXAMPLE
  .\SetupLunit.ps1 -LVVersion "2025" -LVBitness "64"

.NOTES
  [REQ-039] Install VIPM and LUnit for G-CLI package
#>

[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [string]$LVVersion,

    [Parameter(Mandatory = $true)]
    [ValidateSet("32", "64")]
    [string]$LVBitness,
    
    [Parameter(Mandatory = $false)]
    [string]$VipmInstallerUrl = "https://packages.jki.net/vipm/preview/vipm-setup-latest-preview.exe"
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$ProgressPreference = 'SilentlyContinue'

try {
    Write-Verbose "Starting LUnit for G-CLI setup process..."
    Write-Information "Setting up LUnit for LabVIEW $LVVersion ($LVBitness-bit)" -InformationAction Continue
    
    $VipmExe = "C:\Program Files\JKI\VI Package Manager\support\vipm.exe"
    
    # Check if VIPM is already installed
    if (Test-Path $VipmExe) {
        Write-Information "VIPM is already installed at $VipmExe" -InformationAction Continue
        Write-Verbose "Skipping VIPM installation"
    } else {
        Write-Information "VIPM not found. Installing VIPM..." -InformationAction Continue
        
        $VipmInstallerPath = Join-Path $env:TEMP "vipm-setup.exe"
        Write-Verbose "VIPM installer will be downloaded to: $VipmInstallerPath"
        
        # Download VIPM installer
        Write-Information "Downloading VIPM from $VipmInstallerUrl..." -InformationAction Continue
        Invoke-WebRequest -Uri $VipmInstallerUrl -OutFile $VipmInstallerPath
        
        if (-not (Test-Path $VipmInstallerPath)) {
            throw "Failed to download VIPM installer to $VipmInstallerPath"
        }
        
        $installerSize = (Get-Item $VipmInstallerPath).Length / 1MB
        Write-Verbose "Downloaded installer size: $($installerSize.ToString('F2')) MB"
        
        # Install VIPM silently
        Write-Information "Installing VIPM..." -InformationAction Continue
        Write-Verbose "Running installer with arguments: /quiet /norestart"
        
        $process = Start-Process -FilePath $VipmInstallerPath `
                                 -ArgumentList "/quiet", "/norestart" `
                                 -Wait `
                                 -PassThru
        
        $exitCode = $process.ExitCode
        Write-Verbose "VIPM installer exit code: $exitCode"
        
        if ($exitCode -eq 0) {
            Write-Information "VIPM installed successfully" -InformationAction Continue
            
            # Clean up installer
            if (Test-Path $VipmInstallerPath) {
                Remove-Item $VipmInstallerPath -Force
                Write-Verbose "Installer file removed"
            }
        } else {
            throw "VIPM installation failed with exit code: $exitCode"
        }
        
        # Verify VIPM was installed
        if (-not (Test-Path $VipmExe)) {
            throw "VIPM executable not found at $VipmExe after installation"
        }
        
        Write-Verbose "VIPM executable verified at $VipmExe"
    }
    
    # Refresh package list
    Write-Information "Refreshing VIPM package list..." -InformationAction Continue
    Write-Verbose "Running: vipm.exe package-list-refresh"
    
    & $VipmExe package-list-refresh
    
    if ($LASTEXITCODE -ne 0) {
        throw "Failed to refresh VIPM package list (exit code: $LASTEXITCODE)"
    }
    
    Write-Verbose "Package list refreshed successfully"
    
    # Install LUnit for G-CLI
    Write-Information "Installing LUnit for G-CLI for LabVIEW $LVVersion ($LVBitness-bit)..." -InformationAction Continue
    Write-Verbose "Running: vipm.exe install sas_workshops_lib_lunit_for_g_cli --labview-version $LVVersion --labview-bitness $LVBitness"
    
    & $VipmExe install sas_workshops_lib_lunit_for_g_cli `
              --labview-version $LVVersion `
              --labview-bitness $LVBitness
    
    $installExitCode = $LASTEXITCODE
    Write-Verbose "LUnit installation exit code: $installExitCode"
    
    if ($installExitCode -eq 0) {
        Write-Information "LUnit for G-CLI installed successfully!" -InformationAction Continue
        exit 0
    } else {
        throw "Failed to install LUnit for G-CLI (exit code: $installExitCode)"
    }
}
catch {
    Write-Error "SetupLunit failed: $_"
    exit 1
}