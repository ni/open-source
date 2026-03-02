<#
.SYNOPSIS
  Download, install, and activate LabVIEW Community Edition from an ISO image.

.DESCRIPTION
  Downloads a LabVIEW ISO installer, mounts it, runs the installation with passive mode,
  monitors the installation process with a timeout,activates the license using NI License
  Manager, and handles cleanup of hung processes.

.PARAMETER LabVIEWIsoUrl
  URL to download the LabVIEW ISO installer.

.PARAMETER TimeoutSeconds
  Maximum time in seconds to wait for installation to complete before killing hung processes.

.PARAMETER SerialNumber
  LabVIEW serial number for activation (required).

.PARAMETER PackageID
  LabVIEW package ID to activate.

.EXAMPLE
  .\SetupLabview.ps1 -LabVIEWIsoUrl "https://download.ni.com/.../ni-labview-2025-community-x86_25.3.3_offline.iso" -TimeoutSeconds 2700

.NOTES
  [REQ-036] Download, install, and activate LabVIEW Community Edition
#>

[CmdletBinding()]
param(
    [Parameter(Mandatory = $false)]
    [string]$LabVIEWIsoUrl = "https://download.ni.com/support/softlib/labview/labview_development_system/2025_Q3/ni-labview-2025-community-x86_25.3.3_offline.iso",
    
    [Parameter(Mandatory = $false)]
    [int]$TimeoutSeconds = 2700,

    [Parameter(Mandatory = $true)]
    [string]$SerialNumber,
    
    [Parameter(Mandatory = $false)]
    [string]$PackageID = "LabVIEW_COM_PKG 25.0300"
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$ProgressPreference = 'SilentlyContinue'

try {
    Write-Verbose "Starting LabVIEW Community Installation Process..."
    Write-Information "Installing LabVIEW from $LabVIEWIsoUrl (timeout: $TimeoutSeconds seconds)" -InformationAction Continue
    
    $IsoPath = "$env:TEMP\LabVIEW_Community.iso"
    
    # Download the ISO
    Write-Information "Downloading LabVIEW ISO (this may take a while)..." -InformationAction Continue
    Write-Verbose "Download destination: $IsoPath"
    Invoke-WebRequest -Uri $LabVIEWIsoUrl -OutFile $IsoPath
    
    if (-not (Test-Path $IsoPath)) {
        throw "Failed to download ISO to $IsoPath"
    }
    
    $isoSize = (Get-Item $IsoPath).Length / 1MB
    Write-Verbose "Downloaded ISO size: $($isoSize.ToString('F2')) MB"
    
    # Mount the ISO
    Write-Information "Mounting ISO image..." -InformationAction Continue
    $MountResult = Mount-DiskImage -ImagePath $IsoPath -PassThru
    $DriveLetter = ($MountResult | Get-Volume).DriveLetter
    
    if (-not $DriveLetter) {
        throw "Failed to mount ISO or retrieve drive letter"
    }
    
    Write-Verbose "ISO mounted to drive ${DriveLetter}:"
    $InstallExe = "${DriveLetter}:\Install.exe"
    
    if (-not (Test-Path $InstallExe)) {
        throw "Could not find Install.exe on the mounted drive ${DriveLetter}:"
    }
    
    Write-Verbose "Found installer at: $InstallExe"
    
    # Run the Installer
    Write-Information "Running LabVIEW Installer from drive ${DriveLetter}:..." -InformationAction Continue
    Write-Verbose "Installation arguments: --passive --accept-eulas --prevent-reboot"
    
    # Start the process and capture the ID
    $Process = Start-Process -FilePath $InstallExe -ArgumentList '--passive','--accept-eulas','--prevent-reboot' -PassThru
    
    if (-not $Process) {
        throw "Failed to start installation process"
    }
    
    Write-Verbose "Installation process started with PID: $($Process.Id)"
    Write-Information "Monitoring installation (Timeout: $TimeoutSeconds seconds)..." -InformationAction Continue
    
    $Timer = 0
    $CheckInterval = 60
    
    while (-not $Process.HasExited -and $Timer -lt $TimeoutSeconds) {
        Start-Sleep -Seconds $CheckInterval
        $Timer += $CheckInterval
        
        if ($Timer % 300 -eq 0) {
            Write-Verbose "Installation still running... ($Timer / $TimeoutSeconds seconds elapsed)"
        }
    }
    
    # Handle timeout or completion
    if (-not $Process.HasExited) {
        Write-Warning "Installation exceeded timeout of $TimeoutSeconds seconds. Cleaning up hung processes..."
        
        # Kill the installer and any sub-processes it launched
        Stop-Process -Id $Process.Id -Force -ErrorAction SilentlyContinue
        
        # Give it a moment to clean up
        Start-Sleep -Seconds 5
        
        Write-Verbose "Hung processes terminated"
    } else {
        $exitCode = $Process.ExitCode
        Write-Verbose "Installation process exited with code: $exitCode"
        
        if ($exitCode -ne 0) {
            Write-Warning "Installer returned non-zero exit code: $exitCode (this may be normal for LabVIEW installers)"
        }
    }
    
    # Cleanup
    Write-Information "Unmounting ISO and cleaning up..." -InformationAction Continue
    
    try {
        Dismount-DiskImage -ImagePath $IsoPath -ErrorAction SilentlyContinue
        Write-Verbose "ISO unmounted successfully"
    } catch {
        Write-Warning "Failed to unmount ISO: $_"
    }
    
    if (Test-Path $IsoPath) {
        Remove-Item $IsoPath -Force
        Write-Verbose "ISO file removed"
    }
    
    Write-Information "LabVIEW installation complete!" -InformationAction Continue

    Write-Information "Starting LabVIEW activation..." -InformationAction Continue
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
    $ActivationProcess = Start-Process -FilePath $FullExePath `
                                       -ArgumentList $ActivationArgs `
                                       -WorkingDirectory $LicenseManagerPath `
                                       -Wait `
                                       -PassThru `
                                       -NoNewWindow
    
    $activationExitCode = $ActivationProcess.ExitCode
    Write-Verbose "License utility exit code: $activationExitCode"
    
    if ($activationExitCode -eq 0) {
        Write-Information "Activation successful!" -InformationAction Continue
        Write-Information "LabVIEW setup complete!" -InformationAction Continue
        exit 0
    } else {
        throw "Activation failed with exit code: $activationExitCode"
    }
}
catch {
    Write-Error "SetupLabview failed: $_"
    
    # Attempt cleanup on error
    if ($IsoPath -and (Test-Path $IsoPath)) {
        try {
            Dismount-DiskImage -ImagePath $IsoPath -ErrorAction SilentlyContinue
            Remove-Item $IsoPath -Force -ErrorAction SilentlyContinue
        } catch {
            Write-Warning "Cleanup failed: $_"
        }
    }
    
    exit 1
}