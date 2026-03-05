<#
.SYNOPSIS
  Install VI Package Manager (VIPM) and LUnit CLI package.

.DESCRIPTION
  Downloads and installs VIPM if not already installed, refreshes the package list,
  and installs the LUnit CLI package for the specified LabVIEW version and bitness.

.PARAMETER LVVersion
  LabVIEW version (e.g., "2025", "2024").

.PARAMETER LVBitness
  LabVIEW bitness ("32" or "64").

.PARAMETER VipmInstallerUrl
  URL to download the VIPM installer.

.EXAMPLE
  .\SetupLunit.ps1 -LVVersion "2025" -LVBitness "64"

.NOTES
  [REQ-039] Install VIPM and LUnit CLI package
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
    Write-Verbose "Starting LUnit CLI setup process..."
    Write-Information "Setting up LUnit for LabVIEW $LVVersion ($LVBitness-bit)" -InformationAction Continue

    $currentIdentity = [Security.Principal.WindowsIdentity]::GetCurrent()
    $principal = New-Object Security.Principal.WindowsPrincipal($currentIdentity)

    $isElevated = $principal.IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)

    if ($isElevated) {
        Write-Information "PowerShell session is running with administrator privileges" -InformationAction Continue
        Write-Verbose "Current user: $($currentIdentity.Name)"
    }
    else {
        Write-Warning "PowerShell session is NOT running with administrator privileges"
        Write-Warning "VIPM may fail to install LUnit CLI to LabVIEW CLI directory"
    }
    
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

    # Stop any existing VIPM processes to ensure fresh session with correct privileges
    Write-Information "Ensuring VIPM is not running..." -InformationAction Continue
    $vipmProcesses = @("vipm", "VI Package Manager", "VIPM")
    $stoppedCount = 0
    
    foreach ($processName in $vipmProcesses) {
        $processes = Get-Process -Name $processName -ErrorAction SilentlyContinue
        if ($processes) {
            Write-Verbose "Found $($processes.Count) instance(s) of $processName"
            foreach ($proc in $processes) {
                Write-Verbose "Stopping process $processName (PID: $($proc.Id), User: $($proc.UserName))"
                Stop-Process -Id $proc.Id -Force -ErrorAction SilentlyContinue
                $stoppedCount++
            }
        }
    }
    
    if ($stoppedCount -gt 0) {
        Write-Information "Stopped $stoppedCount VIPM process(es)" -InformationAction Continue
        Write-Information "Waiting 5 seconds for process cleanup..." -InformationAction Continue
        Start-Sleep -Seconds 5
    } else {
        Write-Verbose "No running VIPM processes found"
    }

    # Grant write permissions to LabVIEW CLI directory
    $labviewCliDir = "C:\Program Files (x86)\National Instruments\Shared\LabVIEW CLI"
    
    if ($isElevated) {
        # Create directory if it doesn't exist
        if (-not (Test-Path $labviewCliDir)) {
            Write-Information "Creating LabVIEW CLI directory at $labviewCliDir" -InformationAction Continue
            New-Item -Path $labviewCliDir -ItemType Directory -Force | Out-Null
        }
        
        # Grant write permissions using icacls
        Write-Information "Granting write permissions to LabVIEW CLI directory..." -InformationAction Continue
        Write-Verbose "Running: icacls `"$labviewCliDir`" /grant Everyone:(OI)(CI)F /T"
        
        $icaclsOutput = & icacls "$labviewCliDir" /grant "Everyone:(OI)(CI)F" /T 2>&1
        
        if ($LASTEXITCODE -eq 0) {
            Write-Information "Write permissions granted successfully" -InformationAction Continue
            Write-Verbose "icacls output: $icaclsOutput"
        } else {
            Write-Warning "icacls command failed with exit code: $LASTEXITCODE"
            Write-Verbose "icacls output: $icaclsOutput"
        }
        
        # Verify write permissions
        $testFile = Join-Path $labviewCliDir "test_write_permissions.tmp"
        try {
            [System.IO.File]::WriteAllText($testFile, "test")
            Remove-Item $testFile -Force -ErrorAction SilentlyContinue
            Write-Information "Write permissions to LabVIEW CLI directory verified" -InformationAction Continue
        } catch {
            Write-Warning "Cannot write to LabVIEW CLI directory: $($_.Exception.Message)"
            Write-Warning "LUnit CLI installation may fail silently"
        }
    } else {
        Write-Warning "Cannot grant write permissions without administrator privileges"
        if (Test-Path $labviewCliDir) {
            Write-Verbose "LabVIEW CLI directory exists at $labviewCliDir"
        } else {
            Write-Warning "LabVIEW CLI directory does not exist at $labviewCliDir"
        }
    }
    
    # Refresh package list
    Write-Information "Refreshing VIPM package list..." -InformationAction Continue
    Write-Verbose "Running: vipm.exe package-list-refresh"
    
    Write-Host "--- VIPM Package List Refresh Output ---"
    & $VipmExe package-list-refresh *>&1 | ForEach-Object { Write-Host $_ }
    Write-Host "--- End VIPM Output ---"
    
    if ($LASTEXITCODE -ne 0) {
        throw "Failed to refresh VIPM package list (exit code: $LASTEXITCODE)"
    }
    
    Write-Verbose "Package list refreshed successfully"

    Write-Information "Waiting 30 seconds for VIPM to complete background refresh..." -InformationAction Continue
    Start-Sleep -Seconds 30
    Write-Verbose "Wait complete, proceeding with installation"

    # Write-Information "Step 1: Installing LUnit CLI (System) component..." -InformationAction Continue
    # & $VipmExe install astemes_lib_lunit_cli_system --labview-version $LVVersion --labview-bitness $LVBitness
    # Start-Sleep -Seconds 25
    # if ($LASTEXITCODE -ne 0) {
    #     throw "Failed to install LUnit CLI System component (exit code: $LASTEXITCODE)"
    # }
        
    # Install LUnit CLI
    Write-Information "Installing LUnit CLI for LabVIEW $LVVersion ($LVBitness-bit)..." -InformationAction Continue
    Write-Verbose "Running: vipm.exe install astemes_lib_lunit_cli --labview-version $LVVersion --labview-bitness $LVBitness"

    Write-Host "--- VIPM Installation Output ---"
    & $VipmExe install astemes_lib_lunit_cli `
              --labview-version $LVVersion `
              --labview-bitness $LVBitness *>&1 | ForEach-Object { Write-Host $_ }

    Write-Information "Waiting 5 seconds for VIPM to complete background refresh..." -InformationAction Continue
    Start-Sleep -Seconds 5
    Write-Verbose "Wait complete, proceeding with installation"

    # Install LUnit CLI system component first
    Write-Information "Installing LUnit CLI system component..." -InformationAction Continue
    Write-Verbose "Running: vipm.exe install astemes_lib_lunit_cli_system --labview-version $LVVersion --labview-bitness $LVBitness"
    
    & $VipmExe install astemes_lib_lunit_cli_system `
              --labview-version $LVVersion `
              --labview-bitness $LVBitness *>&1 | ForEach-Object { Write-Host $_ }
    Write-Host "--- End VIPM Output ---"
    
    $installExitCode = $LASTEXITCODE
    Write-Verbose "LUnit installation exit code: $installExitCode"
    
    if ($installExitCode -eq 0) {
        Write-Information "LUnit CLI package installation completed" -InformationAction Continue
        
        # Verify VIPM cache and check for dependency packages
        Write-Information "Checking VIPM cache for package files..." -InformationAction Continue
        $vipmCacheDir = "C:\ProgramData\JKI\VIPM\cache"
        
        if (Test-Path $vipmCacheDir) {
            Write-Host "--- VIPM Cache Contents ---"
            $cacheFiles = Get-ChildItem -Path $vipmCacheDir -Filter "astemes_lib_lunit*.vip" -ErrorAction SilentlyContinue
            
            if ($cacheFiles) {
                foreach ($file in $cacheFiles) {
                    Write-Host "Found: $($file.Name)"
                    Write-Host "  Full path: $($file.FullName)"
                    Write-Host "  Size: $([math]::Round($file.Length / 1MB, 2)) MB"
                    Write-Host "  Last modified: $($file.LastWriteTime)"
                    
                    # Check permissions using icacls
                    Write-Host "  Permissions:"
                    $permissions = & icacls "$($file.FullName)" 2>&1
                    $permissions | ForEach-Object { Write-Host "    $_" }
                }
            } else {
                Write-Warning "No astemes_lib_lunit*.vip files found in cache"
            }
            
            # Check specifically for the system component package
            $systemPackagePath = Join-Path $vipmCacheDir "astemes_lib_lunit_cli_system-1.6.1.23.vip"
            if (Test-Path $systemPackagePath) {
                Write-Host "`nSystem component package found: $systemPackagePath"
            } else {
                Write-Warning "System component package NOT found: $systemPackagePath"
                Write-Warning "This indicates the dependency was not downloaded/cached by VIPM"
            }
            
            Write-Host "--- End VIPM Cache Contents ---"
        } else {
            Write-Warning "VIPM cache directory not found at $vipmCacheDir"
        }
        
        # Verify LUnit operation was installed to LabVIEW CLI
        Write-Information "Verifying LUnit CLI installation in LabVIEW CLI directory..." -InformationAction Continue
        
        $operationsDir = Join-Path $labviewCliDir "Operations"
        if (-not (Test-Path $operationsDir)) {
            Write-Warning "LabVIEW CLI Operations directory not found at $operationsDir"
        } else {
            Write-Host "--- LabVIEW CLI Operations Directory ---"
            $lunitOperationDir = Join-Path $operationsDir "LUnit"
            if (-not (Test-Path $lunitOperationDir)) {
                Write-Warning "LUnit operation directory not found at $lunitOperationDir"
                Write-Warning "Listing contents of Operations directory:"
                Get-ChildItem $operationsDir -Directory -ErrorAction SilentlyContinue | ForEach-Object {
                    Write-Warning "  - $($_.Name)"
                }
                throw "LUnit operation was not installed to LabVIEW CLI Operations directory"
            }
            
            # Verify operation files exist
            $lunitFiles = Get-ChildItem -Path $lunitOperationDir -Recurse -ErrorAction SilentlyContinue
            if ($lunitFiles) {
                Write-Information "LUnit operation verified at $lunitOperationDir" -InformationAction Continue
                Write-Verbose "Found $($lunitFiles.Count) files in LUnit operation directory"
                
                # List key files
                Write-Host "LUnit operation files:"
                $lunitFiles | Select-Object -First 10 | ForEach-Object {
                    Write-Host "  - $($_.FullName)"
                }
            } else {
                throw "LUnit operation directory exists but contains no files"
            }
            Write-Host "--- End LabVIEW CLI Operations Directory ---"
        }

        Write-Information "Waiting 180 seconds to complete mass compile..." -InformationAction Continue
        Start-Sleep -Seconds 180
        Write-Verbose "Wait complete, proceeding with installation"
        
        Write-Information "LUnit CLI installed successfully!" -InformationAction Continue
        exit 0
    } else {
        throw "Failed to install LUnit CLI (exit code: $installExitCode)"
    }
}
catch {
    Write-Error "SetupLunit failed: $_"

    Write-Warning "Troubleshooting information:"
    Write-Warning "- Current user: $([Security.Principal.WindowsIdentity]::GetCurrent().Name)"
    Write-Warning "- Is elevated: $isElevated"
    exit 1
}