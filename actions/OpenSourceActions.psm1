# Runs a helper script from the repository's scripts directory.
# ScriptSegments: Path segments under the scripts folder that locate the target script.
# Arguments: Hashtable of arguments forwarded to the script.
# DryRun: If set, writes the command without executing it.
# gcliPath: Optional path prepended to PATH for locating the g CLI.
function Invoke-OpenSourceActionScript {
    param(
        [Parameter(Mandatory)] [string[]] $ScriptSegments,
        [Parameter(Mandatory)] [hashtable] $Arguments,
        [Parameter()] [switch] $DryRun,
        [Parameter()] [string] $gcliPath
    )
    $segments = @('..', 'scripts') + $ScriptSegments
    $scriptPath = $PSScriptRoot
    foreach ($seg in $segments) {
        $scriptPath = [System.IO.Path]::Combine($scriptPath, $seg)
    }
    if ($Arguments.ContainsKey('RelativePath')) {
        $Arguments['RelativePath'] = [System.IO.Path]::TrimEndingDirectorySeparator($Arguments['RelativePath'])
    }
    if ($DryRun) {
        Write-Information "DryRun: & $scriptPath $($Arguments | ConvertTo-Json -Compress)"
        return 0
    }
    $originalPath = $env:PATH
    try {
        if ($gcliPath) {
            $env:PATH = "$gcliPath$([System.IO.Path]::PathSeparator)$originalPath"
        }
        & $scriptPath @Arguments
        if (-not $?) { return 1 }
        return $LASTEXITCODE
    }
    finally {
        $env:PATH = $originalPath
    }
}

# Activates LabVIEW license using NI License Manager utility.
# SerialNumber: LabVIEW serial number for activation.
# PackageID: LabVIEW package ID to activate.
# DryRun: If set, prints the command instead of executing it.
function Invoke-ActivateLabview {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)] [string] $SerialNumber,
        [Parameter()] [string] $PackageID = "LabVIEW_COM_PKG 25.0300",
        [switch] $DryRun
    )
    Write-Information "Activating LabVIEW package $PackageID" -InformationAction Continue
    
    $result = Invoke-OpenSourceActionScript `
        -ScriptSegments @('activate-labview', 'ActivateLabview.ps1') `
        -Arguments @{
            SerialNumber = $SerialNumber
            PackageID = $PackageID
        } `
        -DryRun:$DryRun
    
    return $result
}

# Adds an authentication token to a LabVIEW installation.
# MinimumSupportedLVVersion: Minimum LabVIEW version that the project supports.
# SupportedBitness: Target LabVIEW bitness (32- or 64-bit).
# RelativePath: Normalized path to the project root relative to the working directory.
# DryRun: If set, prints the command instead of executing it.
# gcliPath: Optional path prepended to PATH for locating the g CLI.
function Invoke-AddTokenToLabVIEW {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)] [string] $MinimumSupportedLVVersion,
        [Parameter(Mandatory)] [string] $SupportedBitness,
        [Parameter(Mandatory)] [string] $RelativePath,
        [Parameter()] [switch] $DryRun,
        [Parameter()] [string] $gcliPath
    )
    Write-Information "Executing AddTokenToLabVIEW (DryRun=$DryRun)"
    $args = @{
        MinimumSupportedLVVersion = $MinimumSupportedLVVersion
        SupportedBitness          = $SupportedBitness
        RelativePath              = $RelativePath
    }
    return Invoke-OpenSourceActionScript -ScriptSegments @('add-token-to-labview','AddTokenToLabVIEW.ps1') -Arguments $args -DryRun:$DryRun -gcliPath $gcliPath
}

# Applies a VI Package Configuration to a LabVIEW project.
# MinimumSupportedLVVersion: Minimum LabVIEW version that the project supports.
# VIP_LVVersion: LabVIEW version used to build the VIPC.
# SupportedBitness: Target LabVIEW bitness (32- or 64-bit).
# RelativePath: Normalized path to the project root relative to the working directory.
# VIPCPath: Optional path to the VIPC file.
# DryRun: If set, prints the command instead of executing it.
# gcliPath: Optional path prepended to PATH for locating the g CLI.
function Invoke-ApplyVIPC {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)] [string] $MinimumSupportedLVVersion,
        [Parameter(Mandatory)] [string] $VIP_LVVersion,
        [Parameter(Mandatory)] [string] $SupportedBitness,
        [Parameter(Mandatory)] [string] $RelativePath,
        [Parameter()] [string] $VIPCPath,
        [Parameter()] [switch] $DryRun,
        [Parameter()] [string] $gcliPath
    )
    Write-Information "Executing ApplyVIPC (DryRun=$DryRun)"
    $args = @{
        MinimumSupportedLVVersion = $MinimumSupportedLVVersion
        VIP_LVVersion             = $VIP_LVVersion
        SupportedBitness          = $SupportedBitness
        RelativePath              = $RelativePath
        VIPCPath                  = $VIPCPath
    }
    return Invoke-OpenSourceActionScript -ScriptSegments @('apply-vipc','ApplyVIPC.ps1') -Arguments $args -DryRun:$DryRun -gcliPath $gcliPath
}

# Builds a VI Package using the provided VIPB file and version metadata.
# MinimumSupportedLVVersion: Minimum LabVIEW version that the package supports.
# SupportedBitness: Target LabVIEW bitness (32- or 64-bit).
# LabVIEWMinorRevision: Minor revision of LabVIEW used to build the package.
# RelativePath: Normalized path to the project root relative to the working directory.
# VIPBPath: Path to the VIPB build specification file.
# Major: Major version component.
# Minor: Minor version component.
# Patch: Patch version component.
# Build: Build number component.
# Commit: Commit identifier used for the build metadata.
# DisplayInformationJSON: JSON string containing display information for the package.
# ReleaseNotesFile: Optional path to a release notes file.
# DryRun: If set, prints the command instead of executing it.
# gcliPath: Optional path prepended to PATH for locating the g CLI.
function Invoke-BuildViPackage {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)] [string] $MinimumSupportedLVVersion,
        [Parameter(Mandatory)] [string] $SupportedBitness,
        [Parameter(Mandatory)] [string] $LabVIEWMinorRevision,
        [Parameter(Mandatory)] [string] $RelativePath,
        [Parameter(Mandatory)] [string] $VIPBPath,
        [Parameter(Mandatory)] [int] $Major,
        [Parameter(Mandatory)] [int] $Minor,
        [Parameter(Mandatory)] [int] $Patch,
        [Parameter(Mandatory)] [int] $Build,
        [Parameter(Mandatory)] [string] $Commit,
        [Parameter(Mandatory)] [string] $DisplayInformationJSON,
        [Parameter()] [string] $ReleaseNotesFile,
        [Parameter()] [switch] $DryRun,
        [Parameter()] [string] $gcliPath
    )
    Write-Information "Executing BuildViPackage version $Major.$Minor.$Patch-$Build (DryRun=$DryRun)"
    $args = @{
        MinimumSupportedLVVersion = $MinimumSupportedLVVersion
        SupportedBitness          = $SupportedBitness
        LabVIEWMinorRevision      = $LabVIEWMinorRevision
        RelativePath              = $RelativePath
        VIPBPath                  = $VIPBPath
        Major                     = $Major
        Minor                     = $Minor
        Patch                     = $Patch
        Build                     = $Build
        Commit                    = $Commit
        DisplayInformationJSON    = $DisplayInformationJSON
        ReleaseNotesFile          = $ReleaseNotesFile
    }
    return Invoke-OpenSourceActionScript -ScriptSegments @('build-vi-package','build_vip.ps1') -Arguments $args -DryRun:$DryRun -gcliPath $gcliPath
}

# Builds the project and records version information.
# RelativePath: Normalized path to the project root relative to the working directory.
# Major: Major version component.
# Minor: Minor version component.
# Patch: Patch version component.
# Build: Build number component.
# Commit: Commit identifier used for the build metadata.
# LabVIEWMinorRevision: Minor revision of LabVIEW used for the build.
# CompanyName: Company name recorded in build metadata.
# AuthorName: Author name recorded in build metadata.
# DryRun: If set, prints the command instead of executing it.
# gcliPath: Optional path prepended to PATH for locating the g CLI.
function Invoke-Build {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)] [string] $RelativePath,
        [Parameter(Mandatory)] [int] $Major,
        [Parameter(Mandatory)] [int] $Minor,
        [Parameter(Mandatory)] [int] $Patch,
        [Parameter(Mandatory)] [int] $Build,
        [Parameter(Mandatory)] [string] $Commit,
        [Parameter(Mandatory)] [string] $LabVIEWMinorRevision,
        [Parameter(Mandatory)] [string] $CompanyName,
        [Parameter(Mandatory)] [string] $AuthorName,
        [Parameter()] [switch] $DryRun,
        [Parameter()] [string] $gcliPath
    )
    Write-Information "Executing Build version $Major.$Minor.$Patch-$Build (DryRun=$DryRun)"
    $args = @{
        RelativePath         = $RelativePath
        Major                = $Major
        Minor                = $Minor
        Patch                = $Patch
        Build                = $Build
        Commit               = $Commit
        LabVIEWMinorRevision = $LabVIEWMinorRevision
        CompanyName          = $CompanyName
        AuthorName           = $AuthorName
    }
    return Invoke-OpenSourceActionScript -ScriptSegments @('build','Build.ps1') -Arguments $args -DryRun:$DryRun -gcliPath $gcliPath
}

# Builds a LabVIEW Packed Library using a project and build spec.
# MinimumSupportedLVVersion: Minimum LabVIEW version that the library supports.
# SupportedBitness: Target LabVIEW bitness (32- or 64-bit).
# RelativePath: Normalized path to the project root relative to the working directory.
# LabVIEW_Project: Path to the LabVIEW project file.
# Build_Spec: Name of the build specification within the project.
# Major: Major version component.
# Minor: Minor version component.
# Patch: Patch version component.
# Build: Build number component.
# Commit: Commit identifier used for the build metadata.
# DryRun: If set, prints the command instead of executing it.
# gcliPath: Optional path prepended to PATH for locating the g CLI.
function Invoke-BuildLvlibp {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)] [string] $MinimumSupportedLVVersion,
        [Parameter(Mandatory)] [string] $SupportedBitness,
        [Parameter(Mandatory)] [string] $RelativePath,
        [Parameter(Mandatory)] [string] $LabVIEW_Project,
        [Parameter(Mandatory)] [string] $Build_Spec,
        [Parameter(Mandatory)] [int] $Major,
        [Parameter(Mandatory)] [int] $Minor,
        [Parameter(Mandatory)] [int] $Patch,
        [Parameter(Mandatory)] [int] $Build,
        [Parameter(Mandatory)] [string] $Commit,
        [Parameter()] [switch] $DryRun,
        [Parameter()] [string] $gcliPath
    )
    Write-Information "Executing BuildLvlibp version $Major.$Minor.$Patch-$Build (DryRun=$DryRun)"
    $args = @{
        MinimumSupportedLVVersion = $MinimumSupportedLVVersion
        SupportedBitness          = $SupportedBitness
        RelativePath              = $RelativePath
        LabVIEW_Project           = $LabVIEW_Project
        Build_Spec                = $Build_Spec
        Major                     = $Major
        Minor                     = $Minor
        Patch                     = $Patch
        Build                     = $Build
        Commit                    = $Commit
    }
    return Invoke-OpenSourceActionScript -ScriptSegments @('build-lvlibp','Build_lvlibp.ps1') -Arguments $args -DryRun:$DryRun -gcliPath $gcliPath
}

# Closes any running instance of LabVIEW.
# MinimumSupportedLVVersion: Minimum LabVIEW version that the project supports.
# SupportedBitness: Target LabVIEW bitness (32- or 64-bit).
# DryRun: If set, prints the command instead of executing it.
# gcliPath: Optional path prepended to PATH for locating the g CLI.
function Invoke-CloseLabVIEW {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][Alias('minimum_supported_lv_version')] [string] $MinimumSupportedLVVersion,
        [Parameter(Mandatory)][Alias('supported_bitness')]          [string] $SupportedBitness,
        [Parameter()] [switch] $DryRun,
        [Parameter()] [string] $gcliPath
    )
    Write-Information "Executing CloseLabVIEW (DryRun=$DryRun)"
    $args = @{
        MinimumSupportedLVVersion = $MinimumSupportedLVVersion
        SupportedBitness          = $SupportedBitness
    }
    return Invoke-OpenSourceActionScript -ScriptSegments @('close-labview','Close_LabVIEW.ps1') -Arguments $args -DryRun:$DryRun -gcliPath $gcliPath
}

# Configures LabVIEW settings by updating LabVIEW.ini file.
# LabVIEWVersion: LabVIEW version (e.g., "2025", "2024").
# IniSettings: INI settings to add (multiline string or array).
# LabVIEWWaitSeconds: Seconds to wait for LabVIEW to generate ini file if it does not exist.
# DryRun: If set, prints the command instead of executing it.
function Invoke-ConfigureLabview {
    [CmdletBinding()]
    param(
        [Parameter()] [string] $LabVIEWVersion = "2025",
        [Parameter()] [string] $IniSettings = @"
server.tcp.enabled=TRUE
server.tcp.access=+127.0.0.1;+localhost;+*
server.viscripting.ShowScriptingOperationsInEditor=TRUE
"@,
        [Parameter()] [int] $LabVIEWWaitSeconds = 50,
        [switch] $DryRun
    )
    Write-Information "Configuring LabVIEW $LabVIEWVersion settings" -InformationAction Continue
    
    $result = Invoke-OpenSourceActionScript `
        -ScriptSegments @('configure-labview', 'ConfigureLabview.ps1') `
        -Arguments @{
            LabVIEWVersion = $LabVIEWVersion
            IniSettings = $IniSettings
            LabVIEWWaitSeconds = $LabVIEWWaitSeconds
        } `
        -DryRun:$DryRun
    
    return $result
}

# Generates a release notes file from the project's metadata.
# OutputPath: Path where the release notes should be written.
# DryRun: If set, prints the command instead of executing it.
# gcliPath: Optional path prepended to PATH for locating the g CLI.
function Invoke-GenerateReleaseNotes {
    [CmdletBinding()]
    param(
        [Parameter()] [string] $OutputPath = 'Tooling/deployment/release_notes.md',
        [Parameter()] [switch] $DryRun,
        [Parameter()] [string] $gcliPath
    )
    Write-Information "Executing GenerateReleaseNotes (DryRun=$DryRun)"
    $args = @{ OutputPath = $OutputPath }
    return Invoke-OpenSourceActionScript -ScriptSegments @('generate-release-notes','GenerateReleaseNotes.ps1') -Arguments $args -DryRun:$DryRun -gcliPath $gcliPath
}

# Lists files referenced in a LabVIEW project that are missing on disk.
# LVVersion: LabVIEW version of the project.
# SupportedBitness: Target LabVIEW bitness (32- or 64-bit).
# ProjectFile: Path to the .lvproj file to analyze.
# DryRun: If set, prints the command instead of executing it.
# gcliPath: Optional path prepended to PATH for locating the g CLI.
function Invoke-MissingInProject {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)] [string] $LVVersion,
        [Parameter(Mandatory)] [string] $SupportedBitness,
        [Parameter(Mandatory)] [string] $ProjectFile,
        [Parameter()] [switch] $DryRun,
        [Parameter()] [string] $gcliPath
    )
    Write-Information "Executing MissingInProject (DryRun=$DryRun)"
    $args = @{
        LVVersion        = $LVVersion
        SupportedBitness = $SupportedBitness
        ProjectFile      = $ProjectFile
    }
    return Invoke-OpenSourceActionScript -ScriptSegments @('missing-in-project','Invoke-MissingInProjectCLI.ps1') -Arguments $args -DryRun:$DryRun -gcliPath $gcliPath
}

# Updates display information fields in a VIPB build specification.
# SupportedBitness: Target LabVIEW bitness (32- or 64-bit).
# RelativePath: Normalized path to the project root relative to the working directory.
# VIPBPath: Path to the VIPB build specification file.
# MinimumSupportedLVVersion: Minimum LabVIEW version that the package supports.
# LabVIEWMinorRevision: Minor revision of LabVIEW used for the build.
# Major: Major version component.
# Minor: Minor version component.
# Patch: Patch version component.
# Build: Build number component.
# Commit: Commit identifier used for the build metadata.
# DisplayInformationJSON: JSON string containing display information for the package.
# ReleaseNotesFile: Optional path to a release notes file.
# DryRun: If set, prints the command instead of executing it.
# gcliPath: Optional path prepended to PATH for locating the g CLI.
function Invoke-ModifyVIPBDisplayInfo {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)] [string] $SupportedBitness,
        [Parameter(Mandatory)] [string] $RelativePath,
        [Parameter(Mandatory)] [string] $VIPBPath,
        [Parameter(Mandatory)] [string] $MinimumSupportedLVVersion,
        [Parameter(Mandatory)] [string] $LabVIEWMinorRevision,
        [Parameter(Mandatory)] [int] $Major,
        [Parameter(Mandatory)] [int] $Minor,
        [Parameter(Mandatory)] [int] $Patch,
        [Parameter(Mandatory)] [int] $Build,
        [Parameter(Mandatory)] [string] $Commit,
        [Parameter(Mandatory)] [string] $DisplayInformationJSON,
        [Parameter()] [string] $ReleaseNotesFile,
        [Parameter()] [switch] $DryRun,
        [Parameter()] [string] $gcliPath
    )
    Write-Information "Executing ModifyVIPBDisplayInfo (DryRun=$DryRun)"
    $args = @{
        SupportedBitness       = $SupportedBitness
        RelativePath           = $RelativePath
        VIPBPath               = $VIPBPath
        MinimumSupportedLVVersion = $MinimumSupportedLVVersion
        LabVIEWMinorRevision   = $LabVIEWMinorRevision
        Major                  = $Major
        Minor                  = $Minor
        Patch                  = $Patch
        Build                  = $Build
        Commit                 = $Commit
        DisplayInformationJSON = $DisplayInformationJSON
        ReleaseNotesFile       = $ReleaseNotesFile
    }
    return Invoke-OpenSourceActionScript -ScriptSegments @('modify-vipb-display-info','ModifyVIPBDisplayInfo.ps1') -Arguments $args -DryRun:$DryRun -gcliPath $gcliPath
}

# Prepares a LabVIEW project for source distribution.
# MinimumSupportedLVVersion: Minimum LabVIEW version that the project supports.
# SupportedBitness: Target LabVIEW bitness (32- or 64-bit).
# RelativePath: Normalized path to the project root relative to the working directory.
# LabVIEW_Project: Path to the LabVIEW project file.
# Build_Spec: Name of the build specification within the project.
# DryRun: If set, prints the command instead of executing it.
# gcliPath: Optional path prepended to PATH for locating the g CLI.
function Invoke-PrepareLabVIEWSource {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)] [string] $MinimumSupportedLVVersion,
        [Parameter(Mandatory)] [string] $SupportedBitness,
        [Parameter(Mandatory)] [string] $RelativePath,
        [Parameter(Mandatory)] [string] $LabVIEW_Project,
        [Parameter(Mandatory)] [string] $Build_Spec,
        [Parameter()] [switch] $DryRun,
        [Parameter()] [string] $gcliPath
    )
    Write-Information "Executing PrepareLabVIEWSource (DryRun=$DryRun)"
    $args = @{
        MinimumSupportedLVVersion = $MinimumSupportedLVVersion
       SupportedBitness          = $SupportedBitness
        RelativePath              = $RelativePath
        LabVIEW_Project           = $LabVIEW_Project
        Build_Spec                = $Build_Spec
    }
    return Invoke-OpenSourceActionScript -ScriptSegments @('prepare-labview-source','Prepare_LabVIEW_source.ps1') -Arguments $args -DryRun:$DryRun -gcliPath $gcliPath
}

# Renames a file on disk.
# CurrentFilename: Existing path to the file.
# NewFilename: New path for the file.
# DryRun: If set, prints the command instead of executing it.
# gcliPath: Optional path prepended to PATH for locating the g CLI.
function Invoke-RenameFile {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)] [string] $CurrentFilename,
        [Parameter(Mandatory)] [string] $NewFilename,
        [Parameter()] [switch] $DryRun,
        [Parameter()] [string] $gcliPath
    )
    Write-Information "Executing RenameFile (DryRun=$DryRun)"
    $args = @{
        CurrentFilename = $CurrentFilename
        NewFilename     = $NewFilename
    }
    return Invoke-OpenSourceActionScript -ScriptSegments @('rename-file','Rename-file.ps1') -Arguments $args -DryRun:$DryRun -gcliPath $gcliPath
}

# Restores the Setup LabVIEW source build specification.
# MinimumSupportedLVVersion: Minimum LabVIEW version that the project supports.
# SupportedBitness: Target LabVIEW bitness (32- or 64-bit).
# RelativePath: Normalized path to the project root relative to the working directory.
# LabVIEW_Project: Path to the LabVIEW project file.
# Build_Spec: Name of the build specification within the project.
# DryRun: If set, prints the command instead of executing it.
# gcliPath: Optional path prepended to PATH for locating the g CLI.
function Invoke-RestoreSetupLVSource {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)] [string] $MinimumSupportedLVVersion,
        [Parameter(Mandatory)] [string] $SupportedBitness,
        [Parameter(Mandatory)] [string] $RelativePath,
        [Parameter(Mandatory)] [string] $LabVIEW_Project,
        [Parameter(Mandatory)] [string] $Build_Spec,
        [Parameter()] [switch] $DryRun,
        [Parameter()] [string] $gcliPath
    )
    Write-Information "Executing RestoreSetupLVSource (DryRun=$DryRun)"
    $args = @{
        MinimumSupportedLVVersion = $MinimumSupportedLVVersion
        SupportedBitness          = $SupportedBitness
        RelativePath              = $RelativePath
        LabVIEW_Project           = $LabVIEW_Project
        Build_Spec                = $Build_Spec
    }
    return Invoke-OpenSourceActionScript -ScriptSegments @('restore-setup-lv-source','RestoreSetupLVSource.ps1') -Arguments $args -DryRun:$DryRun -gcliPath $gcliPath
}

# Returns a repository to its previous development mode state.
# RelativePath: Normalized path to the project root relative to the working directory.
# DryRun: If set, prints the command instead of executing it.
# gcliPath: Optional path prepended to PATH for locating the g CLI.
function Invoke-RevertDevelopmentMode {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)] [string] $RelativePath,
        [Parameter()] [switch] $DryRun,
        [Parameter()] [string] $gcliPath
    )
    Write-Information "Executing RevertDevelopmentMode (DryRun=$DryRun)"
    $args = @{ RelativePath = $RelativePath }
    return Invoke-OpenSourceActionScript -ScriptSegments @('revert-development-mode','RevertDevelopmentMode.ps1') -Arguments $args -DryRun:$DryRun -gcliPath $gcliPath
}

# Runs Pester tests located in the specified working directory.
# WorkingDirectory: Path containing the Pester tests to execute.
# DryRun: If set, prints the command instead of executing it.
# gcliPath: Optional path prepended to PATH for locating the g CLI.
function Invoke-RunPesterTests {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)] [string] $WorkingDirectory,
        [Parameter()] [switch] $DryRun,
        [Parameter()] [string] $gcliPath
    )
    Write-Information "Executing RunPesterTests (DryRun=$DryRun)"
    $args = @{ WorkingDirectory = $WorkingDirectory }
    return Invoke-OpenSourceActionScript -ScriptSegments @('run-pester-tests','RunPesterTests.ps1') -Arguments $args -DryRun:$DryRun -gcliPath $gcliPath
}

# Runs LabVIEW unit tests.
# MinimumSupportedLVVersion: Minimum LabVIEW version that the project supports.
# SupportedBitness: Target LabVIEW bitness (32- or 64-bit).
# DryRun: If set, prints the command instead of executing it.
# gcliPath: Optional path prepended to PATH for locating the g CLI.
function Invoke-RunUnitTests {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)] [string] $MinimumSupportedLVVersion,
        [Parameter(Mandatory)] [string] $SupportedBitness,
        [Parameter()] [switch] $DryRun,
        [Parameter()] [string] $gcliPath
    )
    Write-Information "Executing RunUnitTests (DryRun=$DryRun)"
    $args = @{
        MinimumSupportedLVVersion = $MinimumSupportedLVVersion
        SupportedBitness          = $SupportedBitness
    }
    return Invoke-OpenSourceActionScript -ScriptSegments @('run-unit-tests','RunUnitTests.ps1') -Arguments $args -DryRun:$DryRun -gcliPath $gcliPath
}

# Configures the repository for development mode.
# RelativePath: Normalized path to the project root relative to the working directory.
# DryRun: If set, prints the command instead of executing it.
# gcliPath: Optional path prepended to PATH for locating the g CLI.
function Invoke-SetDevelopmentMode {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)] [string] $RelativePath,
        [Parameter()] [switch] $DryRun,
        [Parameter()] [string] $gcliPath
    )
    Write-Information "Executing SetDevelopmentMode (DryRun=$DryRun)"
    $args = @{ RelativePath = $RelativePath }
    return Invoke-OpenSourceActionScript -ScriptSegments @('set-development-mode','Set_Development_Mode.ps1') -Arguments $args -DryRun:$DryRun -gcliPath $gcliPath
}

# Downloads and installs LabVIEW Community Edition from an ISO image.
# LabVIEWIsoUrl: URL to download the LabVIEW ISO installer.
# TimeoutSeconds: Maximum time in seconds to wait for installation to complete.
# DryRun: If set, prints the command instead of executing it.
function Invoke-SetupLabview {
    [CmdletBinding()]
    param(
        [Parameter()] [string] $LabVIEWIsoUrl = "https://download.ni.com/support/softlib/labview/labview_development_system/2025_Q3/ni-labview-2025-community-x86_25.3.3_offline.iso",
        [Parameter()] [int] $TimeoutSeconds = 2700,
        [switch] $DryRun
    )
    Write-Information "Setting up LabVIEW from $LabVIEWIsoUrl (timeout: $TimeoutSeconds seconds)" -InformationAction Continue
    
    $result = Invoke-OpenSourceActionScript `
        -ScriptSegments @('setup-labview', 'SetupLabview.ps1') `
        -Arguments @{
            LabVIEWIsoUrl = $LabVIEWIsoUrl
            TimeoutSeconds = $TimeoutSeconds
            
# Installs and configures NI Package Manager (NIPM).
# NIPMUrl: URL to download the NI Package Manager installer.
# DryRun: If set, prints the command instead of executing it.
function Invoke-SetupNipm {
    [CmdletBinding()]
    param(
        [Parameter()] [string] $NIPMUrl = "https://download.ni.com/support/nipkg/products/ni-package-manager/installers/NIPackageManager25.8.0.exe",
        [switch] $DryRun
    )
    Write-Information "Setting up NI Package Manager from $NIPMUrl" -InformationAction Continue
    
    $result = Invoke-OpenSourceActionScript `
        -ScriptSegments @('setup-nipm', 'SetupNipm.ps1') `
        -Arguments @{
            NIPMUrl = $NIPMUrl
        } `
        -DryRun:$DryRun
    
    return $result
}