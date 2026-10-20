<#
.SYNOPSIS
    Builds a VI Package (.vip) from a VIPB inside the NI LabVIEW Linux Docker container.

.DESCRIPTION
    Mounts the current directory at /workspace and runs build-vip.sh in the LabVIEW
    Linux image. The helper installs and activates the VIPM CLI, runs the optional
    project pre-build script, prepares a build copy of the VIPB and runs `vipm build`.
    When GITHUB_OUTPUT is set, writes vip_path, vip_paths, package_version,
    output_directory and log_directory outputs.

.PARAMETER VipbPath
    Path to the .vipb file, relative to the workspace. May be created by PreBuildScript.

.PARAMETER LabVIEWVersion
    LabVIEW container release (YYYYq1 or YYYYq3).

.PARAMETER DockerImage
    Docker image name.

.PARAMETER ImageTag
    Docker image tag. Defaults to "<LabVIEWVersion>-linux".

.PARAMETER SourceDirectory
    VIPB source folder. Relative paths resolve against the workspace; absolute paths are container paths.

.PARAMETER OutputDirectory
    Directory, relative to the workspace, that receives the built .vip files.

.PARAMETER LogDirectory
    Directory, relative to the workspace, that receives build logs and the prepared VIPB.

.PARAMETER PreBuildScript
    Bash script, relative to the workspace, run inside the container after VIPM activation.

.PARAMETER VipmSerialNumber
    VIPM Pro serial number.

.PARAMETER VipmFullName
    Name used for VIPM Pro activation.

.PARAMETER VipmEmail
    Email used for VIPM Pro activation.

.PARAMETER VipmDebUrl
    URL for the VIPM Linux .deb package.

.PARAMETER VipmDebSha256
    Expected SHA256 of the VIPM .deb package; verification is skipped when empty.

.PARAMETER BuildTimeoutSeconds
    Timeout passed to `vipm build --timeout`.

.PARAMETER DesktopLivelinessTimeout
    VIPM desktop liveliness timeout in seconds.

.NOTES
    Leaf script for the 'build-vip-docker-linux' dispatcher action.
#>

[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [string]$VipbPath,

    [Parameter(Mandatory = $false)]
    [string]$LabVIEWVersion = "2026q1",

    [Parameter(Mandatory = $false)]
    [string]$DockerImage = "nationalinstruments/labview",

    [Parameter(Mandatory = $false)]
    [string]$ImageTag = "",

    [Parameter(Mandatory = $false)]
    [string]$SourceDirectory = ".",

    [Parameter(Mandatory = $false)]
    [string]$OutputDirectory = "builds/vip",

    [Parameter(Mandatory = $false)]
    [string]$LogDirectory = "builds/logs/vip-linux",

    [Parameter(Mandatory = $false)]
    [string]$PreBuildScript = "",

    [Parameter(Mandatory = $false)]
    [string]$VipmSerialNumber = "",

    [Parameter(Mandatory = $false)]
    [string]$VipmFullName = "",

    [Parameter(Mandatory = $false)]
    [string]$VipmEmail = "",

    [Parameter(Mandatory = $false)]
    [string]$VipmDebUrl = "https://traffic.libsyn.com/secure/jkinc/vipm_26.3.1-4025_amd64.deb",

    [Parameter(Mandatory = $false)]
    [string]$VipmDebSha256 = "",

    [Parameter(Mandatory = $false)]
    [string]$BuildTimeoutSeconds = "900",

    [Parameter(Mandatory = $false)]
    [string]$DesktopLivelinessTimeout = "300"
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

function Assert-WorkspaceRelative([string]$Name, [string]$Value) {
    $normalized = $Value -replace '\\', '/'
    if ([System.IO.Path]::IsPathRooted($Value) -or "/$normalized/" -match '/\.\./') {
        throw "$Name must be a path relative to the workspace without '..': $Value"
    }
}

$containerEnv = [ordered]@{}
try {
    if ($LabVIEWVersion -notmatch '^\d{4}q[13]$') {
        throw "Invalid LabVIEWVersion '$LabVIEWVersion'. Expected YYYYq1 or YYYYq3 (for example, 2026q1)."
    }
    foreach ($pair in @(@('BuildTimeoutSeconds', $BuildTimeoutSeconds), @('DesktopLivelinessTimeout', $DesktopLivelinessTimeout))) {
        if ($pair[1] -notmatch '^\d+$') { throw "$($pair[0]) must be a whole number of seconds; got '$($pair[1])'." }
    }
    if ($VipmDebSha256 -and $VipmDebSha256 -notmatch '^[0-9a-fA-F]{64}$') {
        throw 'VipmDebSha256 must be a 64-character hexadecimal SHA256 digest.'
    }
    foreach ($name in 'VipmSerialNumber', 'VipmFullName', 'VipmEmail') {
        if ([string]::IsNullOrWhiteSpace((Get-Variable -Name $name -ValueOnly))) {
            throw "$name is required; vipm build needs VIPM Pro activation."
        }
    }
    Assert-WorkspaceRelative 'VipbPath' $VipbPath
    Assert-WorkspaceRelative 'OutputDirectory' $OutputDirectory
    Assert-WorkspaceRelative 'LogDirectory' $LogDirectory
    if ($PreBuildScript) {
        Assert-WorkspaceRelative 'PreBuildScript' $PreBuildScript
        if (-not (Test-Path -LiteralPath $PreBuildScript -PathType Leaf)) {
            throw "Pre-build script not found: $PreBuildScript"
        }
    }

    $helperScript = Join-Path $PSScriptRoot 'build-vip.sh'
    if (-not (Test-Path -LiteralPath $helperScript)) {
        throw "Helper script not found: $helperScript"
    }

    New-Item -ItemType Directory -Force -Path $OutputDirectory, $LogDirectory | Out-Null

    $tag = if ($ImageTag) { $ImageTag } else { "$LabVIEWVersion-linux" }
    $fullImage = "${DockerImage}:${tag}"
    Write-Information "Docker Image: $fullImage" -InformationAction Continue

    # Passed by name only so values (including secrets) never appear on the docker command line.
    $containerEnv['LABVIEW_VERSION'] = $LabVIEWVersion
    $containerEnv['VIPM_SERIAL_NUMBER'] = $VipmSerialNumber
    $containerEnv['VIPM_FULL_NAME'] = $VipmFullName
    $containerEnv['VIPM_EMAIL'] = $VipmEmail
    $containerEnv['BVD_VIPB_PATH'] = $VipbPath -replace '\\', '/'
    $containerEnv['BVD_SOURCE_DIRECTORY'] = $SourceDirectory -replace '\\', '/'
    $containerEnv['BVD_OUTPUT_DIRECTORY'] = $OutputDirectory -replace '\\', '/'
    $containerEnv['BVD_LOG_DIRECTORY'] = $LogDirectory -replace '\\', '/'
    $containerEnv['BVD_PRE_BUILD_SCRIPT'] = $PreBuildScript -replace '\\', '/'
    $containerEnv['BVD_VIPM_DEB_URL'] = $VipmDebUrl
    $containerEnv['BVD_VIPM_DEB_SHA256'] = $VipmDebSha256
    $containerEnv['BVD_BUILD_TIMEOUT_SECONDS'] = $BuildTimeoutSeconds
    $containerEnv['BVD_DESKTOP_LIVELINESS_TIMEOUT'] = $DesktopLivelinessTimeout

    $envArgs = @()
    foreach ($key in $containerEnv.Keys) {
        Set-Item -Path "env:$key" -Value $containerEnv[$key]
        $envArgs += @('--env', $key)
    }

    docker pull $fullImage *>&1 | ForEach-Object { Write-Information $_ -InformationAction Continue }
    if ($LASTEXITCODE -ne 0) { throw "docker pull $fullImage failed with exit code $LASTEXITCODE" }

    $workspace = (Get-Location).Path
    $dockerArgs = @(
        'run', '--rm'
        '--mount', "type=bind,source=$workspace,target=/workspace"
        '--mount', "type=bind,source=$PSScriptRoot,target=/helpers,readonly"
        '--workdir', '/workspace'
    ) + $envArgs + @($fullImage, 'bash', '/helpers/build-vip.sh')

    Write-Information 'Building VI Package in Docker container...' -InformationAction Continue
    & docker @dockerArgs *>&1 | ForEach-Object { Write-Information $_ -InformationAction Continue }
    $exitCode = $LASTEXITCODE
    if ($exitCode -ne 0) {
        throw "VI Package build inside the LabVIEW Linux container failed with exit code $exitCode. See logs in $LogDirectory."
    }

    $vipFiles = @(Get-ChildItem -LiteralPath $OutputDirectory -Filter '*.vip' -File | Sort-Object Name)
    if ($vipFiles.Count -eq 0) {
        throw "No .vip files found in $OutputDirectory."
    }
    $versionFile = Join-Path $LogDirectory 'package-version.txt'
    $packageVersion = if (Test-Path -LiteralPath $versionFile) { (Get-Content -LiteralPath $versionFile -Raw).Trim() } else { '' }

    foreach ($vip in $vipFiles) { Write-Information "Built: $($vip.FullName)" -InformationAction Continue }

    if ($env:GITHUB_OUTPUT) {
        $vipPathsJson = ConvertTo-Json -InputObject @($vipFiles.FullName) -Compress
        @(
            "vip_path=$($vipFiles[0].FullName)"
            "vip_paths=$vipPathsJson"
            "package_version=$packageVersion"
            "output_directory=$((Resolve-Path -LiteralPath $OutputDirectory).Path)"
            "log_directory=$((Resolve-Path -LiteralPath $LogDirectory).Path)"
        ) | Out-File -FilePath $env:GITHUB_OUTPUT -Append -Encoding utf8
    }

    exit 0
}
catch {
    Write-Error "BuildVipDockerLinux failed: $_"
    exit 1
}
finally {
    foreach ($key in $containerEnv.Keys) {
        Remove-Item -Path "env:$key" -ErrorAction SilentlyContinue
    }
}
