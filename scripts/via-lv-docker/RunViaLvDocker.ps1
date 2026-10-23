<#
.SYNOPSIS
    Run VI Analyzer tests using LabVIEW Docker container.

.DESCRIPTION
    Executes LabVIEW VI Analyzer tests in a Docker container and parses results.
    Supports both static configuration files and dynamic config generation based on changed files.

.PARAMETER ConfigPath
    Path to VI Analyzer configuration file (.viancfg) or LabVIEW files (.vi, .ctl, .llb). If empty, will generate from changed files.

.PARAMETER TemplatePath
    Path to .viancfg template (required when generating config dynamically).

.PARAMETER BaseBranch
    Branch to compare against for changed files (used when generating config).

.PARAMETER LabviewVersion
    LabVIEW Docker image version tag.

.PARAMETER DockerImage
    Full Docker image name.

.PARAMETER DependencyFile
    Optional TOML file, relative to the workspace, listing VIPM packages to install in the
    analyzer container before VI Analyzer runs. Linux images only. When omitted, nothing is installed.

.NOTES
    Requires Docker and bash. The LabVIEW Docker container does not include PowerShell.
#>

param(
    [Parameter(Mandatory=$false)]
    [string]
    $ConfigPath = '',

    [Parameter(Mandatory=$false)]
    [string]
    $TemplatePath = '',

    [Parameter(Mandatory=$false)]
    [string]
    $BaseBranch = 'origin/develop',

    [Parameter(Mandatory=$false)]
    [string]
    $LabviewVersion = '2026q1-linux',

    [Parameter(Mandatory=$false)]
    [string]
    $DockerImage = 'nationalinstruments/labview',

    [Parameter(Mandatory=$false)]
    [string]
    $DependencyFile = ''
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

# Returns 'name' or 'name@version' entries from the [dependencies] table of a vipm.toml-style file.
function Get-ViaDependencyPackages {
    param([string] $Path)

    if ([System.IO.Path]::IsPathRooted($Path) -or ($Path -split '[\\/]') -contains '..') {
        throw "dependency_file must be a path relative to the workspace without '..': $Path"
    }
    if (-not (Test-Path -LiteralPath $Path -PathType Leaf)) {
        throw "dependency_file not found: $Path"
    }

    $packages = [System.Collections.Generic.List[string]]::new()
    $seen = @{}
    $section = ''
    $lineNumber = 0
    foreach ($rawLine in Get-Content -LiteralPath $Path) {
        $lineNumber++
        $line = ($rawLine -replace '(^|\s)#.*$', '').Trim()
        if (-not $line) { continue }

        if ($line -match '^\[\s*([A-Za-z0-9_.-]+)\s*\]$') {
            $section = $Matches[1]
            if ($section -notin @('project', 'dependencies')) {
                throw "${Path}:${lineNumber}: unsupported table [$section]; only [project] and [dependencies] are allowed."
            }
            continue
        }
        if ($line -notmatch '^([A-Za-z0-9_-]+)\s*=\s*(.+)$') {
            throw "${Path}:${lineNumber}: invalid TOML line: $rawLine"
        }
        $key = $Matches[1]
        $value = $Matches[2].Trim()
        if (-not $section) {
            throw "${Path}:${lineNumber}: key '$key' must be inside a [project] or [dependencies] table."
        }
        if ($section -eq 'project') { continue }

        if ($value -match '^"([^"]*)"$' -or $value -match "^'([^']*)'$") {
            $version = $Matches[1]
        } else {
            throw "${Path}:${lineNumber}: version for '$key' must be a quoted string, e.g. $key = `"1.0.0.1`" or $key = `"latest`"."
        }
        if ($key -notmatch '^[a-z0-9_]+$') {
            throw "${Path}:${lineNumber}: invalid package name '$key'; expected lowercase letters, digits and underscores."
        }
        if ($seen.ContainsKey($key)) {
            throw "${Path}:${lineNumber}: package '$key' is declared more than once."
        }
        $seen[$key] = $true

        if ($version -in @('latest', '*')) {
            $packages.Add($key)
        } elseif ($version -match '^[0-9A-Za-z][0-9A-Za-z.-]*$') {
            $packages.Add("$key@$version")
        } else {
            throw "${Path}:${lineNumber}: invalid version '$version' for '$key'; use a version such as 1.10.7.3, or 'latest'."
        }
    }

    if ($packages.Count -eq 0) {
        throw "dependency_file '$Path' does not declare any packages in a [dependencies] table."
    }
    return ,$packages.ToArray()
}

$dependencyPackages = @()
if (-not [string]::IsNullOrWhiteSpace($DependencyFile)) {
    $dependencyPackages = Get-ViaDependencyPackages -Path $DependencyFile
    Write-Information "LabVIEW dependencies from ${DependencyFile}: $($dependencyPackages -join ', ')" -InformationAction Continue
}

# Get the path to the bash scripts
$viaDockerPath = $PSScriptRoot
$generateScript = Join-Path $viaDockerPath 'generate_viancfg.sh'
$runViaScript = Join-Path $viaDockerPath 'run-via.sh'
$parseReportScript = Join-Path $viaDockerPath 'parse-via-report.sh'

# Determine config strategy
$configToUse = $ConfigPath
$shouldGenerate = $false

if ([string]::IsNullOrWhiteSpace($ConfigPath) -or -not (Test-Path $ConfigPath)) {
    Write-Information "Will generate config from changed files" -InformationAction Continue
    $configToUse = 'generated-config.viancfg'
    $shouldGenerate = $true
    
    # Use default template path if not provided
    if ([string]::IsNullOrWhiteSpace($TemplatePath)) {
        $TemplatePath = Join-Path $viaDockerPath 'via_template_linux.viancfg'
    }
} else {
    Write-Information "Using provided config file: $ConfigPath" -InformationAction Continue
}

# Generate VI Analyzer config if needed
if ($shouldGenerate) {
    Write-Information "Generating VI Analyzer config from template: $TemplatePath" -InformationAction Continue
    
    if (-not (Test-Path $generateScript)) {
        throw "Generate script not found: $generateScript"
    }
    
    bash $generateScript `
        --template-path $TemplatePath `
        --output-path $configToUse `
        --target-branch $BaseBranch
    
    if ($LASTEXITCODE -ne 0) {
        throw "Failed to generate VI Analyzer config (exit code: $LASTEXITCODE)"
    }

    if (-not (Test-Path $configToUse)) {
        Write-Information "No LabVIEW files changed. Skipping VI Analyzer tests." -InformationAction Continue
        exit 0
    }
}

# Validate config exists
if (-not (Test-Path $configToUse)) {
    throw "Config file not found at: $configToUse"
}

Write-Information "Using config: $configToUse" -InformationAction Continue

# Pull LabVIEW Docker image
Write-Information "Pulling Docker image: ${DockerImage}:${LabviewVersion}" -InformationAction Continue
docker pull "${DockerImage}:${LabviewVersion}"

if ($LASTEXITCODE -ne 0) {
    throw "Failed to pull Docker image (exit code: $LASTEXITCODE)"
}

$dockerArgs = @(
    'run', '--rm',
    '-v', "${PWD}:/workspace",
    '-v', "${runViaScript}:/tmp/run-via.sh"
)
$statusDir = $null
if ($dependencyPackages.Count -gt 0) {
    $imageOs = docker image inspect --format '{{.Os}}' "${DockerImage}:${LabviewVersion}"
    if ($LASTEXITCODE -ne 0) {
        throw "Failed to inspect Docker image ${DockerImage}:${LabviewVersion} (exit code: $LASTEXITCODE)"
    }
    if ("$imageOs".Trim() -ne 'linux') {
        throw "dependency_file is supported only for Linux LabVIEW images; ${DockerImage}:${LabviewVersion} is '$("$imageOs".Trim())'."
    }
    # run-via.sh writes a marker here only after all packages install successfully.
    $statusDir = Join-Path ([System.IO.Path]::GetTempPath()) "via-status-$([guid]::NewGuid().ToString('N'))"
    New-Item -ItemType Directory -Path $statusDir | Out-Null
    $dockerArgs += @(
        '-v', "${statusDir}:/tmp/via-status",
        '-e', "VIA_PACKAGES=$($dependencyPackages -join ' ')"
    )
}
$dockerArgs += @(
    "${DockerImage}:${LabviewVersion}",
    'bash', '-c', "chmod +x /tmp/run-via.sh && /tmp/run-via.sh '$configToUse'"
)

# Run VI Analyzer in container
Write-Information "Running VI Analyzer in Docker container..." -InformationAction Continue

try {
    docker @dockerArgs
    $viaExitCode = $LASTEXITCODE
    if ($statusDir -and -not (Test-Path -LiteralPath (Join-Path $statusDir 'dependencies-installed'))) {
        throw "LabVIEW dependency installation from '$DependencyFile' failed (container exit code: $viaExitCode). VI Analyzer was not run; see the container log above."
    }
} finally {
    if ($statusDir) { Remove-Item -LiteralPath $statusDir -Recurse -Force -ErrorAction SilentlyContinue }
}
Write-Information "VI Analyzer exit code: $viaExitCode" -InformationAction Continue

# Parse VI Analyzer report
if (Test-Path $parseReportScript) {
    Write-Information "Parsing VI Analyzer report..." -InformationAction Continue
    & bash $parseReportScript $viaExitCode 2>&1 | Write-Information -InformationAction Continue
} else {
    Write-Warning "Report parsing script not found: $parseReportScript. Skipping report parsing."
}

# Exit with VI Analyzer's exit code
exit $viaExitCode