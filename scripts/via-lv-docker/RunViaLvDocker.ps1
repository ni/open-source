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
    $DockerImage = 'nationalinstruments/labview'
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

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

# Run VI Analyzer in container
Write-Information "Running VI Analyzer in Docker container..." -InformationAction Continue

$workspaceMount = "${PWD}:/workspace"
$scriptMount = "${runViaScript}:/tmp/run-via.sh"

docker run --rm `
    -v $workspaceMount `
    -v $scriptMount `
    "${DockerImage}:${LabviewVersion}" `
    bash -c "chmod +x /tmp/run-via.sh && /tmp/run-via.sh '$configToUse'"

$viaExitCode = $LASTEXITCODE
Write-Information "VI Analyzer exit code: $viaExitCode" -InformationAction Continue

# Parse VI Analyzer report
if (Test-Path $parseReportScript) {
    Write-Information "Parsing VI Analyzer report..." -InformationAction Continue
    bash $parseReportScript $viaExitCode
}

# Exit with VI Analyzer's exit code
exit $viaExitCode