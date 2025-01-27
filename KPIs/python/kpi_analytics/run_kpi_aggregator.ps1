# run_kpi_aggregator.ps1
<#
Purpose: 
  - Provide a simple entry point for BFS aggregator with monthly BFS intervals.
  - Sets environment variables, calls python main.py.
  - Overwrites logs, produces PNG charts in user-defined output folder.

Usage Example (PowerShell):
  .\run_kpi_aggregator.ps1 `
    -ScalingRepo "ni/labview-icon-editor" `
    -AllRepos "ni/labview-icon-editor,facebook/react,tensorflow/tensorflow,dotnet/core" `
    -NumMonths 12 `
    -MonthlyBFS $true `
    -GlobalOffset 0 `
    -OutputFolder "kpi_out" `
    -DbHost "localhost" `
    -DbUser "root" `
    -DbPassword "" `
    -DbDatabase "my_kpis_analytics_db" `
    -EndDate "2025-12-31"
	
	 .\run_kpi_aggregator.ps1 `
    -ScalingRepo "ni/labview-icon-editor" `
    -AllRepos "ni/labview-icon-editor,facebook/react,tensorflow/tensorflow,dotnet/core" `
    -NumMonths 12 `
    -MonthlyBFS $true `
    -GlobalOffset 0 `
    -OutputFolder "kpi_out" `
    -DbHost "localhost" `
    -DbUser "root" `
    -DbPassword "" `
    -DbDatabase "my_kpis_analytics_db" `
    -EndDate "2025-12-31"

#>

param(
  [string] $ScalingRepo = "ni/labview-icon-editor",
  [string] $AllRepos = "ni/labview-icon-editor,facebook/react,tensorflow/tensorflow,dotnet/core",
  [int] $NumMonths = 12,
  [bool] $MonthlyBFS = $true,
  [int] $GlobalOffset = 0,
  [string] $OutputFolder = "kpi_out",
  [string] $DbHost = "localhost",
  [string] $DbUser = "root",
  [string] $DbPassword = "root",
  [string] $DbDatabase = "my_kpis_analytics_db",
  [string] $EndDate = ""
)

Write-Host "=== run_kpi_aggregator.ps1 ==="
Write-Host "ScalingRepo=$ScalingRepo"
Write-Host "AllRepos=$AllRepos"
Write-Host "NumMonths=$NumMonths (for BFS aggregator intervals)"
Write-Host "MonthlyBFS=$MonthlyBFS"
Write-Host "GlobalOffset=$GlobalOffset"
Write-Host "OutputFolder=$OutputFolder"
Write-Host "DbHost=$DbHost, DbUser=$DbUser, DbPassword=***, DbDatabase=$DbDatabase"
Write-Host "EndDate=$EndDate"

# Set environment variables
$env:SCALING_REPO = $ScalingRepo
$env:ALL_REPOS = $AllRepos
$env:NUM_MONTHS = $NumMonths
$env:MONTHLY_BFS = if($MonthlyBFS){"1"} else {"0"}
$env:GLOBAL_OFFSET = $GlobalOffset
$env:OUTPUT_FOLDER = $OutputFolder
$env:DB_HOST = $DbHost
$env:DB_USER = $DbUser
$env:DB_PASSWORD = $DbPassword
$env:DB_DATABASE = $DbDatabase
$env:END_DATE = $EndDate

# Execute main script
python .\main.py
