#requires -Version 7.0
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

Describe 'SetupNipm.Workflow' {
    $meta = @{
        requirement = 'REQ-035'
        Owner       = 'DevTools'
        Evidence    = 'tests/pester/SetupNipm.Workflow.Tests.ps1'
    }

    It 'defines setup-nipm action with required inputs [REQ-035]' -Tag 'REQ-035' {
        $repoRoot = (Resolve-Path (Join-Path $PSScriptRoot '..' '..')).Path
        $actionPath = Join-Path $repoRoot 'setup-nipm/action.yml'
        
        $actionPath | Should -Exist
        
        # Use regex-based validation instead of YAML parsing
        $yamlContent = Get-Content -Raw $actionPath
        
        $yamlContent | Should -Match 'name:.*Setup NI Package Manager'
        $yamlContent | Should -Match 'nipm_url:'
        $yamlContent | Should -Match 'default:.*NIPackageManager'
    }

    It 'validates adapter function accepts NIPMUrl parameter [REQ-035]' -Tag 'REQ-035' {
        Import-Module (Join-Path $PSScriptRoot '../../actions/OpenSourceActions.psm1') -Force
        
        $cmd = Get-Command Invoke-SetupNipm -ErrorAction Stop
        $cmd | Should -Not -BeNullOrEmpty
        
        $params = $cmd.Parameters.Keys
        $params | Should -Contain 'NIPMUrl'
        $params | Should -Contain 'DryRun'
    }
}