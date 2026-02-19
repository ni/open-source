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
        
        $action = Get-Content -Raw $actionPath | ConvertFrom-Yaml
        $action.name | Should -Be 'Setup NI Package Manager'
        $action.inputs.nipm_url | Should -Not -BeNullOrEmpty
        $action.inputs.nipm_url.default | Should -Match 'NIPackageManager'
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