#requires -Version 7.0
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

Describe 'SetupLabview.Workflow' {
    $meta = @{
        requirement = 'REQ-036'
        Owner       = 'DevTools'
        Evidence    = 'tests/pester/SetupLabview.Workflow.Tests.ps1'
    }

    It 'defines setup-labview action with required inputs [REQ-036]' -Tag 'REQ-036' {
        $repoRoot = (Resolve-Path (Join-Path $PSScriptRoot '..' '..')).Path
        $actionPath = Join-Path $repoRoot 'setup-labview/action.yml'
        
        $actionPath | Should -Exist
        
        $action = Get-Content -Raw $actionPath | ConvertFrom-Yaml
        $action.name | Should -Be 'Setup LabVIEW'
        $action.inputs.labview_iso_url | Should -Not -BeNullOrEmpty
        $action.inputs.labview_iso_url.default | Should -Match 'labview'
        $action.inputs.timeout_seconds | Should -Not -BeNullOrEmpty
        $action.inputs.timeout_seconds.default | Should -Be '2700'
    }

    It 'validates adapter function accepts LabVIEWIsoUrl and TimeoutSeconds parameters [REQ-036]' -Tag 'REQ-036' {
        Import-Module (Join-Path $PSScriptRoot '../../actions/OpenSourceActions.psm1') -Force
        
        $cmd = Get-Command Invoke-SetupLabview -ErrorAction Stop
        $cmd | Should -Not -BeNullOrEmpty
        
        $params = $cmd.Parameters.Keys
        $params | Should -Contain 'LabVIEWIsoUrl'
        $params | Should -Contain 'TimeoutSeconds'
        $params | Should -Contain 'DryRun'
    }

    It 'validates timeout parameter is an integer [REQ-036]' -Tag 'REQ-036' {
        Import-Module (Join-Path $PSScriptRoot '../../actions/OpenSourceActions.psm1') -Force
        
        $cmd = Get-Command Invoke-SetupLabview -ErrorAction Stop
        $timeoutParam = $cmd.Parameters['TimeoutSeconds']
        
        $timeoutParam.ParameterType.Name | Should -Be 'Int32'
    }
}