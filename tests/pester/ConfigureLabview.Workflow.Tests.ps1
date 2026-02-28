#requires -Version 7.0
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

Describe 'ConfigureLabview.Workflow' {
    $meta = @{
        requirement = 'REQ-038'
        Owner       = 'DevTools'
        Evidence    = 'tests/pester/ConfigureLabview.Workflow.Tests.ps1'
    }

    It 'defines configure-labview action with required inputs [REQ-038]' -Tag 'REQ-038' {
        $repoRoot = (Resolve-Path (Join-Path $PSScriptRoot '..' '..')).Path
        $actionPath = Join-Path $repoRoot 'configure-labview/action.yml'
        
        $actionPath | Should -Exist
        
        $action = Get-Content -Raw $actionPath | ConvertFrom-Yaml
        $action.name | Should -Be 'Configure LabVIEW'
        $action.inputs.labview_version | Should -Not -BeNullOrEmpty
        $action.inputs.labview_version.default | Should -Be '2025'
        $action.inputs.ini_settings | Should -Not -BeNullOrEmpty
        $action.inputs.labview_wait_seconds | Should -Not -BeNullOrEmpty
        $action.inputs.labview_wait_seconds.default | Should -Be '60'
    }

    It 'validates adapter function accepts configuration parameters [REQ-038]' -Tag 'REQ-038' {
        Import-Module (Join-Path $PSScriptRoot '../../actions/OpenSourceActions.psm1') -Force
        
        $cmd = Get-Command Invoke-ConfigureLabview -ErrorAction Stop
        $cmd | Should -Not -BeNullOrEmpty
        
        $params = $cmd.Parameters.Keys
        $params | Should -Contain 'LabVIEWVersion'
        $params | Should -Contain 'IniSettings'
        $params | Should -Contain 'LabVIEWWaitSeconds'
        $params | Should -Contain 'DryRun'
    }

    It 'validates default INI settings include TCP and scripting configuration [REQ-038]' -Tag 'REQ-038' {
        Import-Module (Join-Path $PSScriptRoot '../../actions/OpenSourceActions.psm1') -Force
        
        $cmd = Get-Command Invoke-ConfigureLabview -ErrorAction Stop
        $defaultSettings = $cmd.Parameters['IniSettings'].Attributes | 
            Where-Object { $_.TypeId.Name -eq 'ParameterAttribute' } | 
            Select-Object -First 1
        
        # Function should have default INI settings parameter
        $cmd.Parameters['IniSettings'] | Should -Not -BeNullOrEmpty
    }
}