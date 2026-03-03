#requires -Version 7.0
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

Describe 'ConfigureLabview.Workflow' {
    $meta = @{
        requirement = 'REQ-037'
        Owner       = 'DevTools'
        Evidence    = 'tests/pester/ConfigureLabview.Workflow.Tests.ps1'
    }

    It 'defines configure-labview action with required inputs [REQ-037]' -Tag 'REQ-037' {
        $repoRoot = (Resolve-Path (Join-Path $PSScriptRoot '..' '..')).Path
        $actionPath = Join-Path $repoRoot 'configure-labview/action.yml'
        
        $actionPath | Should -Exist
        
        # Use regex-based validation instead of YAML parsing
        $yamlContent = Get-Content -Raw $actionPath
        
        $yamlContent | Should -Match "name:\s*['\"]?Configure LabVIEW"
        $yamlContent | Should -Match 'labview_version:'
        $yamlContent | Should -Match "default:\s*['\"]?2025"
        $yamlContent | Should -Match 'ini_settings:'
        $yamlContent | Should -Match 'labview_wait_seconds:'
        $yamlContent | Should -Match "default:\s*['\"]?60"
    }

    It 'validates adapter function accepts configuration parameters [REQ-037]' -Tag 'REQ-037' {
        Import-Module (Join-Path $PSScriptRoot '../../actions/OpenSourceActions.psm1') -Force
        
        $cmd = Get-Command Invoke-ConfigureLabview -ErrorAction Stop
        $cmd | Should -Not -BeNullOrEmpty
        
        $params = $cmd.Parameters.Keys
        $params | Should -Contain 'LabVIEWVersion'
        $params | Should -Contain 'IniSettings'
        $params | Should -Contain 'LabVIEWWaitSeconds'
        $params | Should -Contain 'DryRun'
    }

    It 'validates default INI settings include TCP and scripting configuration [REQ-037]' -Tag 'REQ-037' {
        Import-Module (Join-Path $PSScriptRoot '../../actions/OpenSourceActions.psm1') -Force
        
        $cmd = Get-Command Invoke-ConfigureLabview -ErrorAction Stop
        $iniSettingsParam = $cmd.Parameters['IniSettings']
        
        # Verify parameter exists and is a string type
        $iniSettingsParam | Should -Not -BeNullOrEmpty
        $iniSettingsParam.ParameterType.Name | Should -Be 'String'
        
        # Verify the function definition includes expected default settings
        # by checking the AST or function definition
        $functionDef = $cmd.Definition
        $functionDef | Should -Match 'server\.tcp\.enabled'
        $functionDef | Should -Match 'server\.tcp\.access'
        $functionDef | Should -Match 'server\.viscripting\.ShowScriptingOperationsInEditor'
    }
}