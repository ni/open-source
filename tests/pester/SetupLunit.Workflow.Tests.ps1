#requires -Version 7.0
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

Describe 'SetupLunit.Workflow' {
    $meta = @{
        requirement = 'REQ-039'
        Owner       = 'DevTools'
        Evidence    = 'tests/pester/SetupLunit.Workflow.Tests.ps1'
    }

    It 'defines setup-lunit action with required inputs [REQ-039]' -Tag 'REQ-039' {
        $repoRoot = (Resolve-Path (Join-Path $PSScriptRoot '..' '..')).Path
        $actionPath = Join-Path $repoRoot 'setup-lunit/action.yml'
        
        $actionPath | Should -Exist
        
        $action = Get-Content -Raw $actionPath | ConvertFrom-Yaml
        $action.name | Should -Be 'Setup LUnit CLI'
        $action.inputs.lv_version | Should -Not -BeNullOrEmpty
        $action.inputs.lv_version.required | Should -Be $true
        $action.inputs.lv_bitness | Should -Not -BeNullOrEmpty
        $action.inputs.lv_bitness.required | Should -Be $true
        $action.inputs.vipm_installer_url | Should -Not -BeNullOrEmpty
        $action.inputs.vipm_installer_url.default | Should -Match 'vipm'
    }

    It 'validates adapter function accepts LVVersion and LVBitness parameters [REQ-039]' -Tag 'REQ-039' {
        Import-Module (Join-Path $PSScriptRoot '../../actions/OpenSourceActions.psm1') -Force
        
        $cmd = Get-Command Invoke-SetupLunit -ErrorAction Stop
        $cmd | Should -Not -BeNullOrEmpty
        
        $params = $cmd.Parameters.Keys
        $params | Should -Contain 'LVVersion'
        $params | Should -Contain 'LVBitness'
        $params | Should -Contain 'VipmInstallerUrl'
        $params | Should -Contain 'DryRun'
    }

    It 'validates LVVersion parameter is mandatory [REQ-039]' -Tag 'REQ-039' {
        Import-Module (Join-Path $PSScriptRoot '../../actions/OpenSourceActions.psm1') -Force
        
        $cmd = Get-Command Invoke-SetupLunit -ErrorAction Stop
        $versionParam = $cmd.Parameters['LVVersion']
        
        $versionParam.Attributes | Where-Object { $_ -is [Parameter] } | 
            Select-Object -First 1 | ForEach-Object { $_.Mandatory } | Should -Be $true
    }

    It 'validates LVBitness parameter accepts only 32 or 64 [REQ-039]' -Tag 'REQ-039' {
        Import-Module (Join-Path $PSScriptRoot '../../actions/OpenSourceActions.psm1') -Force
        
        $cmd = Get-Command Invoke-SetupLunit -ErrorAction Stop
        $bitnessParam = $cmd.Parameters['LVBitness']
        
        $validateSet = $bitnessParam.Attributes | Where-Object { $_.TypeId.Name -eq 'ValidateSetAttribute' }
        $validateSet | Should -Not -BeNullOrEmpty
        $validateSet.ValidValues | Should -Contain '32'
        $validateSet.ValidValues | Should -Contain '64'
    }
}