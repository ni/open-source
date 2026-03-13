#requires -Version 7.0
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

Describe 'SetupLunit.Workflow' {
    $meta = @{
        requirement = 'REQ-038'
        Owner       = 'DevTools'
        Evidence    = 'tests/pester/SetupLunit.Workflow.Tests.ps1'
    }

    It 'defines setup-lunit action with required inputs [REQ-038]' -Tag 'REQ-038' {
        $repoRoot = (Resolve-Path (Join-Path $PSScriptRoot '..' '..')).Path
        $actionPath = Join-Path $repoRoot 'setup-lunit/action.yml'
        
        $actionPath | Should -Exist
        
        # Use regex-based validation instead of YAML parsing
        $yamlContent = Get-Content -Raw $actionPath
        
        $yamlContent | Should -Match 'name:.*Setup LUnit'
        $yamlContent | Should -Match 'lv_version:'
        $yamlContent | Should -Match 'required:.*true'
        $yamlContent | Should -Match 'lv_bitness:'
        $yamlContent | Should -Match 'vipm_installer_url:'
        $yamlContent | Should -Match 'default:.*vipm'
    }

    It 'validates adapter function accepts LVVersion and LVBitness parameters [REQ-038]' -Tag 'REQ-038' {
        Import-Module (Join-Path $PSScriptRoot '../../actions/OpenSourceActions.psm1') -Force
        
        $cmd = Get-Command Invoke-SetupLunit -ErrorAction Stop
        $cmd | Should -Not -BeNullOrEmpty
        
        $params = $cmd.Parameters.Keys
        $params | Should -Contain 'LVVersion'
        $params | Should -Contain 'LVBitness'
        $params | Should -Contain 'VipmInstallerUrl'
        $params | Should -Contain 'DryRun'
    }

    It 'validates LVVersion parameter is mandatory [REQ-038]' -Tag 'REQ-038' {
        Import-Module (Join-Path $PSScriptRoot '../../actions/OpenSourceActions.psm1') -Force
        
        $cmd = Get-Command Invoke-SetupLunit -ErrorAction Stop
        $versionParam = $cmd.Parameters['LVVersion']
        
        $versionParam.Attributes.Where({$_.TypeId.Name -eq 'ParameterAttribute'}).Mandatory | Should -Contain $true
    }

    It 'validates LVBitness parameter accepts only 32 or 64 [REQ-038]' -Tag 'REQ-038' {
        Import-Module (Join-Path $PSScriptRoot '../../actions/OpenSourceActions.psm1') -Force
        
        $cmd = Get-Command Invoke-SetupLunit -ErrorAction Stop
        $bitnessParam = $cmd.Parameters['LVBitness']
        
        $validateSet = $bitnessParam.Attributes | Where-Object { $_.TypeId.Name -eq 'ValidateSetAttribute' }
        $validateSet | Should -Not -BeNullOrEmpty
        $validateSet.ValidValues | Should -Contain '32'
        $validateSet.ValidValues | Should -Contain '64'
    }
}