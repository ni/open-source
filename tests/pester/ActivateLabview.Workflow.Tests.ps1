#requires -Version 7.0
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

Describe 'ActivateLabview.Workflow' {
    $meta = @{
        requirement = 'REQ-037'
        Owner       = 'DevTools'
        Evidence    = 'tests/pester/ActivateLabview.Workflow.Tests.ps1'
    }

    It 'defines activate-labview action with required inputs [REQ-037]' -Tag 'REQ-037' {
        $repoRoot = (Resolve-Path (Join-Path $PSScriptRoot '..' '..')).Path
        $actionPath = Join-Path $repoRoot 'activate-labview/action.yml'
        
        $actionPath | Should -Exist
        
        $action = Get-Content -Raw $actionPath | ConvertFrom-Yaml
        $action.name | Should -Be 'Activate LabVIEW'
        $action.inputs.serial_number | Should -Not -BeNullOrEmpty
        $action.inputs.serial_number.required | Should -Be $true
        $action.inputs.package_id | Should -Not -BeNullOrEmpty
        $action.inputs.package_id.default | Should -Match 'LabVIEW_COM_PKG'
    }

    It 'validates adapter function accepts SerialNumber and PackageID parameters [REQ-037]' -Tag 'REQ-037' {
        Import-Module (Join-Path $PSScriptRoot '../../actions/OpenSourceActions.psm1') -Force
        
        $cmd = Get-Command Invoke-ActivateLabview -ErrorAction Stop
        $cmd | Should -Not -BeNullOrEmpty
        
        $params = $cmd.Parameters.Keys
        $params | Should -Contain 'SerialNumber'
        $params | Should -Contain 'PackageID'
        $params | Should -Contain 'DryRun'
    }

    It 'validates SerialNumber parameter is mandatory [REQ-037]' -Tag 'REQ-037' {
        Import-Module (Join-Path $PSScriptRoot '../../actions/OpenSourceActions.psm1') -Force
        
        $cmd = Get-Command Invoke-ActivateLabview -ErrorAction Stop
        $serialParam = $cmd.Parameters['SerialNumber']
        
        $serialParam.Attributes | Where-Object { $_ -is [Parameter] } | 
            Select-Object -First 1 | ForEach-Object { $_.Mandatory } | Should -Be $true
    }
}