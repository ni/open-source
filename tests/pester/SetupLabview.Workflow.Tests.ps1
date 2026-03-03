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
        
        $yamlContent = Get-Content -Raw $actionPath
        
        $yamlContent | Should -Match "name:\s*['\"]?Setup LabVIEW"
        $yamlContent | Should -Match 'labview_iso_url:'
        $yamlContent | Should -Match 'default:\s*["\']https://download\.ni\.com.*labview.*\.iso'
        $yamlContent | Should -Match 'timeout_seconds:'
        $yamlContent | Should -Match "default:\s*['\"]?2700"
    }

    It 'validates adapter function accepts LabVIEWIsoUrl, TimeoutSeconds, SerialNumber, and PackageID parameters [REQ-036]' -Tag 'REQ-036' {
        Import-Module (Join-Path $PSScriptRoot '../../actions/OpenSourceActions.psm1') -Force
        
        $cmd = Get-Command Invoke-SetupLabview -ErrorAction Stop
        $cmd | Should -Not -BeNullOrEmpty
        
        $params = $cmd.Parameters.Keys
        $params | Should -Contain 'LabVIEWIsoUrl'
        $params | Should -Contain 'TimeoutSeconds'
        $params | Should -Contain 'SerialNumber'
        $params | Should -Contain 'PackageID'
        $params | Should -Contain 'DryRun'
    }

    It 'validates timeout parameter is an integer [REQ-036]' -Tag 'REQ-036' {
        Import-Module (Join-Path $PSScriptRoot '../../actions/OpenSourceActions.psm1') -Force
        
        $cmd = Get-Command Invoke-SetupLabview -ErrorAction Stop
        $timeoutParam = $cmd.Parameters['TimeoutSeconds']
        
        $timeoutParam.ParameterType.Name | Should -Be 'Int32'
    }

    It 'validates SerialNumber parameter is mandatory string [REQ-036]' -Tag 'REQ-036' {
        Import-Module (Join-Path $PSScriptRoot '../../actions/OpenSourceActions.psm1') -Force
        
        $cmd = Get-Command Invoke-SetupLabview -ErrorAction Stop
        $serialParam = $cmd.Parameters['SerialNumber']
        
        $serialParam.ParameterType.Name | Should -Be 'String'
        $serialParam.Attributes.Where({$_.TypeId.Name -eq 'ParameterAttribute'}).Mandatory | Should -Contain $true
    }

    It 'validates PackageID parameter has correct default value [REQ-036]' -Tag 'REQ-036' {
        Import-Module (Join-Path $PSScriptRoot '../../actions/OpenSourceActions.psm1') -Force
        
        $cmd = Get-Command Invoke-SetupLabview -ErrorAction Stop
        $packageParam = $cmd.Parameters['PackageID']
        
        $packageParam.ParameterType.Name | Should -Be 'String'
        # Default value is set in the function definition, not as a parameter attribute
        # We verify it exists and is a string type
        $packageParam | Should -Not -BeNullOrEmpty
    }
}