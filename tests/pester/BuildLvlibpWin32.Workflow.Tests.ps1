#requires -Version 7.0
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

Describe 'BuildLvlibpWin32.Workflow' {
    $meta = @{
        requirement = 'REQ-041'
        Owner       = 'NI'
        Evidence    = 'tests/pester/BuildLvlibpWin32.Workflow.Tests.ps1'
    }

    BeforeAll {
        $repoRoot = (Resolve-Path (Join-Path $PSScriptRoot '..' '..')).Path
        Import-Module (Join-Path $repoRoot 'actions' 'OpenSourceActions.psd1') -Force
    }

    It 'validates build-lvlibp-win32 action exists in dispatcher registry [REQ-041]' -Tag 'REQ-041' {
        $dispatchersPath = Join-Path $repoRoot 'dispatchers.json'
        $dispatchers = Get-Content $dispatchersPath | ConvertFrom-Json
        
        $dispatchers.'build-lvlibp-win32' | Should -Not -BeNullOrEmpty
        $dispatchers.'build-lvlibp-win32'.function | Should -Be 'Invoke-BuildLvlibpWin32'
    }

    It 'validates required parameters are defined [REQ-041]' -Tag 'REQ-041' {
        $dispatchersPath = Join-Path $repoRoot 'dispatchers.json'
        $dispatchers = Get-Content $dispatchersPath | ConvertFrom-Json
        
        $action = $dispatchers.'build-lvlibp-win32'
        $action.parameters.MinimumSupportedLVVersion | Should -Not -BeNullOrEmpty
        $action.parameters.SupportedBitness | Should -Not -BeNullOrEmpty
        $action.parameters.ProjectPath | Should -Not -BeNullOrEmpty
        $action.parameters.TargetName | Should -Not -BeNullOrEmpty
        $action.parameters.Major | Should -Not -BeNullOrEmpty
        $action.parameters.Minor | Should -Not -BeNullOrEmpty
        $action.parameters.Patch | Should -Not -BeNullOrEmpty
        $action.parameters.Build | Should -Not -BeNullOrEmpty
        $action.parameters.Commit | Should -Not -BeNullOrEmpty
    }

    It 'executes dry-run without errors [REQ-041]' -Tag 'REQ-041' {
        $result = & "$repoRoot/actions/Invoke-OSAction.ps1" `
            -ActionName 'build-lvlibp-win32' `
            -ArgsJson '{"MinimumSupportedLVVersion":"2026","SupportedBitness":"64","ProjectPath":"test.lvproj","TargetName":"My Computer","BuildSpecName":"TestBuild","Major":1,"Minor":0,"Patch":0,"Build":0,"Commit":"abc1234"}' `
            -DryRun
        
        $LASTEXITCODE | Should -Be 0
    }

    It 'validates module function throws when required parameters are missing [REQ-041]' -Tag 'REQ-041' {
        { Invoke-BuildLvlibpWin32 } | Should -Throw
    }

    It 'accepts optional parameters in dry-run [REQ-041]' -Tag 'REQ-041' {
        $result = & "$repoRoot/actions/Invoke-OSAction.ps1" `
            -ActionName 'build-lvlibp-win32' `
            -ArgsJson '{"MinimumSupportedLVVersion":"2021","SupportedBitness":"32","ProjectPath":"test.lvproj","TargetName":"My Computer","BuildSpecName":"","Major":2,"Minor":1,"Patch":0,"Build":5,"Commit":"def5678"}' `
            -DryRun
        
        $LASTEXITCODE | Should -Be 0
    }

    It 'validates action.yml exists [REQ-041]' -Tag 'REQ-041' {
        $actionPath = Join-Path $repoRoot 'build-lvlibp-win32' 'action.yml'
        Test-Path $actionPath | Should -Be $true
    }

    It 'validates implementation script exists [REQ-041]' -Tag 'REQ-041' {
        $scriptPath = Join-Path $repoRoot 'scripts' 'build-lvlibp-win32' 'BuildLvlibpWin32.ps1'
        Test-Path $scriptPath | Should -Be $true
    }
}