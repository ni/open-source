#requires -Version 7.0
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

Describe 'BuildLvlibpDocker.Workflow' {
    $meta = @{
        requirement = 'REQ-039'
        Owner       = 'NI'
        Evidence    = 'tests/pester/BuildLvlibpDocker.Workflow.Tests.ps1'
    }

    BeforeAll {
        $repoRoot = (Resolve-Path (Join-Path $PSScriptRoot '..' '..')).Path
        Import-Module (Join-Path $repoRoot 'actions' 'OpenSourceActions.psd1') -Force
    }

    It 'validates build-lvlibp-docker action exists in dispatcher registry [REQ-039]' -Tag 'REQ-039' {
        $dispatchersPath = Join-Path $repoRoot 'dispatchers.json'
        $dispatchers = Get-Content $dispatchersPath | ConvertFrom-Json
        
        $dispatchers.'build-lvlibp-docker' | Should -Not -BeNullOrEmpty
        $dispatchers.'build-lvlibp-docker'.function | Should -Be 'Invoke-BuildLvlibpDocker'
    }

    It 'validates required parameters are defined [REQ-039]' -Tag 'REQ-039' {
        $dispatchersPath = Join-Path $repoRoot 'dispatchers.json'
        $dispatchers = Get-Content $dispatchersPath | ConvertFrom-Json
        
        $action = $dispatchers.'build-lvlibp-docker'
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

    It 'executes dry-run without errors [REQ-039]' -Tag 'REQ-039' {
        $result = & "$repoRoot/actions/Invoke-OSAction.ps1" `
            -ActionName 'build-lvlibp-docker' `
            -ArgsJson '{"MinimumSupportedLVVersion":"2026","SupportedBitness":"64","ProjectPath":"test.lvproj","TargetName":"My Computer","BuildSpecName":"TestBuild","Major":1,"Minor":0,"Patch":0,"Build":0,"Commit":"abc1234"}' `
            -DryRun
        
        $LASTEXITCODE | Should -Be 0
    }

    It 'validates module function throws when required parameters are missing [REQ-039]' -Tag 'REQ-039' {
        { Invoke-BuildLvlibpDocker } | Should -Throw
    }

    It 'accepts optional parameters in dry-run [REQ-039]' -Tag 'REQ-039' {
        $result = & "$repoRoot/actions/Invoke-OSAction.ps1" `
            -ActionName 'build-lvlibp-docker' `
            -ArgsJson '{"MinimumSupportedLVVersion":"2021","SupportedBitness":"32","ProjectPath":"test.lvproj","TargetName":"My Computer","BuildSpecName":"","Major":2,"Minor":1,"Patch":0,"Build":5,"Commit":"def5678","DockerImage":"custom/labview","ImageTag":"custom-tag"}' `
            -DryRun
        
        $LASTEXITCODE | Should -Be 0
    }

    It 'validates action.yml exists [REQ-039]' -Tag 'REQ-039' {
        $actionPath = Join-Path $repoRoot 'build-lvlibp-docker' 'action.yml'
        Test-Path $actionPath | Should -Be $true
    }

    It 'validates implementation script exists [REQ-039]' -Tag 'REQ-039' {
        $scriptPath = Join-Path $repoRoot 'scripts' 'build-lvlibp-docker' 'BuildLvlibpDocker.ps1'
        Test-Path $scriptPath | Should -Be $true
    }
}