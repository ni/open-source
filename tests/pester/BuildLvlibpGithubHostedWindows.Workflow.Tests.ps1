#requires -Version 7.0
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

Describe 'BuildLvlibpGithubHostedWindows.Workflow' {
    $meta = @{
        requirement = 'REQ-041'
        Owner       = 'NI'
        Evidence    = 'tests/pester/BuildLvlibpGithubHostedWindows.Workflow.Tests.ps1'
    }

    BeforeAll {
        $repoRoot = (Resolve-Path (Join-Path $PSScriptRoot '..' '..')).Path
        Import-Module (Join-Path $repoRoot 'actions' 'OpenSourceActions.psd1') -Force
    }

    It 'validates action.yml exists [REQ-041]' -Tag 'REQ-041' {
        $actionPath = Join-Path $repoRoot 'build-lvlibp-github-hosted-windows' 'action.yml'
        Test-Path $actionPath | Should -Be $true
    }

    It 'validates implementation script exists [REQ-041]' -Tag 'REQ-041' {
        $scriptPath = Join-Path $repoRoot 'scripts' 'build-lvlibp-github-hosted-windows' 'BuildLvlibpGithubHostedWindows.ps1'
        Test-Path $scriptPath | Should -Be $true
    }

    It 'executes dry-run with only required parameters [REQ-041]' -Tag 'REQ-041' {
        $result = & "$repoRoot/actions/Invoke-OSAction.ps1" `
            -ActionName 'build-lvlibp-github-hosted-windows' `
            -ArgsJson '{"MinimumSupportedLVVersion":"2026","SupportedBitness":"64","ProjectPath":"test.lvproj"}' `
            -DryRun
        
        $LASTEXITCODE | Should -Be 0
    }

    It 'executes dry-run with all parameters [REQ-041]' -Tag 'REQ-041' {
        $result = & "$repoRoot/actions/Invoke-OSAction.ps1" `
            -ActionName 'build-lvlibp-github-hosted-windows' `
            -ArgsJson '{"MinimumSupportedLVVersion":"2026","SupportedBitness":"64","ProjectPath":"test.lvproj","TargetName":"My Computer","BuildSpecName":"TestBuild","Major":1,"Minor":0,"Patch":0,"Build":0,"Commit":"abc1234"}' `
            -DryRun
        
        $LASTEXITCODE | Should -Be 0
    }
}