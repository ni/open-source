#requires -Version 7.0
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

Describe 'BuildSpecDockerWindows.Workflow' {
    $meta = @{
        requirement = 'REQ-040'
        Owner       = 'NI'
        Evidence    = 'tests/pester/BuildSpecDockerWindows.Workflow.Tests.ps1'
    }

    BeforeAll {
        $repoRoot = (Resolve-Path (Join-Path $PSScriptRoot '..' '..')).Path
        Import-Module (Join-Path $repoRoot 'actions' 'OpenSourceActions.psd1') -Force
    }

    It 'validates action.yml exists [REQ-040]' -Tag 'REQ-040' {
        $actionPath = Join-Path $repoRoot 'build-spec-docker-windows' 'action.yml'
        Test-Path $actionPath | Should -Be $true
    }

    It 'validates implementation script exists [REQ-040]' -Tag 'REQ-040' {
        $scriptPath = Join-Path $repoRoot 'scripts' 'build-spec-docker-windows' 'BuildSpecDockerWindows.ps1'
        Test-Path $scriptPath | Should -Be $true
    }

    It 'validates inner build script exists [REQ-040]' -Tag 'REQ-040' {
        $scriptPath = Join-Path $repoRoot 'scripts' 'build-spec-docker-windows' 'build-spec.ps1'
        Test-Path $scriptPath | Should -Be $true
    }

    It 'executes dry-run with only required parameters [REQ-040]' -Tag 'REQ-040' {
        $result = & "$repoRoot/actions/Invoke-OSAction.ps1" `
            -ActionName 'build-spec-docker-windows' `
            -ArgsJson '{"MinimumSupportedLVVersion":"2026","SupportedBitness":"64","ProjectPath":"test.lvproj"}' `
            -DryRun
        
        $LASTEXITCODE | Should -Be 0
    }

    It 'executes dry-run with all parameters [REQ-040]' -Tag 'REQ-040' {
        $result = & "$repoRoot/actions/Invoke-OSAction.ps1" `
            -ActionName 'build-spec-docker-windows' `
            -ArgsJson '{"MinimumSupportedLVVersion":"2026","SupportedBitness":"64","ProjectPath":"test.lvproj","TargetName":"My Computer","BuildSpecName":"TestBuild","Major":1,"Minor":0,"Patch":0,"Build":0,"Commit":"abc1234","DockerImage":"nationalinstruments/labview","ImageTag":"2026q1-windows"}' `
            -DryRun
        
        $LASTEXITCODE | Should -Be 0
    }
}