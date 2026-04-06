#requires -Version 7.0
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

Describe 'BuildSpecDockerLinux.Workflow' {
    $meta = @{
        requirement = 'REQ-039'
        Owner       = 'NI'
        Evidence    = 'tests/pester/BuildSpecDockerLinux.Workflow.Tests.ps1'
    }

    BeforeAll {
        $repoRoot = (Resolve-Path (Join-Path $PSScriptRoot '..' '..')).Path
        Import-Module (Join-Path $repoRoot 'actions' 'OpenSourceActions.psd1') -Force
    }

    It 'validates action.yml exists [REQ-039]' -Tag 'REQ-039' {
        $actionPath = Join-Path $repoRoot 'build-spec-docker-linux' 'action.yml'
        Test-Path $actionPath | Should -Be $true
    }

    It 'validates implementation script exists [REQ-039]' -Tag 'REQ-039' {
        $scriptPath = Join-Path $repoRoot 'scripts' 'build-spec-docker-linux' 'BuildSpecDockerLinux.ps1'
        Test-Path $scriptPath | Should -Be $true
    }

    It 'validates inner build script exists [REQ-039]' -Tag 'REQ-039' {
        $scriptPath = Join-Path $repoRoot 'scripts' 'build-spec-docker-linux' 'build-spec.sh'
        Test-Path $scriptPath | Should -Be $true
    }

    It 'executes dry-run with only required parameters [REQ-039]' -Tag 'REQ-039' {
        $result = & "$repoRoot/actions/Invoke-OSAction.ps1" `
            -ActionName 'build-spec-docker-linux' `
            -ArgsJson '{"MinimumSupportedLVVersion":"2026","SupportedBitness":"64","ProjectPath":"test.lvproj"}' `
            -DryRun
        
        $LASTEXITCODE | Should -Be 0
    }

    It 'executes dry-run with all parameters [REQ-039]' -Tag 'REQ-039' {
        $result = & "$repoRoot/actions/Invoke-OSAction.ps1" `
            -ActionName 'build-spec-docker-linux' `
            -ArgsJson '{"MinimumSupportedLVVersion":"2026","SupportedBitness":"64","ProjectPath":"test.lvproj","TargetName":"My Computer","BuildSpecName":"TestBuild","Major":1,"Minor":0,"Patch":0,"Build":0,"Commit":"abc1234","DockerImage":"nationalinstruments/labview","ImageTag":"2026q1-linux"}' `
            -DryRun
        
        $LASTEXITCODE | Should -Be 0
    }
}