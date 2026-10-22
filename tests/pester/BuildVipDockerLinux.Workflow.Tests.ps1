#requires -Version 7.0
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

Describe 'BuildVipDockerLinux.Workflow' {
    $meta = @{
        requirement = 'REQ-045'
        Owner       = 'NI'
        Evidence    = 'tests/pester/BuildVipDockerLinux.Workflow.Tests.ps1'
    }

    BeforeAll {
        $repoRoot = (Resolve-Path (Join-Path $PSScriptRoot '..' '..')).Path
        Import-Module (Join-Path $repoRoot 'actions' 'OpenSourceActions.psd1') -Force
    }

    It 'validates action.yml exists [REQ-045]' -Tag 'REQ-045' {
        Test-Path (Join-Path $repoRoot 'build-vip-docker-linux' 'action.yml') | Should -Be $true
    }

    It 'validates implementation and container scripts exist [REQ-045]' -Tag 'REQ-045' {
        Test-Path (Join-Path $repoRoot 'scripts' 'build-vip-docker-linux' 'BuildVipDockerLinux.ps1') | Should -Be $true
        Test-Path (Join-Path $repoRoot 'scripts' 'build-vip-docker-linux' 'build-vip.sh') | Should -Be $true
    }

    It 'runs the pre-build hook after VIPM activation and before vipm build [REQ-045]' -Tag 'REQ-045' {
        $helper = Get-Content (Join-Path $repoRoot 'scripts' 'build-vip-docker-linux' 'build-vip.sh') -Raw
        $activate = $helper.IndexOf('vipm activate')
        $hook = $helper.IndexOf('run_logged pre-build')
        $build = $helper.IndexOf('run_logged vipm-build')
        $activate | Should -BeGreaterThan -1
        $hook | Should -BeGreaterThan $activate
        $build | Should -BeGreaterThan $hook
    }

    It 'reusable workflow calls the composite action on ubuntu-latest [REQ-045]' -Tag 'REQ-045' {
        $workflow = Get-Content (Join-Path $repoRoot '.github' 'workflows' 'reusable-build-vip-linux.yml') -Raw
        $workflow | Should -Match 'runs-on:\s*ubuntu-latest'
        $workflow | Should -Match 'uses:\s*ni/open-source/build-vip-docker-linux@'
        $workflow | Should -Match 'actions/upload-artifact@'
    }

    It 'executes dry-run with only required parameters [REQ-045]' -Tag 'REQ-045' {
        & "$repoRoot/actions/Invoke-OSAction.ps1" `
            -ActionName 'build-vip-docker-linux' `
            -ArgsJson '{"VipbPath":"Tooling/deployment/Test.vipb"}' `
            -DryRun

        $LASTEXITCODE | Should -Be 0
    }

    It 'executes dry-run with all parameters [REQ-045]' -Tag 'REQ-045' {
        & "$repoRoot/actions/Invoke-OSAction.ps1" `
            -ActionName 'build-vip-docker-linux' `
            -ArgsJson '{"VipbPath":"Tooling/deployment/Test.vipb","LabVIEWVersion":"2026q1","DockerImage":"nationalinstruments/labview","ImageTag":"2026q1-linux","SourceDirectory":"/tmp/source","OutputDirectory":"builds/vip","LogDirectory":"builds/logs/vip-linux","PreBuildScript":"pipeline/prepare.sh","VipmSerialNumber":"x","VipmFullName":"x","VipmEmail":"x","VipmDebSha256":"","BuildTimeoutSeconds":"900","DesktopLivelinessTimeout":"300"}' `
            -DryRun

        $LASTEXITCODE | Should -Be 0
    }
}
