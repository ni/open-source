#requires -Version 7.0
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

Describe 'UpdateVipmDependencies.Workflow' {
    $meta = @{
        requirement = 'REQ-044'
        Owner       = 'NI'
        Evidence    = 'tests/pester/UpdateVipmDependencies.Workflow.Tests.ps1'
    }

    BeforeAll {
        $repoRoot = (Resolve-Path (Join-Path $PSScriptRoot '..' '..')).Path
        Import-Module (Join-Path $repoRoot 'actions' 'OpenSourceActions.psd1') -Force
    }

    It 'validates action.yml exists [REQ-044]' -Tag 'REQ-044' {
        $actionPath = Join-Path $repoRoot 'update-vipm-dependencies' 'action.yml'
        Test-Path $actionPath | Should -Be $true
    }

    It 'validates implementation script exists [REQ-044]' -Tag 'REQ-044' {
        $scriptPath = Join-Path $repoRoot 'scripts' 'update-vipm-dependencies' 'UpdateVipmDependencies.ps1'
        Test-Path $scriptPath | Should -Be $true
    }

    It 'executes dry-run with only required parameters [REQ-044]' -Tag 'REQ-044' {
        $result = & "$repoRoot/actions/Invoke-OSAction.ps1" `
            -ActionName 'update-vipm-dependencies' `
            -ArgsJson '{"WorkingDirectory":"scripts/apply-vipc","LabVIEWVersion":"2021","LabVIEWBitness":"64"}' `
            -DryRun

        $LASTEXITCODE | Should -Be 0
    }
}
