#requires -Version 7.0
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

Describe 'RefreshVipmLock.Workflow' {
    $meta = @{
        requirement = 'REQ-043'
        Owner       = 'NI'
        Evidence    = 'tests/pester/RefreshVipmLock.Workflow.Tests.ps1'
    }

    BeforeAll {
        $repoRoot = (Resolve-Path (Join-Path $PSScriptRoot '..' '..')).Path
        Import-Module (Join-Path $repoRoot 'actions' 'OpenSourceActions.psd1') -Force
    }

    It 'validates action.yml exists [REQ-043]' -Tag 'REQ-043' {
        $actionPath = Join-Path $repoRoot 'refresh-vipm-lock' 'action.yml'
        Test-Path $actionPath | Should -Be $true
    }

    It 'validates implementation script exists [REQ-043]' -Tag 'REQ-043' {
        $scriptPath = Join-Path $repoRoot 'scripts' 'refresh-vipm-lock' 'RefreshVipmLock.ps1'
        Test-Path $scriptPath | Should -Be $true
    }

    It 'executes dry-run with only required parameters [REQ-043]' -Tag 'REQ-043' {
        $result = & "$repoRoot/actions/Invoke-OSAction.ps1" `
            -ActionName 'refresh-vipm-lock' `
            -ArgsJson '{"WorkingDirectory":"scripts/apply-vipc","LabVIEWVersion":"2021","LabVIEWBitness":"64"}' `
            -DryRun

        $LASTEXITCODE | Should -Be 0
    }

    It 'executes dry-run in check-only mode [REQ-043]' -Tag 'REQ-043' {
        $result = & "$repoRoot/actions/Invoke-OSAction.ps1" `
            -ActionName 'refresh-vipm-lock' `
            -ArgsJson '{"WorkingDirectory":"scripts/apply-vipc","LabVIEWVersion":"2021","LabVIEWBitness":"64","CheckOnly":true}' `
            -DryRun

        $LASTEXITCODE | Should -Be 0
    }
}
