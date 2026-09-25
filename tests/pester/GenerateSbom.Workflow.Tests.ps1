#requires -Version 7.0
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

Describe 'GenerateSbom.Workflow' {
    $meta = @{
        requirement = 'REQ-042'
        Owner       = 'NI'
        Evidence    = 'tests/pester/GenerateSbom.Workflow.Tests.ps1'
    }

    BeforeAll {
        $repoRoot = (Resolve-Path (Join-Path $PSScriptRoot '..' '..')).Path
        Import-Module (Join-Path $repoRoot 'actions' 'OpenSourceActions.psd1') -Force
    }

    It 'validates action.yml exists [REQ-042]' -Tag 'REQ-042' {
        $actionPath = Join-Path $repoRoot 'generate-sbom' 'action.yml'
        Test-Path $actionPath | Should -Be $true
    }

    It 'validates implementation script exists [REQ-042]' -Tag 'REQ-042' {
        $scriptPath = Join-Path $repoRoot 'scripts' 'generate-sbom' 'GenerateSbom.ps1'
        Test-Path $scriptPath | Should -Be $true
    }

    It 'executes dry-run with only required parameters [REQ-042]' -Tag 'REQ-042' {
        $result = & "$repoRoot/actions/Invoke-OSAction.ps1" `
            -ActionName 'generate-sbom' `
            -ArgsJson '{"LvprojPath":"test.lvproj","LabVIEWVersion":"2026","LabVIEWBitness":"64","BuildSpecName":"TestBuild","ProductName":"Test Product","ProductVersion":"1.0.0","OutputPath":"sbom.json"}' `
            -DryRun

        $LASTEXITCODE | Should -Be 0
    }

    It 'executes dry-run with all parameters [REQ-042]' -Tag 'REQ-042' {
        $result = & "$repoRoot/actions/Invoke-OSAction.ps1" `
            -ActionName 'generate-sbom' `
            -ArgsJson '{"LvprojPath":"test.lvproj","LabVIEWVersion":"2026","LabVIEWBitness":"64","BuildSpecName":"TestBuild","TargetName":"My Computer","ProductName":"Test Product","ProductVersion":"1.0.0","OutputPath":"sbom.json","Format":"cyclonedx","SchemaVersion":"1.5"}' `
            -DryRun

        $LASTEXITCODE | Should -Be 0
    }
}
