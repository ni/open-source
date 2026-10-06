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

    It 'declares a required caller-supplied base branch in workflow configuration [REQ-044]' -Tag 'REQ-044' {
        $workflow = Get-Content (Join-Path $repoRoot '.github' 'workflows' 'reusable-vipm-update.yml') -Raw
        $target = '${{ inputs.base_branch || github.event.repository.default_branch }}'
        $workflow | Should -Match ([regex]::Escape("ref: $target"))
        $workflow | Should -Match ([regex]::Escape("base: $target"))
        $workflow | Should -Match 'base_branch:\s+description: [^\r\n]+\s+required: true'
    }

    It 'declares opt-in auto-merge only for newly created PRs in workflow configuration [REQ-044]' -Tag 'REQ-044' {
        $workflow = Get-Content (Join-Path $repoRoot '.github' 'workflows' 'reusable-vipm-update.yml') -Raw
        $workflow | Should -Match 'auto_merge:\s+description: [^\r\n]+\s+required: false\s+type: boolean\s+default: false'
        $workflow | Should -Match ([regex]::Escape('if: ${{ inputs.auto_merge && steps.dependency-pr.outputs.pull-request-number && steps.dependency-pr.outputs.pull-request-operation == ''created'' }}'))
        $workflow | Should -Match ([regex]::Escape('GH_TOKEN: ${{ github.token }}'))
        $workflow | Should -Match ([regex]::Escape('gh pr merge "$PR_URL" --auto --squash'))
        $workflow | Should -Match ([regex]::Escape('--match-head-commit "$PR_HEAD_SHA"'))
        $workflow | Should -Not -Match '--admin'
    }

    It 'declares custom dependency-only paths and a fixed commit subject in workflow configuration [REQ-044]' -Tag 'REQ-044' {
        $workflow = Get-Content (Join-Path $repoRoot '.github' 'workflows' 'reusable-vipm-update.yml') -Raw
        $workflow | Should -Match ([regex]::Escape("COMMIT_MESSAGE: 'chore(deps): update VIPM package dependencies'"))
        $workflow | Should -Match ([regex]::Escape('commit-message: ${{ env.COMMIT_MESSAGE }}'))
        $workflow | Should -Match ([regex]::Escape('token: ${{ github.token }}'))
        $workflow | Should -Not -Match '(?m)^\s+(commit_message|PR_TOKEN):'
        $workflow | Should -Match ([regex]::Escape('--subject "$COMMIT_MESSAGE"'))
        $workflow | Should -Match ([regex]::Escape('manifest_filename: ${{ inputs.manifest_filename }}'))
        $workflow | Should -Match ([regex]::Escape('lock_filename: ${{ inputs.lock_filename }}'))
        $workflow | Should -Match ([regex]::Escape('include_dev_dependencies: ${{ inputs.include_dev_dependencies }}'))
        $paths = 'add-paths:\s+\|\s+' + [regex]::Escape('${{ inputs.working_directory }}/${{ inputs.manifest_filename }}') + '\s+' + [regex]::Escape('${{ inputs.working_directory }}/${{ inputs.lock_filename }}') + '\s+labels:'
        $workflow | Should -Match $paths
    }

    It 'executes dry-run with only required parameters [REQ-044]' -Tag 'REQ-044' {
        $result = & "$repoRoot/actions/Invoke-OSAction.ps1" `
            -ActionName 'update-vipm-dependencies' `
            -ArgsJson '{"WorkingDirectory":"scripts/apply-vipc","LabVIEWVersion":"2021","LabVIEWBitness":"64"}' `
            -DryRun

        $LASTEXITCODE | Should -Be 0
    }
}
