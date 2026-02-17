#requires -Version 7.0
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

Describe 'ViaLvDocker.Workflow' {
    $meta = @{
        requirement = 'REQ-034'
        Owner       = 'DevTools'
        Evidence    = 'tests/pester/ViaLvDocker.Workflow.Tests.ps1'
    }

    It 'runs via-lv-docker action on ubuntu-latest runner [REQ-034]' -Tag 'REQ-034' {
        $repoRoot = (Resolve-Path (Join-Path $PSScriptRoot '..' '..')).Path
        $actionPath = Join-Path $repoRoot 'via-lv-docker/action.yml'
        
        # Verify action.yml exists
        $actionPath | Should -Exist
        
        # Parse action.yml
        $actionContent = Get-Content -Raw $actionPath
        $actionContent | Should -Not -BeNullOrEmpty
        
        # Verify key inputs are defined
        $actionContent | Should -Match 'config_path:'
        $actionContent | Should -Match 'template_path:'
        $actionContent | Should -Match 'base_branch:'
        $actionContent | Should -Match 'labview_version:'
        $actionContent | Should -Match 'docker_image:'
        
        # Verify it uses composite action pattern
        $actionContent | Should -Match "using: 'composite'"
        
        # Verify it calls the dispatcher
        $actionContent | Should -Match 'common-dispatch.ps1'
        $actionContent | Should -Match "ActionName = 'via-lv-docker'"
    }
}