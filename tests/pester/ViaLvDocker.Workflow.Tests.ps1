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

    Context 'dependency_file' {
        BeforeAll {
            $repoRoot = (Resolve-Path (Join-Path $PSScriptRoot '..' '..')).Path
            $runVia = Join-Path $repoRoot 'scripts' 'via-lv-docker' 'RunViaLvDocker.ps1'
            $runViaSh = Join-Path $repoRoot 'scripts' 'via-lv-docker' 'run-via.sh'

            function global:docker {
                $global:ViaDockerCalls.Add(@($args))
                $global:LASTEXITCODE = 0
                if ($args[0] -eq 'image') { return $global:ViaImageOs }
                if ($args[0] -eq 'run') {
                    $global:LASTEXITCODE = $global:ViaRunExitCode
                    if ($global:ViaInstallSucceeds) {
                        foreach ($a in $args) {
                            if ("$a" -match '^(.+):/tmp/via-status$') {
                                New-Item -ItemType File -Path (Join-Path $Matches[1] 'dependencies-installed') -Force | Out-Null
                            }
                        }
                    }
                }
            }
            function global:bash {
                $global:ViaBashCalls++
                $global:LASTEXITCODE = 0
            }
        }

        AfterAll {
            Remove-Item function:global:docker, function:global:bash -ErrorAction SilentlyContinue
        }

        BeforeEach {
            $global:ViaDockerCalls = [System.Collections.Generic.List[object]]::new()
            $global:ViaBashCalls = 0
            $global:ViaImageOs = 'linux'
            $global:ViaRunExitCode = 0
            $global:ViaInstallSucceeds = $true
            Push-Location $TestDrive
            Set-Content -Path 'cfg.viancfg' -Value '<VIAnalyzerConfig/>'
        }

        AfterEach {
            Pop-Location
            Get-ChildItem $TestDrive | Remove-Item -Recurse -Force
        }

        It 'passes dependency_file through action.yml and the module [REQ-034]' -Tag 'REQ-034' {
            $action = Get-Content -Raw (Join-Path $repoRoot 'via-lv-docker' 'action.yml')
            $action | Should -Match 'dependency_file:'
            $action | Should -Match 'VIA_DEPENDENCY_FILE: \$\{\{ inputs\.dependency_file \}\}'
            $action | Should -Match 'DependencyFile = \$env:VIA_DEPENDENCY_FILE'
            Get-Content -Raw (Join-Path $repoRoot 'actions' 'OpenSourceActions.psm1') | Should -Match 'DependencyFile\s+= \$DependencyFile'
        }

        It 'accepts DependencyFile through the dispatcher [REQ-034]' -Tag 'REQ-034' {
            & "$repoRoot/actions/Invoke-OSAction.ps1" `
                -ActionName 'via-lv-docker' `
                -ArgsJson '{"ConfigPath":"cfg.viancfg","DependencyFile":".github/labview-dependencies.toml"}' `
                -DryRun -FailOnUnknown
            $LASTEXITCODE | Should -Be 0
        }

        It 'does not install anything when dependency_file is omitted [REQ-034]' -Tag 'REQ-034' {
            & $runVia -ConfigPath 'cfg.viancfg' 6>$null
            $LASTEXITCODE | Should -Be 0

            $global:ViaDockerCalls | Where-Object { $_[0] -eq 'image' } | Should -BeNullOrEmpty
            $run = @($global:ViaDockerCalls | Where-Object { $_[0] -eq 'run' })
            $run.Count | Should -Be 1
            $runLine = $run[0] -join ' '
            $runLine | Should -Not -Match 'VIA_PACKAGES'
            $runLine | Should -Not -Match '/tmp/via-status'
            $runLine | Should -Match "/tmp/run-via.sh 'cfg.viancfg'"
        }

        It 'installs declared packages in the same container that runs VI Analyzer [REQ-034]' -Tag 'REQ-034' {
            Set-Content -Path 'deps.toml' -Value @'
# LUnit test VIs
[project]
labview-version = 2026
labview-bitness = 64

[dependencies]
astemes_lib_lunit = "1.10.7.3"
astemes_lib_lunit_cli = "latest"  # newest
'@
            & $runVia -ConfigPath 'cfg.viancfg' -DependencyFile 'deps.toml' 6>$null
            $LASTEXITCODE | Should -Be 0

            $run = @($global:ViaDockerCalls | Where-Object { $_[0] -eq 'run' })
            $run.Count | Should -Be 1
            $run[0] | Should -Contain 'VIA_PACKAGES=astemes_lib_lunit@1.10.7.3 astemes_lib_lunit_cli'
            ($run[0] -join ' ') | Should -Match "/tmp/run-via.sh 'cfg.viancfg'"
            Get-Content -Raw $runVia | Should -Match 'docker @dockerArgs 2>&1 \| ForEach-Object \{ Write-Host \$_ \}'
            $global:ViaBashCalls | Should -Be 1
        }

        It 'rejects an invalid dependency_file before starting a container: <Name> [REQ-034]' -Tag 'REQ-034' -TestCases @(
            @{ Name = 'missing file';      File = 'missing.toml'; Content = $null; Message = '*dependency_file not found*' }
            @{ Name = 'parent path';       File = '../deps.toml'; Content = $null; Message = "*without '..'*" }
            @{ Name = 'malformed line';    File = 'deps.toml'; Content = "[dependencies]`nastemes_lib_lunit"; Message = '*invalid TOML line*' }
            @{ Name = 'unsupported table'; File = 'deps.toml'; Content = "[dev-dependencies]`nastemes_lib_lunit = `"latest`""; Message = '*unsupported table*' }
            @{ Name = 'unquoted version';  File = 'deps.toml'; Content = "[dependencies]`nastemes_lib_lunit = 1.10"; Message = '*must be a quoted string*' }
            @{ Name = 'unsafe version';    File = 'deps.toml'; Content = "[dependencies]`nastemes_lib_lunit = `"1.0; rm -rf /`""; Message = '*invalid version*' }
            @{ Name = 'invalid name';      File = 'deps.toml'; Content = "[dependencies]`nAstemes-LUnit = `"latest`""; Message = '*invalid package name*' }
            @{ Name = 'duplicate package'; File = 'deps.toml'; Content = "[dependencies]`na = `"latest`"`na = `"1.0`""; Message = '*more than once*' }
            @{ Name = 'no packages';       File = 'deps.toml'; Content = "[project]`nlabview-version = 2026"; Message = '*does not declare any packages*' }
        ) {
            param($Name, $File, $Content, $Message)
            if ($null -ne $Content) { Set-Content -Path $File -Value $Content }
            { & $runVia -ConfigPath 'cfg.viancfg' -DependencyFile $File 6>$null } | Should -Throw $Message
            $global:ViaDockerCalls.Count | Should -Be 0
        }

        It 'fails without parsing a report when installation fails [REQ-034]' -Tag 'REQ-034' {
            Set-Content -Path 'deps.toml' -Value "[dependencies]`nastemes_lib_lunit = `"latest`""
            $global:ViaInstallSucceeds = $false
            $global:ViaRunExitCode = 3

            { & $runVia -ConfigPath 'cfg.viancfg' -DependencyFile 'deps.toml' 6>$null } |
                Should -Throw '*dependency installation*failed*VI Analyzer was not run*'
            $global:ViaBashCalls | Should -Be 0
        }

        It 'rejects dependency_file for non-Linux images [REQ-034]' -Tag 'REQ-034' {
            Set-Content -Path 'deps.toml' -Value "[dependencies]`nastemes_lib_lunit = `"latest`""
            $global:ViaImageOs = 'windows'

            { & $runVia -ConfigPath 'cfg.viancfg' -LabviewVersion '2026q1-windows' -DependencyFile 'deps.toml' 6>$null } |
                Should -Throw '*supported only for Linux*'
            $global:ViaDockerCalls | Where-Object { $_[0] -eq 'run' } | Should -BeNullOrEmpty
        }

        It 'installs with VIPM Free before LabVIEWCLI and only when VIA_PACKAGES is set [REQ-034]' -Tag 'REQ-034' {
            $sh = Get-Content -Raw $runViaSh
            $sh | Should -Not -Match 'vipm activate'
            $sh | Should -Match 'if \[ -n "\$\{VIA_PACKAGES:-\}" \]'
            $sh | Should -Match 'lv_config_dir="/root/natinst/\.config/LabVIEW-\$LV_YEAR"'
            $sh | Should -Match 'server\.tcp\.enabled=True'
            $sh.IndexOf('server.tcp.enabled=True') | Should -BeLessThan $sh.IndexOf('vipm install')
            $sh | Should -Match 'DEBIAN_FRONTEND=noninteractive'
            $sh | Should -Match 'vipm install --color-mode never --labview-version "\$LV_YEAR" --labview-bitness 64'
            $sh.IndexOf('vipm list --installed') | Should -BeGreaterThan $sh.IndexOf('vipm install')
            $sh | Should -Match 'grep -Fqi -- "\$name"'
            $sh | Should -Match 'grep -Fq -- "\$version"'
            $sh.IndexOf('/tmp/via-status/dependencies-installed') | Should -BeGreaterThan $sh.IndexOf('vipm list --installed')
            $sh.IndexOf('LabVIEWCLI \') | Should -BeGreaterThan $sh.IndexOf('/tmp/via-status/dependencies-installed')
        }
    }
}