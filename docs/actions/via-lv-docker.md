# via-lv-docker

## Purpose

Execute LabVIEW VI Analyzer tests using a Docker container and parse results. Supports both static configuration files and dynamic config generation based on changed files in pull requests.

## Parameters

Common parameters are described in [Common parameters](../common-parameters.md).

### Required

None. All parameters have default values.

### Optional

- **ConfigPath** (`string`): Path to VI Analyzer configuration file (.viancfg) or LabVIEW files (.vi/ .ctl/ .llb). If empty, generates config from changed files.
- **TemplatePath** (`string`): Path to .viancfg template (required when generating config dynamically).
- **BaseBranch** (`string`): Branch to compare against for changed files (used when generating config). Default: `origin/develop`.
- **LabviewVersion** (`string`): LabVIEW Docker image version tag. Default: `2026q1-linux`.
- **DockerImage** (`string`): Full Docker image name. Default: `nationalinstruments/labview`.
- **DependencyFile** (`string`): Optional TOML file, relative to the workspace, listing VIPM packages to install in the analyzer container before VI Analyzer runs. Linux images only. See [LabVIEW dependencies](#labview-dependencies).

### GitHub Action inputs

GitHub Action inputs are provided in `snake_case`, while CLI parameters use `PascalCase`. The table below maps each input to its corresponding CLI parameter. For details on shared CLI flags, see [Common parameters](../common-parameters.md).

| Input | CLI parameter | Description |
| --- | --- | --- |
| `config_path` | `ConfigPath` | Path to VI Analyzer configuration file (.viancfg). |
| `template_path` | `TemplatePath` | Path to .viancfg template. |
| `base_branch` | `BaseBranch` | Branch to compare against for changed files. |
| `labview_version` | `LabviewVersion` | LabVIEW Docker image version tag. |
| `docker_image` | `DockerImage` | Full Docker image name. |
| `dependency_file` | `DependencyFile` | Optional TOML file listing VIPM packages to install before analysis (Linux images only). |
| `working_directory` | `WorkingDirectory` | Directory where the action runs. |
| `log_level` | `LogLevel` | Verbosity level (ERROR\|WARN\|INFO\|DEBUG). |
| `dry_run` | `DryRun` | If true, simulate the action without side effects. |

## Examples

### CLI

```powershell
pwsh -File actions/Invoke-OSAction.ps1 -ActionName via-lv-docker -ArgsJson '{
  "ConfigPath": "my-config.viancfg",
  "LabviewVersion": "2026q1-linux"
}'
```

### GitHub Action - Static Config

```yaml
- name: Run VI Analyzer
  uses: ni/open-source/via-lv-docker@v1
  with:
    config_path: '.github/via-config.viancfg'
    labview_version: '2026q1-linux'
```

### GitHub Action - Dynamic Config from Changed Files

This is useful to have for PR checks.

```yaml
- name: Run VI Analyzer on Changed Files
  uses: ni/open-source/via-lv-docker@v1
  with:
    base_branch: 'origin/${{ github.event.pull_request.base.ref }}'
    template_path: '.github/via_template.viancfg'
```

## LabVIEW dependencies

VIs that depend on add-ons that the LabVIEW image does not include (for example LUnit test VIs) are reported as broken. Set `dependency_file` to install those packages in the analyzer container before VI Analyzer runs.

- **Omitted (default):** nothing changes. VIPM is not installed and no packages are installed.
- **Set:** the action creates one container from the LabVIEW image. In it, it installs VIPM Free (no activation, no secrets), starts a headless LabVIEW, runs `vipm refresh` and `vipm install` for the declared packages, and logs `vipm list --installed`. It then restarts that container, which stops VIPM and LabVIEW but keeps the installed packages, and runs VI Analyzer in it. Packages target the LabVIEW found in the image (`/usr/local/natinst/LabVIEW-<year>-64`), 64-bit. LabVIEW 2026 or later is required.
- **Supported platforms:** Linux LabVIEW images only (for example `2026q1-linux`). The action fails with a clear error if the image OS is not `linux`.
- **Failures:** an invalid or missing file fails before the container starts. Any VIPM install failure fails the action with the VIPM output in the log, and VI Analyzer is not run.

### TOML schema

The file uses the `vipm.toml` layout already used in this repository, so an existing manifest can be reused.

```toml
# Optional and informational; the LabVIEW in the container is always used.
[project]
labview-version = 2026
labview-bitness = 64

# Required. Package ID = "exact version" | "latest" | "*"
[dependencies]
astemes_lib_lunit = "1.10.7.3"
astemes_lib_lunit_cli = "latest"
```

Rules:

- Only `[project]` and `[dependencies]` tables are allowed; other tables (for example `[dev-dependencies]`) are rejected.
- `[dependencies]` must declare at least one package. Each package may be declared once.
- Package IDs use lowercase letters, digits and underscores (as on vipm.io).
- Versions are quoted strings: an exact version such as `1.10.7.3` (installed as `id@version`), or `latest` / `*` for the newest available version.
- `#` comments and blank lines are allowed.

### GitHub Action - LUnit dependencies

`.github/labview-dependencies.toml`:

```toml
[dependencies]
astemes_lib_lunit = "latest"
astemes_lib_lunit_cli = "latest"
```

Workflow:

```yaml
- name: Run VI Analyzer
  uses: ni/open-source/via-lv-docker@v1
  with:
    config_path: '.github/via-config.viancfg'
    dependency_file: '.github/labview-dependencies.toml'
```

## Outputs

- **vi-analyzer-report.htm**: HTML report generated by VI Analyzer
- Console output with parsed test results showing passes, failures, and errors

## Return Codes

- `0` – all VI Analyzer tests passed
- non‑zero – tests failed, analyzer error, or `dependency_file` validation/installation failed

See [via-lv-docker/action.yml](../../via-lv-docker/action.yml) and [scripts/via-lv-docker/RunViaLvDocker.ps1](../../scripts/via-lv-docker/RunViaLvDocker.ps1) for implementation details.

For troubleshooting tips, see the [troubleshooting guide](../troubleshooting.md).

## See also

- [Workflow documentation](../workflows/via-lv-docker.md)
- [scripts/via-lv-docker/README.md](../../scripts/via-lv-docker/README.md)
