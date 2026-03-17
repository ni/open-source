# build-lvlibp-docker-windows

## Purpose

Builds LabVIEW Packed Project Library (.lvlibp) files using a Windows LabVIEW Docker container. This action executes the LabVIEW build specification through LabVIEWCLI inside a Docker container, optionally embedding version information and commit metadata.

Use this action when you need to build PPL files in containerized Windows environments (CI/CD pipelines) without installing LabVIEW directly on the host system.

## Parameters

Common parameters are described in [Common parameters](../common-parameters.md).

### Required

- **MinimumSupportedLVVersion** (`string`): LabVIEW version year for the build (e.g., `"2021"`, `"2023"`, `"2026"`).
- **SupportedBitness** (`string`): Bitness of the LabVIEW environment (`"32"` or `"64"`).
- **ProjectPath** (`string`): Path to the LabVIEW project `.lvproj` file that contains the build specification.

### Optional

- **TargetName** (`string`): Target that contains the build specification. Defaults to `"My Computer"` in helper VI. Default: `""`.
- **BuildSpecName** (`string`): Name of the build specification to execute. If empty, builds all build specifications under the specified target. Default: `""`.
- **Major** (`int`): Major version component for the PPL. Omit to skip version setting. Default: `-1`.
- **Minor** (`int`): Minor version component for the PPL. Omit to skip version setting. Default: `-1`.
- **Patch** (`int`): Patch version component for the PPL. Omit to skip version setting. Default: `-1`.
- **Build** (`int`): Build number component for the PPL. Omit to skip version setting. Default: `-1`.
- **Commit** (`string`): Commit hash or identifier recorded in the build. Default: `""`.
- **DockerImage** (`string`): Docker image name. Default: `"nationalinstruments/labview"`.
- **ImageTag** (`string`): Docker image tag. Default: `"2026q1-windows"`.

### GitHub Action inputs

| Input | CLI parameter | Description |
| --- | --- | --- |
| `minimum_supported_lv_version` | `MinimumSupportedLVVersion` | LabVIEW version year for the build. |
| `supported_bitness` | `SupportedBitness` | Bitness (`"32"` or `"64"`). |
| `project_path` | `ProjectPath` | Path to the LabVIEW project `.lvproj` file. |
| `target_name` | `TargetName` | Target that contains the build specification (optional). |
| `build_spec_name` | `BuildSpecName` | Name of the build specification (optional). |
| `major` | `Major` | Major version component (optional). |
| `minor` | `Minor` | Minor version component (optional). |
| `patch` | `Patch` | Patch version component (optional). |
| `build` | `Build` | Build number component (optional). |
| `commit` | `Commit` | Commit hash or identifier (optional). |
| `docker_image` | `DockerImage` | Docker image name. |
| `image_tag` | `ImageTag` | Docker image tag. |
| `working_directory` | `WorkingDirectory` | Base directory for the action. |
| `log_level` | `LogLevel` | Verbosity level (ERROR\|WARN\|INFO\|DEBUG). |
| `dry_run` | `DryRun` | If true, simulate without side effects. |

## Examples

### Minimal CLI Example

Build with only required parameters (uses version from build specification):

```powershell
pwsh -File actions/Invoke-OSAction.ps1 -ActionName build-lvlibp-docker-windows -ArgsJson '{
  "MinimumSupportedLVVersion": "2026",
  "SupportedBitness": "64",
  "ProjectPath": "lv_icon_editor.lvproj"
}'
```

### CLI with Custom Version

```powershell
pwsh -File actions/Invoke-OSAction.ps1 -ActionName build-lvlibp-docker-windows -ArgsJson '{
  "MinimumSupportedLVVersion": "2026",
  "SupportedBitness": "64",
  "ProjectPath": "lv_icon_editor.lvproj",
  "TargetName": "My Computer",
  "BuildSpecName": "Editor Packed Library",
  "Major": 1,
  "Minor": 0,
  "Patch": 0,
  "Build": 0,
  "Commit": "abc1234",
  "DockerImage": "nationalinstruments/labview",
  "ImageTag": "2026-windows"
}'
```

### Minimal GitHub Action

Build with only required parameters:

```yaml
- name: Build PPL with Windows Docker
  uses: owner/repo/build-lvlibp-docker-windows@v1
  with:
    minimum_supported_lv_version: '2026'
    supported_bitness: '64'
    project_path: 'lv_icon_editor.lvproj'
```

### GitHub Action with Custom Version

```yaml
- name: Build PPL with Windows Docker
  uses: owner/repo/build-lvlibp-docker-windows@v1
  with:
    minimum_supported_lv_version: '2026'
    supported_bitness: '64'
    project_path: 'lv_icon_editor.lvproj'
    target_name: 'My Computer'
    build_spec_name: 'Editor Packed Library'
    major: 1
    minor: 0
    patch: 0
    build: 0
    commit: ${{ github.sha }}
    docker_image: 'nationalinstruments/labview'
    image_tag: '2026-windows'
```

### Build All Specifications

```yaml
- name: Build All PPLs
  uses: owner/repo/build-lvlibp-docker-windows@v1
  with:
    minimum_supported_lv_version: '2026'
    supported_bitness: '64'
    project_path: 'lv_icon_editor.lvproj'
    target_name: 'My Computer'
    build_spec_name: ''  # Empty - builds all specifications
    major: 1
    minor: 0
    patch: 0
    build: 0
    commit: ${{ github.sha }}
```

## Behavior

1. Pulls the specified Windows Docker image (e.g., `nationalinstruments/labview:2026-windows`)
2. Mounts the workspace directory into the container at `C:\workspace`
3. Mounts the action repository root (containing helper VIs) into the container at `C:\action`
4. Mounts the PowerShell build script into the container at `C:\scripts\build-lvlibp.ps1`
5. Constructs the LabVIEWPath based on version and bitness
6. If version parameters are provided (all four must be specified):
   - Executes helper VI to set build specification version
   - Helper VI uses positional arguments: ProjectPath, BuildSpecName, TargetName, Version
7. Executes `LabVIEWCLI -OperationName ExecuteBuildSpec` with the specified parameters
8. Copies LabVIEW logs from `%TEMP%` to `C:\workspace\build-logs` for artifact collection
9. On failure, exits with the build error code

## Return Codes

- `0` – success
- non-zero – build failed, version setting failed, or Docker operation failed

## Requirements

- Docker must be installed and running on the host system
- The specified Windows Docker image must be available (pulled or cached)
- The host system must support Windows containers (Windows Server or Windows 10/11 with container support)
- The LabVIEW project file must exist at the specified path
- LabVIEWCLI must be available in the Docker container
- The target and build specification must exist in the project
- If setting version: all four version parameters (Major, Minor, Patch, Build) must be provided or all must be omitted

## Platform Support

This action is designed for Windows Docker containers. Key differences from the Linux variant:

- Uses Windows-style paths (`C:\` drive letters)
- Requires Windows Server or Windows 10/11 with container support
- LabVIEW installation paths differ between 32-bit and 64-bit
- Logs are found in `%TEMP%` instead of `/tmp/`
- Uses PowerShell as the container shell instead of bash

For Linux containers, see [build-lvlibp-docker-linux](build-lvlibp-docker-linux.md).

## Version Setting

The action can optionally set the build version before building:

- **With version**: Provide all four version parameters (`major`, `minor`, `patch`, `build`) to set a custom version
- **Without version**: Omit all version parameters to use the version configured in the build specification

You cannot partially specify version components - either provide all four or none.

## See also

- [build-lvlibp-docker-linux](build-lvlibp-docker-linux.md) – Linux Docker PPL build action
- [build-lvlibp-github-hosted-windows](build-lvlibp-github-hosted-windows.md) – Windows native PPL build action
- [build](build.md) – General LabVIEW build action
- [Architecture documentation](../architecture.md)
