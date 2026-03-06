# build-lvlibp-docker-linux

## Purpose

Builds LabVIEW Packed Project Library (.lvlibp) files using a Linux LabVIEW Docker container. This action executes the LabVIEW build specification through LabVIEWCLI inside a Docker container, embedding version information and commit metadata, then renames the resulting artifact with a standardized naming convention.

Use this action when you need to build PPL files in containerized Linux environments (CI/CD pipelines) without installing LabVIEW directly on the host system.

## Parameters

Common parameters are described in [Common parameters](../common-parameters.md).

### Required

- **MinimumSupportedLVVersion** (`string`): LabVIEW version year for the build (e.g., `"2021"`, `"2023"`, `"2026"`).
- **SupportedBitness** (`string`): Bitness of the LabVIEW environment (`"32"` or `"64"`).
- **ProjectPath** (`string`): Path to the LabVIEW project `.lvproj` file that contains the build specification.
- **TargetName** (`string`): Target that contains the build specification (e.g., `"My Computer"`).
- **Major** (`int`): Major version component for the PPL.
- **Minor** (`int`): Minor version component for the PPL.
- **Patch** (`int`): Patch version component for the PPL.
- **Build** (`int`): Build number component for the PPL.
- **Commit** (`string`): Commit hash or identifier recorded in the build.

### Optional

- **BuildSpecName** (`string`): Name of the build specification to execute. If empty, builds all build specifications under the specified target. Default: `""`.
- **DockerImage** (`string`): Docker image name. Default: `"nationalinstruments/labview"`.
- **ImageTag** (`string`): Docker image tag. Default: `"2026q1-linux"`.

### GitHub Action inputs

| Input | CLI parameter | Description |
| --- | --- | --- |
| `minimum_supported_lv_version` | `MinimumSupportedLVVersion` | LabVIEW version year for the build. |
| `supported_bitness` | `SupportedBitness` | Bitness (`"32"` or `"64"`). |
| `project_path` | `ProjectPath` | Path to the LabVIEW project `.lvproj` file. |
| `target_name` | `TargetName` | Target that contains the build specification. |
| `build_spec_name` | `BuildSpecName` | Name of the build specification (optional). |
| `major` | `Major` | Major version component. |
| `minor` | `Minor` | Minor version component. |
| `patch` | `Patch` | Patch version component. |
| `build` | `Build` | Build number component. |
| `commit` | `Commit` | Commit hash or identifier. |
| `docker_image` | `DockerImage` | Docker image name. |
| `image_tag` | `ImageTag` | Docker image tag. |
| `working_directory` | `WorkingDirectory` | Base directory for the action. |
| `log_level` | `LogLevel` | Verbosity level (ERROR\|WARN\|INFO\|DEBUG). |
| `dry_run` | `DryRun` | If true, simulate without side effects. |

## Examples

### CLI

```powershell
pwsh -File actions/Invoke-OSAction.ps1 -ActionName build-lvlibp-docker-linux -ArgsJson '{
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
  "ImageTag": "2026q1-linux"
}'
```

### GitHub Action

```yaml
- name: Build PPL with Linux Docker
  uses: ni/open-source/build-lvlibp-docker-linux@v1
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
    image_tag: '2026q1-linux'
```

### Build All Specifications

```yaml
- name: Build All PPLs
  uses: ni/open-source/build-lvlibp-docker-linux@v1
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

1. Pulls the specified Linux Docker image (e.g., `nationalinstruments/labview:2026q1-linux`)
2. Mounts the workspace directory into the container at `/workspace`
3. Mounts the bash build script into the container at `/tmp/build-lvlibp.sh`
4. Constructs the LabVIEWPath (`/usr/local/natinst/LabVIEW-{version}-{bitness}/labview`)
5. Executes the bash script which runs `LabVIEWCLI -OperationName ExecuteBuildSpec` with the specified parameters
6. On success, searches for `.lvlibp` files in the `builds` directory and renames them with version and commit metadata (e.g., `lv_icon_x64_v1.0.0.0+gabc1234.lvlibp`)
7. On failure, exits with the build error code

## Return Codes

- `0` – success
- non-zero – build failed or Docker operation failed

## Requirements

- Docker must be installed and running on the host system
- The specified Linux Docker image must be available (pulled or cached)
- The LabVIEW project file must exist at the specified path
- LabVIEWCLI must be available in the Docker container
- The target and build specification must exist in the project

## See also

- [build-lvlibp](build-lvlibp.md) – Non-Docker PPL build action
- [build](build.md) – General LabVIEW build action
- [Architecture documentation](../architecture.md)
