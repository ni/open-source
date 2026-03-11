# build-lvlibp-docker-linux

## Purpose

Builds LabVIEW Packed Project Library (.lvlibp) files using a Linux LabVIEW Docker container. This action executes the LabVIEW build specification through LabVIEWCLI inside a containerized Linux environment, optionally embedding version information and commit metadata.

Use this action when you need to build PPL files in a Linux Docker container, ensuring a consistent and isolated build environment.

## Parameters

Common parameters are described in [Common parameters](../common-parameters.md).

### Required

- **MinimumSupportedLVVersion** (`string`): LabVIEW version year for the build (e.g., `"2021"`, `"2023"`, `"2026"`).
- **SupportedBitness** (`string`): Bitness of the LabVIEW environment (`"32"` or `"64"`).
- **ProjectPath** (`string`): Path to the LabVIEW project `.lvproj` file that contains the build specification.

### Optional

- **TargetName** (`string`): Target that contains the build specification. Defaults to `"My Computer"` in helper VI if not provided.
- **BuildSpecName** (`string`): Name of the build specification to execute. If empty, builds all build specifications in the project.
- **Major** (`int`): Major version component for the PPL. If not provided (or < 0), version setting is skipped.
- **Minor** (`int`): Minor version component for the PPL. If not provided (or < 0), version setting is skipped.
- **Patch** (`int`): Patch version component for the PPL. If not provided (or < 0), version setting is skipped.
- **Build** (`int`): Build number component for the PPL. If not provided (or < 0), version setting is skipped.
- **Commit** (`string`): Commit hash or identifier recorded in the build (for documentation purposes).
- **DockerImage** (`string`): Docker image name. Default: `"nationalinstruments/labview"`.
- **ImageTag** (`string`): Docker image tag. Default: `"2026q1-linux"`.

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
| `docker_image` | `DockerImage` | Docker image name (optional). |
| `image_tag` | `ImageTag` | Docker image tag (optional). |
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
    major: 1
    minor: 0
    patch: 0
    build: 0
    commit: ${{ github.sha }}
```

## Behavior

1. **Version Setting**:

   - If all version components (`Major`, `Minor`, `Patch`, `Build`) are provided (≥ 0), the helper VI is called to set the version
   - If any version component is missing or < 0, version setting is skipped and build specs use their own versions
   - When version is provided:
     - If `BuildSpecName` is specified: version is set on that build spec only
     - If `BuildSpecName` is omitted: version is set on **all** build specs in the project

2. **Docker Image Pull**:

   - Pulls the specified Docker image (or default `nationalinstruments/labview:2026q1-linux`)
   - Verifies the pull was successful

3. **LabVIEW Path Construction**:

   - Container path: `/usr/local/natinst/LabVIEW-{version}-{bitness}/labview`
   - Example: `/usr/local/natinst/LabVIEW-2026-64/labview`

4. **Build Execution**:

   - Mounts current directory to `/workspace` in container
   - Mounts build script to `/tmp/build-lvlibp.sh` in container
   - Calls helper VI to set version (if version parameters provided)
   - Executes `LabVIEWCLI -OperationName ExecuteBuildSpec` inside container

5. **Log Collection**:

   - Copies LabVIEW logs from `/tmp` to `/workspace/build-logs` for artifact collection

6. **Error Handling**:

   - On failure, exits with the build error code

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
