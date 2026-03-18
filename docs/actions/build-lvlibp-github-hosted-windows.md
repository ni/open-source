# build-lvlibp-github-hosted-windows

## Purpose

Builds LabVIEW Packed Project Library (.lvlibp) files using LabVIEW installed on a Windows GitHub-hosted runner. This action executes the LabVIEW build specification through LabVIEWCLI (32-bit), optionally embedding version information and commit metadata.

Use this action when you need to build PPL files on GitHub-hosted Windows runners with locally installed LabVIEW (no Docker required).

## Parameters

Common parameters are described in [Common parameters](../common-parameters.md).

### Required

- **MinimumSupportedLVVersion** (`string`): LabVIEW version year for the build (e.g., `"2021"`, `"2023"`, `"2025"`).
- **SupportedBitness** (`string`): Bitness of the LabVIEW environment (`"32"` or `"64"`).
- **ProjectPath** (`string`): Path to the LabVIEW project `.lvproj` file that contains the build specification.

### Optional

- **TargetName** (`string`): Target that contains the build specification. Defaults to `"My Computer"` in helper VI if not provided.
- **BuildSpecName** (`string`): Name of the build specification to execute. If empty, builds all build specifications under the specified target.
- **Major** (`int`): Major version component for the PPL. If not provided (or < 0), version setting is skipped.
- **Minor** (`int`): Minor version component for the PPL. If not provided (or < 0), version setting is skipped.
- **Patch** (`int`): Patch version component for the PPL. If not provided (or < 0), version setting is skipped.
- **Build** (`int`): Build number component for the PPL. If not provided (or < 0), version setting is skipped.
- **Commit** (`string`): Commit hash or identifier recorded in the build.

### GitHub Action inputs

| Input | CLI parameter | Description |
| --- | --- | --- |
| `minimum_supported_lv_version` | `MinimumSupportedLVVersion` | LabVIEW version year for the build. |
| `supported_bitness` | `SupportedBitness` | Bitness (`"32"` or `"64"`). |
| `project_path` | `ProjectPath` | Path to the LabVIEW project `.lvproj` file. |
| `target_name` | `TargetName` | Target that contains the build specification ("My Computer" is used if empty). |
| `build_spec_name` | `BuildSpecName` | Name of the build specification (Uses all build specs if empty). |
| `major` | `Major` | Major version component (optional). |
| `minor` | `Minor` | Minor version component (optional). |
| `patch` | `Patch` | Patch version component (optional). |
| `build` | `Build` | Build number component (optional). |
| `commit` | `Commit` | Commit hash or identifier (optional). |
| `working_directory` | `WorkingDirectory` | Base directory for the action. |
| `log_level` | `LogLevel` | Verbosity level (ERROR\|WARN\|INFO\|DEBUG). |
| `dry_run` | `DryRun` | If true, simulate without side effects. |

## Examples

### CLI

```powershell
pwsh -File actions/Invoke-OSAction.ps1 -ActionName build-lvlibp-github-hosted-windows -ArgsJson '{
  "MinimumSupportedLVVersion": "2025",
  "SupportedBitness": "32",
  "ProjectPath": "lv_icon_editor.lvproj",
  "TargetName": "My Computer",
  "BuildSpecName": "Editor Packed Library",
  "Major": 1,
  "Minor": 0,
  "Patch": 0,
  "Build": 0,
  "Commit": "abc1234"
}'
```

### GitHub Action

```yaml
- name: Build PPL with GitHub-hosted Runner
  uses: ni/open-source/build-lvlibp-github-hosted-windows@v1
  with:
    minimum_supported_lv_version: '2025'
    supported_bitness: '32'
    project_path: 'lv_icon_editor.lvproj'
    target_name: 'My Computer'
    build_spec_name: 'Editor Packed Library'
    major: 1
    minor: 0
    patch: 0
    build: 0
    commit: ${{ github.sha }}
```

### CLI without Version (Skip setting version)

```powershell
pwsh -File actions/Invoke-OSAction.ps1 -ActionName build-lvlibp-github-hosted-windows -ArgsJson '{
  "MinimumSupportedLVVersion": "2025",
  "SupportedBitness": "32",
  "ProjectPath": "lv_icon_editor.lvproj",
  "BuildSpecName": "Editor Packed Library"
}'
```

### Complete Workflow with LabVIEW Setup

```yaml
name: Build LVLIBP

on:
  push:
    branches: [main]

jobs:
  build:
    runs-on: windows-latest
    steps:
      - name: Checkout
        uses: actions/checkout@v4

      - name: Setup NI Package Manager
        uses: ni/open-source/setup-nipm@v1
        with:
          log_level: 'INFO'

      - name: Setup LabVIEW
        uses: ni/open-source/setup-labview@v1
        with:
          labview_iso_url: 'https://download.ni.com/.../ni-labview-2025-community-x86_25.3.3_offline.iso'
          timeout_seconds: 1800
          serial_number: ${{ secrets.LABVIEW_SERIAL_NUMBER }}
          package_id: 'LabVIEW_COM_PKG 25.0300'

      - name: Extract version
        id: version
        shell: pwsh
        run: |
          $pkg = Get-Content package.json | ConvertFrom-Json
          $parts = $pkg.version -split '\.'
          "major=$($parts[0])" >> $env:GITHUB_OUTPUT
          "minor=$($parts[1])" >> $env:GITHUB_OUTPUT
          "patch=$($parts[2])" >> $env:GITHUB_OUTPUT

      - name: Build PPL
        uses: ni/open-source/build-lvlibp-github-hosted-windows@v1
        with:
          minimum_supported_lv_version: '2025'
          supported_bitness: '32'
          project_path: 'lv_icon_editor.lvproj'
          target_name: 'My Computer'
          build_spec_name: 'Editor Packed Library'
          major: ${{ steps.version.outputs.major }}
          minor: ${{ steps.version.outputs.minor }}
          patch: ${{ steps.version.outputs.patch }}
          build: ${{ github.run_number }}
          commit: ${{ github.sha }}

      - name: Upload Built PPL
        uses: actions/upload-artifact@v4
        if: success()
        with:
          name: lv_icon_x86_v${{ steps.version.outputs.major }}.${{ steps.version.outputs.minor }}.${{ steps.version.outputs.patch }}.${{ github.run_number }}+g${{ github.sha }}
          path: builds/*.lvlibp
          retention-days: 7

      - name: Upload Build Logs
        uses: actions/upload-artifact@v4
        if: always()
        with:
          name: build-logs
          path: build-logs/*.log
          retention-days: 3
          if-no-files-found: ignore
```

### Build All Specifications

Omit `build_spec_name` to build all build specifications. The provided version is applied to all specs:

```yaml
- name: Build All PPLs
  uses: ni/open-source/build-lvlibp-github-hosted-windows@v1
  with:
    minimum_supported_lv_version: '2025'
    supported_bitness: '32'
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
   - If any version component is missing or < 0, version setting is skipped
   - When version is provided:
     - If `BuildSpecName` is specified: version is set on that build spec only
     - If `BuildSpecName` is omitted: version is set on **all** build specs in the project

2. **LabVIEW Path Construction**:

   - 32-bit: `C:\Program Files (x86)\National Instruments\LabVIEW {lv_version}\LabVIEW.exe`

3. **Build Execution**:

   - Verifies LabVIEW exists at the expected path
   - Verifies project file exists
   - Calls helper VI to set version (if version parameters provided)
   - Executes `LabVIEWCLI -OperationName ExecuteBuildSpec` with specified parameters

4. **Log Collection**:

   - Copies LabVIEW logs from `%TEMP%` to `build-logs\` directory for artifact collection

5. **Error Handling**:

   - On failure, exits with the build error code

## Return Codes

- `0` – success
- non-zero – build failed or LabVIEW not found

## Requirements

- LabVIEW must be installed at the expected path (use `setup-labview` action)
- LabVIEWCLI must be available in the PATH
- The LabVIEW project file must exist at the specified path
- The target and build specification must exist in the project
- Windows runner (Windows Server 2019, 2022, or Windows 10/11)
- Helper VI (`SetBuildVersionCaller.vi`) must be present in `scripts/build-lvlibp-helpers/`

## Platform Support

This action is designed for Windows GitHub-hosted runners with locally installed LabVIEW.

For containerized builds, see:

- [build-lvlibp-docker-windows](build-lvlibp-docker-windows.md) – Windows Docker PPL build action
- [build-lvlibp-docker-linux](build-lvlibp-docker-linux.md) – Linux Docker PPL build action

## See also

- [build-lvlibp-docker-windows](build-lvlibp-docker-windows.md) – Windows Docker PPL build action
- [build-lvlibp-docker-linux](build-lvlibp-docker-linux.md) – Linux Docker PPL build action
- [setup-labview](setup-labview.md) – LabVIEW installation action
- [setup-nipm](setup-nipm.md) – NI Package Manager setup action
- [Architecture documentation](../architecture.md)
