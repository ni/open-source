# Build LabVIEW Build Specification with Windows Runner 🖥️📦

Call **`BuildSpecGithubHostedWindows.ps1`** to compile LabVIEW packed libraries using LabVIEWCLI on a Windows GitHub-hosted runner.

## Inputs

| Name | Required | Default | Example | Description |
|------|----------|---------|---------|-------------|
| `minimum_supported_lv_version` | **Yes** | - | `2025` | LabVIEW version year to use. |
| `supported_bitness` | **Yes** | - | `32` or `64` | Target LabVIEW bitness. |
| `project_path` | **Yes** | - | `lv_icon_editor.lvproj` | Path to the LabVIEW project file. |
| `target_name` | No | `""` | `My Computer` | Target containing the build spec. Defaults to "My Computer" in helper VI. |
| `build_spec_name` | No | `""` | `Editor Packed Library` | Build spec name. If empty, builds all specs. |
| `major` | No | `-1` | `1` | Major version component. If < 0, version setting is skipped. |
| `minor` | No | `-1` | `0` | Minor version component. If < 0, version setting is skipped. |
| `patch` | No | `-1` | `0` | Patch version component. If < 0, version setting is skipped. |
| `build` | No | `-1` | `1` | Build number component. If < 0, version setting is skipped. |
| `commit` | No | `""` | `abcdef` | Commit identifier. |

## Quick-start

The following example builds using LabVIEW 2025 (32-bit) on a Windows runner.

```yaml
- uses: ./.github/actions/build-spec-github-hosted-windows
  with:
    minimum_supported_lv_version: 2025
    supported_bitness: 32
    project_path: lv_icon_editor.lvproj
    target_name: My Computer
    build_spec_name: Editor Packed Library
    major: 1
    minor: 0
    patch: 0
    build: 1
    commit: ${{ github.sha }}
```

## Complete Example with LabVIEW Setup

This example shows the full workflow including LabVIEW installation:

```yaml
name: Build Spec

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
        uses: ./.github/actions/setup-nipm
        with:
          log_level: 'INFO'

      - name: Setup LabVIEW
        uses: ./.github/actions/setup-labview
        with:
          labview_iso_url: 'https://download.ni.com/.../ni-labview-2025-community-x86_25.3.3_offline.iso'
          timeout_seconds: 1800
          serial_number: ${{ secrets.LABVIEW_SERIAL_NUMBER }}
          package_id: 'LabVIEW_COM_PKG 25.0300'

      - name: Build PPL
        uses: ./.github/actions/build-spec-github-hosted-windows
        with:
          minimum_supported_lv_version: 2025
          supported_bitness: 32
          project_path: lv_icon_editor.lvproj
          target_name: My Computer
          build_spec_name: Editor Packed Library
          major: 1
          minor: 0
          patch: 0
          build: 1
          commit: ${{ github.sha }}

      - name: Upload Built PPL
        uses: actions/upload-artifact@v4
        with:
          name: lv_icon_x86_v1.0.0.1
          path: builds/*.spec
```

## Build All Specifications with Version Override

Leave `build_spec_name` empty and provide version to set the same version on all build specs:

```yaml
- uses: ./.github/actions/build-spec-github-hosted-windows
  with:
    minimum_supported_lv_version: 2025
    supported_bitness: 32
    project_path: lv_icon_editor.lvproj
    target_name: My Computer
    major: 1
    minor: 0
    patch: 0
    build: 1
    commit: ${{ github.sha }}
```

## Skip Version setting

Omit version parameters to use versions to skip setting versions in build specifications:

```yaml
- uses: ./.github/actions/build-spec-github-hosted-windows
  with:
    minimum_supported_lv_version: 2025
    supported_bitness: 32
    project_path: lv_icon_editor.lvproj
    build_spec_name: Editor Packed Library
```

## Version Behavior

- **All version components provided** (`major`, `minor`, `patch`, `build` all ≥ 0):
  - Helper VI is called to set the version
  - If `build_spec_name` is provided: version is set on that build spec only
  - If `build_spec_name` is omitted: version is set on **all** build specs in the project

- **Any version component omitted** (< 0 or not provided):
  - Version setting is skipped
  - Build specifications use their own version settings from the project file

- For **Zip Files**, Version is not set.

## Requirements

- LabVIEW must be installed on the runner (use `setup-labview` action)
- LabVIEWCLI must be available in the PATH
- Helper VI must exist at `scripts/build-spec-helpers/SetBuildVersionCaller.vi`
- Windows runner (Windows Server 2019, 2022, or Windows 10/11)

## Platform Notes

This action requires **Windows runners** with locally installed LabVIEW.

For containerized builds:

- Windows containers: [build-spec-docker-windows](build-spec-docker-windows.md)
- Linux containers: [build-spec-docker-linux](build-spec-docker-linux.md)

See also: [docs/actions/build-spec-github-hosted-windows.md](../actions/build-spec-github-hosted-windows.md)

## License

This directory inherits the root repository's license (MIT, unless otherwise noted).
