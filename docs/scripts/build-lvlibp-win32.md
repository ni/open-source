# Build Packed Library with Windows Runner 🖥️📦

Call **`BuildLvlibpWin32.ps1`** to compile LabVIEW packed libraries using LabVIEWCLI on a Windows GitHub-hosted runner.

## Inputs

| Name | Required | Example | Description |
|------|----------|---------|-------------|
| `minimum_supported_lv_version` | **Yes** | `2025` | LabVIEW version year to use. |
| `supported_bitness` | **Yes** | `32` or `64` | Target LabVIEW bitness. |
| `project_path` | **Yes** | `lv_icon_editor.lvproj` | Path to the LabVIEW project file. |
| `target_name` | **Yes** | `My Computer` | Target that contains the build specification. |
| `build_spec_name` | No | `Editor Packed Library` | Build specification name. Leave empty to build all. |
| `major` | **Yes** | `1` | Major version component. |
| `minor` | **Yes** | `0` | Minor version component. |
| `patch` | **Yes** | `0` | Patch version component. |
| `build` | **Yes** | `1` | Build number component. |
| `commit` | **Yes** | `abcdef` | Commit identifier. |

## Quick-start

The following example builds using LabVIEW 2025 (32-bit) on a Windows runner.

```yaml
- uses: ./.github/actions/build-lvlibp-win32
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
        uses: ./.github/actions/build-lvlibp-win32
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
          path: builds/*.lvlibp
```

## Build All Specifications

Leave `build_spec_name` empty to build all build specifications under the target:

```yaml
- uses: ./.github/actions/build-lvlibp-win32
  with:
    minimum_supported_lv_version: 2025
    supported_bitness: 32
    project_path: lv_icon_editor.lvproj
    target_name: My Computer
    build_spec_name: ''
    major: 1
    minor: 0
    patch: 0
    build: 1
    commit: ${{ github.sha }}
```

## Requirements

- LabVIEW must be installed on the runner (use `setup-labview` action)
- LabVIEWCLI must be available in the PATH
- Windows runner (Windows Server 2019, 2022, or Windows 10/11)

## Platform Notes

This action requires **Windows runners** with locally installed LabVIEW.

For containerized builds:

- Windows containers: [build-lvlibp-docker-windows](build-lvlibp-docker-windows.md)
- Linux containers: [build-lvlibp-docker-linux](build-lvlibp-docker-linux.md)

See also: [docs/actions/build-lvlibp-win32.md](../actions/build-lvlibp-win32.md)

## License

This directory inherits the root repository's license (MIT, unless otherwise noted).
