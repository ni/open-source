# Build LabVIEW Build Specification with Docker Windows 🐳📦

Call **`BuildSpecDockerWindows.ps1`** to compile LabVIEW build specification using LabVIEWCLI inside a Windows Docker container.

## Inputs

| Name | Required | Example | Description |
|------|----------|---------|-------------|
| `minimum_supported_lv_version` | **Yes** | `2026` | LabVIEW version year to use. |
| `supported_bitness` | **Yes** | `32` or `64` | Target LabVIEW bitness. |
| `project_path` | **Yes** | `lv_icon_editor.lvproj` | Path to the LabVIEW project file. |
| `target_name` | No | `My Computer` | Target that contains the build specification. Defaults to "My Computer" in helper VI. |
| `build_spec_name` | No | `Editor Packed Library` | Build specification name. Leave empty to build all. |
| `major` | No | `1` | Major version component. Omit to skip version setting. |
| `minor` | No | `0` | Minor version component. Omit to skip version setting. |
| `patch` | No | `0` | Patch version component. Omit to skip version setting. |
| `build` | No | `1` | Build number component. Omit to skip version setting. |
| `commit` | No | `abcdef` | Commit identifier. |
| `docker_image` | No | `nationalinstruments/labview` | Docker image name. |
| `image_tag` | No | `2026q1-windows` | Docker image tag. |

## Quick-start

The following example builds using LabVIEW 2026 inside a Windows Docker container with custom version:

```yaml
- uses: ./.github/actions/build-spec-docker-windows
  with:
    minimum_supported_lv_version: 2026
    supported_bitness: 64
    project_path: lv_icon_editor.lvproj
    target_name: My Computer
    build_spec_name: Editor Packed Library
    major: 1
    minor: 0
    patch: 0
    build: 1
    commit: ${{ github.sha }}
    docker_image: nationalinstruments/labview
    image_tag: 2026q1-windows
```

## Minimal Example

Build with only required parameters (skips version setting, builds all specifications in "My Computer"):

```yaml
- uses: ./.github/actions/build-spec-docker-windows
  with:
    minimum_supported_lv_version: 2026
    supported_bitness: 64
    project_path: lv_icon_editor.lvproj
```

## Build All Specifications

Leave `build_spec_name` empty to build all build specifications under the target:

```yaml
- uses: ./.github/actions/build-spec-docker-windows
  with:
    minimum_supported_lv_version: 2026
    supported_bitness: 64
    project_path: lv_icon_editor.lvproj
    target_name: My Computer
    build_spec_name: ''
    major: 1
    minor: 0
    patch: 0
    build: 1
    commit: ${{ github.sha }}
```

- Note: For **Zip Files**, Version is not set.

## Requirements

- Docker must be installed and running on the host system
- The host must support Windows containers (Windows Server or Windows 10/11 with Docker Desktop)
- The specified Windows Docker image must contain LabVIEWCLI
- The LabVIEW project file must exist at the specified path

## Platform Notes

This action requires **Windows containers**, which are only available on:

- Windows Server 2016 or later
- Windows 10/11 with Docker Desktop configured for Windows containers

For Linux-based builds, use [build-spec-docker-linux](build-spec-docker-linux.md).

See also: [docs/actions/build-spec-docker-windows.md](../actions/build-spec-docker-windows.md)

## License

This directory inherits the root repository's license (MIT, unless otherwise noted).
