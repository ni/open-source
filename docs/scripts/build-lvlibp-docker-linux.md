# Build Packed Library with Docker Linux 🐳📦

Call **`BuildLvlibpDockerLinux.ps1`** to compile LabVIEW packed libraries using LabVIEWCLI inside a Linux Docker container.

## Inputs

| Name | Required | Example | Description |
|------|----------|---------|-------------|
| `minimum_supported_lv_version` | **Yes** | `2026` | LabVIEW version year to use. |
| `supported_bitness` | **Yes** | `32` or `64` | Target LabVIEW bitness. |
| `project_path` | **Yes** | `lv_icon_editor.lvproj` | Path to the LabVIEW project file. |
| `target_name` | **Yes** | `My Computer` | Target that contains the build specification. |
| `build_spec_name` | No | `Editor Packed Library` | Build specification name. Leave empty to build all. |
| `major` | **Yes** | `1` | Major version component. |
| `minor` | **Yes** | `0` | Minor version component. |
| `patch` | **Yes** | `0` | Patch version component. |
| `build` | **Yes** | `1` | Build number component. |
| `commit` | **Yes** | `abcdef` | Commit identifier. |
| `docker_image` | No | `nationalinstruments/labview` | Docker image name. |
| `image_tag` | No | `2026q1-linux` | Docker image tag. |

## Quick-start

The following example builds using LabVIEW 2026 inside a Linux Docker container.

```yaml
- uses: ./.github/actions/build-lvlibp-docker-linux
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
    image_tag: 2026q1-linux
```

## Build All Specifications

Leave `build_spec_name` empty to build all build specifications under the target:

```yaml
- uses: ./.github/actions/build-lvlibp-docker-linux
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

## Requirements

- Docker must be installed and running on the host system
- The specified Linux Docker image must contain LabVIEWCLI
- The LabVIEW project file must exist at the specified path

See also: [docs/actions/build-lvlibp-docker-linux.md](../actions/build-lvlibp-docker-linux.md)

## License

This directory inherits the root repository's license (MIT, unless otherwise noted).
