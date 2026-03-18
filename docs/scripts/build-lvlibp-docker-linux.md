# Build Packed Library with Docker Linux 🐳📦

Call **`BuildLvlibpDockerLinux.ps1`** to compile LabVIEW packed libraries using LabVIEWCLI inside a Linux Docker container.

## Inputs

| Name | Required | Default | Example | Description |
|------|----------|---------|---------|-------------|
| `minimum_supported_lv_version` | **Yes** | - | `2026` | LabVIEW version year to use. |
| `supported_bitness` | **Yes** | - | `32` or `64` | Target LabVIEW bitness. |
| `project_path` | **Yes** | - | `lv_icon_editor.lvproj` | Path to the LabVIEW project file. |
| `target_name` | No | `""` | `My Computer` | Target containing the build spec. Defaults to "My Computer" in helper VI. |
| `build_spec_name` | No | `""` | `Editor Packed Library` | Build spec name. If empty, builds all specs. |
| `major` | No | `-1` | `1` | Major version component. If < 0, version setting is skipped. |
| `minor` | No | `-1` | `0` | Minor version component. If < 0, version setting is skipped. |
| `patch` | No | `-1` | `0` | Patch version component. If < 0, version setting is skipped. |
| `build` | No | `-1` | `1` | Build number component. If < 0, version setting is skipped. |
| `commit` | No | `""` | `abcdef` | Commit identifier. |
| `docker_image` | No | `nationalinstruments/labview` | Custom image name | Docker image to use. |
| `image_tag` | No | `2026q1-linux` | Custom tag | Docker image tag. |

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

## Build All Specifications with Version Override

Leave `build_spec_name` empty and provide version to set the same version on all build specs:

```yaml
- uses: ./.github/actions/build-lvlibp-docker-linux
  with:
    minimum_supported_lv_version: 2026
    supported_bitness: 64
    project_path: lv_icon_editor.lvproj
    major: 1
    minor: 0
    patch: 0
    build: 1
    commit: ${{ github.sha }}
```

## Version Behavior

- **All version components provided** (`major`, `minor`, `patch`, `build` all ≥ 0):
  - Helper VI is called to set the version
  - If `build_spec_name` is provided: version is set on that build spec only
  - If `build_spec_name` is omitted: version is set on **all** build specs in the project

- **Any version component omitted** (< 0 or not provided):
  - Version setting is skipped

## Requirements

- Docker must be installed and running on the host system
- The specified Linux Docker image must contain LabVIEWCLI
- The LabVIEW project file must exist at the specified path

See also: [docs/actions/build-lvlibp-docker-linux.md](../actions/build-lvlibp-docker-linux.md)

## License

This directory inherits the root repository's license (MIT, unless otherwise noted).
