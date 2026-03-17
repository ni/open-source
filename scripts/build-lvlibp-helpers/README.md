# Build LVLIBP Common Scripts

This directory contains shared utilities used by all LVLIBP build actions.

## SetBuildVersion.vi

Helper VI that sets the build version for LabVIEW build specification(s).

### VI Parameters

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `ProjectPath` | String | **Yes** | - | Path to the LabVIEW project file |
| `BuildSpecName` | String | No | *all build specs* | Build specification name |
| `TargetName` | String | No | `"My Computer"` | Target containing the build spec(s) |
| `Version` | String | **Yes** | - | Version string in format `M.m.p.b` |

### Behavior

- **ProjectPath** and **Version** are mandatory
- **TargetName** defaults to `"My Computer"` if not provided
- **BuildSpecName**:

  - If **not provided**, the VI sets the version on **all build specifications** in the project
  - If **provided**, the VI sets the version on only the specified build specification

### Usage with LabVIEWCLI

**Set version on specific build spec:**

```bash
LabVIEWCLI \
  -OperationName RunVI \
  -LabVIEWPath "/usr/local/natinst/LabVIEW-2025-64/labview" \
  -VIPath "/workspace/scripts/build-lvlibp-common/SetBuildVersion.vi" \
  "/workspace/project.lvproj" "My PPL" "My Computer" "1.0.0.4"\
  -Headless
```

**Set version on all build specs:**

```bash
LabVIEWCLI \
  -OperationName RunVI \
  -LabVIEWPath "/usr/local/natinst/LabVIEW-2025-64/labview" \
  -VIPath "/workspace/scripts/build-lvlibp-common/SetBuildVersion.vi" \
  "/workspace/project.lvproj" "" "My Computer" "2.0.0.4"\
  -Headless
```

### Used By

- [`build-lvlibp-docker-linux`](../build-lvlibp-docker-linux/BuildLvlibpDockerLinux.ps1) – When version parameters are provided
- [`build-lvlibp-docker-windows`](../build-lvlibp-docker-windows/BuildLvlibpDockerWindows.ps1) – When version parameters are provided
- [`build-lvlibp-github-hosted-windows`](../build-lvlibp-github-hosted-windows/BuildLvlibpGithubHostedWindows.ps1) – When version parameters are provided

### When is the Helper VI Called?

The helper VI is **only called** when:

1. All version components (`Major`, `Minor`, `Patch`, `Build`) are provided **and** ≥ 0

The helper VI is **not called** when:

1. Any version component is missing or < 0 (build specs use their own versions)

### Notes

- When **no version is provided** to the build scripts, the helper VI is **not called**
- Each build specification will use its own version settings from the project file
- When version is provided but BuildSpecName is omitted, **all build specs get the same version**

## See Also

- [Architecture documentation](../../docs/architecture.md)
- [CREATE-NEW-ACTION.md](../../CREATE-NEW-ACTION.md)
