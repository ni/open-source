# generate-sbom

## Purpose

Generate a CycloneDX Software Bill of Materials (SBOM) for a LabVIEW build specification using `vipm sbom`. Minimal implementation ported from [ni/labview-icon-editor#540](https://github.com/ni/labview-icon-editor/pull/540): requires VIPM CLI to be available on the runner and LabVIEW to be available for the target project. The reusable workflow installs VIPM itself, and additional execution modes (Docker-based builds, headless LabVIEW/VI Server warm-up) can be added later if a real consumer requires them.

## Parameters

Common parameters are described in [Common parameters](../common-parameters.md).

### Required

- **LvprojPath** (`string`): Path to the `.lvproj` file to scan.
- **LabVIEWVersion** (`string`): LabVIEW version (`YYYY`).
- **LabVIEWBitness** (`string`): `"32"` or `"64"`.
- **BuildSpecName** (`string`): Name of the build specification to scope the SBOM to.
- **ProductName** (`string`): Name recorded in the SBOM's `metadata.component`.
- **ProductVersion** (`string`): Version recorded in the SBOM's `metadata.component`.
- **OutputPath** (`string`): Output file path for the generated SBOM.

### Optional

- **TargetName** (`string`): LabVIEW project target containing the build spec (default: `"My Computer"`).
- **Format** (`string`): SBOM output format (default: `"cyclonedx"`).
- **SchemaVersion** (`string`): CycloneDX schema version (default: `"1.5"`).

### GitHub Action inputs

| Input | CLI parameter | Description |
| --- | --- | --- |
| `lvproj_path` | `LvprojPath` | Path to the `.lvproj` file to scan. |
| `labview_version` | `LabVIEWVersion` | LabVIEW version (`YYYY`). |
| `labview_bitness` | `LabVIEWBitness` | `"32"` or `"64"`. |
| `build_spec_name` | `BuildSpecName` | Build specification to scope the SBOM to. |
| `target_name` | `TargetName` | LabVIEW project target (default: `"My Computer"`). |
| `product_name` | `ProductName` | Name recorded in the SBOM. |
| `product_version` | `ProductVersion` | Version recorded in the SBOM. |
| `output_path` | `OutputPath` | Output file path for the generated SBOM. |
| `format` | `Format` | SBOM output format (default: `"cyclonedx"`). |
| `schema_version` | `SchemaVersion` | CycloneDX schema version (default: `"1.5"`). |
| `log_level` | `LogLevel` | Verbosity level (ERROR\|WARN\|INFO\|DEBUG). |
| `dry_run` | `DryRun` | If true, simulate the action without side effects. |

## Prerequisites

This action does **not** install or activate VIPM, and does not set up LabVIEW. The caller's workflow (or the reusable workflow below) must ensure `vipm` is on `PATH` and LabVIEW is available before this step runs.

## Examples

### CLI

```powershell
pwsh -File actions/Invoke-OSAction.ps1 -ActionName generate-sbom -ArgsJson '{
  "LvprojPath": "lv_icon_editor.lvproj",
  "LabVIEWVersion": "2026",
  "LabVIEWBitness": "64",
  "BuildSpecName": "Editor Packed Library",
  "ProductName": "Icon Editor Packed Library",
  "ProductVersion": "1.0.0.0",
  "OutputPath": "builds/lv_icon_editor_Editor Packed Library.cdx.sbom.json"
}'
```

### GitHub Action (direct, as a step in an existing job)

```yaml
- name: Install VIPM CLI
  # ... existing install/activate steps ...
- name: Generate SBOM
  uses: ni/open-source/generate-sbom@v1
  with:
    lvproj_path: lv_icon_editor.lvproj
    labview_version: '2026'
    labview_bitness: '64'
    build_spec_name: 'Editor Packed Library'
    product_name: 'Icon Editor Packed Library'
    product_version: '1.0.0.0'
    output_path: 'builds/lv_icon_editor_Editor Packed Library.cdx.sbom.json'
```

### Example consumer workflow (reusable workflow)

The reusable workflow is self-contained (it installs VIPM CLI itself, since `workflow_call` runs in its own fresh job) and also uploads the SBOM as an artifact:

```yaml
name: Generate SBOM

on:
  push:
    branches: [main]
  workflow_dispatch:

jobs:
  generate-sbom:
    uses: ni/open-source/.github/workflows/reusable-generate-sbom.yml@v1
    with:
      lvproj_path: lv_icon_editor.lvproj
      labview_version: '2026'
      labview_bitness: '64'
      build_spec_name: 'Editor Packed Library'
      product_name: 'Icon Editor Packed Library'
      product_version: '1.0.0.0'
      output_path: 'builds/lv_icon_editor_Editor Packed Library.cdx.sbom.json'
      artifact_name: 'sbom'
```

## Return Codes

- `0` - success
- non-zero - `vipm` not found on `PATH`, `vipm sbom` failure, or no SBOM file produced
