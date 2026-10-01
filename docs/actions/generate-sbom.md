# generate-sbom

## Purpose

Generate a CycloneDX Software Bill of Materials (SBOM) for a LabVIEW build specification using `vipm sbom`. The reusable workflow runs on `ubuntu-latest` and invokes VIPM inside the matching `nationalinstruments/labview:<version>-linux` container, following the Linux flow validated in [ni/labview-icon-editor#540](https://github.com/ni/labview-icon-editor/pull/540). The standalone composite action remains available for callers that already have VIPM and LabVIEW configured.

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

The standalone composite action does **not** install or activate VIPM, and does not set up LabVIEW. The reusable Linux workflow is self-contained: it checks out the caller repository, starts the LabVIEW Linux container, installs VIPM from its Debian package, and generates the SBOM there. Its `labview_version` input is the container release (`YYYYq1` or `YYYYq3`, minimum `2025q3`); the workflow derives the year passed to VIPM.

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

### Reusable workflow (Linux container)

Actor Framework, Icon Editor, and other consumer repositories can call the reusable workflow. It uploads the generated SBOM as an artifact retained for 90 days:

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
      labview_version: '2026q1'
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
