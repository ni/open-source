# setup-lunit

## Purpose

Installs VI Package Manager (VIPM) and the LUnit for G-CLI package for LabVIEW automation testing. This action is typically used in CI/CD pipelines to set up unit testing infrastructure for LabVIEW projects using the G-CLI command-line interface.

The action automatically:

- Detects if VIPM is already installed and skips installation if found
- Downloads and installs VIPM if not present
- Refreshes the VIPM package repository
- Installs the LUnit for G-CLI package for the specified LabVIEW version and bitness

## Parameters

Common parameters are described in [Common parameters](../common-parameters.md).

### Required

- **LVVersion** (`string`): LabVIEW version (e.g., "2025", "2024", "2023").
- **LVBitness** (`string`): LabVIEW bitness. Must be either "32" or "64".

### Optional

- **VipmInstallerUrl** (`string`): URL to download the VIPM installer. Default: `https://packages.jki.net/vipm/preview/vipm-setup-latest-preview.exe`

### GitHub Action inputs

| Input | CLI parameter | Description |
| --- | --- | --- |
| `lv_version` | `LVVersion` | LabVIEW version (e.g., "2025"). |
| `lv_bitness` | `LVBitness` | LabVIEW bitness ("32" or "64"). |
| `vipm_installer_url` | `VipmInstallerUrl` | URL to download VIPM installer. |
| `working_directory` | `WorkingDirectory` | Base directory for the action. |
| `log_level` | `LogLevel` | Verbosity level (ERROR\|WARN\|INFO\|DEBUG). |
| `dry_run` | `DryRun` | If true, simulate without side effects. |

## Examples

### CLI

```powershell
pwsh -File actions/Invoke-OSAction.ps1 -ActionName setup-lunit -ArgsJson '{
  "LVVersion": "2025",
  "LVBitness": "64"
}'
```

### GitHub Action

```yaml
- name: Setup LUnit for G-CLI
  uses: ni/open-source/setup-lunit@v1
  with:
    lv_version: '2025'
    lv_bitness: '64'
    vipm_installer_url: 'https://packages.jki.net/vipm/preview/vipm-setup-latest-preview.exe'
```

## Return Codes

- `0` – VIPM and LUnit installed successfully
- non‑zero – installation failed

## Notes

- **Windows only**: This action requires Windows runners
- **VIPM detection**: If VIPM is already installed at `C:\Program Files\JKI\VI Package Manager\support\vipm.exe`, installation is skipped
- **Silent installation**: VIPM is installed with `/quiet` and `/norestart` flags
- **Package refresh**: The VIPM package repository is automatically refreshed before installing LUnit
- **LUnit package**: Installs `sas_workshops_lib_lunit_for_g_cli` from the JKI package repository
- **Version-specific**: LUnit is installed for the specific LabVIEW version and bitness combination
- **G-CLI requirement**: This action is designed for use with G-CLI-based testing workflows

## VIPM Installation Location

VIPM is installed to:
```plain text
C:\Program Files\JKI\VI Package Manager\
```

The `vipm.exe` command-line tool is located at:
```plain text
C:\Program Files\JKI\VI Package Manager\support\vipm.exe
```

## Troubleshooting

- **VIPM not found after installation**: Verify the installer completed successfully and check the installation path
- **Package installation fails**: Ensure the LabVIEW version and bitness match an installed LabVIEW instance
- **Network issues**: Package download requires internet access to JKI package servers
- **Permission denied**: Ensure the runner has write access to `C:\Program Files\JKI\`
- **LUnit not available**: Verify the package name `sas_workshops_lib_lunit_for_g_cli` is correct and available in the VIPM repository

## See also

- [Setup LabVIEW](setup-labview.md)
- [Configure LabVIEW](configure-labview.md)
- [Run Unit Tests](run-unit-tests.md)
- [VI Package Manager Documentation](https://www.vipm.io/documentation/)
- [LUnit for G-CLI Package](https://www.vipm.io/package/sas_workshops_lib_lunit_for_g_cli/)
