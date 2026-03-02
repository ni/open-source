# setup-labview

## Purpose

Downloads and installs LabVIEW Community Edition from an ISO image for CI/CD environments. This action downloads the LabVIEW ISO installer, mounts it, runs the installation in passive mode, monitors the installation process with configurable timeout, and handles cleanup of hung processes and temporary files.

## Parameters

Common parameters are described in [Common parameters](../common-parameters.md).

### Required

None. All parameters have defaults.

### Optional

- **LabVIEWIsoUrl** (`string`): URL to download the LabVIEW ISO installer. Default: `https://download.ni.com/support/softlib/labview/labview_development_system/2025_Q3/ni-labview-2025-community-x86_25.3.3_offline.iso`
- **TimeoutSeconds** (`int`): Maximum time in seconds to wait for installation to complete before terminating hung processes. Default: `2700` (45 minutes)

### GitHub Action inputs

| Input | CLI parameter | Description |
| --- | --- | --- |
| `labview_iso_url` | `LabVIEWIsoUrl` | URL to download the LabVIEW ISO installer. |
| `timeout_seconds` | `TimeoutSeconds` | Maximum installation time in seconds. |
| `working_directory` | `WorkingDirectory` | Base directory for the action. |
| `log_level` | `LogLevel` | Verbosity level (ERROR\|WARN\|INFO\|DEBUG). |
| `dry_run` | `DryRun` | If true, simulate without side effects. |

## Examples

### CLI

```powershell
pwsh -File actions/Invoke-OSAction.ps1 -ActionName setup-labview -ArgsJson '{}'
```

With custom ISO and timeout:

```powershell
pwsh -File actions/Invoke-OSAction.ps1 -ActionName setup-labview -ArgsJson '{
  "LabVIEWIsoUrl": "https://download.ni.com/support/softlib/labview/labview_development_system/2025_Q3/ni-labview-2025-community-x86_25.3.3_offline.iso",
  "TimeoutSeconds": 3600
}'
```

### GitHub Action

```yaml
- name: Setup LabVIEW
  uses: ni/open-source/setup-labview@v1
```

With custom version and timeout:

```yaml
- name: Setup LabVIEW 2024
  uses: ni/open-source/setup-labview@v1
  with:
    labview_iso_url: 'https://download.ni.com/support/softlib/labview/labview_development_system/2025_Q3/ni-labview-2025-community-x86_25.3.3_offline.iso'
    timeout_seconds: 3600
```

## Return Codes

- `0` – LabVIEW installed successfully
- non‑zero – installation failed

## Notes

- **Windows only**: This action requires Windows runners with administrative privileges
- **ISO mounting**: Uses Windows `Mount-DiskImage` cmdlet
- **Installation mode**: Runs with `--passive`, `--accept-eulas`, and `--prevent-reboot` flags
- **Timeout handling**: If installation exceeds the timeout, hung processes are forcefully terminated
- **Automatic cleanup**: ISO is unmounted and temporary files are removed after installation
- **Progress monitoring**: Installation progress is checked every 60 seconds
- **Large download**: LabVIEW ISOs are typically large; ensure adequate network bandwidth and disk space

## Troubleshooting

- **Timeout too short**: Increase `timeout_seconds` if installation consistently times out
- **Download failures**: Verify the ISO URL is accessible and correct
- **Disk space**: Ensure adequate free space in `%TEMP%` for ISO and installation
- **Hung processes**: The action automatically kills hung processes after timeout; check logs for warnings

## See also

- [LabVIEW Community Edition](https://www.ni.com/en-us/support/downloads/software-products/download.labview.html)
- [NI Package Manager setup](setup-nipm.md)
