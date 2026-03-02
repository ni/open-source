# setup-nipm

## Purpose

Installs and configures NI Package Manager (NIPM) for LabVIEW package management in CI/CD environments. This action downloads the NIPM installer, installs it silently, adds it to the system PATH, and configures it by disabling package caching for optimal CI performance.

## Parameters

Common parameters are described in [Common parameters](../common-parameters.md).

### Required

None. All parameters have defaults.

### Optional

- **NIPMUrl** (`string`): URL to download the NI Package Manager installer. Default: `https://download.ni.com/support/nipkg/products/ni-package-manager/installers/NIPackageManager25.8.0.exe`

### GitHub Action inputs

| Input | CLI parameter | Description |
| --- | --- | --- |
| `nipm_url` | `NIPMUrl` | URL to download the NI Package Manager installer. |
| `working_directory` | `WorkingDirectory` | Base directory for the action. |
| `log_level` | `LogLevel` | Verbosity level (ERROR\|WARN\|INFO\|DEBUG). |
| `dry_run` | `DryRun` | If true, simulate without side effects. |

## Examples

### CLI

```powershell
pwsh -File actions/Invoke-OSAction.ps1 -ActionName setup-nipm -ArgsJson '{}'
```

With custom NIPM version:

```powershell
pwsh -File actions/Invoke-OSAction.ps1 -ActionName setup-nipm -ArgsJson '{
  "NIPMUrl": "https://download.ni.com/support/nipkg/products/ni-package-manager/installers/NIPackageManager24.5.0.exe"
}'
```

### GitHub Action

```yaml
- name: Setup NI Package Manager
  uses: ni/open-source/setup-nipm@v1
```

With custom version:

```yaml
- name: Setup NI Package Manager
  uses: ni/open-source/setup-nipm@v1
  with:
    nipm_url: 'https://download.ni.com/support/nipkg/products/ni-package-manager/installers/NIPackageManager24.5.0.exe'
```

## Return Codes

- `0` – NIPM installed and configured successfully
- non‑zero – installation or configuration failed

## Notes

- This action requires Windows runners
- The action installs NIPM with `--quiet`, `--accept-eulas`, and `--prevent-reboot` flags
- Package caching is disabled via `nipkg.exe set-config nipkg.cachepackages=false`
- NIPM is added to both the machine PATH and the current session PATH

## See also

- [NI Package Manager Documentation](https://www.ni.com/en-us/support/downloads/software-products/download.ni-package-manager.html)
