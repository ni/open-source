# activate-labview

## Purpose

Activates a LabVIEW license using the NI License Manager utility (`nilmUtil.exe`). This action is typically used in CI/CD pipelines after installing LabVIEW to enable full functionality with a valid license.

## Parameters

Common parameters are described in [Common parameters](../common-parameters.md).

### Required

- **SerialNumber** (`string`): LabVIEW serial number for activation. This should be stored as a secret in your repository.

### Optional

- **PackageID** (`string`): LabVIEW package ID to activate. Default: `LabVIEW_COM_PKG 25.0300` (for LabVIEW 2025)

### GitHub Action inputs

| Input | CLI parameter | Description |
| --- | --- | --- |
| `serial_number` | `SerialNumber` | LabVIEW serial number (store as secret). |
| `package_id` | `PackageID` | Package ID to activate (e.g., "LabVIEW_COM_PKG 25.0300"). |
| `working_directory` | `WorkingDirectory` | Base directory for the action. |
| `log_level` | `LogLevel` | Verbosity level (ERROR\|WARN\|INFO\|DEBUG). |
| `dry_run` | `DryRun` | If true, simulate without side effects. |

## Examples

### CLI

```powershell
pwsh -File actions/Invoke-OSAction.ps1 -ActionName activate-labview -ArgsJson '{
  "SerialNumber": "ABCD-1234-5678-90EF"
}'
```

With custom package ID:

```powershell
pwsh -File actions/Invoke-OSAction.ps1 -ActionName activate-labview -ArgsJson '{
  "SerialNumber": "ABCD-1234-5678-90EF",
  "PackageID": "LabVIEW_COM_PKG 25.0300"
}'
```

### GitHub Action

**Important**: Store your serial number as a repository secret!

```yaml
- name: Activate LabVIEW
  uses: owner/repo/activate-labview@v1
  with:
    serial_number: ${{ secrets.LABVIEW_SERIAL_NUMBER }}
```

With custom package ID for LabVIEW 2025:

```yaml
- name: Activate LabVIEW 2025
  uses: owner/repo/activate-labview@v1
  with:
    serial_number: ${{ secrets.LABVIEW_SERIAL_NUMBER }}
    package_id: 'LabVIEW_COM_PKG 25.0300'
```

## Return Codes

- `0` – activation successful
- non‑zero – activation failed

## Notes

- **Windows only**: This action requires Windows runners
- **NI License Manager required**: The action expects `nilmUtil.exe` at `C:\Program Files (x86)\National Instruments\Shared\License Manager\bin\`
- **License Manager installation**: Typically installed with LabVIEW or can be installed separately
- **Serial number security**: Always store serial numbers as GitHub secrets, never in plain text
- **Activation modes**: Uses silent mode (`-s`) for non-interactive activation
- **Network activation**: Requires internet connectivity to contact NI license servers

## Security Best Practices

### Storing Serial Numbers as Secrets

1. Go to your repository **Settings** → **Secrets and variables** → **Actions**
2. Click **New repository secret**
3. Name: `LABVIEW_SERIAL_NUMBER`
4. Value: Your LabVIEW serial number
5. Click **Add secret**

### Using in Workflows

```yaml
- name: Activate LabVIEW
  uses: owner/repo/activate-labview@v1
  with:
    serial_number: ${{ secrets.LABVIEW_SERIAL_NUMBER }}
```

**Never** commit serial numbers directly in workflow files!

## Troubleshooting

- **nilmUtil.exe not found**: Ensure NI License Manager is installed before running this action
- **Activation failed**: Verify serial number is correct and valid for the package ID
- **Network issues**: Activation requires internet access to NI license servers
- **Wrong package ID**: Ensure package ID matches your LabVIEW version

## See also

- [Setup LabVIEW](setup-labview.md)
- [NI License Manager Documentation](https://www.ni.com/en-us/support/documentation/supplemental/06/ni-license-manager.html)
