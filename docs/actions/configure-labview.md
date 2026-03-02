# configure-labview

## Purpose

Configures LabVIEW settings by updating the `LabVIEW.ini` file. This action is typically used in CI/CD pipelines to enable TCP/IP server access and VI scripting operations for automated testing and remote control scenarios. If the ini file does not exist, LabVIEW is automatically launched to generate it.

## Parameters

Common parameters are described in [Common parameters](../common-parameters.md).

### Required

None. All parameters have defaults.

### Optional

- **LabVIEWVersion** (`string`): LabVIEW version to configure (e.g., "2025", "2024"). Default: `2025`
- **IniSettings** (`string`): INI settings to add to LabVIEW.ini. Can be multiline string or comma-separated. Default includes TCP server and scripting settings.
- **LabVIEWWaitSeconds** (`int`): Seconds to wait for LabVIEW to generate the ini file if it does not exist. Default: `60`

### GitHub Action inputs

| Input | CLI parameter | Description |
| --- | --- | --- |
| `labview_version` | `LabVIEWVersion` | LabVIEW version (e.g., "2025", "2024"). |
| `ini_settings` | `IniSettings` | INI settings to add (one per line). |
| `labview_wait_seconds` | `LabVIEWWaitSeconds` | Wait time for ini file generation. |
| `working_directory` | `WorkingDirectory` | Base directory for the action. |
| `log_level` | `LogLevel` | Verbosity level (ERROR\|WARN\|INFO\|DEBUG). |
| `dry_run` | `DryRun` | If true, simulate without side effects. |

## Default Settings

The action applies these settings by default:

```ini
server.tcp.enabled=TRUE
server.tcp.access=+127.0.0.1;+localhost;+*
server.viscripting.ShowScriptingOperationsInEditor=TRUE
```

## Examples

### CLI

```powershell
pwsh -File actions/Invoke-OSAction.ps1 -ActionName configure-labview -ArgsJson '{}'
```

With custom version and settings:

```powershell
pwsh -File actions/Invoke-OSAction.ps1 -ActionName configure-labview -ArgsJson '{
  "LabVIEWVersion": "2024",
  "IniSettings": "server.tcp.enabled=TRUE\nserver.tcp.port=3363"
}'
```

### GitHub Action

```yaml
- name: Configure LabVIEW
  uses: ni/open-source/configure-labview@v1
```

With custom settings:

```yaml
- name: Configure LabVIEW for Remote Access
  uses: ni/open-source/configure-labview@v1
  with:
    labview_version: '2025'
    ini_settings: |
      server.tcp.enabled=TRUE
      server.tcp.access=+*
      server.tcp.port=3363
      server.viscripting.ShowScriptingOperationsInEditor=TRUE
    labview_wait_seconds: 60
```

For LabVIEW 2024:

```yaml
- name: Configure LabVIEW 2024
  uses: ni/open-source/configure-labview@v1
  with:
    labview_version: '2024'
```

## Common INI Settings

### TCP/IP Server

```ini
server.tcp.enabled=TRUE
server.tcp.port=3363
server.tcp.access=+127.0.0.1;+localhost;+*
```

### VI Scripting

```ini
server.viscripting.ShowScriptingOperationsInEditor=TRUE
```

### Disable Splash Screen

```ini
NoSplashScreen=TRUE
```

### Error Handling

```ini
ExternalNodesEnabled=TRUE
ThreadedDIAGLoad=1
```

## Return Codes

- `0` – configuration successful
- non‑zero – configuration failed

## Notes

- **Windows only**: This action requires Windows runners
- **LabVIEW must be installed**: The action expects LabVIEW at `C:\Program Files (x86)\National Instruments\LabVIEW {version}\`
- **Automatic ini generation**: If `LabVIEW.ini` does not exist, LabVIEW is launched to create it
- **Duplicate prevention**: Settings already present in the ini file are not added again
- **ASCII encoding**: The ini file is updated using ASCII encoding to maintain compatibility
- **No overwrites**: Existing settings are preserved; only new settings are appended

## INI File Location

The action updates the ini file at:

```plain text
C:\Program Files (x86)\National Instruments\LabVIEW {version}\LabVIEW.ini
```

## Workflow Example

Complete setup workflow with NIPM, LabVIEW installation, activation, and configuration:

```yaml
jobs:
  setup-labview-env:
    runs-on: windows-latest
    steps:
      - uses: actions/checkout@v4
      
      - name: Setup NIPM
        uses: ni/open-source/setup-nipm@v1
      
      - name: Setup LabVIEW
        uses: ni/open-source/setup-labview@v1
        with:
          timeout_seconds: 3600
      
      - name: Configure LabVIEW
        uses: ni/open-source/configure-labview@v1
        with:
          ini_settings: |
            server.tcp.enabled=TRUE
            server.tcp.access=+*
            server.viscripting.ShowScriptingOperationsInEditor=TRUE
```

## Troubleshooting

- **LabVIEW.exe not found**: Verify LabVIEW is installed at the expected path
- **INI file not created**: Increase `labview_wait_seconds` if LabVIEW takes longer to initialize
- **Permission denied**: Ensure the runner has write access to the LabVIEW installation directory
- **Settings not applied**: Check the logs to see if settings were marked as "Already exists"

## See also

- [Setup LabVIEW](setup-labview.md)
- [LabVIEW Configuration File Documentation](https://www.ni.com/docs/en-US/bundle/labview/page/lvhowto/configuring_the_labview_environment.html)
