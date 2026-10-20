# build-vip-docker-linux

## Purpose

Build a VI Package (`.vip`) from a `.vipb` with the VIPM CLI inside the `nationalinstruments/labview:<version>-linux` container. The action contains only the steps shared by every project: container launch, Xvfb, VIPM install and Pro activation, VIPB preparation, `vipm build`, package discovery, secret redaction and diagnostics. Project-specific preparation runs in an optional `pre_build_script` hook inside the same container.

## Build sequence

1. Validate inputs and pull the LabVIEW Linux image.
2. Start the container with the workspace mounted at `/workspace`.
3. Install base APT packages and start Xvfb on `:99`.
4. Download (optionally verify SHA256), install and activate VIPM.
5. Run `pre_build_script`, if provided.
6. Write a build copy of the VIPB to `log_directory` with the source and output folders set, clearing a referenced `.vipc` that does not exist.
7. Run `vipm build --labview-version <year> --labview-bitness 64`.
8. Fail if no `.vip` was produced; set outputs.

## Pre-build hook

The hook runs as root inside the container after VIPM activation and before VIPB preparation. A non-zero exit fails the build, and its output is saved to `pre-build.log`. These environment variables are available:

| Variable | Example |
| --- | --- |
| `WORKSPACE` | `/workspace` |
| `LV_ROOT` | `/usr/local/natinst/LabVIEW-2026-64` |
| `LV_YEAR` | `2026` |
| `LV_NUMERIC_VERSION` | `26.1` |
| `SOURCE_DIR` | Resolved `source_directory` |
| `OUTPUT_DIR` | Resolved `output_directory` |
| `LOG_DIR` | Resolved `log_directory` |

Typical uses: moving built-in LabVIEW copies out of `vi.lib`, staging sources or PPLs, `vipm install` of dependencies, mass compiling, or writing a modified VIPB that is then passed as `vipb_path`.

## Parameters

Common parameters are described in [Common parameters](../common-parameters.md).

### GitHub Action inputs

| Input | CLI parameter | Default | Description |
| --- | --- | --- | --- |
| `vipb_path` | `VipbPath` | | Path to the .vipb file, relative to the workspace. May be a file created by pre_build_script. |
| `labview_version` | `LabVIEWVersion` | 2026q1 | LabVIEW container release (YYYYq1 or YYYYq3). |
| `docker_image` | `DockerImage` | nationalinstruments/labview | Docker image name. |
| `image_tag` | `ImageTag` | | Docker image tag. Defaults to "{labview_version}-linux". |
| `source_directory` | `SourceDirectory` | . | VIPB source folder. Relative to the workspace, or an absolute container path. |
| `output_directory` | `OutputDirectory` | builds/vip | Directory, relative to the workspace, that receives the built .vip files. |
| `log_directory` | `LogDirectory` | builds/logs/vip-linux | Directory, relative to the workspace, that receives build logs and the prepared VIPB. |
| `pre_build_script` | `PreBuildScript` | | Optional bash script, relative to the workspace, run inside the container after VIPM activation and before vipm build. |
| `vipm_serial_number` | `VipmSerialNumber` | | VIPM Pro serial number (pass from a secret in the caller). |
| `vipm_full_name` | `VipmFullName` | | Name used for VIPM Pro activation. |
| `vipm_email` | `VipmEmail` | | Email used for VIPM Pro activation. |
| `vipm_deb_url` | `VipmDebUrl` | https://traffic.libsyn.com/secure/jkinc/vipm_26.3.1-4025_amd64.deb | Override URL for the VIPM Linux .deb package. |
| `vipm_deb_sha256` | `VipmDebSha256` | | Expected SHA256 of the VIPM .deb package. Verification is skipped when empty. |
| `build_timeout_seconds` | `BuildTimeoutSeconds` | 900 | Timeout in seconds passed to vipm build. |
| `desktop_liveliness_timeout` | `DesktopLivelinessTimeout` | 300 | VIPM desktop liveliness timeout in seconds. |
| `working_directory` | `WorkingDirectory` | | Working directory mounted as the container workspace. |
| `log_level` | `LogLevel` | INFO | Verbosity level (ERROR\|WARN\|INFO\|DEBUG). |
| `dry_run` | `DryRun` | false | If true, simulate the action without side effects. |

### Outputs

| Output | Description |
| --- | --- |
| `vip_path` | Absolute path of the first built `.vip` file. |
| `vip_paths` | JSON array of absolute paths of all built `.vip` files. |
| `package_version` | Package version read from the VIPB. |
| `output_directory` | Absolute path of the directory containing the built `.vip` files. |
| `log_directory` | Absolute path of the build log directory. |

## Examples

### CLI

```powershell
pwsh -File actions/Invoke-OSAction.ps1 -ActionName build-vip-docker-linux -ArgsJson '{
  "VipbPath": "Tooling/deployment/My Package.vipb",
  "LabVIEWVersion": "2026q1",
  "VipmSerialNumber": "<serial>",
  "VipmFullName": "<name>",
  "VipmEmail": "<email>"
}'
```

### GitHub Action

```yaml
- uses: ni/open-source/build-vip-docker-linux@actions
  id: vip
  with:
    vipb_path: Tooling/deployment/My Package.vipb
    labview_version: '2026q1'
    vipm_serial_number: ${{ secrets.VIPM_SERIAL_NUMBER }}
    vipm_full_name: ${{ secrets.VIPM_FULL_NAME }}
    vipm_email: ${{ secrets.VIPM_EMAIL }}
- uses: actions/upload-artifact@v4
  with:
    name: vip-${{ steps.vip.outputs.package_version }}
    path: ${{ steps.vip.outputs.output_directory }}/*.vip
```

### Reusable workflow

`.github/workflows/reusable-build-vip-linux.yml` checks out the caller, optionally downloads an artifact from the same run, runs this action, and uploads the VIP (7 days) and logs (14 days).

Actor Framework (distribution built by an earlier job; hook isolates bundled copies, stages source, installs dependencies and mass compiles):

```yaml
jobs:
  create-distribution:
    uses: ./.github/workflows/create-distribution-artifact.yml
    with:
      upload-artifact: true
  build-vip:
    needs: create-distribution
    uses: ni/open-source/.github/workflows/reusable-build-vip-linux.yml@actions
    with:
      labview_version: '2026q1'
      download_artifact_name: actor-framework-distribution
      download_path: .af-distribution
      pre_build_script: pipeline/scripts/Prepare-AF-Linux-Vip.sh
      vipb_path: builds/logs/vip-linux/Actor Framework.linux.vipb
      source_directory: /tmp/af-source/Core
      desktop_liveliness_timeout: '900'
    secrets:
      VIPM_SERIAL_NUMBER: ${{ secrets.VIPM_SERIAL_NUMBER }}
      VIPM_FULL_NAME: ${{ secrets.VIPM_FULL_NAME }}
      VIPM_EMAIL: ${{ secrets.VIPM_EMAIL }}
```

Icon Editor (PPL built by an earlier job; hook stages the x64 PPL and writes a 64-bit VIPB):

```yaml
jobs:
  build-ppl:
    uses: ./.github/workflows/build-lvlibp-linux-container.yml
  build-vip:
    needs: build-ppl
    uses: ni/open-source/.github/workflows/reusable-build-vip-linux.yml@actions
    with:
      labview_version: ${{ needs.build-ppl.outputs.labview_version }}
      download_artifact_pattern: lv_icon_x64_v*_${{ needs.build-ppl.outputs.labview_version }}-linux
      download_path: builds/ppl-artifact
      pre_build_script: Tooling/Prepare-Linux-Vip.sh
      vipb_path: Tooling/deployment/NI Icon editor.linux.vipb
      output_directory: builds/VI Package
    secrets:
      VIPM_SERIAL_NUMBER: ${{ secrets.VIPM_SERIAL_NUMBER }}
      VIPM_FULL_NAME: ${{ secrets.VIPM_FULL_NAME }}
      VIPM_EMAIL: ${{ secrets.VIPM_EMAIL }}
```

## Return Codes

- `0` - success
- non-zero - invalid input, image pull failure, VIPM install or activation failure, pre-build script failure, `vipm build` failure, or no `.vip` produced

For troubleshooting tips, see the [troubleshooting guide](../troubleshooting.md).
