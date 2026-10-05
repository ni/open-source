# refresh-vipm-lock

## Purpose

Generate or verify a VIPM lock file from a VIPM manifest using the VIPM CLI inside a Linux Docker container. Supports bootstrapping the initial lock file for a repository that has a manifest but no lock yet.

## Parameters

Common parameters are described in [Common parameters](../common-parameters.md).

### Required

- **WorkingDirectory** (`string`): Path (relative to the repo root) containing the manifest.
- **LabVIEWVersion** (`string`): LabVIEW version (YYYY) matching the `[project]` section of the manifest.
- **LabVIEWBitness** (`string`): "32" or "64" bitness matching the `[project]` section of the manifest.

### Optional

- **CheckOnly** (`switch`): If set, verifies an existing lock with `vipm lock --check`, or generates the initial lock when absent. If not set (default), always regenerates the lock with `vipm lock`.
- **ManifestFilename** (`string`, default `vipm.toml`): Manifest filename, relative to `WorkingDirectory`. Override to support alternate names such as `runner_dependencies.toml`.
- **LockFilename** (`string`, default `vipm.lock`): Lock filename, relative to `WorkingDirectory`. Pairs with `ManifestFilename`, e.g. `runner_dependencies.lock`.
- **VipmSerialNumber** (`string`): VIPM Pro serial number. Omit to skip activation (Free edition).
- **VipmFullName** (`string`): Name used for VIPM Pro activation.
- **VipmEmail** (`string`): Email used for VIPM Pro activation.
- **VipmDebUrl** (`string`, default `https://traffic.libsyn.com/secure/jkinc/vipm_26.3.1-4025_amd64.deb`): Override URL for the VIPM Linux `.deb` package.
- **DockerImage** (`string`, default `nationalinstruments/labview`): Docker image name.
- **ImageTag** (`string`, default `latest-linux`): Docker image tag.

The VIPM CLI itself only recognizes `vipm.toml`/`vipm.lock` in the working directory. When `ManifestFilename`/`LockFilename` differ from the defaults, the action stages the file under the expected name for the duration of the `vipm` calls and restores the original name afterward — no extra steps are needed beyond setting the inputs.

### GitHub Action inputs

GitHub Action inputs are provided in `snake_case`, while CLI parameters use `PascalCase`. The table below maps each input to its corresponding CLI parameter. For details on shared CLI flags, see [Common parameters](../common-parameters.md).

| Input | CLI parameter | Description |
| --- | --- | --- |
| `working_directory` | `WorkingDirectory` | Path (relative to the caller repo root) containing the manifest. |
| `labview_version` | `LabVIEWVersion` | LabVIEW version (YYYY) matching the `[project]` section. |
| `labview_bitness` | `LabVIEWBitness` | "32" or "64" bitness matching the `[project]` section. |
| `check_only` | `CheckOnly` | If true, checks an existing lock or generates the initial lock when absent. |
| `manifest_filename` | `ManifestFilename` | Manifest filename (default `vipm.toml`). |
| `lock_filename` | `LockFilename` | Lock filename (default `vipm.lock`). |
| `vipm_serial_number` | `VipmSerialNumber` | VIPM Pro serial number (pass from a secret in the caller). |
| `vipm_full_name` | `VipmFullName` | Name used for VIPM Pro activation. |
| `vipm_email` | `VipmEmail` | Email used for VIPM Pro activation. |
| `vipm_deb_url` | `VipmDebUrl` | Override URL for the VIPM Linux `.deb` package. |
| `docker_image` | `DockerImage` | Docker image name. |
| `image_tag` | `ImageTag` | Docker image tag. |
| `log_level` | `LogLevel` | Verbosity level (ERROR\|WARN\|INFO\|DEBUG). |
| `dry_run` | `DryRun` | If true, simulate the action without side effects. |

## Onboarding a new repository (no existing lock)

1. Author a manifest (`vipm.toml`, or a custom name via `manifest_filename`) by hand: `[project]`, `[dependencies]`, optionally `[dev-dependencies]`.
2. Call this action, or the `reusable-vipm-lock.yml` reusable workflow, with `check_only: false` (the default). Since no lock exists yet, `vipm lock` generates it.
3. Once merged, switch subsequent runs to `check_only: true` to use this as a verification gate instead of regenerating/opening a PR on every run.

## Expected PR behavior (reusable workflow)

When called via `.github/workflows/reusable-vipm-lock.yml`:

- `check_only: false` (default): opens a pull request in the **caller repository** containing the generated/updated lock file. If there is no diff, no branch or PR is created.
- `check_only: true`: no PR is created — the resulting lock file is uploaded as a downloadable workflow artifact for inspection only.

The branch and PR are always created in the repository that calls the reusable workflow, never in `ni/open-source`.

## Examples

### CLI

```powershell
pwsh -File actions/Invoke-OSAction.ps1 -ActionName refresh-vipm-lock -ArgsJson '{
  "WorkingDirectory": "scripts/apply-vipc",
  "LabVIEWVersion": "2021",
  "LabVIEWBitness": "64",
  "ManifestFilename": "runner_dependencies.toml",
  "LockFilename": "runner_dependencies.lock"
}'
```

### GitHub Action

```yaml
- name: Refresh VIPM lock
  uses: ni/open-source/refresh-vipm-lock@actions
  with:
    working_directory: 'scripts/apply-vipc'
    labview_version: '2021'
    labview_bitness: '64'
    manifest_filename: 'runner_dependencies.toml'
    lock_filename: 'runner_dependencies.lock'
```

### Reusable workflow

```yaml
jobs:
  refresh-lock:
    uses: ni/open-source/.github/workflows/reusable-vipm-lock.yml@actions
    with:
      working_directory: .
      labview_version: '2021'
      labview_bitness: '64'
      base_branch: main
      manifest_filename: runner_dependencies.toml
      lock_filename: runner_dependencies.lock
    secrets:
      VIPM_SERIAL_NUMBER: ${{ secrets.VIPM_SERIAL_NUMBER }}
      VIPM_FULL_NAME: ${{ secrets.VIPM_FULL_NAME }}
      VIPM_EMAIL: ${{ secrets.VIPM_EMAIL }}
```

## Return Codes

- `0` – lock generated or verified successfully
- non‑zero – VIPM Pro activation failed, or `vipm lock`/`vipm lock --check` failed

For troubleshooting tips, see the [troubleshooting guide](../troubleshooting.md).
