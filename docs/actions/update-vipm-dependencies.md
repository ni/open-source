# update-vipm-dependencies

## Purpose

Bump every dependency in a VIPM manifest to its latest available version using `vipm add`, then regenerate the lock file. Optionally bumps `[dev-dependencies]` alongside `[dependencies]`.

## Parameters

Common parameters are described in [Common parameters](../common-parameters.md).

### Required

- **WorkingDirectory** (`string`): Path (relative to the repo root) containing the manifest.
- **LabVIEWVersion** (`string`): LabVIEW version (YYYY) matching the `[project]` section of the manifest.
- **LabVIEWBitness** (`string`): "32" or "64" bitness matching the `[project]` section of the manifest.

### Optional

- **ManifestFilename** (`string`, default `vipm.toml`): Manifest filename, relative to `WorkingDirectory`. Override to support alternate names such as `runner_dependencies.toml`.
- **LockFilename** (`string`, default `vipm.lock`): Lock filename, relative to `WorkingDirectory`. Pairs with `ManifestFilename`, e.g. `runner_dependencies.lock`.
- **IncludeDevDependencies** (`switch`): Also bump packages declared under `[dev-dependencies]` in addition to `[dependencies]`.
- **VipmSerialNumber** (`string`): VIPM Pro serial number. `vipm add` requires Community or Professional edition.
- **VipmFullName** (`string`): Name used for VIPM Pro activation.
- **VipmEmail** (`string`): Email used for VIPM Pro activation.
- **VipmDebUrl** (`string`, default `https://traffic.libsyn.com/secure/jkinc/vipm_26.3.1-4025_amd64.deb`): Override URL for the VIPM Linux `.deb` package.
- **DockerImage** (`string`, default `nationalinstruments/labview`): Docker image name.
- **ImageTag** (`string`, default `latest-linux`): Docker image tag.

The VIPM CLI itself only recognizes `vipm.toml`/`vipm.lock` in the working directory. When `ManifestFilename`/`LockFilename` differ from the defaults, the action stages the file under the expected name for the duration of the `vipm` calls and restores the original name afterward — no extra steps are needed beyond setting the inputs.

If any package fails to resolve or update, the action fails the job and does **not** generate a partial lock file or open a pull request with an incomplete update.

### GitHub Action inputs

GitHub Action inputs are provided in `snake_case`, while CLI parameters use `PascalCase`. The table below maps each input to its corresponding CLI parameter. For details on shared CLI flags, see [Common parameters](../common-parameters.md).

| Input | CLI parameter | Description |
| --- | --- | --- |
| `working_directory` | `WorkingDirectory` | Path (relative to the caller repo root) containing the manifest. |
| `labview_version` | `LabVIEWVersion` | LabVIEW version (YYYY) matching the `[project]` section. |
| `labview_bitness` | `LabVIEWBitness` | "32" or "64" bitness matching the `[project]` section. |
| `manifest_filename` | `ManifestFilename` | Manifest filename (default `vipm.toml`). |
| `lock_filename` | `LockFilename` | Lock filename (default `vipm.lock`). |
| `include_dev_dependencies` | `IncludeDevDependencies` | If true, also bumps `[dev-dependencies]` (default `true`). |
| `vipm_serial_number` | `VipmSerialNumber` | VIPM Pro serial number (pass from a secret in the caller). |
| `vipm_full_name` | `VipmFullName` | Name used for VIPM Pro activation. |
| `vipm_email` | `VipmEmail` | Email used for VIPM Pro activation. |
| `vipm_deb_url` | `VipmDebUrl` | Override URL for the VIPM Linux `.deb` package. |
| `docker_image` | `DockerImage` | Docker image name. |
| `image_tag` | `ImageTag` | Docker image tag. |
| `log_level` | `LogLevel` | Verbosity level (ERROR\|WARN\|INFO\|DEBUG). |
| `dry_run` | `DryRun` | If true, simulate the action without side effects. |

## Expected PR behavior (reusable workflow)

When called via `.github/workflows/reusable-vipm-update.yml`, a pull request is opened in the **caller repository** with the updated manifest and lock file. The branch and PR are always created in the caller's repository, never in `ni/open-source`.

- If `vipm add` resolves no version changes, no branch or PR is created (the check is diff-based, scoped to the manifest/lock paths only).
- The default update branch (`automation/vipm-dependency-update`) is reused and updated on subsequent runs rather than creating duplicates. Set `pr_branch` explicitly if you run this action against multiple manifests in the same repository, so concurrent runs don't collide on the same branch name.

## Examples

### CLI

```powershell
pwsh -File actions/Invoke-OSAction.ps1 -ActionName update-vipm-dependencies -ArgsJson '{
  "WorkingDirectory": "scripts/apply-vipc",
  "LabVIEWVersion": "2021",
  "LabVIEWBitness": "64",
  "IncludeDevDependencies": true
}'
```

### GitHub Action

```yaml
- name: Update VIPM dependencies
  uses: ni/open-source/update-vipm-dependencies@actions
  with:
    working_directory: 'scripts/apply-vipc'
    labview_version: '2021'
    labview_bitness: '64'
    include_dev_dependencies: true
```

### Reusable workflow

```yaml
jobs:
  update-deps:
    uses: ni/open-source/.github/workflows/reusable-vipm-update.yml@actions
    with:
      working_directory: .
      labview_version: '2021'
      labview_bitness: '64'
      base_branch: main
      manifest_filename: runner_dependencies.toml
      lock_filename: runner_dependencies.lock
      include_dev_dependencies: true
    secrets:
      VIPM_SERIAL_NUMBER: ${{ secrets.VIPM_SERIAL_NUMBER }}
      VIPM_FULL_NAME: ${{ secrets.VIPM_FULL_NAME }}
      VIPM_EMAIL: ${{ secrets.VIPM_EMAIL }}
```

## Return Codes

- `0` – all dependencies updated and lock regenerated successfully
- non‑zero – VIPM Pro activation failed, or one or more packages failed to resolve/update

For troubleshooting tips, see the [troubleshooting guide](../troubleshooting.md).
