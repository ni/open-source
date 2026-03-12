# via-lv-docker workflow

## Purpose

Dispatch the [via-lv-docker](../actions/via-lv-docker.md) action to a target repository through `Invoke-OSAction.ps1`.

## Parameters

### Inputs

| Parameter | Description |
| --- | --- |
| `repository` | Repository in `owner/repo` format to operate on. |
| `ref` | Branch or tag to check out. Defaults to `main`. |

### Secrets

| Secret | Description |
| --- | --- |
| `REPO_TOKEN` | Personal access token with permission to read the target repository. |

## Examples

```yaml
name: via-lv-docker
on:
  workflow_dispatch:
    inputs:
      repository:
        description: 'owner/repo of the repository to target'
        required: true
      ref:
        description: 'Branch or tag to check out'
        required: false
        default: 'main'
jobs:
  via-lv-docker:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - name: Checkout target repository
        uses: actions/checkout@v4
        with:
          repository: ${{ inputs.repository }}
          ref: ${{ inputs.ref }}
          path: target
          token: ${{ secrets.REPO_TOKEN }}
      - name: Run VI Analyzer
        shell: pwsh
        run: |
          ./actions/Invoke-OSAction.ps1 -ActionName via-lv-docker -WorkingDirectory "${{ github.workspace }}/target" -ArgsJson '{
            "ConfigPath": "",
            "BaseBranch": "origin/develop",
            "LabviewVersion": "2026q1-linux",
            "DockerImage": "nationalinstruments/labview"
          }'
      - name: Upload VI Analyzer Report
        uses: actions/upload-artifact@v4
        if: always()
        with:
          name: vi-analyzer-report
          path: target/vi-analyzer-report.htm
```
After the workflow completes, the VI Analyzer HTML report will be available as an artifact.

## Return Codes

- N/A

## See also

- [Action documentation](../actions/via-lv-docker.md)
