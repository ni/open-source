# LabVIEW VI Analyzer GitHub Action

A GitHub Action that runs LabVIEW VI Analyzer tests in a Docker container and parses the results. Supports both static configuration files and dynamic config generation based on changed files in pull requests.

## Features

- **Containerized Testing**: Runs VI Analyzer in official LabVIEW Docker containers
- **Dynamic Config Generation**: Automatically analyzes only changed `.vi`, `.ctl`, and `.llb` files in PRs
- **Static Configuration**: Use pre-defined VI Analyzer configs for manual runs or specific test suites
- **Detailed Reporting**: Parses and displays test results, failures, and errors in console output
- **Flexible**: Works with both automated PR testing and manual workflow triggers

## Prerequisites

- Repository with LabVIEW code (`.vi`, `.ctl`, `.llb` files)
- GitHub-hosted runner (Ubuntu) or self-hosted runner with Docker support
- Access to `nationalinstruments/labview` Docker images

## Inputs

| Input | Description | Required | Default |
|-------|-------------|----------|---------|
| `config-path` | Path to existing VI Analyzer config file (`.viancfg`). If not provided, will generate from changed files. | No | `''` |
| `template-path` | Path to `.viancfg` template (used when generating dynamic config) | No | `.github/actions/via-lv-docker/via_template_linux.viancfg` |
| `base-branch` | Branch to compare against for changed files (used when generating config) | No | `origin/develop` |
| `labview-version` | LabVIEW Docker image version tag | No | `2026q1-linux` |
| `docker-image` | Full Docker image name | No | `nationalinstruments/labview` |

## Outputs

| Output | Description |
|--------|-------------|
| `via-exit-code` | VI Analyzer exit code (0 = success, non-zero = failures/errors) |
| `report-path` | Path to generated HTML report (`vi-analyzer-report.htm`) |
| `config-used` | Path to the config file that was used |

## Usage Examples

### Scenario 1: PR Testing (Dynamic Config)

Automatically test only changed LabVIEW files in pull requests:

```yaml
name: VI Analyzer Tests

on:
  pull_request:
    branches: [main, develop]

jobs:
  analyze:
    runs-on: ubuntu-latest
    steps:
      - name: Checkout code
        uses: actions/checkout@v4
        with:
          fetch-depth: 0  # Required for git diff

      - name: Run VI Analyzer
        uses: your-org/labview-via-action@v1
        with:
          base-branch: origin/${{ github.event.pull_request.base.ref }}

      - name: Upload Report
        uses: actions/upload-artifact@v4
        if: always()
        with:
          name: vi-analyzer-report
          path: vi-analyzer-report.htm
```

### Scenario 2: Manual Testing (Static Config)

Use a pre-defined configuration for manual workflow runs:

```yaml
name: VI Analyzer Tests

on:
  workflow_dispatch:

jobs:
  analyze:
    runs-on: ubuntu-latest
    steps:
      - name: Checkout code
        uses: actions/checkout@v4

      - name: Run VI Analyzer
        uses: your-org/labview-via-action@v1
        with:
          config-path: '.github/via-config.viancfg'

      - name: Upload Report
        uses: actions/upload-artifact@v4
        if: always()
        with:
          name: vi-analyzer-report
          path: vi-analyzer-report.htm
```

### Scenario 3: Combined Workflow

Support both PR-based dynamic testing and manual runs with static config:

```yaml
name: VI Analyzer Tests

on:
  pull_request:
    branches: [main, develop]
  push:
    branches: [main, develop]
  workflow_dispatch:

jobs:
  analyze:
    runs-on: ubuntu-latest
    steps:
      - name: Checkout code
        uses: actions/checkout@v4
        with:
          fetch-depth: 0

      - name: Run VI Analyzer
        uses: your-org/labview-via-action@v1
        with:
          # Use static config for manual triggers, dynamic for PRs
          config-path: ${{ github.event_name == 'workflow_dispatch' && '.github/via-config.viancfg' || '' }}
          base-branch: origin/${{ github.event.pull_request.base.ref || 'develop' }}

      - name: Upload Report
        uses: actions/upload-artifact@v4
        if: always()
        with:
          name: vi-analyzer-report
          path: vi-analyzer-report.htm
```

### Scenario 4: Custom LabVIEW Version

Test with a specific LabVIEW version:

```yaml
- name: Run VI Analyzer
  uses: your-org/labview-via-action@v1
  with:
    labview-version: '2025q3-linux'
    config-path: '.github/via-config.viancfg'
```

## How It Works

1. **Config Determination**:
   - If `config-path` is provided and exists → use it
   - Otherwise → generate config from changed files via `git diff`

2. **Dynamic Config Generation** (when no config provided):
   - Compares current branch against `base-branch`
   - Finds all changed `.vi`, `.ctl`, `.llb` files
   - Generates `.viancfg` file with only those files
   - Skips if no LabVIEW files changed

3. **Docker Execution**:
   - Pulls specified LabVIEW Docker image
   - Mounts workspace and scripts
   - Runs `LabVIEWCLI` with VI Analyzer operation
   - Generates HTML report

4. **Report Parsing**:
   - Extracts statistics (VIs analyzed, tests passed/failed)
   - Parses and displays detailed errors:
     - Failed test details
     - VI not loadable errors
     - Test errors (password-protected VIs, etc.)

## Creating a Static Config File

For manual testing, create `.github/via-config.viancfg`:

```xml
<?xml version="1.0" encoding="utf-8"?>
<VIAnalyzerConfig>
  <Settings>
    <SaveResults>true</SaveResults>
  </Settings>
  <ItemsToAnalyze>
    <Item>
      <Path>".."</Path>  <!-- Analyze entire repository -->
      <Removed>FALSE</Removed>
    </Item>
  </ItemsToAnalyze>
  <!-- Add your test configurations here -->
</VIAnalyzerConfig>
```

## Console Output Example

```plain text
==================================================
 VI Analyzer Results
==================================================

VIs Analyzed:   417
Total Tests:    187000
Passed:         184868
Failed:         2132
Skipped:        0

==================================================
 Failed Tests Summary
==================================================

 MyVI.vi
---
 Panel Size and Position
     → This VI's front panel does not reside entirely within the specified bounds

==================================================
 Test Error Out Errors
==================================================

[Wire Sources]
  Post Build Icon Editor PPL.vi
    → Error 1040. VI is password protected...
```

## Supported LabVIEW Versions

This action uses the official National Instruments LabVIEW Docker images:

- `2026q1-linux` (default)
- `latest-linux`
- `2025q3-linux`

Check [Docker Hub](https://hub.docker.com/r/nationalinstruments/labview) for available versions.

## Troubleshooting

### No Config Generated

If you see "No LabVIEW files changed; skipping VI Analyzer tests":

- Ensure you're using `fetch-depth: 0` in checkout
- Verify changed files match `.vi`, `.ctl`, or `.llb` extensions
- Check the `base-branch` is correct
