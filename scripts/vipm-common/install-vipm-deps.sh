#!/bin/bash
# Resolve the VIPM CLI (installing it via .deb if missing), verify it supports
# vipm.toml/vipm.lock, and install a project's dependencies from a manifest.
# Used by the Linux container build path. A too-old CLI is a hard error because
# dependencies were explicitly requested.

set -euo pipefail

VIPM_TOML=""
VIPM_LOCK=""
MIN_VERSION="25.3"
VIPM_DEB_URL="https://traffic.libsyn.com/secure/jkinc/vipm_26.3.1-4025_amd64.deb"

while [[ $# -gt 0 ]]; do
    case $1 in
        --vipm-toml)
            VIPM_TOML="$2"; shift 2 ;;
        --vipm-lock)
            VIPM_LOCK="$2"; shift 2 ;;
        --min-version)
            MIN_VERSION="$2"; shift 2 ;;
        *)
            echo "Unknown argument: $1"; exit 1 ;;
    esac
done

if [[ -z "$VIPM_TOML" ]]; then
    echo "No vipm.toml provided; skipping dependency installation."
    exit 0
fi

if ! command -v vipm >/dev/null 2>&1; then
    echo "VIPM CLI not found in container; installing from $VIPM_DEB_URL..."
    apt-get update
    apt-get install -y wget
    wget --no-check-certificate -O /tmp/vipm.deb "$VIPM_DEB_URL"
    dpkg -i /tmp/vipm.deb
    vipm --version
    rm -f /tmp/vipm.deb

    if ! command -v vipm >/dev/null 2>&1; then
        echo "Error: VIPM CLI still not found after installation."
        exit 1
    fi
    echo "VIPM installed: $(vipm --version)"
fi

# Real VIPM versions are plain dotted numbers (e.g. 26.3.0), not YYYY.Q -
# capture the whole dotted run (2-4 parts) so multi-part versions compare correctly.
VERSION_RAW="$(vipm --version 2>&1 || true)"
if ! echo "$VERSION_RAW" | grep -qoE '[0-9]+(\.[0-9]+){1,3}'; then
    VERSION_RAW="$(vipm version 2>&1 || true)"
fi
VIPM_VERSION="$(echo "$VERSION_RAW" | grep -oE '[0-9]+(\.[0-9]+){1,3}' | head -n1 || true)"

if [[ -z "$VIPM_VERSION" ]]; then
    echo "Error: Unable to determine VIPM CLI version. Version >= $MIN_VERSION is required for vipm.toml/vipm.lock support."
    exit 1
fi

# Version-aware compare (sort -V) handles multi-part dotted versions correctly;
# the smaller of the two strings sorts first, so MIN_VERSION sorting first (or
# tying) means VIPM_VERSION is new enough.
lowest="$(printf '%s\n%s\n' "$MIN_VERSION" "$VIPM_VERSION" | sort -V | head -n1)"
if [[ "$lowest" != "$MIN_VERSION" ]]; then
    echo "Error: VIPM $VIPM_VERSION does not support vipm.toml/vipm.lock. Version >= $MIN_VERSION is required."
    exit 1
fi
echo "Using VIPM $VIPM_VERSION"

# Activation is only attempted when credentials are provided via environment.
if [[ -n "${VIPM_SERIAL_NUMBER:-}" ]]; then
    echo "Activating VIPM Pro..."
    vipm activate --serial-number "$VIPM_SERIAL_NUMBER" --name "${VIPM_FULL_NAME:-}" --email "${VIPM_EMAIL:-}"
fi

echo "Refreshing VIPM package list..."
vipm package-list-refresh

if [[ ! -f "$VIPM_TOML" ]]; then
    echo "Error: vipm.toml not found at $VIPM_TOML"; exit 1
fi

# Match Windows behavior: default to vipm.lock next to vipm.toml, fail if missing.
if [[ -z "$VIPM_LOCK" ]]; then
    VIPM_LOCK="$(dirname "$VIPM_TOML")/vipm.lock"
fi
if [[ ! -f "$VIPM_LOCK" ]]; then
    echo "Error: vipm.lock not found at $VIPM_LOCK. Run 'vipm lock' next to vipm.toml locally and commit the result."
    exit 1
fi
echo "Verifying vipm.lock is in sync..."
(cd "$(dirname "$VIPM_TOML")" && vipm lock --check)

echo "Installing dependencies from $VIPM_TOML..."
vipm install "$VIPM_TOML" --no-dev
echo "VIPM dependencies installed successfully."
