#!/usr/bin/env bash
# Installs/activates VIPM, refreshes the repository cache, and generates or
# checks vipm.lock from vipm.toml. Executed inside the VIPM Linux container by
# RefreshVipmLock.ps1.
set -euo pipefail

WORKING_DIRECTORY=""
LABVIEW_VERSION=""
LABVIEW_BITNESS=""
CHECK_ONLY="false"
VIPM_DEB_URL=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --working-directory) WORKING_DIRECTORY="$2"; shift 2 ;;
    --labview-version) LABVIEW_VERSION="$2"; shift 2 ;;
    --labview-bitness) LABVIEW_BITNESS="$2"; shift 2 ;;
    --check-only) CHECK_ONLY="$2"; shift 2 ;;
    --vipm-deb-url) VIPM_DEB_URL="$2"; shift 2 ;;
    *) echo "error: unknown argument '$1'" >&2; exit 2 ;;
  esac
done

if [ -z "${VIPM_SERIAL_NUMBER:-}" ] || [ -z "${VIPM_FULL_NAME:-}" ] || [ -z "${VIPM_EMAIL:-}" ]; then
  echo "error: VIPM Pro activation requires VIPM_SERIAL_NUMBER, VIPM_FULL_NAME, and VIPM_EMAIL secrets." >&2
  exit 2
fi

apt-get update
apt-get install -y wget ca-certificates xvfb

if ! command -v vipm >/dev/null 2>&1; then
  wget -O /tmp/vipm.deb "${VIPM_DEB_URL}"
  apt-get install -y /tmp/vipm.deb
fi

export DISPLAY=:99
export VIPM_TIMEOUT=500
Xvfb "$DISPLAY" -screen 0 1280x720x24 -ac +extension GLX +render -noreset >/tmp/xvfb.log 2>&1 &

vipm --version

# VIPM writes detailed failures to its error-log dirs, not stdout.
dump_vipm_logs() {
  echo "----- VIPM error logs -----"
  for d in /usr/local/jki/vipm/VIPM-CLI/error /usr/local/jki/vipm/error; do
    [ -d "$d" ] && find "$d" -type f -print -exec cat {} \; 2>/dev/null || true
  done
  echo "----- end VIPM error logs -----"
}

echo "VIPM Pro credentials are configured; attempting activation."
if vipm activate --serial-number "$VIPM_SERIAL_NUMBER" --name "$VIPM_FULL_NAME" --email "$VIPM_EMAIL"; then
  echo "VIPM Pro activation succeeded."
else
  ec=$?
  echo "error: VIPM Pro activation failed with exit code $ec" >&2
  dump_vipm_logs
  exit "$ec"
fi

cd "$WORKING_DIRECTORY"

# Clean runner has no repository cache; populate it for this project's LabVIEW
# target before lock resolution (see docs.vipm.io/cli/command-reference#vipm-refresh).
vipm refresh --labview-version "$LABVIEW_VERSION" --labview-bitness "$LABVIEW_BITNESS"

if [ "$CHECK_ONLY" = "true" ]; then
  if [ ! -f vipm.lock ]; then
    echo "error: vipm.lock does not exist yet; cannot run 'vipm lock --check'. Generate and commit an initial vipm.lock first." >&2
    exit 1
  fi
  if vipm lock --check; then
    :
  else
    ec=$?
    echo "error: 'vipm lock --check' failed with exit code $ec" >&2
    dump_vipm_logs
    exit "$ec"
  fi
else
  if vipm lock; then
    :
  else
    ec=$?
    echo "error: 'vipm lock' failed with exit code $ec" >&2
    dump_vipm_logs
    exit "$ec"
  fi
fi
