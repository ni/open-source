#!/usr/bin/env bash
# Bumps every dependency declared in vipm.toml's [dependencies] table to its
# latest available version (vipm add <pkg> with no version suffix resolves to
# latest per docs.vipm.io/cli/command-reference#vipm-add), then regenerates
# vipm.lock. Executed inside the VIPM Linux container by
# UpdateVipmDependencies.ps1.
set -euo pipefail

WORKING_DIRECTORY=""
LABVIEW_VERSION=""
LABVIEW_BITNESS=""
VIPM_DEB_URL=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --working-directory) WORKING_DIRECTORY="$2"; shift 2 ;;
    --labview-version) LABVIEW_VERSION="$2"; shift 2 ;;
    --labview-bitness) LABVIEW_BITNESS="$2"; shift 2 ;;
    --vipm-deb-url) VIPM_DEB_URL="$2"; shift 2 ;;
    *) echo "error: unknown argument '$1'" >&2; exit 2 ;;
  esac
done

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

if [ -n "${VIPM_SERIAL_NUMBER:-}" ]; then
  echo "VIPM serial is configured; attempting Pro activation."
  if vipm activate --serial-number "$VIPM_SERIAL_NUMBER" --name "$VIPM_FULL_NAME" --email "$VIPM_EMAIL"; then
    echo "VIPM Pro activation succeeded."
  else
    ec=$?
    echo "error: VIPM Pro activation failed with exit code $ec" >&2
    dump_vipm_logs
    exit "$ec"
  fi
else
  echo "VIPM_SERIAL_NUMBER is unset; skipping Pro activation."
fi

cd "$WORKING_DIRECTORY"

vipm refresh --labview-version "$LABVIEW_VERSION" --labview-bitness "$LABVIEW_BITNESS"

# Extract dependency names from the [dependencies] table (stops at the next [section]).
mapfile -t deps < <(awk '
  /^\[dependencies\]/ { in_section=1; next }
  /^\[/ { in_section=0 }
  in_section && NF { split($0, parts, "="); gsub(/[ \t"]/, "", parts[1]); if (parts[1] != "") print parts[1] }
' vipm.toml)

echo "Found ${#deps[@]} dependencies to check: ${deps[*]}"

failed=()
for pkg in "${deps[@]}"; do
  echo "::group::vipm add $pkg"
  # No version suffix: vipm add resolves and pins the latest available version
  # (docs.vipm.io/cli/command-reference#vipm-add, example "Add one or more
  # dependencies (latest available version)").
  if vipm add "$pkg" --labview-version "$LABVIEW_VERSION" --labview-bitness "$LABVIEW_BITNESS"; then
    :
  else
    ec=$?
    echo "error: could not resolve/update $pkg (exit $ec)" >&2
    dump_vipm_logs
    failed+=("$pkg")
  fi
  echo "::endgroup::"
done

if [ "${#failed[@]}" -gt 0 ]; then
  echo "error: failed to update ${#failed[@]} package(s): ${failed[*]}" >&2
  echo "Not generating vipm.lock or opening a PR with a partial/incomplete update." >&2
  exit 1
fi

# vipm add never creates/fully reconciles vipm.lock (docs.vipm.io/vipm-toml/lock-files),
# so regenerate it explicitly - this is the only command that writes the lock.
vipm lock
