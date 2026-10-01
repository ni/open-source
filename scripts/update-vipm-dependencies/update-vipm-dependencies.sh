#!/usr/bin/env bash
# Bumps every dependency declared in vipm.toml's [dependencies] table (and,
# when requested, [dev-dependencies]) to its latest available version
# (vipm add <pkg> with no version suffix resolves to latest per
# docs.vipm.io/cli/command-reference#vipm-add), then regenerates vipm.lock.
# Executed inside the VIPM Linux container by UpdateVipmDependencies.ps1.
set -euo pipefail

WORKING_DIRECTORY=""
LABVIEW_VERSION=""
LABVIEW_BITNESS=""
VIPM_DEB_URL=""
MANIFEST_FILENAME="vipm.toml"
LOCK_FILENAME="vipm.lock"
INCLUDE_DEV_DEPENDENCIES="false"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --working-directory) WORKING_DIRECTORY="$2"; shift 2 ;;
    --labview-version) LABVIEW_VERSION="$2"; shift 2 ;;
    --labview-bitness) LABVIEW_BITNESS="$2"; shift 2 ;;
    --vipm-deb-url) VIPM_DEB_URL="$2"; shift 2 ;;
    --manifest-filename) MANIFEST_FILENAME="$2"; shift 2 ;;
    --lock-filename) LOCK_FILENAME="$2"; shift 2 ;;
    --include-dev-dependencies) INCLUDE_DEV_DEPENDENCIES="$2"; shift 2 ;;
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

# vipm's CLI only recognizes vipm.toml/vipm.lock in the CWD; stage custom
# filenames under those names for the duration of the vipm calls.
restore_filenames() {
  if [ "$MANIFEST_FILENAME" != "vipm.toml" ] && [ -f vipm.toml ]; then
    mv vipm.toml "$MANIFEST_FILENAME"
  fi
  if [ "$LOCK_FILENAME" != "vipm.lock" ] && [ -f vipm.lock ]; then
    mv vipm.lock "$LOCK_FILENAME"
  fi
  return 0
}
trap restore_filenames EXIT
if [ "$MANIFEST_FILENAME" != "vipm.toml" ] && [ -f "$MANIFEST_FILENAME" ]; then
  mv "$MANIFEST_FILENAME" vipm.toml
fi
if [ "$LOCK_FILENAME" != "vipm.lock" ] && [ -f "$LOCK_FILENAME" ]; then
  mv "$LOCK_FILENAME" vipm.lock
fi

vipm refresh --labview-version "$LABVIEW_VERSION" --labview-bitness "$LABVIEW_BITNESS"

# Extract dependency names from [dependencies] (and, when requested,
# [dev-dependencies]); stops each section at the next [section].
wanted_sections="dependencies"
if [ "$INCLUDE_DEV_DEPENDENCIES" = "true" ]; then
  wanted_sections="dependencies dev-dependencies"
fi
mapfile -t deps < <(awk -v sections="$wanted_sections" '
  BEGIN { n = split(sections, arr, " "); for (i = 1; i <= n; i++) wanted["[" arr[i] "]"] = 1 }
  /^\[/ { in_section = ($0 in wanted); next }
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
