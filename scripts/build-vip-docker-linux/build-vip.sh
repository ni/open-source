#!/usr/bin/env bash
# Builds a VI Package inside the NI LabVIEW Linux container with the VIPM CLI.
# Configuration arrives through BVD_* environment variables set by BuildVipDockerLinux.ps1.
set -Eeuo pipefail

WORKSPACE=/workspace
DEFAULT_VIPM_DEB_URL="https://traffic.libsyn.com/secure/jkinc/vipm_26.3.1-4025_amd64.deb"

resolve_path() {
    local path="$1"
    if [[ "$path" = /* ]]; then
        printf '%s' "$path"
    else
        printf '%s/%s' "$WORKSPACE" "${path#./}"
    fi
}

LOG_DIR="$(resolve_path "${BVD_LOG_DIRECTORY:-builds/logs/vip-linux}")"
OUTPUT_DIR="$(resolve_path "${BVD_OUTPUT_DIRECTORY:-builds/vip}")"
SOURCE_DIR="$(resolve_path "${BVD_SOURCE_DIRECTORY:-.}")"
SOURCE_DIR="${SOURCE_DIR%/.}"
VIPM_DEB_URL="${BVD_VIPM_DEB_URL:-$DEFAULT_VIPM_DEB_URL}"
VIPM_DEB_SHA256="${BVD_VIPM_DEB_SHA256:-}"
BUILD_TIMEOUT_SECONDS="${BVD_BUILD_TIMEOUT_SECONDS:-900}"
VIPM_DESKTOP_LIVELINESS_TIMEOUT="${BVD_DESKTOP_LIVELINESS_TIMEOUT:-300}"
RUN_START_MARKER=/tmp/.build-vip-run-start
XVFB_PID=""

mkdir -p "$LOG_DIR" "$OUTPUT_DIR"
touch "$RUN_START_MARKER"

redact_secrets() {
    local file="$1"
    [[ -f "$file" ]] || return 0
    perl -0777 -i -pe '
        for my $key (qw(VIPM_SERIAL_NUMBER VIPM_FULL_NAME VIPM_EMAIL)) {
            my $value = $ENV{$key};
            next unless defined($value) && length($value);
            s/\Q$value\E/[REDACTED]/g;
        }
    ' "$file"
}

collect_diagnostics() {
    local status="${1:-unknown}"
    local base file
    local index=0
    {
        printf 'timestamp: %s\n' "$(date --iso-8601=seconds 2>/dev/null || date)"
        printf 'exit status: %s\n' "$status"
        printf 'display: %s\n' "${DISPLAY:-unset}"
        printf 'LabVIEW version: %s\n' "${LABVIEW_VERSION:-unset}"
        printf '\n-- OS --\n'
        cat /etc/os-release 2>/dev/null || true
        printf '\n-- memory --\n'
        free -h 2>/dev/null || true
        printf '\n-- disk --\n'
        df -h "$WORKSPACE" /tmp 2>/dev/null || true
        printf '\n-- VIPM/LabVIEW processes --\n'
        ps -eo pid,ppid,stat,etime,comm 2>/dev/null | grep -iE 'vipm|labview|xvfb' || true
        printf '\n-- Xvfb log --\n'
        tail -n 100 "$LOG_DIR/xvfb.log" 2>/dev/null || true
    } > "$LOG_DIR/diagnostics.log"

    mkdir -p "$LOG_DIR/runtime-logs"
    : > "$LOG_DIR/runtime-logs/index.txt"
    while IFS= read -r -d '' file; do
        index=$((index + 1))
        cp "$file" "$LOG_DIR/runtime-logs/${index}-$(basename "$file")" 2>/dev/null || true
        printf '%s\t%s\n' "$index" "$file" >> "$LOG_DIR/runtime-logs/index.txt"
    done < <(find "$HOME" /tmp /var/log /usr/local/natinst -xdev -type f -iname '*.log' -size -20M -newer "$RUN_START_MARKER" -not -path "$LOG_DIR/*" -print0 2>/dev/null)
    for base in "$HOME/.config/JKI/VIPM" "$HOME/.local/share/JKI/VIPM" "$HOME/.cache/JKI/VIPM" /var/log/JKI/VIPM; do
        [[ -d "$base" ]] || continue
        while IFS= read -r -d '' file; do
            index=$((index + 1))
            cp "$file" "$LOG_DIR/runtime-logs/${index}-$(basename "$file")" 2>/dev/null || true
            printf '%s\t%s\n' "$index" "$file" >> "$LOG_DIR/runtime-logs/index.txt"
        done < <(find "$base" -type f -iname '*.log' -print0 2>/dev/null)
    done
}

on_exit() {
    local status=$?
    if (( status != 0 )); then
        collect_diagnostics "$status" || true
    fi
    while IFS= read -r -d '' file; do
        redact_secrets "$file" || true
    done < <(find "$LOG_DIR" -type f -print0 2>/dev/null)
    # Files are root-owned inside the container; the runner user uploads them.
    chmod -R a+rwX "$LOG_DIR" "$OUTPUT_DIR" 2>/dev/null || true
    if [[ -n "$XVFB_PID" ]]; then
        kill "$XVFB_PID" 2>/dev/null || true
    fi
    return "$status"
}
trap on_exit EXIT

run_logged() {
    local name="$1"
    local timeout_seconds="$2"
    local log_file="$LOG_DIR/${name}.log"
    local status
    shift 2
    printf '\n[%s] Running %s (timeout %ss)\n' "$(date --iso-8601=seconds 2>/dev/null || date)" "$name" "$timeout_seconds"
    set +e
    timeout --signal=TERM --kill-after=30s "${timeout_seconds}s" "$@" 2>&1 | tee "$log_file"
    status=${PIPESTATUS[0]}
    set -e
    redact_secrets "$log_file"
    if (( status != 0 )); then
        printf '[%s] failed with exit status %s\n' "$name" "$status" >&2
        return "$status"
    fi
}

require_workspace_relative() {
    local label="$1" value="$2"
    if [[ "$value" = /* || "/$value/" == */../* ]]; then
        echo "$label must be a path relative to the workspace without '..': $value" >&2
        exit 1
    fi
}

# ---------------------------------------------------------------- validation
if [[ "$(id -u)" -ne 0 ]]; then
    echo 'The NI container helper must run as root to install VIPM.' >&2
    exit 1
fi
for name in LABVIEW_VERSION VIPM_SERIAL_NUMBER VIPM_FULL_NAME VIPM_EMAIL BVD_VIPB_PATH; do
    if [[ -z "${!name:-}" ]]; then
        echo "Required environment variable $name is missing." >&2
        exit 1
    fi
done
if [[ ! "$LABVIEW_VERSION" =~ ^([0-9]{4})q([13])$ ]]; then
    echo "Unsupported LabVIEW version '$LABVIEW_VERSION'; expected YYYYq1 or YYYYq3." >&2
    exit 1
fi
LV_YEAR="${BASH_REMATCH[1]}"
LV_QUARTER="${BASH_REMATCH[2]}"
LV_NUMERIC_VERSION="$((LV_YEAR - 2000)).${LV_QUARTER}"
LV_ROOT="/usr/local/natinst/LabVIEW-${LV_YEAR}-64"
require_workspace_relative vipb_path "$BVD_VIPB_PATH"
if [[ -n "${BVD_PRE_BUILD_SCRIPT:-}" ]]; then
    require_workspace_relative pre_build_script "$BVD_PRE_BUILD_SCRIPT"
fi

export WORKSPACE LOG_DIR OUTPUT_DIR SOURCE_DIR LV_ROOT LV_YEAR LV_QUARTER LV_NUMERIC_VERSION
export DISPLAY=:99
export VIPM_NONINTERACTIVE=1
export VIPM_DEBUG=1
export VIPM_TIMEOUT="$BUILD_TIMEOUT_SECONDS"
export VIPM_DESKTOP_LIVELINESS_TIMEOUT
export VIPM_LABVIEW_LAUNCH_TIMEOUT=300

# ------------------------------------------------------------- OS + display
run_logged apt-update 600 apt-get update
run_logged apt-install 900 apt-get install -y --no-install-recommends ca-certificates curl perl procps python3 x11-utils xvfb

mkdir -p /tmp/natinst
printf '1\n' > /tmp/natinst/LVContainer.txt
Xvfb :99 -screen 0 1280x720x24 -ac +extension GLX +render -noreset > "$LOG_DIR/xvfb.log" 2>&1 &
XVFB_PID=$!
for _ in $(seq 1 30); do
    if xdpyinfo -display "$DISPLAY" >/dev/null 2>&1; then break; fi
    if ! kill -0 "$XVFB_PID" 2>/dev/null; then
        cat "$LOG_DIR/xvfb.log" >&2 || true
        echo 'Xvfb exited before the headless display became ready.' >&2
        exit 1
    fi
    sleep 1
done
if ! xdpyinfo -display "$DISPLAY" >/dev/null 2>&1; then
    echo 'Xvfb did not become ready within 30 seconds.' >&2
    exit 1
fi

# ------------------------------------------------------ VIPM install + login
run_logged vipm-download 900 curl --fail --location --retry 3 --retry-all-errors --output /tmp/vipm.deb "$VIPM_DEB_URL"
if [[ -n "$VIPM_DEB_SHA256" ]]; then
    printf '%s  /tmp/vipm.deb\n' "$VIPM_DEB_SHA256" | sha256sum --check --strict -
fi
if ! run_logged vipm-dpkg-install 900 dpkg -i /tmp/vipm.deb; then
    run_logged apt-repair 900 apt-get -f install -y
    run_logged vipm-dpkg-install-retry 900 dpkg -i /tmp/vipm.deb
fi
if ! command -v vipm >/dev/null 2>&1; then
    echo 'VIPM CLI was not found on PATH after installing its Debian package.' >&2
    exit 1
fi
dpkg-query -W -f='${Package} ${Version}\n' vipm | tee "$LOG_DIR/vipm-version.log"

activation_log="$LOG_DIR/vipm-activation.log"
if timeout --signal=TERM --kill-after=30s 600s vipm activate --serial-number "$VIPM_SERIAL_NUMBER" --name "$VIPM_FULL_NAME" --email "$VIPM_EMAIL" > "$activation_log" 2>&1; then
    activation_status=0
else
    activation_status=$?
fi
redact_secrets "$activation_log"
cat "$activation_log"
if (( activation_status != 0 )); then
    printf 'VIPM Pro activation failed with exit status %s.\n' "$activation_status" >&2
    exit "$activation_status"
fi

# ------------------------------------------------------- project pre-build
if [[ -n "${BVD_PRE_BUILD_SCRIPT:-}" ]]; then
    pre_build="$WORKSPACE/$BVD_PRE_BUILD_SCRIPT"
    [[ -f "$pre_build" ]] || { echo "Pre-build script not found: $pre_build" >&2; exit 1; }
    run_logged pre-build 3600 bash "$pre_build"
fi

# ---------------------------------------------------------- prepare VIPB
vipb_source="$WORKSPACE/$BVD_VIPB_PATH"
[[ -f "$vipb_source" ]] || { echo "VIPB not found: $vipb_source" >&2; exit 1; }
# VIPM resolves relative VIPB paths (e.g. custom action VIs) against the VIPB's own directory.
vipb_output="$(dirname "$vipb_source")/$(basename "$vipb_source" .vipb).build.vipb"
vipb_log="$LOG_DIR/vipb-preparation.log"
if ! python3 - "$vipb_source" "$vipb_output" "$SOURCE_DIR" "$OUTPUT_DIR" "$LOG_DIR/package-version.txt" > "$vipb_log" 2>&1 <<'PY'
import sys
import xml.etree.ElementTree as ET
from pathlib import Path

source, output, source_dir, output_dir, version_file = sys.argv[1:]
source = Path(source)
tree = ET.parse(source)
root = tree.getroot()


def require(parent, path):
    node = parent.find(path)
    if node is None:
        raise ValueError(f"Required VIPB XML node is missing: {path}")
    return node


general = require(root, "Library_General_Settings")
require(general, "Library_Source_Folder").text = source_dir
require(general, "Library_Output_Folder").text = output_dir
package_version = (require(general, "Library_Version").text or "").strip()
if not package_version:
    raise ValueError("VIPB Library_Version is empty")

vipc = root.find("Advanced_Settings/VI_Package_Configuration_File")
if vipc is not None and vipc.text and not (source.parent / vipc.text.replace("\\", "/")).is_file():
    print(f"Clearing VIPC reference that does not exist: {vipc.text}")
    vipc.text = ""

Path(output).parent.mkdir(parents=True, exist_ok=True)
tree.write(output, encoding="utf-8", xml_declaration=True)
Path(version_file).write_text(package_version + "\n", encoding="utf-8")
print(f"Prepared {output} from {source}")
print(f"Source folder: {source_dir}; output folder: {output_dir}; package version: {package_version}")
PY
then
    cat "$vipb_log" >&2
    exit 1
fi
cat "$vipb_log"
cp -f -- "$vipb_output" "$LOG_DIR/" || true

# ------------------------------------------------------------------- build
run_logged vipm-build "$((BUILD_TIMEOUT_SECONDS + 900))" vipm build "$vipb_output" --labview-version "$LV_YEAR" --labview-bitness 64 --timeout "$BUILD_TIMEOUT_SECONDS" --verbose
shopt -s nullglob
vip_files=("$OUTPUT_DIR/"*.vip)
if (( ${#vip_files[@]} == 0 )); then
    echo "VIPM reported success but produced no .vip under $OUTPUT_DIR." >&2
    exit 1
fi
printf 'Built VIP package(s):\n'
printf '  %s\n' "${vip_files[@]}"
