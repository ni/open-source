#!/bin/bash
set -e

# VI Analyzer script for GitHub Actions

# Accept config path as first argument, default to generated-config.viancfg
CONFIG_PATH="${1:-generated-config.viancfg}"
ABSOLUTE_CONFIG="/workspace/$CONFIG_PATH"
ABSOLUTE_REPORT="/workspace/vi-analyzer-report.htm"
LV_YEAR="${LV_YEAR:-2025}"
LABVIEW_PATH="/usr/local/natinst/LabVIEW-${LV_YEAR}-64/labview"

# Verify that the configuration path exists
if [ ! -d "$ABSOLUTE_CONFIG" ] && [ ! -f "$ABSOLUTE_CONFIG" ]; then
  echo "Error: Configuration path not found at $ABSOLUTE_CONFIG"
  exit 1
fi

# VIA_PACKAGES ("name" or "name@version", space separated) selects the install-only run used with dependency_file.
# RunViaLvDocker.ps1 then restarts the container, which stops VIPM, LabVIEW and Xvfb, and runs this script again to analyze.
if [ -n "${VIA_PACKAGES:-}" ]; then
  VIPM_DEB_URL="https://traffic.libsyn.com/secure/jkinc/vipm_26.3.1-4025_amd64.deb"

  fail_dependencies() {
    echo "Error: LabVIEW dependency installation failed: $1" >&2
    exit "${2:-1}"
  }

  install_step() {
    local label="$1" status=0
    shift
    echo "==> $label"
    "$@" || status=$?
    if [ "$status" -ne 0 ]; then
      fail_dependencies "'$label' exited with code $status" "$status"
    fi
  }

  lv_dirs=(/usr/local/natinst/LabVIEW-*-64)
  if [ "${#lv_dirs[@]}" -ne 1 ] || [ ! -d "${lv_dirs[0]}" ]; then
    fail_dependencies "expected exactly one 64-bit LabVIEW in /usr/local/natinst, found: ${lv_dirs[*]}"
  fi
  LV_YEAR="$(basename "${lv_dirs[0]}" | sed -E 's/^LabVIEW-([0-9]{4})-64$/\1/')"
  if [[ ! "$LV_YEAR" =~ ^[0-9]{4}$ ]]; then
    fail_dependencies "could not determine the LabVIEW year from ${lv_dirs[0]}"
  fi
  if (( LV_YEAR < 2026 )); then
    fail_dependencies "headless LabVIEW, which VIPM needs in the container, requires LabVIEW 2026 or later (found $LV_YEAR)"
  fi
  labview_exe="${lv_dirs[0]}/labviewprofull"
  [ -x "$labview_exe" ] || labview_exe="${lv_dirs[0]}/labview"
  read -r -a packages <<< "$VIA_PACKAGES"
  echo "Installing LabVIEW dependencies into LabVIEW $LV_YEAR (64-bit): ${packages[*]}"

  export DEBIAN_FRONTEND=noninteractive NO_COLOR=1
  export VIPM_NONINTERACTIVE=1 VIPM_TIMEOUT=900 VIPM_DESKTOP_LIVELINESS_TIMEOUT=900 VIPM_LABVIEW_LAUNCH_TIMEOUT=300
  install_step "apt-get update" apt-get -qq update
  install_step "install prerequisites" apt-get -qq install -y --no-install-recommends ca-certificates wget xvfb

  mkdir -p /tmp/natinst
  echo 1 > /tmp/natinst/LVContainer.txt
  Xvfb :99 -screen 0 1280x720x24 -ac +extension GLX +render -noreset > /tmp/xvfb.log 2>&1 &
  xvfb_pid=$!
  for _ in $(seq 1 30); do
    [ -S /tmp/.X11-unix/X99 ] && break
    kill -0 "$xvfb_pid" 2>/dev/null || break
    sleep 1
  done
  if [ ! -S /tmp/.X11-unix/X99 ]; then
    cat /tmp/xvfb.log >&2 || true
    fail_dependencies "Xvfb did not start"
  fi
  export DISPLAY=:99

  install_step "download VIPM" wget -q -O /tmp/vipm.deb "$VIPM_DEB_URL"
  install_step "install VIPM" apt-get -qq install -y /tmp/vipm.deb
  install_step "vipm version" vipm version
  # VIPM Free: no activation.
  install_step "vipm refresh" timeout --signal=TERM --kill-after=30s 900s vipm refresh

  # VIPM must use a headless LabVIEW; a non-headless one would block LabVIEWCLI later.
  echo "==> start headless LabVIEW ($labview_exe)"
  "$labview_exe" --headless > /tmp/labview-setup.log 2>&1 &
  labview_pid=$!
  ready=false
  for _ in $(seq 1 90); do
    if ! kill -0 "$labview_pid" 2>/dev/null; then
      cat /tmp/labview-setup.log >&2 || true
      fail_dependencies "headless LabVIEW exited before VI Server was ready"
    fi
    if (echo > /dev/tcp/127.0.0.1/3363) 2>/dev/null; then ready=true; break; fi
    sleep 2
  done
  [ "$ready" = true ] || fail_dependencies "LabVIEW VI Server did not become ready on port 3363"

  echo "==> vipm install"
  status=0
  timeout --signal=TERM --kill-after=30s 1800s vipm install -y --labview-version "$LV_YEAR" --labview-bitness 64 "${packages[@]}" || status=$?
  # 9 = PACKAGE_ALREADY_INSTALLED
  if [ "$status" -ne 0 ] && [ "$status" -ne 9 ]; then
    fail_dependencies "'vipm install' exited with code $status" "$status"
  fi

  # vipm list can crash after a successful install, so its result only checks pinned versions when it works.
  echo "==> vipm list --installed"
  status=0
  installed="$(timeout 300 vipm list --installed --labview-version "$LV_YEAR" --labview-bitness 64 2>&1)" || status=$?
  printf '%s\n' "$installed"
  if [ "$status" -ne 0 ]; then
    echo "Warning: 'vipm list --installed' exited with code $status; continuing because 'vipm install' succeeded."
  else
    for spec in "${packages[@]}"; do
      name="${spec%%@*}"
      if ! grep -Fqi -- "$name" <<< "$installed"; then
        fail_dependencies "package '$name' is not installed in LabVIEW $LV_YEAR (64-bit); see 'vipm list --installed' output above"
      fi
      if [[ "$spec" == *@* ]] && ! grep -Fq -- "${spec#*@}" <<< "$installed"; then
        fail_dependencies "package '$name' is installed, but version '${spec#*@}' was not found; see 'vipm list --installed' output above"
      fi
    done
  fi

  echo "LabVIEW dependencies installed."
  exit 0
fi

echo "Running LabVIEWCLI VIAnalyzer with the following parameters:"
echo "ConfigPath: $ABSOLUTE_CONFIG"
echo "ReportPath: $ABSOLUTE_REPORT"
echo "LabVIEWPath: $LABVIEW_PATH"
echo ""

# Run VI Analyzer
via_status=0
LabVIEWCLI \
  -OperationName RunVIAnalyzer \
  -ConfigPath "$ABSOLUTE_CONFIG" \
  -ReportPath "$ABSOLUTE_REPORT" \
  -ReportSaveType HTML \
  -LabVIEWPath "$LABVIEW_PATH" \
  -Headless || via_status=$?

if [ "$via_status" -ne 0 ] && [ -n "${VIA_DEPENDENCIES_INSTALLED:-}" ]; then
  for log in /tmp/lvtemporary_*.log; do
    [ -f "$log" ] || continue
    echo "----- $log -----"
    tail -n 200 "$log"
  done
fi
exit "$via_status"