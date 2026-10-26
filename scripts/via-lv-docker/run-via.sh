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

# VIA_PACKAGES ("name" or "name@version", space separated) is set only when the caller passes dependency_file.
if [ -n "${VIA_PACKAGES:-}" ]; then
  VIPM_DEB_URL="https://traffic.libsyn.com/secure/jkinc/vipm_26.3.1-4025_amd64.deb"

  fail_dependencies() {
    echo "Error: LabVIEW dependency installation failed: $1" >&2
    echo "VI Analyzer was not run." >&2
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
  LABVIEW_PATH="${lv_dirs[0]}/labview"
  read -r -a packages <<< "$VIA_PACKAGES"
  echo "Installing LabVIEW dependencies into LabVIEW $LV_YEAR (64-bit): ${packages[*]}"

  export DEBIAN_FRONTEND=noninteractive
  install_step "apt-get update" apt-get -qq update
  install_step "install prerequisites" apt-get -qq install -y --no-install-recommends ca-certificates wget xvfb
  install_step "download VIPM" wget -q -O /tmp/vipm.deb "$VIPM_DEB_URL"
  install_step "install VIPM" apt-get -qq install -y /tmp/vipm.deb
  install_step "vipm --version" vipm --version

  # One display for all VIPM commands: LabVIEW started by VIPM must keep its display until it is closed.
  Xvfb :99 -screen 0 1280x720x24 -ac +extension GLX +render -noreset > /tmp/xvfb.log 2>&1 &
  xvfb_pid=$!
  for _ in $(seq 1 30); do
    [ -S /tmp/.X11-unix/X99 ] && break
    sleep 1
  done
  if [ ! -S /tmp/.X11-unix/X99 ]; then
    cat /tmp/xvfb.log >&2 || true
    fail_dependencies "Xvfb did not start"
  fi
  export DISPLAY=:99

  # VIPM Free: no activation.
  export VIPM_NONINTERACTIVE=1
  install_step "vipm refresh" timeout 600 vipm refresh --color-mode never --labview-version "$LV_YEAR" --labview-bitness 64

  echo "==> vipm install"
  status=0
  timeout 1800 vipm install --color-mode never --labview-version "$LV_YEAR" --labview-bitness 64 "${packages[@]}" || status=$?
  # 9 = PACKAGE_ALREADY_INSTALLED; the check below still verifies every package.
  if [ "$status" -ne 0 ] && [ "$status" -ne 9 ]; then
    fail_dependencies "'vipm install' exited with code $status" "$status"
  fi

  echo "==> vipm list --installed"
  status=0
  installed="$(timeout 300 vipm list --color-mode never --installed --labview-version "$LV_YEAR" --labview-bitness 64 2>&1)" || status=$?
  printf '%s\n' "$installed"
  if [ "$status" -ne 0 ]; then
    fail_dependencies "'vipm list --installed' exited with code $status" "$status"
  fi
  for spec in "${packages[@]}"; do
    name="${spec%%@*}"
    if ! grep -Fqi -- "$name" <<< "$installed"; then
      fail_dependencies "package '$name' is not installed in LabVIEW $LV_YEAR (64-bit); see 'vipm list --installed' output above"
    fi
    if [[ "$spec" == *@* ]]; then
      version="${spec#*@}"
      if ! grep -Fq -- "$version" <<< "$installed"; then
        fail_dependencies "package '$name' is installed, but version '$version' was not found in LabVIEW $LV_YEAR (64-bit); see 'vipm list --installed' output above"
      fi
    fi
  done

  # VIPM leaves its LabVIEW running on VI Server port 3363; LabVIEWCLI needs that port. Uses /proc because procps may be absent.
  labview_pids() {
    local p exe
    for p in /proc/[0-9]*; do
      exe="$(readlink "$p/exe" 2>/dev/null)" || continue
      case "$exe" in /usr/local/natinst/LabVIEW-*/labview*) echo "${p#/proc/}" ;; esac
    done
  }
  pids="$(labview_pids)"
  if [ -n "$pids" ]; then
    echo "==> stopping LabVIEW started by VIPM (PIDs: $(echo $pids))"
    kill $pids 2>/dev/null || true
    for _ in $(seq 1 30); do
      [ -z "$(labview_pids)" ] && break
      sleep 1
    done
    pids="$(labview_pids)"
    if [ -n "$pids" ]; then
      kill -9 $pids 2>/dev/null || true
      sleep 2
    fi
    if [ -n "$(labview_pids)" ]; then
      fail_dependencies "LabVIEW started by VIPM is still running (PIDs: $(echo $(labview_pids)))"
    fi
  fi
  kill "$xvfb_pid" 2>/dev/null || true
  unset DISPLAY
  echo "==> LabVIEW VI Server settings"
  grep -Hs '^server\.tcp' /root/natinst/.config/LabVIEW-"$LV_YEAR"/*.conf "${lv_dirs[0]}"/*.conf || echo "(no server.tcp entries found)"
  mkdir -p /tmp/via-status
  touch /tmp/via-status/dependencies-installed
  echo "LabVIEW dependencies installed."
  echo ""
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

if [ "$via_status" -ne 0 ] && [ -n "${VIA_PACKAGES:-}" ]; then
  for log in /tmp/lvtemporary_*.log; do
    [ -f "$log" ] || continue
    echo "----- $log -----"
    tail -n 200 "$log"
  done
fi
exit "$via_status"