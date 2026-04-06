#!/bin/bash
# Build LabVIEW Build Specification using LabVIEWCLI
# [REQ-039] Build LabVIEW build specification using Docker container

set -euo pipefail

# Parse arguments
LABVIEW_PATH=""
PROJECT_PATH=""
TARGET_NAME=""
BUILD_SPEC_NAME=""
VERSION=""

while [[ $# -gt 0 ]]; do
    case $1 in
        --labview-path)
            LABVIEW_PATH="$2"
            shift 2
            ;;
        --project-path)
            PROJECT_PATH="$2"
            shift 2
            ;;
        --target-name)
            TARGET_NAME="$2"
            shift 2
            ;;
        --build-spec-name)
            BUILD_SPEC_NAME="$2"
            shift 2
            ;;
        --version)
            VERSION="$2"
            shift 2
            ;;
        *)
            echo "Unknown argument: $1"
            exit 1
            ;;
    esac
done

# Validate required arguments
if [[ -z "$LABVIEW_PATH" ]] || [[ -z "$PROJECT_PATH" ]]; then
    echo "Error: Missing required arguments"
    echo "Usage: $0 --labview-path <path> --project-path <path> [--target-name <name>] [--build-spec-name <name>] [--version <M.m.p.b>]"
    exit 1
fi

echo "Building LabVIEW Build Specification..."
echo "LabVIEW: $LABVIEW_PATH"
echo "Project: $PROJECT_PATH"
echo "Target: ${TARGET_NAME:-<My Computer>}"
echo "Build Spec: ${BUILD_SPEC_NAME:-<all>}"
echo "Version: ${VERSION:-<from build spec>}"

if [[ -n "$VERSION" ]]; then
    echo "Setting build version..."
    HELPER_VI="/helpers/scripts/build-spec-helpers/SetBuildVersionCaller.vi"

    if [[ ! -f "$HELPER_VI" ]]; then
        echo "Error: Helper VI not found at $HELPER_VI"
        exit 1
    fi

    # SetBuildVersionCaller.vi expects positional arguments:
    # 1. ProjectPath (required)
    # 2. BuildSpecName (optional, empty string to apply to all)
    # 3. TargetName (optional, defaults to "My Computer" in VI)
    # 4. Version (optional, empty string to skip version setting)
    
    echo "Executing: LabVIEWCLI -OperationName RunVI -LabVIEWPath \"$LABVIEW_PATH\" -VIPath $HELPER_VI \"$PROJECT_PATH\" \"$BUILD_SPEC_NAME\" \"$TARGET_NAME\" \"$VERSION\""
    
    # Pass all positional arguments, preserving empty strings
    LabVIEWCLI \
        -OperationName RunVI \
        -LabVIEWPath "$LABVIEW_PATH" \
        -VIPath "$HELPER_VI" \
        "$PROJECT_PATH" \
        "$BUILD_SPEC_NAME" \
        "$TARGET_NAME" \
        "$VERSION" \
        -Headless

    SET_VERSION_EXIT=$?

    if [[ $SET_VERSION_EXIT -ne 0 ]]; then
        echo "Failed to set build version (exit code: $SET_VERSION_EXIT)"
        exit $SET_VERSION_EXIT
    fi

    echo "Build version set successfully"
else
    echo "Skipping version set"
fi

# Construct LabVIEWCLI command
CLI_ARGS=(
    "-OperationName" "ExecuteBuildSpec"
    "-LabVIEWPath" "$LABVIEW_PATH"
    "-ProjectPath" "$PROJECT_PATH"
)

if [[ -n "$TARGET_NAME" ]]; then
    CLI_ARGS+=("-TargetName" "$TARGET_NAME")
fi

if [[ -n "$BUILD_SPEC_NAME" ]]; then
    CLI_ARGS+=("-BuildSpecName" "$BUILD_SPEC_NAME")
fi

CLI_ARGS+=("-Headless")

# Execute LabVIEWCLI
echo "Executing: LabVIEWCLI ${CLI_ARGS[*]}"
LabVIEWCLI "${CLI_ARGS[@]}"

EXIT_CODE=$?
echo "Build exit code: $EXIT_CODE"

# Copy LabVIEW logs to workspace for artifact collection
echo "Copying LabVIEW logs to workspace..."
mkdir -p /workspace/build-logs
if ls /tmp/lvtemporary_*.log 1> /dev/null 2>&1; then
    cp /tmp/lvtemporary_*.log /workspace/build-logs/ 2>/dev/null || true
    echo "Logs copied to /workspace/build-logs/"
else
    echo "No LabVIEW log files found in /tmp/"
fi

if [[ $EXIT_CODE -ne 0 ]]; then
    echo "Build failed"
    exit $EXIT_CODE
fi

echo "Build completed successfully"
exit 0