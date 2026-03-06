#!/bin/bash
# Build LabVIEW Packed Project Library using LabVIEWCLI
# [REQ-039] Build LVLIBP using Docker container

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
if [[ -z "$LABVIEW_PATH" ]] || [[ -z "$PROJECT_PATH" ]] || [[ -z "$TARGET_NAME" ]] || [[ -z "$VERSION" ]]; then
    echo "Error: Missing required arguments"
    echo "Usage: $0 --labview-path <path> --project-path <path> --target-name <name> [--build-spec-name <name>] --version <version>"
    exit 1
fi

echo "Building LabVIEW Packed Project Library..."
echo "LabVIEW: $LABVIEW_PATH"
echo "Project: $PROJECT_PATH"
echo "Target: $TARGET_NAME"
echo "Build Spec: ${BUILD_SPEC_NAME:-<all>}"
echo "Version: $VERSION"

# Construct LabVIEWCLI command
CLI_ARGS=(
    "-OperationName" "ExecuteBuildSpec"
    "-LabVIEWPath" "$LABVIEW_PATH"
    "-ProjectPath" "$PROJECT_PATH"
    "-TargetName" "$TARGET_NAME"
)

if [[ -n "$BUILD_SPEC_NAME" ]]; then
    CLI_ARGS+=("-BuildSpecName" "$BUILD_SPEC_NAME")
fi

CLI_ARGS+=("-Version" "$VERSION")

# Execute LabVIEWCLI
echo "Executing: LabVIEWCLI ${CLI_ARGS[*]}"
LabVIEWCLI "${CLI_ARGS[@]}"

EXIT_CODE=$?
echo "Build exit code: $EXIT_CODE"
exit $EXIT_CODE