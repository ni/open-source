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
COMMIT=""
BITNESS=""

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
        --commit)
            COMMIT="$2"
            shift 2
            ;;
        --bitness)
            BITNESS="$2"
            shift 2
            ;;
        *)
            echo "Unknown argument: $1"
            exit 1
            ;;
    esac
done

# Validate required arguments
if [[ -z "$LABVIEW_PATH" ]] || [[ -z "$PROJECT_PATH" ]] || [[ -z "$TARGET_NAME" ]] || [[ -z "$VERSION" ]] || [[ -z "$COMMIT" ]] || [[ -z "$BITNESS" ]]; then
    echo "Error: Missing required arguments"
    echo "Usage: $0 --labview-path <path> --project-path <path> --target-name <name> [--build-spec-name <name>] --version <version> --commit <commit> --bitness <bitness>"
    exit 1
fi

echo "Building LabVIEW Packed Project Library..."
echo "LabVIEW: $LABVIEW_PATH"
echo "Project: $PROJECT_PATH"
echo "Target: $TARGET_NAME"
echo "Build Spec: ${BUILD_SPEC_NAME:-<all>}"
echo "Version: $VERSION"
echo "Commit: $COMMIT"
echo "Bitness: $BITNESS"

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

# Rename PPL artifacts with version and commit metadata
echo "Renaming PPL artifacts..."

# Determine bitness tag
if [[ "$BITNESS" == "32" ]]; then
    BITNESS_TAG="x86"
else
    BITNESS_TAG="x64"
fi

# Get short commit (first 7 characters)
SHORT_COMMIT="${COMMIT:0:7}"
VERSION_TAG="v${VERSION}+g${SHORT_COMMIT}"

# Find and rename .lvlibp files in builds directory
FOUND_FILES=false
if [[ -d "/workspace/builds" ]]; then
    while IFS= read -r -d '' LVLIBP_FILE; do
        FOUND_FILES=true
        BASENAME=$(basename "$LVLIBP_FILE" .lvlibp)
        DIRNAME=$(dirname "$LVLIBP_FILE")
        NEW_NAME="${BASENAME}_${BITNESS_TAG}_${VERSION_TAG}.lvlibp"
        echo "Renaming: $LVLIBP_FILE -> $NEW_NAME"
        mv "$LVLIBP_FILE" "${DIRNAME}/${NEW_NAME}"
    done < <(find /workspace/builds -type f -name "*.lvlibp" -print0)
fi

if [[ "$FOUND_FILES" == "false" ]]; then
    echo "Warning: No .lvlibp files found in /workspace/builds directory"
fi

echo "Build and rename completed successfully"
exit 0