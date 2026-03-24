#!/usr/bin/env bash

set -euo pipefail

if [[ $# -lt 5 ]]; then
  echo "usage: $0 <package_source_root> <requirements_file> <python_version> <output_zip> <source_file> [source_file...]" >&2
  exit 1
fi

PACKAGE_SOURCE_ROOT="$1"
REQUIREMENTS_FILE="$2"
PYTHON_VERSION="$3"
OUTPUT_ZIP="$4"
shift 4
SOURCE_FILES=("$@")
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REQUIREMENTS_DIR="$(cd "$(dirname "$REQUIREMENTS_FILE")" && pwd)"
REQUIREMENTS_BASENAME="$(basename "$REQUIREMENTS_FILE")"

if [[ "$OUTPUT_ZIP" != /* ]]; then
  OUTPUT_ZIP="${SCRIPT_DIR}/${OUTPUT_ZIP#./}"
fi

BUILD_DIR="$(mktemp -d)"
PACKAGE_DIR="$BUILD_DIR/package"

cleanup() {
  rm -rf "$BUILD_DIR"
}

trap cleanup EXIT

mkdir -p "$PACKAGE_DIR"

copy_source() {
  local relpath
  for relpath in "${SOURCE_FILES[@]}"; do
    mkdir -p "$PACKAGE_DIR/$(dirname "$relpath")"
    cp "$PACKAGE_SOURCE_ROOT/$relpath" "$PACKAGE_DIR/$relpath"
  done
}

if command -v docker >/dev/null 2>&1; then
  COPY_SCRIPT=""
  for relpath in "${SOURCE_FILES[@]}"; do
    COPY_SCRIPT+="mkdir -p \"/var/task/package/$(dirname "$relpath")\" && "
    COPY_SCRIPT+="cp \"/var/task/source/$relpath\" \"/var/task/package/$relpath\" && "
  done

  docker run --rm \
    --user "$(id -u):$(id -g)" \
    -v "$PACKAGE_SOURCE_ROOT:/var/task/source:ro" \
    -v "$REQUIREMENTS_DIR:/var/task/requirements:ro" \
    -v "$PACKAGE_DIR:/var/task/package" \
    public.ecr.aws/sam/build-python"${PYTHON_VERSION}":latest \
    /bin/sh -c "pip install --no-cache-dir -r \"/var/task/requirements/$REQUIREMENTS_BASENAME\" -t /var/task/package && ${COPY_SCRIPT% && }"
else
  python"${PYTHON_VERSION}" -m pip install --no-cache-dir -r "$REQUIREMENTS_FILE" -t "$PACKAGE_DIR"
  copy_source
fi

mkdir -p "$(dirname "$OUTPUT_ZIP")"
rm -f "$OUTPUT_ZIP"

(
  cd "$PACKAGE_DIR"
  zip -qr "$OUTPUT_ZIP" .
)
