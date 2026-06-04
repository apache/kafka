#!/usr/bin/env bash
# =============================================================================
# setup.sh — one-time bootstrap on a fresh checkout
#
# The Apache Kafka repository intentionally does NOT commit
# gradle/wrapper/gradle-wrapper.jar. This script obtains it so `./gradlew` works.
# Requires network access and a JDK 17+ on PATH.
# =============================================================================
set -euo pipefail

cd "$(dirname "$0")"

WRAPPER_JAR="gradle/wrapper/gradle-wrapper.jar"
GRADLE_VERSION="9.4.1"

if [ -f "$WRAPPER_JAR" ]; then
  echo "[setup] $WRAPPER_JAR already present — nothing to do."
  exit 0
fi

echo "[setup] gradle-wrapper.jar is missing (Kafka does not ship it). Bootstrapping..."

if command -v gradle >/dev/null 2>&1; then
  # If a system Gradle is available, let it generate the wrapper.
  echo "[setup] Found system gradle; generating wrapper $GRADLE_VERSION"
  gradle wrapper --gradle-version "$GRADLE_VERSION"
else
  # Otherwise download the official wrapper jar for the pinned version.
  echo "[setup] No system gradle found; downloading the official wrapper jar"
  URL="https://raw.githubusercontent.com/gradle/gradle/v${GRADLE_VERSION}/gradle/wrapper/gradle-wrapper.jar"
  if command -v curl >/dev/null 2>&1; then
    curl -fL "$URL" -o "$WRAPPER_JAR"
  elif command -v wget >/dev/null 2>&1; then
    wget -O "$WRAPPER_JAR" "$URL"
  else
    echo "[setup] ERROR: need curl or wget, or a system 'gradle' install." >&2
    exit 1
  fi
fi

echo "[setup] Done. You can now run: ./gradlew :connect:mirror:jar"
