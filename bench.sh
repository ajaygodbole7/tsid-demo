#!/usr/bin/env bash
# =============================================================================
# bench.sh — Run the TsidScalingBench main() with Java 26
# =============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

SDKMAN_JAVA="$HOME/.sdkman/candidates/java/current"
if [ ! -x "$SDKMAN_JAVA/bin/java" ]; then
  echo "ERROR: $SDKMAN_JAVA/bin/java is not executable." >&2
  echo "Install Java 26 via SDKMAN: sdk install java 26.0.1-amzn" >&2
  exit 1
fi
if ! "$SDKMAN_JAVA/bin/java" -version 2>&1 | grep -q '"26\.'; then
  echo "ERROR: SDKMAN current Java is not version 26." >&2
  echo "  Current: $("$SDKMAN_JAVA/bin/java" -version 2>&1 | head -1)" >&2
  echo "  Switch:  sdk use java 26.0.1-amzn" >&2
  exit 1
fi
export JAVA_HOME="$SDKMAN_JAVA"
export MAVEN_OPTS="${MAVEN_OPTS:-} --enable-final-field-mutation=ALL-UNNAMED"

cd "$SCRIPT_DIR"
exec mvn -q compile exec:java -Dexec.mainClass=com.example.tsid.TsidScalingBench
