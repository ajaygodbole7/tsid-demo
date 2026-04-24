#!/usr/bin/env bash
# =============================================================================
# run.sh — Run the TsidDemo main() with Java 26
# =============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Java 26 via SDKMAN current symlink (Corretto or OpenJDK, either works).
# macOS /usr/libexec/java_home does not index SDKMAN JDKs, so we resolve directly.
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

# Silence the Java 26 warning about reflective final-field mutation in older
# Maven plexus/sisu libs — harmless but noisy for a live demo.
export MAVEN_OPTS="${MAVEN_OPTS:-} --enable-final-field-mutation=ALL-UNNAMED"

cd "$SCRIPT_DIR"
exec mvn -q compile exec:java -Dexec.mainClass=com.example.tsid.TsidDemo
