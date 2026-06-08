#!/usr/bin/env bash
# Download the Flink connector JARs required by the toll streaming job.
#
# These large binaries are intentionally NOT committed to the repo. Run this
# script once (locally and in CI) to populate the ./jars directory before
# running the streaming job or the integration tests.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
JARS_DIR="${FLINK_JARS_DIR:-${REPO_ROOT}/jars}"
MAVEN="https://repo1.maven.org/maven2"

# Pinned versions compatible with apache-flink==1.20.0.
KAFKA_JAR="flink-sql-connector-kafka-3.3.0-1.20.jar"
JDBC_JAR="flink-connector-jdbc-3.2.0-1.19.jar"
MYSQL_JAR="mysql-connector-j-8.0.33.jar"

mkdir -p "${JARS_DIR}"

download() {
  local url="$1" dest="$2"
  if [[ -f "${dest}" ]]; then
    echo "exists: $(basename "${dest}")"
  else
    echo "downloading: $(basename "${dest}")"
    curl -fsSL -o "${dest}" "${url}"
  fi
}

download "${MAVEN}/org/apache/flink/flink-sql-connector-kafka/3.3.0-1.20/${KAFKA_JAR}" "${JARS_DIR}/${KAFKA_JAR}"
download "${MAVEN}/org/apache/flink/flink-connector-jdbc/3.2.0-1.19/${JDBC_JAR}" "${JARS_DIR}/${JDBC_JAR}"
download "${MAVEN}/com/mysql/mysql-connector-j/8.0.33/${MYSQL_JAR}" "${JARS_DIR}/${MYSQL_JAR}"

echo "Connector JARs ready in ${JARS_DIR}"
