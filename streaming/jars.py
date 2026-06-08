"""Locate the Flink connector JARs the streaming job depends on.

The connector JARs (Kafka, JDBC, MySQL driver) are *not* committed to the repo
because they are large binaries. ``scripts/download_connectors.sh`` downloads
them into the ``jars/`` directory at the repo root (this is also done in CI).

These helpers turn the on-disk JARs into the ``file://`` URLs that
``StreamExecutionEnvironment.add_jars`` expects, and fail loudly with an
actionable message if a required JAR is missing.
"""

from __future__ import annotations

import glob
import os
from typing import List

# Repo root = parent of this ``streaming`` package directory.
_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
JARS_DIR = os.environ.get("FLINK_JARS_DIR", os.path.join(_REPO_ROOT, "jars"))

# Glob patterns (version-independent) for each connector we may need.
KAFKA_CONNECTOR_GLOB = "flink-sql-connector-kafka-*.jar"
JDBC_CONNECTOR_GLOB = "flink-connector-jdbc-*.jar"
MYSQL_DRIVER_GLOB = "mysql-connector-j-*.jar"


def _find(pattern: str) -> List[str]:
    return sorted(glob.glob(os.path.join(JARS_DIR, pattern)))


def _to_url(path: str) -> str:
    return "file://" + os.path.abspath(path)


def has_kafka_connector() -> bool:
    return bool(_find(KAFKA_CONNECTOR_GLOB))


def kafka_jar_urls() -> List[str]:
    """Return ``file://`` URLs for the Kafka connector JAR.

    Raises:
        FileNotFoundError: if the Kafka connector JAR is not present.
    """
    matches = _find(KAFKA_CONNECTOR_GLOB)
    if not matches:
        raise FileNotFoundError(
            f"Kafka connector JAR ({KAFKA_CONNECTOR_GLOB}) not found in {JARS_DIR}. "
            "Run scripts/download_connectors.sh first."
        )
    return [_to_url(matches[-1])]


def jdbc_jar_urls() -> List[str]:
    """Return ``file://`` URLs for the JDBC connector + MySQL driver JARs.

    Raises:
        FileNotFoundError: if either JAR is not present.
    """
    jdbc = _find(JDBC_CONNECTOR_GLOB)
    driver = _find(MYSQL_DRIVER_GLOB)
    missing = []
    if not jdbc:
        missing.append(JDBC_CONNECTOR_GLOB)
    if not driver:
        missing.append(MYSQL_DRIVER_GLOB)
    if missing:
        raise FileNotFoundError(
            f"JDBC sink JAR(s) {missing} not found in {JARS_DIR}. "
            "Run scripts/download_connectors.sh first."
        )
    return [_to_url(jdbc[-1]), _to_url(driver[-1])]
