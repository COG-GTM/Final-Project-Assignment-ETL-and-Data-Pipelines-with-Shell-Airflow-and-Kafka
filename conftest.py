"""Pytest configuration shared by the streaming tests.

Ensures the repo root is importable (so ``import streaming`` works) and defines
the ``kafka`` marker used by the optional real-broker end-to-end test.
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))


def pytest_configure(config):
    config.addinivalue_line(
        "markers",
        "kafka: end-to-end test requiring a real Kafka broker (KAFKA_BOOTSTRAP).",
    )
