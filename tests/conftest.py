import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "spark_jobs"))


@pytest.fixture(scope="session")
def spark():
    """One local SparkSession for the whole run: JVM startup is the slow part."""
    from pyspark.sql import SparkSession

    s = (SparkSession.builder.master("local[1]").appName("tests")
         .config("spark.sql.shuffle.partitions", "1")  # tiny data, don't spawn 200 tasks
         .config("spark.ui.enabled", "false")
         .config("spark.sql.session.timeZone", "UTC")
         .getOrCreate())
    yield s
    s.stop()