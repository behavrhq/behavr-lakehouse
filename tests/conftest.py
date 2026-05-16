import pytest
from py4j.protocol import Py4JJavaError
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    try:
        return (
            SparkSession.builder.master("local[1]")
            .appName("behavr-lakehouse-tests")
            .config("spark.sql.shuffle.partitions", "2")
            .config("spark.sql.session.timeZone", "UTC")
            .getOrCreate()
        )
    except (Py4JJavaError, Exception) as e:
        pytest.skip(f"Local Spark unavailable (use JDK 17–21 for local PySpark): {e}")
