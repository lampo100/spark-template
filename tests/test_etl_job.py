"""
test_etl_job.py
~~~~~~~~~~~~~~~

This module contains unit tests for the transformation steps of the ETL
job defined in etl_job.py. It makes use of a local version of PySpark
that is bundled with the PySpark package.
"""
import pytest
import json
from pyspark.sql.functions import mean
from jobs.etl_job import transform_data
from dependencies.spark import start_spark
from typing import Dict, Any
from pyspark.sql import SparkSession

class SparkTestObject:
    def __init__(self, config: Any, spark: SparkSession, test_data_path: str):
        self.config = config
        self.spark = spark
        self.test_data_path = test_data_path


@pytest.fixture()
def spark_test_object() -> SparkTestObject:
    """Start Spark, define config and path to test data
    """
    config = json.loads("""{"steps_per_floor": 21}""")
    spark_session, logger, c = start_spark()
    test_data_path = 'tests/test_data/'
    sp = SparkTestObject(config, spark_session, test_data_path)
    yield sp
    sp.spark.stop()

def test_transform_data(spark_test_object: SparkTestObject):
    """Test data transformer.
    Using small chunks of input data and expected output data, we
    test the transformation step to make sure it's working as
    expected.
    """
    # assemble
    input_data = (
        spark_test_object.spark
            .read
            .parquet(spark_test_object.test_data_path + 'employees'))

    expected_data = (
        spark_test_object.spark
            .read
            .parquet(spark_test_object.test_data_path + 'employees_report'))

    expected_cols = len(expected_data.columns)
    expected_rows = expected_data.count()
    expected_avg_steps = (
        expected_data
            .agg(mean('steps_to_desk').alias('avg_steps_to_desk'))
            .collect()[0]
        ['avg_steps_to_desk'])

    # act
    data_transformed = transform_data(input_data, 21)

    cols = len(expected_data.columns)
    rows = expected_data.count()
    avg_steps = (
        expected_data
            .agg(mean('steps_to_desk').alias('avg_steps_to_desk'))
            .collect()[0]
        ['avg_steps_to_desk'])

    # assert
    assert expected_cols == cols
    assert expected_rows == rows
    assert expected_avg_steps == avg_steps
    assert [col in expected_data.columns for col in data_transformed.columns]