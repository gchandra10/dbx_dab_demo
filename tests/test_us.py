import pytest
from databricks.connect import DatabricksSession
from pyspark.testing.utils import assertSchemaEqual 
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import sys, os

databricks_env_path = os.path.join(os.getcwd(), '.databricks', '.databricks.env')

from dotenv import load_dotenv
load_dotenv(databricks_env_path)

sys.path.append('./src')
from common.utils import *

@pytest.fixture(scope="module")
def spark_session():
    try:
        cluster_id = os.environ.get("DATABRICKS_CLUSTER_ID")
        return DatabricksSession.builder.clusterId(cluster_id).getOrCreate()
        #return DatabricksSession.builder.serverless(True).getOrCreate()
    except ImportError:
        print("No Databricks Connect, build and return local SparkSession")
        exit(1)
    
def test_row_count(spark_session):
    df = load_us_states(spark_session)
    assert df.count() >= 5

def test_second_row(spark_session):
    df = load_us_states(spark_session)
    assert df.collect()[1]['state'] == 'Washington'

def test_columns(spark_session):
    df = load_us_states(spark_session)
    assert df.columns == ['date', 'state', 'fips', 'cases', 'deaths'] 

def test_schema(spark_session):
    df = load_us_states(spark_session)
    schema = StructType([
        StructField("date", StringType(), True),
        StructField("state", StringType(), True),
        StructField("fips", StringType(), True),
        StructField("cases", StringType(), True),
        StructField("deaths", StringType(), True)
    ])
    assertSchemaEqual (df.schema, schema)