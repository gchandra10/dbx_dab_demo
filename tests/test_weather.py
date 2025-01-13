import pytest
from databricks.connect import DatabricksSession
import sys
import os

databricks_env_path = os.path.join(os.getcwd(), '.databricks', '.databricks.env')

from dotenv import load_dotenv
load_dotenv(databricks_env_path)

sys.path.append('./src')
from common.utils import load_weatherinfo

@pytest.fixture(scope="module")
def spark_session():
    try:
        cluster_id = os.environ.get("DATABRICKS_CLUSTER_ID")
        return DatabricksSession.builder.clusterId(cluster_id).getOrCreate()
        #return DatabricksSession.builder.serverless(True).getOrCreate()
    except ImportError:
        print("No Databricks Connect, build and return local SparkSession")
        exit(1)

def test_row_count():
    df = load_weatherinfo(spark_session)
    assert df.count() >= 20

# def test_columns():
#     df = load_weatherinfo(spark_session)
#     assert df.columns == ['code', 'providence', 'date', 'avg_temp', 'min_temp', 'max_temp', 'precipitation', 'most_wind_direction', 'avg_relative_humidity'] 

# def test_wind_direction_range():
#     df = load_weatherinfo(spark_session)
    
#     """Test that all wind directions are within valid range (0-360 degrees)"""
#     # Check minimum value
#     assert df['most_wind_direction'].min() >= 0, \
#         f"Wind direction minimum ({df['most_wind_direction'].min()}) is less than 0 degrees"
    
#     # Check maximum value
#     assert df['most_wind_direction'].max() <= 360, \
#         f"Wind direction maximum ({df['most_wind_direction'].max()}) is greater than 360 degrees"

# def test_wind_direction_not_null():
#     df = load_weatherinfo(spark_session)
    
#     """Test that wind direction values are not null"""
#     assert not df['most_wind_direction'].isnull().any(), \
#         "Found null values in wind direction data"