from databricks.connect import DatabricksSession
import os,time
from dotenv import load_dotenv

databricks_env_path = os.path.join(os.getcwd(), '.databricks', '.databricks.env')
load_dotenv(databricks_env_path)
cluster_id = os.environ.get("DATABRICKS_CLUSTER_ID")
spark = DatabricksSession.builder.clusterId(cluster_id).getOrCreate()

from common.utils import load_weatherinfo

def main():
    df1 = load_weatherinfo(spark)
    print(df1.count())
    
    df1.show()
    df1.printSchema()

if __name__ == "__main__":
    main()

# spark.stop()