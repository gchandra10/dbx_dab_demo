from databricks.connect import DatabricksSession
import os,time
from dotenv import load_dotenv
from common.utils import load_us_states

databricks_env_path = os.path.join(os.getcwd(), '.databricks', '.databricks.env')
load_dotenv(databricks_env_path)
cluster_id = os.environ.get("DATABRICKS_CLUSTER_ID")

spark = DatabricksSession.builder.clusterId(cluster_id).getOrCreate()

#spark = DatabricksSession.builder.serverless(True).getOrCreate()

def get_us_states():
    return load_us_states(spark)

def main():
    df = get_us_states()
    df.show()
    
if __name__ == "__main__":
    start_time = time.time()
    main()
    end_time = time.time()
    time_taken = end_time - start_time
    print(f"Time taken: {time_taken:.2f} seconds")