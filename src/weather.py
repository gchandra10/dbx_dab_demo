from databricks.connect import DatabricksSession
spark = DatabricksSession.builder.getOrCreate()
#spark = DatabricksSession.builder.serverless(True).getOrCreate()

from common.utils import load_weatherinfo

def main():
    df1 = load_weatherinfo(spark)
    df1.show()
    df1.printSchema()

if __name__ == "__main__":
    main()

# spark.stop()