from databricks.connect import DatabricksSession
spark = DatabricksSession.builder.getOrCreate()
#spark = DatabricksSession.builder.serverless(True).getOrCreate()

from common.utils import  load_patientinfo

def main():
    df2 = load_patientinfo(spark)
    df2.show()
    df2.printSchema()

if __name__ == "__main__":
    main()

# spark.stop()