from databricks.connect import DatabricksSession
spark = DatabricksSession.builder.getOrCreate()
#spark = DatabricksSession.builder.serverless(True).getOrCreate()

from common.utils import load_us_states

def get_us_states():
    return load_us_states(spark)

def main():
    df = get_us_states()
    #df.show()
    #df.printSchema()
    df.createOrReplaceTempView("us_states")
    spark.sql("SELECT * FROM us_states").show()
    spark.sql("select state, count(*) as count from us_states group by state order by state").show()
    
if __name__ == "__main__":
    main()

# spark.stop()