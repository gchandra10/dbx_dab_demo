from pyspark.sql import SparkSession

def load_us_states(spark):
    df = spark.read.format("csv").load("dbfs:/databricks-datasets/COVID/covid-19-data/us-states.csv", header=True)
    return df

def load_weatherinfo(spark):
    df1 = spark.read.format("csv").load("dbfs:/databricks-datasets/COVID/coronavirusdataset/Weather.csv", header=True)
    return df1

def load_patientinfo(spark):
    df2 = spark.read.format("csv").load("dbfs:/databricks-datasets/COVID/coronavirusdataset/PatientRoute.csv", header=True)
    return df2

def get_spark_session():
    return SparkSession.builder.appName("COVID-19").getOrCreate()
