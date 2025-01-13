def load_us_states(spark):
    df = spark.read.csv("dbfs:/databricks-datasets/COVID/covid-19-data/us-states.csv", header=True)
    return df

def load_weatherinfo(spark):
    df1 = spark.read.csv("dbfs:/databricks-datasets/COVID/coronavirusdataset/Weather.csv", header=True)
    return df1

def load_patientinfo(spark):
    df2 = spark.read.csv("dbfs:/databricks-datasets/COVID/coronavirusdataset/PatientRoute.csv", header=True)
    return df2

