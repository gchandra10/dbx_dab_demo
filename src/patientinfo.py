# from databricks.connect import DatabricksSession
#spark = DatabricksSession.builder.getOrCreate()
#spark = DatabricksSession.builder.serverless(True).getOrCreate()

import sys

from common.utils import get_spark_session
spark = get_spark_session()

from common.utils import  load_patientinfo

def main():
    df2 = load_patientinfo(spark)
    df2.show()
    df2.printSchema()


if __name__ == "__main__":
    gcparam1 = sys.argv[1]
    gcparam2 = sys.argv[2]
    print(f"from JOB param {gcparam1} - {gcparam2}",  )
    
    #main()
