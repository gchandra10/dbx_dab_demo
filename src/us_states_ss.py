from pyspark.sql.session import SparkSession
spark = SparkSession.builder.appName("COVID-19").getOrCreate()
import time

from common.utils import load_us_states

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