# mimic_explorer_csv.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg, min, max, stddev, desc
import time
import os

def initialize_spark():
    '''
    Initialize Spark Session

    Args: 
        -None
    Output: 
        -Spark session
    '''
    spark = SparkSession.builder \
                        .appName("MIMIC-III LOS Prediction") \
                        .config("spark.driver.memory", "12g") \
                        .config("spark.executor.memory", "12g") \
                        .config("spark.driver.maxResultSize", "6g") \
                        .getOrCreate()

    # Check initialization
    print("Success! Spark is working.")
    print(f"Spark version: {spark.version}\n")

    return spark

def load_csv_table(spark, csv_dir, table_name):
    """Load a CSV table with performance timing."""
    csv_path = os.path.join(csv_dir, f"{table_name.upper()}.csv")
    
    print(f"Loading {table_name} from CSV...")
    start_time = time.time()
    
    
    df = spark.read.csv(csv_path, header=True, inferSchema=True)
    
    # Force action to measure full load time
    count = df.count()
    end_time = time.time()
    
    print(f"Loaded {table_name}: {count} rows in {end_time - start_time:.2f} seconds")
    return df

if __name__ == "__main__":

    

    spark = initialize_spark()

    # Load tables
    admissions_df = load_csv_table(spark, "../data/raw", "admissions")
    print("\n")
    callout_df = load_csv_table(spark, "../data/raw", "callout")
    print("\n")
    chart_events_df = load_csv_table(spark, "../data/raw", "chartevents")
    print("\n")
    d_icd_diagnoses = load_csv_table(spark, "../data/raw", "d_icd_diagnoses")
    print("\n")
    datetime_events = load_csv_table(spark, "../data/raw", "datetimeevents")
    print("\n")
    diagnoses_icd_df = load_csv_table(spark, "../data/raw", "diagnoses_icd")
    print("\n")
    icustays_df = load_csv_table(spark, "../data/raw", "icustays")
    print("\n")
    patients_df = load_csv_table(spark, "../data/raw", "patients")
    print("\n")
    services_df = load_csv_table(spark, "../data/raw", "services")

