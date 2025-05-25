# feature_engineering_csv.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, datediff, hour, when, expr, lit
import time
import os

def initialize_spark(app_name="MIMIC-III CSV Feature Engineering", memory="10g"):
    """Initialize Spark session with appropriate configuration."""
    spark = SparkSession.builder \
        .appName(app_name) \
        .config("spark.driver.memory", memory) \
        .config("spark.executor.memory", memory) \
        .getOrCreate()
    
    return spark

def load_csv_table(spark, csv_dir, table_name):
    """Load a CSV table with performance timing."""
    csv_path = os.path.join(csv_dir, f"{table_name.upper()}.csv")
    
    print(f"Loading {table_name} from CSV...")
    start_time = time.time()
    
    # For large tables, reduce header and schema inference sampling
    if table_name in ["chartevents", "labevents"]:
        df = spark.read.csv(
            csv_path,
            header=True,
            inferSchema=True,
            samplingRatio=0.1  # Sample for schema inference
        )
    else:
        df = spark.read.csv(csv_path, header=True, inferSchema=True)
    
    # For measurement tables, cache them to improve performance
    if table_name in ["icustays", "patients", "admissions"]:
        df.cache()
    
    # Force action to measure full load time
    count = df.count()
    end_time = time.time()
    
    print(f"Loaded {table_name}: {count} rows in {end_time - start_time:.2f} seconds")
    return df

def create_base_features(spark, csv_dir, window_hours=24):
    """
    Create base features for length of stay prediction.
    
    Args:
        spark: SparkSession
        csv_dir: Directory containing CSV files
        window_hours: Number of hours to use for feature extraction
    
    Returns:
        DataFrame with base features
    """
    # Load core tables
    patients = load_csv_table(spark, csv_dir, "patients")
    admissions = load_csv_table(spark, csv_dir, "admissions")
    icustays = load_csv_table(spark, csv_dir, "icustays")
    
    # Register tables for SQL
    patients.createOrReplaceTempView("patients")
    admissions.createOrReplaceTempView("admissions")
    icustays.createOrReplaceTempView("icustays")
    
    # Create base features using SQL for readability
    print("Creating base features...")
    base_query = """
    SELECT 
        i.ICUSTAY_ID,
        i.SUBJECT_ID,
        i.HADM_ID,
        p.GENDER,
        DATEDIFF(a.ADMITTIME, p.DOB) / 365.25 AS AGE,
        a.ADMISSION_TYPE,
        i.FIRST_CAREUNIT,
        i.LOS AS TOTAL_LOS,
        -- Target: Remaining LOS after window_hours
        GREATEST(i.LOS - (%d / 24.0), 0) AS REMAINING_LOS,
        a.HOSPITAL_EXPIRE_FLAG,
        hour(a.ADMITTIME) AS ADMISSION_HOUR,
        CASE 
            WHEN LOWER(a.ADMISSION_TYPE) LIKE '%%emerg%%' THEN 1 
            ELSE 0 
        END AS IS_EMERGENCY,
        CASE
            WHEN i.FIRST_CAREUNIT = i.LAST_CAREUNIT THEN 1
            ELSE 0
        END AS SAME_UNIT_STAY
    FROM icustays i
    JOIN patients p ON i.SUBJECT_ID = p.SUBJECT_ID
    JOIN admissions a ON i.HADM_ID = a.HADM_ID
    WHERE i.LOS IS NOT NULL AND i.LOS > 0
    """ % window_hours
    
    base_features = spark.sql(base_query)
    
    # Cache for better performance
    base_features.cache()
    base_count = base_features.count()
    print(f"Created base features for {base_count} ICU stays")
    
    return base_features

def add_vitals_sample(spark, csv_dir, base_features, window_hours=24):
    """
    Add a sample of vital signs features from CHARTEVENTS (using a subset for performance).
    
    Args:
        spark: SparkSession
        csv_dir: Directory containing CSV files
        base_features: Base feature DataFrame
        window_hours: Time window for feature extraction
    
    Returns:
        DataFrame with added vital sign features
    """
    # Register base features for SQL
    base_features.createOrReplaceTempView("base_features")
    
    # Measure performance
    start_time = time.time()
    
    # Start by loading specific vitals first - focus on heart rate
    # This is more efficient than loading the entire CHARTEVENTS table
    vitals_query = f"""
    WITH heart_rate_data AS (
      SELECT 
        c.ICUSTAY_ID,
        c.CHARTTIME,
        c.ITEMID,
        CAST(c.VALUENUM AS DOUBLE) as VALUENUM
      FROM csv.`{os.path.join(csv_dir, "CHARTEVENTS.csv")}` c
      JOIN base_features bf ON c.ICUSTAY_ID = bf.ICUSTAY_ID
      JOIN icustays i ON bf.ICUSTAY_ID = i.ICUSTAY_ID
      WHERE 
        c.ITEMID IN (211, 220045) -- Heart rate ITEMIDs
        AND c.VALUENUM IS NOT NULL
        AND c.VALUENUM > 0
        AND c.VALUENUM < 300  -- Reasonable upper limit
        AND c.CHARTTIME BETWEEN i.INTIME AND date_add(i.INTIME, {window_hours/24})
    )
    
    SELECT
      bf.ICUSTAY_ID,
      AVG(hd.VALUENUM) as HR_MEAN,
      MIN(hd.VALUENUM) as HR_MIN,
      MAX(hd.VALUENUM) as HR_MAX
    FROM base_features bf
    LEFT JOIN heart_rate_data hd ON bf.ICUSTAY_ID = hd.ICUSTAY_ID
    GROUP BY bf.ICUSTAY_ID
    """
    
    # Execute query
    print("Extracting heart rate features...")
    hr_features = spark.sql(vitals_query)
    hr_count = hr_features.count()
    
    # Join with base features
    enhanced_features = base_features.join(
        hr_features.select("ICUSTAY_ID", "HR_MEAN", "HR_MIN", "HR_MAX"),
        "ICUSTAY_ID",
        "left"
    )
    
    end_time = time.time()
    print(f"Added vital sign features in {end_time - start_time:.2f} seconds")
    print(f"Feature counts: Heart rate data for {hr_count} ICU stays")
    
    return enhanced_features

def add_sample_lab_features(spark, csv_dir, base_features, window_hours=24):
    """
    Add a sample of laboratory test features (using a subset for performance).
    
    Args:
        spark: SparkSession
        csv_dir: Directory containing CSV files
        base_features: Base feature DataFrame
        window_hours: Time window for feature extraction
    
    Returns:
        DataFrame with added lab features
    """
    # Register base features for SQL
    base_features.createOrReplaceTempView("base_features")
    
    # Measure performance
    start_time = time.time()
    
    # Focus on a few key lab tests first
    # Example: Creatinine (ITEMID: 50912)
    lab_query = f"""
    WITH creatinine_data AS (
      SELECT 
        l.HADM_ID,
        l.CHARTTIME,
        l.ITEMID,
        CAST(l.VALUENUM AS DOUBLE) as VALUENUM
      FROM csv.`{os.path.join(csv_dir, "LABEVENTS.csv")}` l
      JOIN base_features bf ON l.HADM_ID = bf.HADM_ID
      JOIN icustays i ON bf.ICUSTAY_ID = i.ICUSTAY_ID
      WHERE 
        l.ITEMID = 50912  -- Creatinine
        AND l.VALUENUM IS NOT NULL
        AND l.VALUENUM > 0
        AND l.CHARTTIME BETWEEN i.INTIME AND date_add(i.INTIME, {window_hours/24})
    )
    
    SELECT
      bf.ICUSTAY_ID,
      AVG(cd.VALUENUM) as CREATININE_MEAN,
      MAX(cd.VALUENUM) as CREATININE_MAX
    FROM base_features bf
    LEFT JOIN creatinine_data cd ON bf.HADM_ID = cd.HADM_ID
    GROUP BY bf.ICUSTAY_ID
    """
    
    # Execute query
    print("Extracting creatinine lab features...")
    lab_features = spark.sql(lab_query)
    lab_count = lab_features.count()
    
    # Join with base features
    enhanced_features = base_features.join(
        lab_features.select("ICUSTAY_ID", "CREATININE_MEAN", "CREATININE_MAX"),
        "ICUSTAY_ID",
        "left"
    )
    
    end_time = time.time()
    print(f"Added lab features in {end_time - start_time:.2f} seconds")
    print(f"Feature counts: Creatinine data for {lab_count} ICU stays")
    
    return enhanced_features

def add_diagnoses_features(spark, csv_dir, base_features):
    """
    Add diagnosis-based features.
    
    Args:
        spark: SparkSession
        csv_dir: Directory containing CSV files
        base_features: Base feature DataFrame
    
    Returns:
        DataFrame with added diagnosis features
    """
    # Load diagnoses tables
    diagnoses_icd = load_csv_table(spark, csv_dir, "diagnoses_icd")
    d_icd_diagnoses = load_csv_table(spark, csv_dir, "d_icd_diagnoses")
    
    # Register tables for SQL
    diagnoses_icd.createOrReplaceTempView("diagnoses_icd")
    d_icd_diagnoses.createOrReplaceTempView("d_icd_diagnoses")
    base_features.createOrReplaceTempView("base_features")
    
    # Measure performance
    start_time = time.time()
    
    # Create features for major disease categories
    diag_query = """
    WITH patient_diagnoses AS (
      SELECT 
        di.SUBJECT_ID,
        di.HADM_ID,
        d.SHORT_TITLE,
        CASE 
          WHEN LOWER(d.SHORT_TITLE) LIKE '%cardiac%' OR 
               LOWER(d.SHORT_TITLE) LIKE '%heart%' OR
               LOWER(d.SHORT_TITLE) LIKE '%myo%infarct%' OR
               LOWER(d.SHORT_TITLE) LIKE '%coronary%' THEN 'cardiac'
          WHEN LOWER(d.SHORT_TITLE) LIKE '%pneumonia%' OR
               LOWER(d.SHORT_TITLE) LIKE '%respiratory%' OR
               LOWER(d.SHORT_TITLE) LIKE '%copd%' OR
               LOWER(d.SHORT_TITLE) LIKE '%lung%' THEN 'respiratory'
          WHEN LOWER(d.SHORT_TITLE) LIKE '%renal%' OR
               LOWER(d.SHORT_TITLE) LIKE '%kidney%' THEN 'renal'
          WHEN LOWER(d.SHORT_TITLE) LIKE '%sepsis%' OR
               LOWER(d.SHORT_TITLE) LIKE '%septic%' OR
               LOWER(d.SHORT_TITLE) LIKE '%infection%' THEN 'infectious'
          WHEN LOWER(d.SHORT_TITLE) LIKE '%diabetes%' OR
               LOWER(d.SHORT_TITLE) LIKE '%diabetic%' THEN 'diabetes'
          WHEN LOWER(d.SHORT_TITLE) LIKE '%stroke%' OR
               LOWER(d.SHORT_TITLE) LIKE '%cerebral%' THEN 'stroke'
          WHEN LOWER(d.SHORT_TITLE) LIKE '%trauma%' OR
               LOWER(d.SHORT_TITLE) LIKE '%fracture%' OR
               LOWER(d.SHORT_TITLE) LIKE '%injury%' THEN 'trauma'
          ELSE 'other'
        END as CATEGORY
      FROM diagnoses_icd di
      JOIN d_icd_diagnoses d ON di.ICD9_CODE = d.ICD9_CODE
    )
    
    SELECT 
      bf.ICUSTAY_ID,
      MAX(CASE WHEN pd.CATEGORY = 'cardiac' THEN 1 ELSE 0 END) as HAS_CARDIAC,
      MAX(CASE WHEN pd.CATEGORY = 'respiratory' THEN 1 ELSE 0 END) as HAS_RESPIRATORY,
      MAX(CASE WHEN pd.CATEGORY = 'renal' THEN 1 ELSE 0 END) as HAS_RENAL,
      MAX(CASE WHEN pd.CATEGORY = 'infectious' THEN 1 ELSE 0 END) as HAS_INFECTIOUS,
      MAX(CASE WHEN pd.CATEGORY = 'diabetes' THEN 1 ELSE 0 END) as HAS_DIABETES,
      MAX(CASE WHEN pd.CATEGORY = 'stroke' THEN 1 ELSE 0 END) as HAS_STROKE,
      MAX(CASE WHEN pd.CATEGORY = 'trauma' THEN 1 ELSE 0 END) as HAS_TRAUMA,
      COUNT(DISTINCT pd.CATEGORY) as DISEASE_COUNT
    FROM base_features bf
    JOIN patient_diagnoses pd ON bf.HADM_ID = pd.HADM_ID
    GROUP BY bf.ICUSTAY_ID
    """
    
    # Execute query
    print("Extracting diagnosis features...")
    diag_features = spark.sql(diag_query)
    diag_count = diag_features.count()
    
    # Join with base features
    enhanced_features = base_features.join(
        diag_features,
        "ICUSTAY_ID",
        "left"
    )
    
    end_time = time.time()
    print(f"Added diagnosis features in {end_time - start_time:.2f} seconds")
    print(f"Feature counts: Diagnosis data for {diag_count} ICU stays")
    
    return enhanced_features

def save_features(features_df, output_path):
    """
    Save the features DataFrame to disk.
    
    Args:
        features_df: DataFrame with features
        output_path: Path to save the features
    """
    # Save as CSV for now (will compare with Parquet later)
    print(f"Saving features to {output_path}...")
    start_time = time.time()
    
    features_df.write.csv(output_path, mode="overwrite", header=True)
    
    end_time = time.time()
    print(f"Saved features in {end_time - start_time:.2f} seconds")
    print(f"Total features: {len(features_df.columns)}")
    
    # Also save as small sample for easy inspection
    sample_path = f"{output_path}_sample"
    features_df.limit(1000).write.csv(sample_path, mode="overwrite", header=True)
    print(f"Saved sample to {sample_path}")

def main():
    """Main function to build feature dataset from CSV files."""
    spark = initialize_spark()
    
    csv_dir = input("Enter path to MIMIC-III CSV files: ")
    output_path = input("Enter path to save feature dataset: ")
    window_hours = int(input("Enter window hours for feature extraction (e.g., 24): "))
    
    print(f"Creating features using first {window_hours} hours of data...")
    
    # Measure total time
    total_start_time = time.time()
    
    # Create base features
    base_features = create_base_features(spark, csv_dir, window_hours)
    
    # Add vital sign features (sample)
    features_with_vitals = add_vitals_sample(spark, csv_dir, base_features, window_hours)
    
    # Add lab features (sample)
    features_with_labs = add_sample_lab_features(spark, csv_dir, features_with_vitals, window_hours)
    
    # Add diagnosis features
    final_features = add_diagnoses_features(spark, csv_dir, features_with_labs)
    
    # Save features
    save_features(final_features, output_path)
    
    total_end_time = time.time()
    print(f"Total feature engineering time: {total_end_time - total_start_time:.2f} seconds")
    
    spark.stop()
    print("Feature engineering completed.")

if __name__ == "__main__":
    main()