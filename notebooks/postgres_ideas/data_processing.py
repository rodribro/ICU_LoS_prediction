import psycopg2
from psycopg2 import sql

def mimic_user_creation(host_user: str, db_name: str) -> dict[str, str]:
    '''
    Connects to PostGreSQL default database through super user.
    Then, creates a user with CREATEDB priviliges, which will be used to create and store the MIMICIII database.

    Args:
        -host_user: string (super user created at the time of installation)
        -db_name: string (postgres by deafult)

    Output:
        -No output
    '''
    # Admin connection (connect as the default PostgreSQL superuser)
    conn = psycopg2.connect(dbname="postgres", user="rodrigocastro")  # No password needed if peer authentication is enabled
    conn.autocommit = True
    cur = conn.cursor()

    # Fetch the current user
    cur.execute("SELECT current_user;")
    current_user = cur.fetchone()[0]

    print(f"Connected to PostgreSQL successfully as user: {current_user}\n")

    # New user credentials and new database name
    print(f"[Creating new user for ICU LoS database]\n")
    new_db_user = input("Enter new username:")

    # Query to check if new user exists
    cur.execute("SELECT 1 FROM pg_roles WHERE rolname = %s;\n", (new_db_user,))
    user_exists = cur.fetchone() is not None
    
    if user_exists:
        print(f"User '{new_db_user}' has already been created. Try a new username.\n")
        new_db_user = input("Enter new username:")
        new_db_password = input("Enter password for new user:")

        cur.execute(
        sql.SQL("CREATE ROLE {} WITH LOGIN PASSWORD %s CREATEDB;").format(sql.Identifier(new_db_user)),
        [new_db_password])

        print(f"User '{new_db_user}' created successfuly\n")
    
    else:
        new_db_password = input("Enter password for new user:")
        print(f"User '{new_db_user}' will be created.\n")
    
        cur.execute(
        sql.SQL("CREATE ROLE {} WITH LOGIN PASSWORD %s CREATEDB;").format(sql.Identifier(new_db_user)),
        [new_db_password])

        print(f"User '{new_db_user}' created successfuly\n")


    print(f"Closing connection to database")
    cur.close()
    conn.close()

    return {"new_user":new_db_user,"new_password":new_db_password}

def create_mimic_iii_db(new_user:str, new_password:str) -> None:
    
    # MIMIC III database
    mimic_db = "mimic_iii"

    conn = psycopg2.connect(dbname="postgres", user=new_user, password=new_password, host="localhost", port="5432")
    conn.autocommit = True  # Database cannot be inside a transaction
    cur = conn.cursor()


    cur.execute(f"CREATE DATABASE {mimic_db};")
    print(f"Database '{mimic_db}' created successfully by '{new_user}'.")

    # Cleanup
    cur.close()
    conn.close()

    return mimic_db

def create_mimic_iii_db(new_user:str, new_password:str, db_name:str="mimiciii") -> str:

    conn = psycopg2.connect(dbname="postgres", user=new_user, password=new_password, host="localhost", port="5432")
    conn.autocommit = True  # Database cannot be inside a transaction
    cur = conn.cursor()

    # Check if database already exists
    cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (db_name,))
    db_exists = cur.fetchone() is not None
    
    if not db_exists:
        cur.execute(f"CREATE DATABASE {db_name};")
        print(f"Database '{db_name}' created successfully by '{new_user}'.")
    else:
        print(f"Database '{db_name}' already exists.")

    # Cleanup
    cur.close()
    conn.close()
    
    return db_name

def create_mimic_schema(db_name: str, new_user: str, new_password: str) -> None:
    """
    Creates the MIMIC-III schema with all the necessary tables, indexes, and constraints.
    
    Args:
        db_name: string (name of the MIMIC-III database)
        new_user: string (username to connect to the database)
        new_password: string (password for the user)
    """
    print(f"\nConnecting to {db_name} database to create schema...")
    conn = psycopg2.connect(dbname=db_name, user=new_user, password=new_password, host="localhost", port="5432")
    conn.autocommit = False  # We want to use transactions for table creation
    cur = conn.cursor()
    
    try:
        # Create the mimiciii schema
        print("Creating MIMICIII schema...")
        cur.execute("CREATE SCHEMA IF NOT EXISTS mimiciii;")
        cur.execute("SET search_path TO mimiciii;")
        
        # Create tables
        print("Creating PATIENTS table...")
        cur.execute("""
        CREATE TABLE IF NOT EXISTS patients (
            row_id INT NOT NULL,
            subject_id INT NOT NULL,
            gender VARCHAR(5) NOT NULL,
            dob TIMESTAMP(0) NOT NULL,
            dod TIMESTAMP(0),
            dod_hosp TIMESTAMP(0),
            dod_ssn TIMESTAMP(0),
            expire_flag VARCHAR(5) NOT NULL,
            CONSTRAINT patients_pk PRIMARY KEY (subject_id)
        );
        """)
        
        print("Creating ADMISSIONS table...")
        cur.execute("""
        CREATE TABLE IF NOT EXISTS admissions (
            row_id INT NOT NULL,
            subject_id INT NOT NULL,
            hadm_id INT NOT NULL,
            admittime TIMESTAMP(0) NOT NULL,
            dischtime TIMESTAMP(0),
            deathtime TIMESTAMP(0),
            admission_type VARCHAR(50) NOT NULL,
            admission_location VARCHAR(50) NOT NULL,
            discharge_location VARCHAR(50),
            insurance VARCHAR(255) NOT NULL,
            language VARCHAR(10),
            religion VARCHAR(50),
            marital_status VARCHAR(50),
            ethnicity VARCHAR(200) NOT NULL,
            edregtime TIMESTAMP(0),
            edouttime TIMESTAMP(0),
            diagnosis VARCHAR(300),
            hospital_expire_flag SMALLINT,
            has_chartevents_data SMALLINT,
            CONSTRAINT admissions_pk PRIMARY KEY (hadm_id),
            CONSTRAINT admissions_fk_patients FOREIGN KEY (subject_id) REFERENCES patients(subject_id)
        );
        CREATE INDEX IF NOT EXISTS admissions_idx01 ON admissions (subject_id);
        """)
        
        print("Creating ICUSTAYS table...")
        cur.execute("""
        CREATE TABLE IF NOT EXISTS icustays (
            row_id INT NOT NULL,
            subject_id INT NOT NULL,
            hadm_id INT NOT NULL,
            icustay_id INT NOT NULL,
            dbsource VARCHAR(20) NOT NULL,
            first_careunit VARCHAR(20) NOT NULL,
            last_careunit VARCHAR(20) NOT NULL,
            first_wardid SMALLINT NOT NULL,
            last_wardid SMALLINT NOT NULL,
            intime TIMESTAMP(0) NOT NULL,
            outtime TIMESTAMP(0),
            los DOUBLE PRECISION,
            CONSTRAINT icustays_pk PRIMARY KEY (icustay_id),
            CONSTRAINT icustays_fk_patients FOREIGN KEY (subject_id) REFERENCES patients(subject_id),
            CONSTRAINT icustays_fk_admissions FOREIGN KEY (hadm_id) REFERENCES admissions(hadm_id)
        );
        CREATE INDEX IF NOT EXISTS icustays_idx01 ON icustays (subject_id);
        CREATE INDEX IF NOT EXISTS icustays_idx02 ON icustays (hadm_id);
        CREATE INDEX IF NOT EXISTS icustays_idx03 ON icustays (icustay_id, intime, outtime);
        """)
        
        print("Creating D_ITEMS table...")
        cur.execute("""
        CREATE TABLE IF NOT EXISTS d_items (
            row_id INT NOT NULL,
            itemid INT NOT NULL,
            label VARCHAR(200) NOT NULL,
            abbreviation VARCHAR(100),
            dbsource VARCHAR(20) NOT NULL,
            linksto VARCHAR(50),
            category VARCHAR(100),
            unitname VARCHAR(100),
            param_type VARCHAR(30),
            conceptid INT,
            CONSTRAINT d_items_pk PRIMARY KEY (itemid)
        );
        CREATE INDEX IF NOT EXISTS d_items_idx01 ON d_items (category);
        CREATE INDEX IF NOT EXISTS d_items_idx02 ON d_items (label);
        """)
        
        print("Creating D_LABITEMS table...")
        cur.execute("""
        CREATE TABLE IF NOT EXISTS d_labitems (
            row_id INT NOT NULL,
            itemid INT NOT NULL,
            label VARCHAR(100) NOT NULL,
            fluid VARCHAR(100) NOT NULL,
            category VARCHAR(100) NOT NULL,
            loinc_code VARCHAR(100),
            CONSTRAINT d_labitems_pk PRIMARY KEY (itemid)
        );
        CREATE INDEX IF NOT EXISTS d_labitems_idx01 ON d_labitems (category);
        CREATE INDEX IF NOT EXISTS d_labitems_idx02 ON d_labitems (label);
        """)
        
        print("Creating CAREGIVERS table...")
        cur.execute("""
        CREATE TABLE IF NOT EXISTS caregivers (
            row_id INT NOT NULL,
            cgid INT NOT NULL,
            label VARCHAR(15),
            description VARCHAR(30),
            CONSTRAINT caregivers_pk PRIMARY KEY (cgid)
        );
        """)
        
        print("Creating D_ICD_DIAGNOSES table...")
        cur.execute("""
        CREATE TABLE IF NOT EXISTS d_icd_diagnoses (
            row_id INT NOT NULL,
            icd9_code VARCHAR(10) NOT NULL,
            short_title VARCHAR(50) NOT NULL,
            long_title VARCHAR(300) NOT NULL,
            CONSTRAINT d_icd_diagnoses_pk PRIMARY KEY (icd9_code)
        );
        CREATE INDEX IF NOT EXISTS d_icd_diagnoses_idx01 ON d_icd_diagnoses (short_title);
        """)
        
        print("Creating DIAGNOSES_ICD table...")
        cur.execute("""
        CREATE TABLE IF NOT EXISTS diagnoses_icd (
            row_id INT NOT NULL,
            subject_id INT NOT NULL,
            hadm_id INT NOT NULL,
            seq_num INT,
            icd9_code VARCHAR(10),
            CONSTRAINT diagnoses_icd_fk_patients FOREIGN KEY (subject_id) REFERENCES patients(subject_id),
            CONSTRAINT diagnoses_icd_fk_admissions FOREIGN KEY (hadm_id) REFERENCES admissions(hadm_id),
            CONSTRAINT diagnoses_icd_fk_icd9 FOREIGN KEY (icd9_code) REFERENCES d_icd_diagnoses(icd9_code)
        );
        CREATE INDEX IF NOT EXISTS diagnoses_icd_idx01 ON diagnoses_icd (subject_id, hadm_id);
        CREATE INDEX IF NOT EXISTS diagnoses_icd_idx02 ON diagnoses_icd (icd9_code);
        """)
        
        print("Creating CALLOUT table...")
        cur.execute("""
        CREATE TABLE IF NOT EXISTS callout (
            row_id INT NOT NULL,
            subject_id INT NOT NULL,
            hadm_id INT NOT NULL,
            submit_wardid INT,
            submit_careunit VARCHAR(15),
            curr_wardid INT,
            curr_careunit VARCHAR(15),
            callout_wardid INT,
            callout_service VARCHAR(10),
            request_tele SMALLINT,
            request_resp SMALLINT,
            request_cdiff SMALLINT,
            request_mrsa SMALLINT,
            request_vre SMALLINT,
            callout_status VARCHAR(20),
            callout_outcome VARCHAR(20),
            discharge_wardid INT,
            acknowledge_status VARCHAR(20),
            createtime TIMESTAMP(0),
            updatetime TIMESTAMP(0),
            acknowledgetime TIMESTAMP(0),
            outcometime TIMESTAMP(0),
            firstreservationtime TIMESTAMP(0),
            currentreservationtime TIMESTAMP(0),
            CONSTRAINT callout_fk_patients FOREIGN KEY (subject_id) REFERENCES patients(subject_id),
            CONSTRAINT callout_fk_admissions FOREIGN KEY (hadm_id) REFERENCES admissions(hadm_id)
        );
        CREATE INDEX IF NOT EXISTS callout_idx01 ON callout (subject_id, hadm_id);
        CREATE INDEX IF NOT EXISTS callout_idx02 ON callout (createtime, outcometime);
        """)
        
        # Create CHARTEVENTS table with partitioning
        print("Creating CHARTEVENTS table with partitioning...")
        cur.execute("""
        CREATE TABLE IF NOT EXISTS chartevents (
            row_id INT NOT NULL,
            subject_id INT NOT NULL,
            hadm_id INT,
            icustay_id INT,
            itemid INT NOT NULL,
            charttime TIMESTAMP(0) NOT NULL,
            storetime TIMESTAMP(0),
            cgid INT,
            value VARCHAR(200),
            valuenum DOUBLE PRECISION,
            valueuom VARCHAR(20),
            warning SMALLINT,
            error SMALLINT,
            resultstatus VARCHAR(20),
            stopped VARCHAR(20),
            CONSTRAINT chartevents_fk_patients FOREIGN KEY (subject_id) REFERENCES patients(subject_id),
            CONSTRAINT chartevents_fk_admissions FOREIGN KEY (hadm_id) REFERENCES admissions(hadm_id),
            CONSTRAINT chartevents_fk_icustays FOREIGN KEY (icustay_id) REFERENCES icustays(icustay_id),
            CONSTRAINT chartevents_fk_d_items FOREIGN KEY (itemid) REFERENCES d_items(itemid)
        ) PARTITION BY RANGE (itemid);
        
        -- Create partitions for CHARTEVENTS by ITEMID ranges
        CREATE TABLE IF NOT EXISTS chartevents_vitals PARTITION OF chartevents
            FOR VALUES FROM (0) TO (1000);
            
        CREATE TABLE IF NOT EXISTS chartevents_labs PARTITION OF chartevents
            FOR VALUES FROM (1000) TO (50000);
            
        CREATE TABLE IF NOT EXISTS chartevents_metavision PARTITION OF chartevents
            FOR VALUES FROM (50000) TO (1000000);

        -- Add indexes to each partition
        CREATE INDEX IF NOT EXISTS chartevents_vitals_idx01 ON chartevents_vitals (subject_id, charttime);
        CREATE INDEX IF NOT EXISTS chartevents_vitals_idx02 ON chartevents_vitals (icustay_id, charttime);
        CREATE INDEX IF NOT EXISTS chartevents_vitals_idx03 ON chartevents_vitals (itemid, charttime);

        CREATE INDEX IF NOT EXISTS chartevents_labs_idx01 ON chartevents_labs (subject_id, charttime);
        CREATE INDEX IF NOT EXISTS chartevents_labs_idx02 ON chartevents_labs (icustay_id, charttime);
        CREATE INDEX IF NOT EXISTS chartevents_labs_idx03 ON chartevents_labs (itemid, charttime);

        CREATE INDEX IF NOT EXISTS chartevents_metavision_idx01 ON chartevents_metavision (subject_id, charttime);
        CREATE INDEX IF NOT EXISTS chartevents_metavision_idx02 ON chartevents_metavision (icustay_id, charttime);
        CREATE INDEX IF NOT EXISTS chartevents_metavision_idx03 ON chartevents_metavision (itemid, charttime);
        """)
        
        print("Creating LABEVENTS table with partitioning...")
        cur.execute("""
        CREATE TABLE IF NOT EXISTS labevents (
            row_id INT NOT NULL,
            subject_id INT NOT NULL,
            hadm_id INT,
            itemid INT NOT NULL,
            charttime TIMESTAMP(0) NOT NULL,
            value VARCHAR(200),
            valuenum DOUBLE PRECISION,
            valueuom VARCHAR(20),
            flag VARCHAR(20),
            CONSTRAINT labevents_fk_patients FOREIGN KEY (subject_id) REFERENCES patients(subject_id),
            CONSTRAINT labevents_fk_admissions FOREIGN KEY (hadm_id) REFERENCES admissions(hadm_id),
            CONSTRAINT labevents_fk_d_labitems FOREIGN KEY (itemid) REFERENCES d_labitems(itemid)
        ) PARTITION BY RANGE (charttime);

        -- Create partitions for LABEVENTS by year
        CREATE TABLE IF NOT EXISTS labevents_y2001_2005 PARTITION OF labevents
            FOR VALUES FROM ('2001-01-01') TO ('2006-01-01');
            
        CREATE TABLE IF NOT EXISTS labevents_y2006_2010 PARTITION OF labevents
            FOR VALUES FROM ('2006-01-01') TO ('2011-01-01');
            
        CREATE TABLE IF NOT EXISTS labevents_y2011_2015 PARTITION OF labevents
            FOR VALUES FROM ('2011-01-01') TO ('2016-01-01');

        -- Add indexes to each partition
        CREATE INDEX IF NOT EXISTS labevents_y2001_2005_idx01 ON labevents_y2001_2005 (subject_id, charttime);
        CREATE INDEX IF NOT EXISTS labevents_y2001_2005_idx02 ON labevents_y2001_2005 (hadm_id, charttime);
        CREATE INDEX IF NOT EXISTS labevents_y2001_2005_idx03 ON labevents_y2001_2005 (itemid, charttime);

        CREATE INDEX IF NOT EXISTS labevents_y2006_2010_idx01 ON labevents_y2006_2010 (subject_id, charttime);
        CREATE INDEX IF NOT EXISTS labevents_y2006_2010_idx02 ON labevents_y2006_2010 (hadm_id, charttime);
        CREATE INDEX IF NOT EXISTS labevents_y2006_2010_idx03 ON labevents_y2006_2010 (itemid, charttime);

        CREATE INDEX IF NOT EXISTS labevents_y2011_2015_idx01 ON labevents_y2011_2015 (subject_id, charttime);
        CREATE INDEX IF NOT EXISTS labevents_y2011_2015_idx02 ON labevents_y2011_2015 (hadm_id, charttime);
        CREATE INDEX IF NOT EXISTS labevents_y2011_2015_idx03 ON labevents_y2011_2015 (itemid, charttime);
        """)
        
        print("Creating INPUTEVENTS_CV table...")
        cur.execute("""
        CREATE TABLE IF NOT EXISTS inputevents_cv (
            row_id INT NOT NULL,
            subject_id INT NOT NULL,
            hadm_id INT,
            icustay_id INT,
            charttime TIMESTAMP(0) NOT NULL,
            itemid INT NOT NULL,
            amount DOUBLE PRECISION,
            amountuom VARCHAR(30),
            rate DOUBLE PRECISION,
            rateuom VARCHAR(30),
            storetime TIMESTAMP(0),
            cgid INT,
            orderid INT,
            linkorderid INT,
            stopped VARCHAR(30),
            newbottle INT,
            originalamount DOUBLE PRECISION,
            originalamountuom VARCHAR(30),
            originalroute VARCHAR(30),
            originalrate DOUBLE PRECISION,
            originalrateuom VARCHAR(30),
            originalsite VARCHAR(30),
            CONSTRAINT inputevents_cv_fk_patients FOREIGN KEY (subject_id) REFERENCES patients(subject_id),
            CONSTRAINT inputevents_cv_fk_admissions FOREIGN KEY (hadm_id) REFERENCES admissions(hadm_id),
            CONSTRAINT inputevents_cv_fk_icustays FOREIGN KEY (icustay_id) REFERENCES icustays(icustay_id),
            CONSTRAINT inputevents_cv_fk_d_items FOREIGN KEY (itemid) REFERENCES d_items(itemid)
        );
        CREATE INDEX IF NOT EXISTS inputevents_cv_idx01 ON inputevents_cv (subject_id, charttime);
        CREATE INDEX IF NOT EXISTS inputevents_cv_idx02 ON inputevents_cv (icustay_id, charttime);
        CREATE INDEX IF NOT EXISTS inputevents_cv_idx03 ON inputevents_cv (itemid, charttime);
        """)
        
        print("Creating INPUTEVENTS_MV table...")
        cur.execute("""
        CREATE TABLE IF NOT EXISTS inputevents_mv (
            row_id INT NOT NULL,
            subject_id INT NOT NULL,
            hadm_id INT,
            icustay_id INT,
            starttime TIMESTAMP(0) NOT NULL,
            endtime TIMESTAMP(0) NOT NULL,
            itemid INT NOT NULL,
            amount DOUBLE PRECISION,
            amountuom VARCHAR(30),
            rate DOUBLE PRECISION,
            rateuom VARCHAR(30),
            storetime TIMESTAMP(0),
            cgid BIGINT,
            orderid INT,
            linkorderid INT,
            ordercategoryname VARCHAR(100),
            secondaryordercategoryname VARCHAR(100),
            ordercomponenttypedescription VARCHAR(200),
            ordercategorydescription VARCHAR(50),
            patientweight DOUBLE PRECISION,
            totalamount DOUBLE PRECISION,
            totalamountuom VARCHAR(50),
            isopenbag SMALLINT,
            continueinnextdept SMALLINT,
            cancelreason SMALLINT,
            statusdescription VARCHAR(30),
            comments_status VARCHAR(30),
            comments_title VARCHAR(100),
            comments_date TIMESTAMP(0),
            originalamount DOUBLE PRECISION,
            originalrate DOUBLE PRECISION,
            CONSTRAINT inputevents_mv_fk_patients FOREIGN KEY (subject_id) REFERENCES patients(subject_id),
            CONSTRAINT inputevents_mv_fk_admissions FOREIGN KEY (hadm_id) REFERENCES admissions(hadm_id),
            CONSTRAINT inputevents_mv_fk_icustays FOREIGN KEY (icustay_id) REFERENCES icustays(icustay_id),
            CONSTRAINT inputevents_mv_fk_d_items FOREIGN KEY (itemid) REFERENCES d_items(itemid)
        );
        CREATE INDEX IF NOT EXISTS inputevents_mv_idx01 ON inputevents_mv (subject_id, starttime, endtime);
        CREATE INDEX IF NOT EXISTS inputevents_mv_idx02 ON inputevents_mv (icustay_id, starttime, endtime);
        CREATE INDEX IF NOT EXISTS inputevents_mv_idx03 ON inputevents_mv (itemid, starttime, endtime);
        """)
        
        print("Creating OUTPUTEVENTS table...")
        cur.execute("""
        CREATE TABLE IF NOT EXISTS outputevents (
            row_id INT NOT NULL,
            subject_id INT NOT NULL,
            hadm_id INT,
            icustay_id INT,
            charttime TIMESTAMP(0) NOT NULL,
            itemid INT NOT NULL,
            value DOUBLE PRECISION,
            valueuom VARCHAR(30),
            storetime TIMESTAMP(0),
            cgid BIGINT,
            stopped VARCHAR(30),
            newbottle INT,
            iserror SMALLINT,
            CONSTRAINT outputevents_fk_patients FOREIGN KEY (subject_id) REFERENCES patients(subject_id),
            CONSTRAINT outputevents_fk_admissions FOREIGN KEY (hadm_id) REFERENCES admissions(hadm_id),
            CONSTRAINT outputevents_fk_icustays FOREIGN KEY (icustay_id) REFERENCES icustays(icustay_id),
            CONSTRAINT outputevents_fk_d_items FOREIGN KEY (itemid) REFERENCES d_items(itemid)
        );
        CREATE INDEX IF NOT EXISTS outputevents_idx01 ON outputevents (subject_id, charttime);
        CREATE INDEX IF NOT EXISTS outputevents_idx02 ON outputevents (icustay_id, charttime);
        CREATE INDEX IF NOT EXISTS outputevents_idx03 ON outputevents (itemid, charttime);
        """)
        
        print("Creating DATETIMEEVENTS table...")
        cur.execute("""
        CREATE TABLE IF NOT EXISTS datetimeevents (
            row_id INT NOT NULL,
            subject_id INT NOT NULL,
            hadm_id INT,
            icustay_id INT,
            itemid INT NOT NULL,
            charttime TIMESTAMP(0) NOT NULL,
            storetime TIMESTAMP(0),
            cgid INT,
            value TIMESTAMP(0),
            valueuom VARCHAR(50),
            warning SMALLINT,
            error SMALLINT,
            resultstatus VARCHAR(50),
            stopped VARCHAR(50),
            CONSTRAINT datetimeevents_fk_patients FOREIGN KEY (subject_id) REFERENCES patients(subject_id),
            CONSTRAINT datetimeevents_fk_admissions FOREIGN KEY (hadm_id) REFERENCES admissions(hadm_id),
            CONSTRAINT datetimeevents_fk_icustays FOREIGN KEY (icustay_id) REFERENCES icustays(icustay_id),
            CONSTRAINT datetimeevents_fk_d_items FOREIGN KEY (itemid) REFERENCES d_items(itemid)
        );
        CREATE INDEX IF NOT EXISTS datetimeevents_idx01 ON datetimeevents (subject_id, charttime);
        CREATE INDEX IF NOT EXISTS datetimeevents_idx02 ON datetimeevents (icustay_id, charttime);
        CREATE INDEX IF NOT EXISTS datetimeevents_idx03 ON datetimeevents (itemid, charttime);
        """)
        
        print("Creating MICROBIOLOGYEVENTS table...")
        cur.execute("""
        CREATE TABLE IF NOT EXISTS microbiologyevents (
            row_id INT NOT NULL,
            subject_id INT NOT NULL,
            hadm_id INT,
            chartdate TIMESTAMP(0),
            charttime TIMESTAMP(0),
            spec_itemid INT,
            spec_type_desc VARCHAR(100),
            org_itemid INT,
            org_name VARCHAR(100),
            isolate_num SMALLINT,
            ab_itemid INT,
            ab_name VARCHAR(30),
            dilution_text VARCHAR(10),
            dilution_comparison VARCHAR(20),
            dilution_value DOUBLE PRECISION,
            interpretation VARCHAR(5),
            CONSTRAINT microbiologyevents_fk_patients FOREIGN KEY (subject_id) REFERENCES patients(subject_id),
            CONSTRAINT microbiologyevents_fk_admissions FOREIGN KEY (hadm_id) REFERENCES admissions(hadm_id)
        );
        CREATE INDEX IF NOT EXISTS microbiologyevents_idx01 ON microbiologyevents (subject_id, charttime);
        CREATE INDEX IF NOT EXISTS microbiologyevents_idx02 ON microbiologyevents (hadm_id, charttime);
        CREATE INDEX IF NOT EXISTS microbiologyevents_idx03 ON microbiologyevents (org_itemid);
        """)
        
        # Commit the transaction
        conn.commit()
        print("Schema creation completed successfully.")
        
    except Exception as e:
        conn.rollback()
        print(f"Error creating schema: {str(e)}")
        raise
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":

    # Mimic user and pass
    mimic_credentials = mimic_user_creation(host_user="mimic_test", db_name="mimic_iii")
    user = mimic_credentials["new_user"]
    password = mimic_credentials["new_password"]

    #DB creation
    mimic_db = create_mimic_iii_db(new_user=user, new_password=password)
    

    # Create the schema with all tables
    create_mimic_schema(db_name=mimic_db, new_user=user, new_password=password)

