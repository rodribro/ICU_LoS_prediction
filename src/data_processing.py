import psycopg2
from psycopg2 import sql

def mimic_user_creation(host_user: str, db_name: str):
    '''
    Connect to PostGreSQL database using Super User

    Args:
        host_user: string (super user created at the time of installation)
        db_name: string (postgres by deafult)
    '''
    # Admin connection (connect as the default PostgreSQL superuser)
    conn = psycopg2.connect(dbname="postgres", user="rodrigocastro")  # No password needed if peer authentication is enabled
    conn.autocommit = True
    cur = conn.cursor()

    # Fetch the current user
    cur = conn.cursor()
    cur.execute("SELECT current_user;")
    current_user = cur.fetchone()[0]

    print(f"Connected to PostgreSQL successfully as {current_user} !\n")

    # New user credentials and new database name
    new_db_user = "mimic_user"
    new_db_password = "lengthofstay"
    new_database = "mimic_icu"


    # Query to check if mimic user exists
    cur.execute("SELECT 1 FROM pg_roles WHERE rolname = %s;\n", (new_db_user,))
    user_exists = cur.fetchone() is not None

    if user_exists:
        print(f"User '{new_db_user}' has already been created!\n")
    else:
        print(f"User '{new_db_user}' will be created.\n")
    
        cur.execute(
        sql.SQL("CREATE ROLE {} WITH LOGIN PASSWORD %s;").format(sql.Identifier(new_db_user)),
        [new_db_password]
    )

    print(f"Closing connection to database ")
    conn.close()

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