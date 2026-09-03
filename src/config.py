import os
from dotenv import load_dotenv

# ===================================================================
# 1. LOAD ENVIRONMENT VARIABLES
# ===================================================================
load_dotenv()

# ===================================================================
# 2. CENTRAL DIRECTORY PATH MANAGEMENT (Task 17 Compliant)
# ===================================================================
# the project root directory (Banking_Data_Pipeline/) using absolute paths
SRC_DIR = os.path.dirname(os.path.abspath(__file__))
BASE_DIR = os.path.dirname(SRC_DIR)

# paths of all projects' directories
RAW_DATA_DIR = os.path.join(BASE_DIR, 'raw_data')
PROCESSED_DIR = os.path.join(BASE_DIR, 'processed')
QUARANTINE_DIR = os.path.join(BASE_DIR, 'quarantine')
FRAUD_DIR = os.path.join(BASE_DIR, 'fraud_data')
LOGS_DIR = os.path.join(BASE_DIR, 'logs')
REPORTS_DIR = os.path.join(BASE_DIR, 'reports')

# Automatically create the directories if they do not exist
os.makedirs(PROCESSED_DIR, exist_ok=True)
os.makedirs(QUARANTINE_DIR, exist_ok=True)
os.makedirs(FRAUD_DIR, exist_ok= True)
os.makedirs(LOGS_DIR, exist_ok=True)
os.makedirs(REPORTS_DIR, exist_ok=True)

# ===================================================================
# 3. CENTRAL FILE PATH CONFIGURATIONS
# ===================================================================
# Input Source Files (Task 17 Inputs)
NORTH_FILE_PATH = os.path.join(RAW_DATA_DIR, 'north_transactions.csv')
SOUTH_FILE_PATH = os.path.join(RAW_DATA_DIR, 'south_transactions.csv')
WEST_FILE_PATH = os.path.join(RAW_DATA_DIR, 'west_transactions.csv')

# Output Target Files (Task 4 & Task 17 Expected Structures)
CLEAN_CSV_PATH = os.path.join(PROCESSED_DIR, 'clean_transactions.csv')
QUARANTINE_CSV_PATH = os.path.join(QUARANTINE_DIR, 'quarantine_transactions.csv')
FRAUD_CSV_PATH = os.path.join(FRAUD_DIR, 'fraud_transactions.csv')

# ===================================================================
# 4. SECURE DATABASE CONFIGURATION (Task 15 & Task 16 Compliant)
# ===================================================================
# DB_USER = os.getenv('DB_USER')
# DB_PASSWORD = os.getenv('DB_PASSWORD')
# DB_HOST = os.getenv('DB_HOST')
# DB_PORT = os.getenv('DB_PORT')
# DB_NAME = os.getenv('DB_NAME')

DB_USER = os.getenv('DB_USER', 'airflow')
DB_PASSWORD = os.getenv('DB_PASSWORD', 'airflow')
# DB_HOST = os.getenv('DB_HOST', 'postgres')
DB_PORT = os.getenv('DB_PORT', '5432')
DB_NAME = os.getenv('DB_NAME', 'airflow')

# DYNAMIC HOSTNAME RECOGNITION:
if os.path.exists('/.dockerenv') or 'AIRFLOW_HOME' in os.environ:
    # Inside Airflow/Docker container environment
    DB_HOST = os.getenv('DB_HOST', 'postgres')
else:
    # Running via local terminal instance (main.py)
    DB_HOST = 'localhost'

# Connection string syntax creation for SQLAlchemy Engine
DB_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
