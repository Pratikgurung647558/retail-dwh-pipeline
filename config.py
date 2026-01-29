
import os
# Gets the absolute path of the folder where config.py is located
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# Create complete file paths
BRONZE_PATH = os.path.join(BASE_DIR, "data", "bronze", "online_retail_raw.csv")
SILVER_PATH = os.path.join(BASE_DIR, "data", "silver", "cleaned_sales.parquet")
DB_PATH = os.path.join(BASE_DIR,"data","gold", "retail_dwh.db")
