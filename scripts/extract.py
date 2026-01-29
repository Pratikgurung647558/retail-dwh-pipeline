import os
import pandas as pd
from config import BRONZE_PATH

def extract_bronze():
    print("Extracting bronze data...")
    print("Resolved BRONZE_PATH:", BRONZE_PATH)
    print("File exists?", os.path.exists(BRONZE_PATH))

    df = pd.read_csv(BRONZE_PATH, encoding="ISO-8859-1")
    return df
