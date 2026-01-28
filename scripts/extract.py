import pandas as pd
BRONZE_PATH = r"C:\Users\pratik\Desktop\prop\DE\New folder\retail-dwh-pipeline\data\bronze\online_retail_raw.csv"

def extarct_bronze():
    df = pd.read_csv(BRONZE_PATH, encoding="ISO-8859-1")
    return df