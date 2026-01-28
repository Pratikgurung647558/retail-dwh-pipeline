import pandas as pd
import numpy as np
from config import SILVER_PATH

def bronze_to_silver(df):
    # Remove cancelled invoices
    df = df[~df['InvoiceNo'].str.startswith('C')]

    # Remove returns
    df = df[df['Quantity'] > 0]

    # Remove missing customers
    df = df.dropna(subset=['CustomerID'])

    # Type conversions
    df['InvoiceDate'] = pd.to_datetime(df['InvoiceDate'])
    df['CustomerID'] = df['CustomerID'].astype(int)

    # Feature engineering
    df['total_amount'] = np.round(
        df['Quantity'] * df['UnitPrice'], 2
    )

    # Save Silver layer
    df.to_parquet(SILVER_PATH, index=False)

    return df
