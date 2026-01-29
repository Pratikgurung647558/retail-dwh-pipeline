import sqlite3
import pandas as pd
import os
from config import DB_PATH

def load_gold(df):
    """
    Load data to gold layer with incremental loading and indexes
    """
    conn = sqlite3.connect(DB_PATH)
    
    print("Loading gold warehouse...")
    
    # Check if first run
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='fact_sales'")
    first_run = cursor.fetchone() is None
    
    if first_run:
        print("First run - creating tables")
        load_mode = "replace"
    else:
        print("Incremental run - checking for new data")
        existing = pd.read_sql("SELECT DISTINCT InvoiceNo FROM fact_sales", conn)
        original_count = len(df)
        df = df[~df['InvoiceNo'].isin(existing['InvoiceNo'])]
        
        if len(df) == 0:
            print("No new data to load")
            conn.close()
            return
            
        load_mode = "append"
        print(f"Loading {len(df)} new rows")
    
    # Load dimension tables
    dim_customer = df[['CustomerID', 'Country']].drop_duplicates()
    dim_product = df[['StockCode', 'Description']].drop_duplicates()
    
    dim_customer.to_sql("dim_customer", conn, if_exists="replace", index=False)
    dim_product.to_sql("dim_product", conn, if_exists="replace", index=False)
    
    # Load fact table
    fact_sales = df[['InvoiceNo', 'InvoiceDate', 'CustomerID',
                     'StockCode', 'Quantity', 'UnitPrice', 'total_amount']]
    
    fact_sales.to_sql("fact_sales", conn, if_exists=load_mode, index=False)
    
    # Create indexes
    create_indexes(conn, first_run)
    
    # Show counts
    print(f"\nLoaded: {len(fact_sales)} rows to fact_sales")
    
    conn.close()
    print("Gold layer loaded successfully!")

def create_indexes(conn, first_run=False):
    """
    Create indexes - uses schema.sql if exists
    """
    if not first_run:
        return  # Only create indexes on first run
    
    cursor = conn.cursor()
    
    # Try to use schema.sql
    if os.path.exists("sql/schema.sql"):
        try:
            with open("sql/schema.sql", 'r') as f:
                cursor.executescript(f.read())
            print("Indexes created from schema.sql")
        except Exception as e:
            print(f"Error reading schema.sql: {e}")
            create_default_indexes(cursor)
    else:
        create_default_indexes(cursor)
    
    conn.commit()

def create_default_indexes(cursor):
    """Create default indexes"""
    indexes = [
        "CREATE INDEX idx_fact_date ON fact_sales(InvoiceDate)",
        "CREATE INDEX idx_fact_customer ON fact_sales(CustomerID)",
        "CREATE INDEX idx_fact_product ON fact_sales(StockCode)",
    ]
    
    for sql in indexes:
        try:
            cursor.execute(sql)
        except:
            pass  # Index might already exist
    
    print("Created default indexes")

def run_analytics():
    """
    Run analytics from analytics.sql file
    """
    print("\nAnalytics Report")
    print("-" * 40)
    
    conn = sqlite3.connect(DB_PATH)
    
    # Try to read from file
    if os.path.exists("sql/analytics.sql"):
        try:
            with open("sql/analytics.sql", 'r') as f:
                queries = f.read().split(';')
            
            for query in queries:
                query = query.strip()
                if not query:
                    continue
                
                # Get query name from comment
                lines = query.split('\n')
                name = lines[0].replace('--', '').strip() if lines[0].startswith('--') else "Query"
                
                print(f"\n{name}:")
                try:
                    result = pd.read_sql(query, conn)
                    print(result.to_string(index=False))
                except Exception as e:
                    print(f"Error: {e}")
        
        except Exception as e:
            print(f"Error reading analytics.sql: {e}")
            run_default_analytics(conn)
    else:
        print("analytics.sql not found - running default")
        run_default_analytics(conn)
    
    conn.close()

def run_default_analytics(conn):
    """Fallback analytics"""
    # Simple revenue by country
    print("\nRevenue by Country (Top 5):")
    result = pd.read_sql("""
        SELECT c.Country, SUM(f.total_amount) as revenue
        FROM fact_sales f
        JOIN dim_customer c ON f.CustomerID = c.CustomerID
        GROUP BY c.Country
        ORDER BY revenue DESC
        LIMIT 5
    """, conn)
    print(result.to_string(index=False))
    
    # Monthly trend
    print("\nMonthly Revenue:")
    result = pd.read_sql("""
        SELECT strftime('%Y-%m', InvoiceDate) as month,
               SUM(total_amount) as revenue
        FROM fact_sales
        GROUP BY month
        ORDER BY month
    """, conn)
    print(result.to_string(index=False))

def load_gold_with_analytics(df):
    """Load gold and run analytics"""
    load_gold(df)
    run_analytics()