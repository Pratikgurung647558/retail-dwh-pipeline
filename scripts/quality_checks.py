def run_quality_checks(df):
    checks = {
        "no_null_customer": df['CustomerID'].isnull().sum() == 0,
        "positive_quantity": (df['Quantity'] > 0).all(),
        "positive_revenue": (df['total_amount'] > 0).all(),
        "unique_invoice_line": df[['InvoiceNo', 'StockCode']].duplicated().sum() == 0
    }

    failed = [k for k, v in checks.items() if not v]

    if failed:
        raise Exception(f" Data quality failed: {failed}")
    else:
        print(" Data quality checks passed")
