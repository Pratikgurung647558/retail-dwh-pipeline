from scripts.extract import extract_bronze
from scripts.bronze_to_silver import bronze_to_silver
from scripts.quality_checks import run_quality_checks
from scripts.silver_to_gold import load_gold_with_analytics

def run_pipeline():
    print("=" * 60)
    print("STARTING RETAIL DATA PIPELINE")
    print("=" * 60)
    
    try:
        print("\n1️  EXTRACTING BRONZE DATA...")
        df = extract_bronze()
        print(f"   ✓ Extracted {len(df)} rows")
        
        print("\n2️  TRANSFORMING TO SILVER...")
        df = bronze_to_silver(df)
        print(f"   ✓ Transformed to {len(df)} rows")
        
        print("\n3️  RUNNING QUALITY CHECKS...")
        run_quality_checks(df)
        print("   ✓ Quality checks passed")
        
        print("\n4️  LOADING GOLD WAREHOUSE WITH ANALYTICS...")
        load_gold_with_analytics(df) 
        print("   ✓ Gold layer loaded with analytics")
        
        print("\n" + "=" * 60)
        print(" PIPELINE COMPLETED SUCCESSFULLY!")
        print("=" * 60)
        
    except Exception as e:
        print(f"\n PIPELINE FAILED: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    run_pipeline()