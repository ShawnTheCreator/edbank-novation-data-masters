"""
Validation Runner for Gold Layer

Implements the three scoring validation queries:
1. Transaction Volume by Type
2. Zero Unlinked Accounts  
3. Province Distribution

Also performs additional schema conformance checks.
"""

import sys
from typing import Dict, Any
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, count, sum as spark_sum, round, countDistinct, isnan, when

from pipeline.utils import load_config, create_spark_session, get_output_path


class ValidationException(Exception):
    """Exception raised when validation checks fail."""
    pass


def get_gold_tables(spark: SparkSession, config: Dict[str, Any]) -> Dict[str, DataFrame]:
    """Load all Gold layer tables."""
    gold_path = get_output_path(config, 'gold')
    
    tables = {}
    try:
        tables['fact_transactions'] = spark.read.format("delta").load(f"{gold_path}/fact_transactions")
        tables['dim_accounts'] = spark.read.format("delta").load(f"{gold_path}/dim_accounts")
        tables['dim_customers'] = spark.read.format("delta").load(f"{gold_path}/dim_customers")
    except Exception as e:
        raise ValidationException(f"Failed to load Gold layer tables: {e}")
    
    return tables


def validate_query_1_transaction_volume(fact_transactions: DataFrame) -> Dict[str, Any]:
    """
    Query 1: Transaction Volume by Type
    
    Expected: Exactly 4 rows (CREDIT, DEBIT, FEE, REVERSAL)
    """
    print("\n" + "=" * 60)
    print("QUERY 1: Transaction Volume by Type")
    print("=" * 60)
    
    result = fact_transactions.groupBy("transaction_type").agg(
        count("*").alias("record_count"),
        spark_sum("amount").alias("total_amount"),
        round(count("*") * 100.0 / fact_transactions.count(), 2).alias("pct_of_total")
    ).orderBy("transaction_type")
    
    result.show()
    
    # Validate expected transaction types
    expected_types = {"CREDIT", "DEBIT", "FEE", "REVERSAL"}
    actual_types = {row.transaction_type for row in result.collect()}
    
    missing = expected_types - actual_types
    extra = actual_types - expected_types
    
    status = "PASS" if not missing and len(actual_types) == 4 else "FAIL"
    
    print(f"Expected types: {sorted(expected_types)}")
    print(f"Actual types:   {sorted(actual_types)}")
    
    if missing:
        print(f"  ✗ Missing types: {missing}")
    if extra:
        print(f"  ✗ Unexpected types: {extra}")
    
    print(f"  Status: {status}")
    
    return {
        "query": "transaction_volume_by_type",
        "status": status,
        "expected_count": 4,
        "actual_count": len(actual_types),
        "missing_types": list(missing),
        "extra_types": list(extra)
    }


def validate_query_2_zero_unlinked_accounts(
    dim_accounts: DataFrame, 
    dim_customers: DataFrame
) -> Dict[str, Any]:
    """
    Query 2: Zero Unlinked Accounts
    
    Every account must be linked to a known customer via customer_id.
    Expected: 0 unlinked accounts
    """
    print("\n" + "=" * 60)
    print("QUERY 2: Zero Unlinked Accounts")
    print("=" * 60)
    
    # Check if customer_id exists in dim_accounts
    if "customer_id" not in dim_accounts.columns:
        print("  ✗ CRITICAL: dim_accounts missing 'customer_id' column!")
        print("     Field must be at position 3 per output_schema_spec.md §3")
        return {
            "query": "zero_unlinked_accounts",
            "status": "FAIL",
            "unlinked_count": None,
            "error": "dim_accounts missing customer_id column"
        }
    
    # Left join accounts to customers
    unlinked = dim_accounts.join(
        dim_customers,
        on="customer_id",
        how="left_anti"  # Accounts that don't have matching customers
    )
    
    unlinked_count = unlinked.count()
    
    status = "PASS" if unlinked_count == 0 else "FAIL"
    
    print(f"  Unlinked accounts: {unlinked_count}")
    print(f"  Expected: 0")
    print(f"  Status: {status}")
    
    if unlinked_count > 0:
        print("  Sample unlinked accounts:")
        unlinked.select("account_id", "customer_id").show(5, truncate=False)
    
    return {
        "query": "zero_unlinked_accounts",
        "status": status,
        "unlinked_count": unlinked_count,
        "expected": 0
    }


def validate_query_3_province_distribution(
    dim_accounts: DataFrame, 
    dim_customers: DataFrame
) -> Dict[str, Any]:
    """
    Query 3: Province Distribution
    
    Expected: Exactly 9 rows (all 9 SA provinces)
    """
    print("\n" + "=" * 60)
    print("QUERY 3: Province Distribution")
    print("=" * 60)
    
    expected_provinces = {
        "Eastern Cape", "Free State", "Gauteng", "KwaZulu-Natal",
        "Limpopo", "Mpumalanga", "North West", "Northern Cape", "Western Cape"
    }
    
    # Join accounts to customers and group by province
    result = dim_accounts.join(
        dim_customers,
        on="customer_id",
        how="inner"
    ).groupBy("province").agg(
        countDistinct("account_id").alias("account_count")
    ).orderBy("province")
    
    result.show()
    
    actual_provinces = {row.province for row in result.collect()}
    
    missing = expected_provinces - actual_provinces
    extra = actual_provinces - expected_provinces
    
    status = "PASS" if not missing and len(actual_provinces) == 9 else "FAIL"
    
    print(f"Expected provinces: {len(expected_provinces)}")
    print(f"Actual provinces:   {len(actual_provinces)}")
    
    if missing:
        print(f"  ✗ Missing provinces: {missing}")
    if extra:
        print(f"  ✗ Unexpected provinces: {extra}")
    
    print(f"  Status: {status}")
    
    return {
        "query": "province_distribution",
        "status": status,
        "expected_count": 9,
        "actual_count": len(actual_provinces),
        "missing_provinces": list(missing),
        "extra_provinces": list(extra)
    }


def validate_schema_conformance(tables: Dict[str, DataFrame]) -> Dict[str, Any]:
    """
    Validate schema conformance per output_schema_spec.md.
    
    - dim_accounts: exactly 11 fields, customer_id at position 3
    - fact_transactions: exactly 14 fields
    - dim_customers: exactly 9 fields
    """
    print("\n" + "=" * 60)
    print("SCHEMA CONFORMANCE CHECKS")
    print("=" * 60)
    
    results = {}
    all_passed = True
    
    # Check dim_accounts
    accounts_cols = tables['dim_accounts'].columns
    accounts_expected = [
        'account_sk', 'account_id', 'customer_id', 'account_type', 
        'account_status', 'open_date', 'product_tier', 'digital_channel',
        'credit_limit', 'current_balance', 'last_activity_date'
    ]
    
    if len(accounts_cols) != 11:
        print(f"  ✗ dim_accounts has {len(accounts_cols)} fields, expected 11")
        all_passed = False
    else:
        print(f"  ✓ dim_accounts has correct field count (11)")
    
    if 'customer_id' not in accounts_cols:
        print(f"  ✗ dim_accounts missing customer_id (required for Query 2)")
        all_passed = False
    elif accounts_cols[2] != 'customer_id':
        print(f"  ✗ customer_id not at position 3 (found at position {accounts_cols.index('customer_id') + 1})")
        all_passed = False
    else:
        print(f"  ✓ customer_id at correct position (3)")
    
    # Check dim_customers
    customers_cols = tables['dim_customers'].columns
    customers_expected = 9
    
    if len(customers_cols) != customers_expected:
        print(f"  ✗ dim_customers has {len(customers_cols)} fields, expected {customers_expected}")
        all_passed = False
    else:
        print(f"  ✓ dim_customers has correct field count ({customers_expected})")
    
    # Check fact_transactions
    transactions_cols = tables['fact_transactions'].columns
    transactions_expected = 14
    
    if len(transactions_cols) != transactions_expected:
        print(f"  ✗ fact_transactions has {len(transactions_cols)} fields, expected {transactions_expected}")
        all_passed = False
    else:
        print(f"  ✓ fact_transactions has correct field count ({transactions_expected})")
    
    return {
        "query": "schema_conformance",
        "status": "PASS" if all_passed else "FAIL"
    }


def run_all_validations(config_path: str = "/app/config/pipeline_config.yaml") -> None:
    """
    Run all validation queries and schema checks.
    
    Exit code:
    - 0 if all validations pass
    - 1 if any validation fails
    """
    print("=" * 60)
    print("GOLD LAYER VALIDATION")
    print("=" * 60)
    
    config = load_config(config_path)
    spark = create_spark_session(config)
    
    try:
        # Load Gold tables
        print("\nLoading Gold layer tables...")
        tables = get_gold_tables(spark, config)
        print(f"  Loaded: fact_transactions, dim_accounts, dim_customers")
        
        # Run all validations
        results = []
        
        results.append(validate_query_1_transaction_volume(tables['fact_transactions']))
        results.append(validate_query_2_zero_unlinked_accounts(
            tables['dim_accounts'], tables['dim_customers']
        ))
        results.append(validate_query_3_province_distribution(
            tables['dim_accounts'], tables['dim_customers']
        ))
        results.append(validate_schema_conformance(tables))
        
        # Summary
        print("\n" + "=" * 60)
        print("VALIDATION SUMMARY")
        print("=" * 60)
        
        all_passed = all(r['status'] == 'PASS' for r in results)
        
        for r in results:
            status_icon = "✓" if r['status'] == 'PASS' else "✗"
            print(f"  {status_icon} {r['query']}: {r['status']}")
        
        print("=" * 60)
        
        if all_passed:
            print("ALL VALIDATIONS PASSED ✓")
            sys.exit(0)
        else:
            print("SOME VALIDATIONS FAILED ✗")
            sys.exit(1)
            
    except ValidationException as e:
        print(f"\nVALIDATION ERROR: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"\nUNEXPECTED ERROR: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        spark.stop()


if __name__ == "__main__":
    config_path = sys.argv[1] if len(sys.argv) > 1 else "/app/config/pipeline_config.yaml"
    run_all_validations(config_path)
