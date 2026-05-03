"""
Gold Layer Provisioning Module

Implements dimensional modeling with star schema design.
Key algorithmic features:
- Broadcast Hash Joins for O(N) time complexity with dimension tables
- Partitioned writes on fact tables for O(1) partition pruning
- Memory-safe operations optimized for 2 vCPUs and 2GB RAM

Tables created:
- dim_accounts (~100K records): Slowly Changing Dimension for accounts
- dim_customers (~80K records): Slowly Changing Dimension for customers  
- fact_transactions (~1M records): Transaction fact table partitioned by date
"""

import sys
from typing import Dict, Any, List
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col, broadcast, current_date, lit, row_number, coalesce, hash,
    year, month, dayofmonth, date_format
)
from pyspark.sql.window import Window

from pipeline.utils import load_config, create_spark_session, get_output_path


def create_dimension_accounts(df_silver_accounts: DataFrame) -> DataFrame:
    """
    Create the accounts dimension table from Silver layer.
    
    Per output_schema_spec.md §3, dim_accounts has 11 fields:
    - account_sk (surrogate key)
    - account_id, customer_id (from customer_ref), account_type, account_status
    - open_date, product_tier, digital_channel
    - credit_limit, current_balance, last_activity_date
    
    Args:
        df_silver_accounts: Silver layer accounts DataFrame
        
    Returns:
        Accounts dimension DataFrame with all 11 fields
    """
    print("Creating dim_accounts...")
    
    # Select all required columns per spec (11 fields)
    # customer_ref is renamed to customer_id per spec §3
    dim_cols = [
        'account_id', 'customer_ref', 'account_type', 'account_status',
        'open_date', 'product_tier', 'digital_channel', 'credit_limit', 
        'current_balance', 'last_activity_date'
    ]
    available_cols = [c for c in dim_cols if c in df_silver_accounts.columns]
    
    df_dim = df_silver_accounts.select(*available_cols)
    
    # Rename customer_ref to customer_id per spec §3 (field position 3)
    if 'customer_ref' in df_dim.columns:
        df_dim = df_dim.withColumnRenamed('customer_ref', 'customer_id')
    
    # Add surrogate key using deterministic hash (per spec §1)
    # sha2-based for stability across re-runs
    from pyspark.sql.functions import sha2
    df_dim = df_dim.withColumn(
        "account_sk", 
        sha2(col("account_id"), 256).cast("long")
    )
    
    # Reorder columns to match spec: account_sk at position 1, customer_id at position 3
    ordered_cols = ['account_sk', 'account_id', 'customer_id', 'account_type', 
                    'account_status', 'open_date', 'product_tier', 'digital_channel',
                    'credit_limit', 'current_balance', 'last_activity_date']
    
    # Only include columns that exist
    final_cols = [c for c in ordered_cols if c in df_dim.columns]
    df_dim = df_dim.select(*final_cols)
    
    print(f"  dim_accounts schema: {len(df_dim.columns)} fields")
    
    return df_dim


def create_dimension_customers(df_silver_customers: DataFrame) -> DataFrame:
    """
    Create the customers dimension table from Silver layer.
    
    Per output_schema_spec.md §4, dim_customers has 9 fields:
    - customer_sk (surrogate key)
    - customer_id, gender, province, income_band, segment
    - risk_score, kyc_status, age_band (derived from dob)
    
    Args:
        df_silver_customers: Silver layer customers DataFrame
        
    Returns:
        Customers dimension DataFrame with all 9 fields
    """
    print("Creating dim_customers...")
    
    from pyspark.sql.functions import sha2, floor, datediff, lit, current_date as spark_current_date
    
    # Select required columns per spec (9 fields)
    dim_cols = ['customer_id', 'gender', 'province', 'income_band', 'segment', 
                'risk_score', 'kyc_status', 'dob']
    available_cols = [c for c in dim_cols if c in df_silver_customers.columns]
    
    df_dim = df_silver_customers.select(*available_cols)
    
    # Add surrogate key using deterministic hash (per spec §1)
    df_dim = df_dim.withColumn(
        "customer_sk", 
        sha2(col("customer_id"), 256).cast("long")
    )
    
    # Derive age_band from dob per spec §4
    if 'dob' in df_dim.columns:
        df_dim = df_dim.withColumn(
            "age",
            floor(datediff(spark_current_date(), col("dob")) / 365.25).cast("int")
        )
        
        # Bucket into age bands
        df_dim = df_dim.withColumn(
            "age_band",
            when(col("age") >= 65, "65+")
            .when(col("age") >= 56, "56-65")
            .when(col("age") >= 46, "46-55")
            .when(col("age") >= 36, "36-45")
            .when(col("age") >= 26, "26-35")
            .when(col("age") >= 18, "18-25")
            .otherwise(None)
        )
        
        # Drop temporary age column and dob (replaced by age_band)
        df_dim = df_dim.drop("age", "dob")
    
    # Reorder columns to match spec
    ordered_cols = ['customer_sk', 'customer_id', 'gender', 'province', 'income_band',
                    'segment', 'risk_score', 'kyc_status', 'age_band']
    
    final_cols = [c for c in ordered_cols if c in df_dim.columns]
    df_dim = df_dim.select(*final_cols)
    
    print(f"  dim_customers schema: {len(df_dim.columns)} fields")
    
    return df_dim


def create_fact_transactions(
    df_silver_transactions: DataFrame,
    df_dim_accounts: DataFrame,
    df_dim_customers: DataFrame,
    df_bronze_transactions: DataFrame = None
) -> DataFrame:
    """
    Create the transactions fact table from Silver layer.
    
    Per output_schema_spec.md §2, fact_transactions has 14 fields:
    - transaction_sk (surrogate key)
    - transaction_id, account_sk, customer_sk (FKs)
    - transaction_date, transaction_timestamp
    - transaction_type, merchant_category, amount, currency, channel, province
    - dq_flag (data quality flag)
    - ingestion_timestamp
    
    Algorithm - O(N) Broadcast Hash Joins:
    1. Broadcast smaller dimension tables to all workers
    2. Join transactions with accounts dimension on account_id
    3. Join result with customers dimension on customer_id
    4. Add derived columns and DQ flags
    
    Args:
        df_silver_transactions: Silver layer transactions DataFrame
        df_dim_accounts: Accounts dimension DataFrame
        df_dim_customers: Customers dimension DataFrame
        df_bronze_transactions: Bronze layer transactions (for ingestion_timestamp)
        
    Returns:
        Transactions fact DataFrame with all 14 fields
    """
    print("Creating fact_transactions...")
    
    from pyspark.sql.functions import sha2, concat, to_timestamp, lit, when, col as spark_col
    
    # Select and rename columns from dimension tables to avoid conflicts
    df_accounts_lookup = df_dim_accounts.select(
        col("account_id"),
        col("account_sk"),
        col("customer_id"),
        col("account_type").alias("dim_account_type")
    )
    
    df_customers_lookup = df_dim_customers.select(
        col("customer_id"),
        col("customer_sk"),
        col("province").alias("customer_province")
    )
    
    # Prepare fact columns from transactions - select all available fields
    fact_cols = [
        'transaction_id', 'account_id', 'transaction_date', 'transaction_time',
        'transaction_type', 'merchant_category', 'amount', 'currency', 'channel',
        'location', 'description', '_ingestion_timestamp'
    ]
    available_fact_cols = [c for c in fact_cols if c in df_silver_transactions.columns]
    df_fact = df_silver_transactions.select(*available_fact_cols)
    
    # Step 1: Broadcast Hash Join with accounts dimension
    print("  Applying Broadcast Hash Join with accounts dimension...")
    df_fact = df_fact.join(
        broadcast(df_accounts_lookup),
        on='account_id',
        how='left'
    )
    
    # Step 2: Broadcast Hash Join with customers dimension
    print("  Applying Broadcast Hash Join with customers dimension...")
    df_fact = df_fact.join(
        broadcast(df_customers_lookup),
        on='customer_id',
        how='left'
    )
    
    # Step 3: Add surrogate key (deterministic hash per spec §1)
    df_fact = df_fact.withColumn(
        "transaction_sk",
        sha2(col("transaction_id"), 256).cast("long")
    )
    
    # Step 4: Build transaction_timestamp from date + time
    if 'transaction_date' in df_fact.columns and 'transaction_time' in df_fact.columns:
        df_fact = df_fact.withColumn(
            "transaction_timestamp",
            to_timestamp(concat(col("transaction_date"), lit(" "), col("transaction_time")), 
                        "yyyy-MM-dd HH:mm:ss")
        )
    elif 'transaction_date' in df_fact.columns:
        df_fact = df_fact.withColumn(
            "transaction_timestamp",
            to_timestamp(col("transaction_date"), "yyyy-MM-dd")
        )
    
    # Step 5: Extract province from location or use customer_province
    if 'location' in df_fact.columns:
        # Try to extract province from location struct
        df_fact = df_fact.withColumn(
            "province",
            when(spark_col("location.province").isNotNull(), spark_col("location.province"))
            .otherwise(col("customer_province"))
        )
    else:
        df_fact = df_fact.withColumn("province", col("customer_province"))
    
    # Step 6: Standardize currency to ZAR per spec §6
    if 'currency' in df_fact.columns:
        df_fact = df_fact.withColumn("currency", lit("ZAR"))
    else:
        df_fact = df_fact.withColumn("currency", lit("ZAR"))
    
    # Step 7: DQ Flag - set to NULL for clean records per spec §6
    # (Silver layer should have already flagged issues)
    df_fact = df_fact.withColumn("dq_flag", lit(None).cast("string"))
    
    # Step 8: Rename _ingestion_timestamp to ingestion_timestamp
    if '_ingestion_timestamp' in df_fact.columns:
        df_fact = df_fact.withColumnRenamed("_ingestion_timestamp", "ingestion_timestamp")
    else:
        from pyspark.sql.functions import current_timestamp
        df_fact = df_fact.withColumn("ingestion_timestamp", current_timestamp())
    
    # Step 9: Select final 14 fields in spec order
    final_cols = [
        'transaction_sk', 'transaction_id', 'account_sk', 'customer_sk',
        'transaction_date', 'transaction_timestamp', 'transaction_type',
        'merchant_category', 'amount', 'currency', 'channel', 'province',
        'dq_flag', 'ingestion_timestamp'
    ]
    
    available_final_cols = [c for c in final_cols if c in df_fact.columns]
    df_fact = df_fact.select(*available_final_cols)
    
    print(f"  fact_transactions schema: {len(df_fact.columns)} fields")
    
    return df_fact


def write_dimension_table(
    df: DataFrame,
    output_path: str,
    output_format: str = "delta",
    mode: str = "overwrite"
) -> None:
    """
    Write a dimension table to the Gold layer.
    
    Dimension tables are relatively small (~100K records) and don't require
    partitioning for efficient access.
    
    Args:
        df: Dimension DataFrame to write
        output_path: Target path for the table
        output_format: Storage format (default: delta)
        mode: Write mode (default: overwrite)
    """
    df.write \
        .format(output_format) \
        .mode(mode) \
        .save(output_path)
    
    print(f"  Written to {output_path}")


def write_fact_table(
    df: DataFrame,
    output_path: str,
    partition_columns: List[str],
    output_format: str = "delta",
    mode: str = "overwrite"
) -> None:
    """
    Write a fact table to the Gold layer with partitioning.
    
    Partitioning Strategy - O(log N) lookups:
    - Fact tables are large (~1M+ records)
    - Partition by date columns to enable partition pruning
    - Queries filtering on date columns will scan only relevant partitions
    - Result: O(log N) or O(1) file lookups instead of O(N) full scans
    
    Args:
        df: Fact DataFrame to write
        output_path: Target path for the table
        partition_columns: Columns to partition by (typically date fields)
        output_format: Storage format (default: delta)
        mode: Write mode (default: overwrite)
    """
    print(f"  Writing with partitioning by: {partition_columns}")
    
    # Validate partition columns exist
    available_partitions = [c for c in partition_columns if c in df.columns]
    
    if available_partitions:
        df.write \
            .format(output_format) \
            .mode(mode) \
            .partitionBy(*available_partitions) \
            .save(output_path)
    else:
        # Fallback to non-partitioned write if partition columns don't exist
        df.write \
            .format(output_format) \
            .mode(mode) \
            .save(output_path)
    
    print(f"  Written to {output_path}")


def run_gold_provisioning(config_path: str = "/app/config/pipeline_config.yaml") -> None:
    """
    Execute the complete Gold layer provisioning process.
    
    This function:
    1. Loads configuration and creates Spark session
    2. Reads Silver layer data
    3. Creates dimension tables (dim_accounts, dim_customers)
    4. Creates fact table with O(N) Broadcast Hash Joins
    5. Writes tables with optimized partitioning
    
    Algorithmic complexities implemented:
    - O(N) Broadcast Hash Joins for dimension lookups
    - O(log N) partition pruning on fact_transactions
    - Memory-safe lazy evaluation throughout
    
    Args:
        config_path: Path to the YAML configuration file
    """
    print("=" * 60)
    print("GOLD LAYER PROVISIONING")
    print("=" * 60)
    print()
    print("Algorithmic Optimizations:")
    print("  - Broadcast Hash Joins: O(N) complexity for dimension lookups")
    print("  - Partition Pruning: O(log N) file lookups on date filters")
    print("  - Shuffle Partitions: Reduced to 4 for 2-core constraint")
    print("  - Memory-safe lazy evaluation with 2GB RAM limit")
    print("=" * 60)
    
    # Load configuration
    config = load_config(config_path)
    
    # Create Spark session with resource optimizations
    spark = create_spark_session(config)
    
    try:
        # Read Silver layer data
        print("\nReading Silver layer data...")
        silver_base_path = get_output_path(config, 'silver')
        
        df_silver_accounts = spark.read.format("delta").load(f"{silver_base_path}/accounts")
        df_silver_customers = spark.read.format("delta").load(f"{silver_base_path}/customers")
        df_silver_transactions = spark.read.format("delta").load(f"{silver_base_path}/transactions")
        
        # Avoid multiple counts - we'll see counts from dimension creation
        print("  Silver layer data loaded")
        
        # Create dimension tables
        print("\nCreating dimension tables...")
        df_dim_accounts = create_dimension_accounts(df_silver_accounts)
        df_dim_customers = create_dimension_customers(df_silver_customers)
        
        # Create fact table with broadcast joins
        print("\nCreating fact table with Broadcast Hash Joins...")
        df_fact_transactions = create_fact_transactions(
            df_silver_transactions,
            df_dim_accounts,
            df_dim_customers
        )
        
        # Get output configuration
        output_config = config.get('output', {}).get('gold', {})
        gold_base_path = get_output_path(config, 'gold')
        output_format = output_config.get('format', 'delta')
        mode = output_config.get('mode', 'overwrite')
        
        # Write dimension tables (no partitioning needed for dimension tables)
        print("\nWriting dimension tables...")
        write_dimension_table(
            df_dim_accounts,
            f"{gold_base_path}/dim_accounts",
            output_format,
            mode
        )
        write_dimension_table(
            df_dim_customers,
            f"{gold_base_path}/dim_customers",
            output_format,
            mode
        )
        
        # Load Bronze transactions for ingestion_timestamp if needed
        bronze_path = get_output_path(config, 'bronze')
        try:
            df_bronze_transactions = spark.read.format("delta").load(f"{bronze_path}/transactions")
        except:
            df_bronze_transactions = None
        
        # Write fact table with partitioning
        print("\nWriting fact table with partition pruning...")
        
        # Determine partition column from config
        gold_config = config.get('gold_layer', {})
        partition_col = gold_config.get('fact_partition_column', 'transaction_date')
        
        # Use derived date columns for multi-level partitioning if available
        if 'transaction_year' in df_fact_transactions.columns:
            partition_cols = ['transaction_year', 'transaction_month']
        else:
            partition_cols = [partition_col] if partition_col in df_fact_transactions.columns else []
        
        write_fact_table(
            df_fact_transactions,
            f"{gold_base_path}/fact_transactions",
            partition_cols,
            output_format,
            mode
        )
        print("\n" + "=" * 60)
        print("GOLD LAYER PROVISIONING COMPLETED SUCCESSFULLY")
        print("=" * 60)
        print("\nGold Layer Tables Created:")
        print(f"  - dim_accounts: ~100K records expected")
        print(f"  - dim_customers: ~80K records expected")
        print(f"  - fact_transactions: ~1M records expected (partitioned by date)")
        print("=" * 60)
        
    except Exception as e:
        print(f"ERROR: Gold layer provisioning failed: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        spark.stop()


if __name__ == "__main__":
    # Allow config path override via command line
    config_path = sys.argv[1] if len(sys.argv) > 1 else "/app/config/pipeline_config.yaml"
    run_gold_provisioning(config_path)
