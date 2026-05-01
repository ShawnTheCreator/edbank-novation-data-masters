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
    
    Algorithm:
    - Select relevant columns from accounts
    - Add surrogate key (hash of natural key + effective date)
    - Add SCD Type 2 metadata columns for tracking changes
    
    Expected size: ~100K records
    
    Args:
        df_silver_accounts: Silver layer accounts DataFrame
        
    Returns:
        Accounts dimension DataFrame
    """
    print("Creating dim_accounts...")
    
    # Select relevant columns
    dim_cols = ['account_id', 'customer_id', 'account_type', 'balance', 'created_date']
    available_cols = [c for c in dim_cols if c in df_silver_accounts.columns]
    
    df_dim = df_silver_accounts.select(*available_cols)
    
    # Add surrogate key (hash of account_id for simplicity in this implementation)
    df_dim = df_dim.withColumn("account_sk", hash(col("account_id")).cast("long"))
    
    # Add SCD Type 2 tracking columns
    df_dim = df_dim \
        .withColumn("effective_date", current_date()) \
        .withColumn("expiration_date", lit(None).cast("date")) \
        .withColumn("is_current", lit(True))
    
    record_count = df_dim.count()
    print(f"  dim_accounts created: {record_count:,} records")
    
    return df_dim


def create_dimension_customers(df_silver_customers: DataFrame) -> DataFrame:
    """
    Create the customers dimension table from Silver layer.
    
    Algorithm:
    - Select relevant columns from customers
    - Add surrogate key (hash of natural key)
    - Add SCD Type 2 metadata columns
    
    Expected size: ~80K records
    
    Args:
        df_silver_customers: Silver layer customers DataFrame
        
    Returns:
        Customers dimension DataFrame
    """
    print("Creating dim_customers...")
    
    # Select relevant columns
    dim_cols = ['customer_id', 'first_name', 'last_name', 'email', 'phone', 'registration_date']
    available_cols = [c for c in dim_cols if c in df_silver_customers.columns]
    
    df_dim = df_silver_customers.select(*available_cols)
    
    # Add surrogate key
    df_dim = df_dim.withColumn("customer_sk", hash(col("customer_id")).cast("long"))
    
    # Add SCD Type 2 tracking columns
    df_dim = df_dim \
        .withColumn("effective_date", current_date()) \
        .withColumn("expiration_date", lit(None).cast("date")) \
        .withColumn("is_current", lit(True))
    
    record_count = df_dim.count()
    print(f"  dim_customers created: {record_count:,} records")
    
    return df_dim


def create_fact_transactions(
    df_silver_transactions: DataFrame,
    df_dim_accounts: DataFrame,
    df_dim_customers: DataFrame
) -> DataFrame:
    """
    Create the transactions fact table from Silver layer.
    
    Algorithm - O(N) Broadcast Hash Joins:
    1. Broadcast smaller dimension tables to all workers
    2. Join transactions with accounts dimension on account_id
    3. Join result with customers dimension on customer_id
    4. Add derived columns for partitioning (year, month, day)
    
    The broadcast() function forces a Broadcast Hash Join:
    - Small tables (<100MB) are copied to each worker
    - Large transaction table streams through without shuffle
    - Result: O(N) complexity instead of O(N log N) SortMergeJoin
    
    Expected size: ~1M records
    
    Args:
        df_silver_transactions: Silver layer transactions DataFrame
        df_dim_accounts: Accounts dimension DataFrame
        df_dim_customers: Customers dimension DataFrame
        
    Returns:
        Transactions fact DataFrame
    """
    print("Creating fact_transactions...")
    
    # Select and rename columns from dimension tables to avoid conflicts
    df_accounts_lookup = df_dim_accounts.select(
        col("account_id"),
        col("account_sk"),
        col("customer_id"),
        col("account_type").alias("dim_account_type")
    )
    
    df_customers_lookup = df_dim_customers.select(
        col("customer_id"),
        col("customer_sk")
    )
    
    # Prepare fact columns from transactions
    fact_cols = [
        'transaction_id', 'account_id', 'transaction_date', 'amount',
        'transaction_type', 'description'
    ]
    available_fact_cols = [c for c in fact_cols if c in df_silver_transactions.columns]
    df_fact = df_silver_transactions.select(*available_fact_cols)
    
    # Step 1: Broadcast Hash Join with accounts dimension
    # This is O(N) because accounts table is small and broadcasted
    print("  Applying Broadcast Hash Join with accounts dimension...")
    df_fact = df_fact.join(
        broadcast(df_accounts_lookup),
        on='account_id',
        how='left'
    )
    
    # Step 2: Broadcast Hash Join with customers dimension
    # This is O(N) because customers table is small and broadcasted
    print("  Applying Broadcast Hash Join with customers dimension...")
    df_fact = df_fact.join(
        broadcast(df_customers_lookup),
        on='customer_id',
        how='left'
    )
    
    # Step 3: Add derived partitioning columns
    # These enable O(1) or O(log N) partition pruning on queries
    if 'transaction_date' in df_fact.columns:
        df_fact = df_fact \
            .withColumn("transaction_year", year(col("transaction_date"))) \
            .withColumn("transaction_month", month(col("transaction_date"))) \
            .withColumn("transaction_day", dayofmonth(col("transaction_date"))) \
            .withColumn("date_key", date_format(col("transaction_date"), "yyyyMMdd").cast("int"))
    
    # Select final fact columns (using surrogate keys instead of natural keys where applicable)
    final_cols = [
        'transaction_id',
        'account_sk',
        'customer_sk',
        'date_key',
        'transaction_date',
        'transaction_year',
        'transaction_month',
        'transaction_day',
        'amount',
        'transaction_type',
        'description'
    ]
    
    # Only include columns that exist
    available_final_cols = [c for c in final_cols if c in df_fact.columns]
    df_fact = df_fact.select(*available_final_cols)
    
    record_count = df_fact.count()
    print(f"  fact_transactions created: {record_count:,} records")
    
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
        
        print(f"  Accounts: {df_silver_accounts.count():,} records")
        print(f"  Customers: {df_silver_customers.count():,} records")
        print(f"  Transactions: {df_silver_transactions.count():,} records")
        
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
        
        # Write dimension tables
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
        print("\nGold Layer Tables:")
        print(f"  - dim_accounts: ~100K records")
        print(f"  - dim_customers: ~80K records")
        print(f"  - fact_transactions: ~1M records (partitioned by date)")
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
