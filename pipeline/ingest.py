"""
Bronze Layer Ingestion Module

Implements raw data ingestion from CSV and JSONL sources.
Key features:
- Raw data preservation (no transformations)
- Audit metadata addition (_ingestion_timestamp, _source_system)
- Partitioned writes by source_system for physical separation
- Delta Parquet format for ACID compliance
"""

import sys
from typing import Dict, Any
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import current_timestamp, lit

from pipeline.utils import (
    load_config, create_spark_session, get_input_config, get_output_path,
    generate_batch_id, add_audit_metadata, write_with_idempotency
)
from pipeline.data_quality import CircuitBreaker, DataQualityException


def read_csv_source(spark: SparkSession, config: Dict[str, Any]) -> DataFrame:
    """
    Read a CSV source file using configured options.
    
    Args:
        spark: SparkSession instance
        config: Input configuration dictionary
        
    Returns:
        DataFrame containing raw CSV data
    """
    path = config.get('path')
    options = config.get('options', {})
    
    reader = spark.read.format("csv")
    
    # Apply configured options
    for key, value in options.items():
        reader = reader.option(key, value)
    
    return reader.load(path)


def read_json_source(spark: SparkSession, config: Dict[str, Any]) -> DataFrame:
    """
    Read a JSON/JSONL source file using configured options.
    
    Args:
        spark: SparkSession instance
        config: Input configuration dictionary
        
    Returns:
        DataFrame containing raw JSON data
    """
    path = config.get('path')
    options = config.get('options', {})
    
    reader = spark.read.format("json")
    
    # Apply configured options
    for key, value in options.items():
        reader = reader.option(key, value)
    
    return reader.load(path)


def add_audit_columns(df: DataFrame, source_system: str) -> DataFrame:
    """
    Add audit metadata columns to the DataFrame.
    Adds _ingestion_timestamp and _source_system columns.
    
    Args:
        df: Input DataFrame
        source_system: Name of the source system
        
    Returns:
        DataFrame with audit columns added
    """
    return df \
        .withColumn("_ingestion_timestamp", current_timestamp()) \
        .withColumn("_source_system", lit(source_system))


def ingest_dataset(
    spark: SparkSession,
    config: Dict[str, Any],
    dataset_name: str,
    read_func,
    batch_id: str = None
) -> DataFrame:
    """
    Ingest a single dataset into the Bronze layer.
    
    Algorithm:
    1. Read raw data using the appropriate format reader
    2. Add audit metadata columns
    3. Write to Delta format partitioned by source_system
    
    Args:
        spark: SparkSession instance
        config: Full pipeline configuration
        dataset_name: Name of the dataset (accounts, transactions, customers)
        read_func: Function to read the source (read_csv_source or read_json_source)
    """
    input_config = get_input_config(config, dataset_name)
    output_config = config.get('output', {}).get('bronze', {})
    
    source_path = input_config.get('path')
    source_system = input_config.get('source_system', dataset_name)
    output_path = f"{get_output_path(config, 'bronze')}/{dataset_name}"
    
    print(f"Ingesting {dataset_name} from {source_path}...")
    
    # Step 1: Read raw data (no transformations)
    df = read_func(spark, input_config)
    
    # Get record count for logging
    record_count = df.count()
    print(f"  Read {record_count:,} raw records")
    
    # Step 2: Add audit metadata with batch_id and source file for lineage
    df_with_audit = add_audit_metadata(
        df, 
        source_system=source_system,
        batch_id=batch_id,
        source_file=source_path
    )
    
    # Step 3: Write to Bronze layer with idempotency
    # Partition by source_system for physical separation
    # Use Delta format for ACID compliance
    write_with_idempotency(
        df_with_audit,
        output_path=output_path,
        batch_id=batch_id,
        format=output_config.get('format', 'delta'),
        mode=output_config.get('mode', 'overwrite'),
        partition_cols=["_source_system"]
    )
    
    print(f"  Successfully ingested with batch_id: {batch_id}")
    return df_with_audit


def run_bronze_ingestion(config_path: str = "/app/config/pipeline_config.yaml") -> None:
    """
    Execute the complete Bronze layer ingestion process.
    
    This function:
    1. Loads configuration
    2. Creates optimized Spark session
    3. Ingests all configured datasets (accounts, transactions, customers)
    4. Adds audit metadata and partitions by source_system
    
    Args:
        config_path: Path to the YAML configuration file
    """
    print("=" * 60)
    print("BRONZE LAYER INGESTION")
    print("=" * 60)
    
    # Load configuration
    config = load_config(config_path)
    
    # Create Spark session with resource optimizations
    spark = create_spark_session(config)
    
    # Generate unique batch_id for idempotency
    batch_id = generate_batch_id()
    print(f"Pipeline batch_id: {batch_id}")
    
    # Initialize circuit breaker for data quality
    circuit_breaker = CircuitBreaker(config)
    
    ingested_datasets = {}
    
    try:
        # Ingest accounts (CSV)
        df_accounts = ingest_dataset(spark, config, 'accounts', read_csv_source, batch_id)
        ingested_datasets['accounts'] = df_accounts
        
        # Ingest transactions (JSONL)
        df_transactions = ingest_dataset(spark, config, 'transactions', read_json_source, batch_id)
        ingested_datasets['transactions'] = df_transactions
        
        # Ingest customers (CSV)
        df_customers = ingest_dataset(spark, config, 'customers', read_csv_source, batch_id)
        ingested_datasets['customers'] = df_customers
        
        # Circuit breaker: Validate data quality before proceeding
        print("\nRunning data quality checks with circuit breaker...")
        circuit_breaker.validate_all(
            spark, 
            config, 
            bronze_accounts=ingested_datasets.get('accounts'),
            bronze_customers=ingested_datasets.get('customers'),
            bronze_transactions=ingested_datasets.get('transactions')
        )
        
        print("=" * 60)
        print("BRONZE LAYER INGESTION COMPLETED SUCCESSFULLY")
        print(f"Batch ID: {batch_id}")
        print("=" * 60)
        
    except DataQualityException as e:
        print(f"\nCIRCUIT BREAKER TRIPPED: {str(e)}")
        print("Pipeline stopped to prevent loading corrupted data.")
        sys.exit(1)
    except Exception as e:
        print(f"ERROR: Bronze layer ingestion failed: {str(e)}")
        sys.exit(1)
    finally:
        spark.stop()


if __name__ == "__main__":
    # Allow config path override via command line
    config_path = sys.argv[1] if len(sys.argv) > 1 else "/app/config/pipeline_config.yaml"
    run_bronze_ingestion(config_path)
