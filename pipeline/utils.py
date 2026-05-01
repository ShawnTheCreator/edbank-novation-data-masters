"""
Utility module for Spark session initialization and helper functions.
Optimized for constrained resource environments (2 vCPUs, 2GB RAM).
"""

import yaml
from typing import Dict, Any
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip


def load_config(config_path: str = "/app/config/pipeline_config.yaml") -> Dict[str, Any]:
    """
    Load pipeline configuration from YAML file.
    
    Args:
        config_path: Path to the YAML configuration file
        
    Returns:
        Dictionary containing configuration parameters
    """
    with open(config_path, 'r') as f:
        return yaml.safe_load(f)


def create_spark_session(config: Dict[str, Any]) -> SparkSession:
    """
    Initialize and configure Spark Session with resource constraints.
    
    Critical optimizations for 2 vCPUs and 2GB RAM:
    - spark.sql.shuffle.partitions=4 (vs default 200) prevents memory fragmentation
    - Memory allocation split between driver and executor
    - Kryo serialization for compact memory usage
    - Delta Lake integration for ACID transactions
    
    Args:
        config: Configuration dictionary containing spark settings
        
    Returns:
        Configured SparkSession instance
    """
    spark_config = config.get('spark', {})
    
    # Build Spark session with optimized configurations for 2 vCPUs and 2GB RAM:
    # - spark.sql.shuffle.partitions=4 (vs default 200) prevents memory fragmentation
    # - Memory allocation split between driver and executor
    # - Kryo serialization for compact memory usage
    # - Delta Lake integration for ACID transactions
    builder = SparkSession.builder \
        .appName(spark_config.get('app_name', 'DataPipeline')) \
        .master("local[*]") \
        .config("spark.driver.memory", spark_config.get('driver_memory', '1g')) \
        .config("spark.executor.memory", spark_config.get('executor_memory', '1g')) \
        .config("spark.executor.cores", str(spark_config.get('executor_cores', 2))) \
        .config("spark.sql.shuffle.partitions", str(spark_config.get('sql_shuffle_partitions', 4))) \
        .config("spark.serializer", spark_config.get('serializer', 'org.apache.spark.serializer.KryoSerializer')) \
        .config("spark.kryo.registrationRequired", spark_config.get('kryo_registration_required', 'false')) \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.sql.adaptive.skewJoin.enabled", "true") \
        .config("spark.memory.fraction", "0.8") \
        .config("spark.memory.storageFraction", "0.3")
    
    # Configure with Delta support
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    
    # Set log level to reduce noise
    spark.sparkContext.setLogLevel("WARN")
    
    return spark


def get_input_config(config: Dict[str, Any], dataset: str) -> Dict[str, Any]:
    """
    Get input configuration for a specific dataset.
    
    Args:
        config: Full configuration dictionary
        dataset: Name of the dataset (accounts, transactions, customers)
        
    Returns:
        Input configuration for the specified dataset
    """
    return config.get('input', {}).get(dataset, {})


def get_output_path(config: Dict[str, Any], layer: str, dataset: str = None) -> str:
    """
    Get output path for a specific layer and dataset.
    
    Args:
        config: Full configuration dictionary
        layer: Layer name (bronze, silver, gold)
        dataset: Optional dataset name for subdirectories
        
    Returns:
        Full output path
    """
    base_path = config.get('output', {}).get(layer, {}).get('base_path', f"/data/output/{layer}")
    if dataset:
        return f"{base_path}/{dataset}"
    return base_path


def generate_batch_id() -> str:
    """
    Generate a unique batch identifier for idempotency.
    
    Idempotency Pattern (Designing Data-Intensive Applications):
    - Each pipeline run gets a unique batch_id
    - Delta Lake atomic commits ensure all-or-nothing writes
    - Re-running with same batch_id replaces data atomically
    
    Returns:
        Unique batch identifier (UUID-based)
    """
    import uuid
    from datetime import datetime
    
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    unique_id = str(uuid.uuid4())[:8]
    return f"{timestamp}_{unique_id}"


def add_audit_metadata(
    df,
    source_system: str,
    batch_id: str = None,
    source_file: str = None
):
    """
    Add audit metadata columns to a DataFrame.
    
    Audit Dimension Pattern (The Data Warehouse Toolkit):
    Captures metadata about data lineage - where data came from,
    extract versions, and load times. Enables traceability and
    confidence in data quality.
    
    Args:
        df: Input DataFrame
        source_system: Name of the source system
        batch_id: Unique batch identifier for idempotency (auto-generated if None)
        source_file: Source file path for lineage tracking
        
    Returns:
        DataFrame with added audit columns
    """
    from pyspark.sql.functions import current_timestamp, lit
    
    if batch_id is None:
        batch_id = generate_batch_id()
    
    result = df \
        .withColumn("_ingestion_timestamp", current_timestamp()) \
        .withColumn("_source_system", lit(source_system)) \
        .withColumn("_batch_id", lit(batch_id))
    
    if source_file:
        result = result.withColumn("_source_file_name", lit(source_file))
    
    return result


def write_with_idempotency(
    df,
    output_path: str,
    batch_id: str,
    format: str = "delta",
    mode: str = "overwrite",
    partition_cols: list = None
):
    """
    Write DataFrame with idempotency guarantees using Delta Lake.
    
    Idempotency Strategy:
    1. Use Delta Lake's transactional writes for atomicity
    2. Replace entire dataset on re-runs (mode='overwrite')
    3. Batch ID in metadata enables lineage tracking
    
    As per Designing Data-Intensive Applications, this provides
    all-or-nothing guarantees: if an error occurs, the transaction
    is aborted and partial writes are discarded.
    
    Args:
        df: DataFrame to write
        output_path: Target path
        batch_id: Unique batch identifier
        format: Storage format (default: delta)
        mode: Write mode (default: overwrite for idempotency)
        partition_cols: Optional partition columns
    """
    writer = df.write.format(format).mode(mode)
    
    if partition_cols:
        writer = writer.partitionBy(*partition_cols)
    
    writer.save(output_path)
    
    print(f"  Written batch {batch_id} to {output_path} (format={format}, mode={mode})")
