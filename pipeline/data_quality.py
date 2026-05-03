"""
Data Quality Module with Circuit Breaker Pattern

Implements data quality checks with threshold-based circuit breaker logic.
As per The Data Warehouse Toolkit, data must be rigorously cleansed and
verified for completeness before publication to the Gold layer.

Key Features:
- Null percentage checks with configurable thresholds
- Schema validation
- Row count validation
- Circuit breaker: stops pipeline if quality checks fail
"""

import sys
from typing import Dict, Any, List, Optional
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, count, when, isnan, isnull


class DataQualityException(Exception):
    """Exception raised when data quality checks fail."""
    pass


class CircuitBreaker:
    """
    Circuit breaker for data quality validation.
    
    Stops the pipeline before loading corrupted data to downstream layers.
    Implements the pattern from Designing Data-Intensive Applications:
    fail fast and prevent cascade failures.
    """
    
    def __init__(self, config: Dict[str, Any]):
        self.max_null_percentage = config.get('data_quality', {}).get('max_null_percentage', 0.50)
        self.min_row_count = config.get('data_quality', {}).get('min_row_count', 1)
        self.critical_columns = config.get('data_quality', {}).get('critical_columns', [])
        self.failed_checks = []
    
    def check_null_percentage(
        self,
        df: DataFrame,
        dataset_name: str,
        columns: Optional[List[str]] = None
    ) -> bool:
        """
        Check null percentage in specified columns.
        
        Uses single-pass aggregation to avoid multiple .count() calls.
        Optimized for large datasets with 2GB RAM constraint.
        
        Args:
            df: DataFrame to check
            dataset_name: Name of dataset for error messages
            columns: Columns to check (defaults to all columns)
            
        Returns:
            True if check passes, False otherwise
        """
        if columns is None:
            columns = df.columns
        
        # Single-pass: Get total count and filter in one aggregation
        from pyspark.sql.functions import count, when, col as spark_col, isnan as spark_isnan
        
        # Build aggregation expressions for all columns at once
        agg_exprs = [count("*").alias("total_count")]
        
        for column in columns:
            if column not in df.columns:
                continue
            # Count nulls for this column
            null_expr = count(
                when(
                    (spark_col(column).isNull()) | 
                    (spark_isnan(spark_col(column))) |
                    (spark_col(column) == ""),
                    1
                )
            ).alias(f"{column}_nulls")
            agg_exprs.append(null_expr)
        
        # Single aggregation pass
        agg_result = df.agg(*agg_exprs).collect()[0]
        
        total_count = agg_result["total_count"]
        if total_count == 0:
            self.failed_checks.append(f"{dataset_name}: Dataset is empty (0 rows)")
            return False
        
        check_passed = True
        
        for column in columns:
            if column not in df.columns:
                continue
                
            null_count = agg_result[f"{column}_nulls"]
            null_percentage = null_count / total_count if total_count > 0 else 0
            
            if null_percentage > self.max_null_percentage:
                self.failed_checks.append(
                    f"{dataset_name}.{column}: {null_percentage:.2%} nulls exceeds threshold of {self.max_null_percentage:.2%}"
                )
                check_passed = False
        
        return check_passed
    
    def check_row_count(
        self,
        df: DataFrame,
        dataset_name: str,
        min_count: Optional[int] = None
    ) -> bool:
        """
        Validate minimum row count.
        
        Args:
            df: DataFrame to check
            dataset_name: Name of dataset for error messages
            min_count: Minimum expected rows (defaults to config)
            
        Returns:
            True if check passes, False otherwise
        """
        min_count = min_count or self.min_row_count
        row_count = df.count()
        
        if row_count < min_count:
            self.failed_checks.append(
                f"{dataset_name}: Row count {row_count} below minimum {min_count}"
            )
            return False
        
        return True
    
    def check_primary_key_uniqueness(
        self,
        df: DataFrame,
        dataset_name: str,
        primary_key: str
    ) -> bool:
        """
        Check for duplicate primary keys.
        
        Optimized single-pass aggregation using count + countDistinct.
        
        Args:
            df: DataFrame to check
            dataset_name: Name of dataset for error messages
            primary_key: Primary key column name
            
        Returns:
            True if check passes, False otherwise
        """
        if primary_key not in df.columns:
            self.failed_checks.append(f"{dataset_name}: Primary key column '{primary_key}' not found")
            return False
        
        # Single-pass aggregation
        from pyspark.sql.functions import count, countDistinct
        agg_result = df.agg(
            count("*").alias("total"),
            countDistinct(primary_key).alias("distinct")
        ).collect()[0]
        
        total_count = agg_result["total"]
        distinct_count = agg_result["distinct"]
        
        if total_count != distinct_count:
            duplicates = total_count - distinct_count
            self.failed_checks.append(
                f"{dataset_name}: {duplicates} duplicate primary keys found in column '{primary_key}'"
            )
            return False
        
        return True
    
    def check_schema_compliance(
        self,
        df: DataFrame,
        dataset_name: str,
        expected_schema: Dict[str, str]
    ) -> bool:
        """
        Verify DataFrame schema matches expected structure.
        
        Args:
            df: DataFrame to check
            dataset_name: Name of dataset for error messages
            expected_schema: Dictionary of column_name -> data_type
            
        Returns:
            True if check passes, False otherwise
        """
        check_passed = True
        actual_columns = set(df.columns)
        expected_columns = set(expected_schema.keys())
        
        # Check for missing columns
        missing = expected_columns - actual_columns
        if missing:
            self.failed_checks.append(f"{dataset_name}: Missing columns: {missing}")
            check_passed = False
        
        # Check for unexpected columns (warning only)
        extra = actual_columns - expected_columns
        if extra and not extra.startswith('_'):
            print(f"  WARNING: {dataset_name}: Unexpected columns: {extra}")
        
        return check_passed
    
    def validate_all(
        self,
        spark: SparkSession,
        config: Dict[str, Any],
        bronze_datasets: Dict[str, DataFrame] = None,
        silver_datasets: Dict[str, DataFrame] = None
    ) -> bool:
        """
        Run all data quality checks and trip circuit breaker if any fail.
        
        Args:
            spark: SparkSession
            config: Pipeline configuration
            bronze_datasets: Dictionary of Bronze layer DataFrames
            silver_datasets: Dictionary of Silver layer DataFrames
            
        Returns:
            True if all checks pass
            
        Raises:
            DataQualityException: If any quality check fails
        """
        print("\n" + "=" * 60)
        print("DATA QUALITY VALIDATION")
        print("=" * 60)
        
        all_passed = True
        
        # Check Bronze layer datasets
        if bronze_datasets:
            print("\nValidating Bronze layer...")
            for name, df in bronze_datasets.items():
                print(f"  Checking {name}...")
                
                # Row count check
                if not self.check_row_count(df, f"bronze.{name}"):
                    all_passed = False
                
                # Null percentage check on critical columns
                input_config = config.get('input', {}).get(name, {})
                source_system = input_config.get('source_system', name)
                
                # Check metadata columns exist
                metadata_cols = [c for c in ['_ingestion_timestamp', '_source_system'] if c in df.columns]
                if metadata_cols:
                    if not self.check_null_percentage(df, f"bronze.{name}", metadata_cols):
                        all_passed = False
        
        # Check Silver layer datasets
        if silver_datasets:
            print("\nValidating Silver layer...")
            
            dedup_config = config.get('data_quality', {}).get('deduplication', {})
            
            for name, df in silver_datasets.items():
                print(f"  Checking {name}...")
                
                # Row count check
                if not self.check_row_count(df, f"silver.{name}"):
                    all_passed = False
                
                # Primary key uniqueness check
                pk_column = dedup_config.get(f"{name}_key", f"{name[:-1]}_id")
                if pk_column in df.columns:
                    if not self.check_primary_key_uniqueness(df, f"silver.{name}", pk_column):
                        all_passed = False
                
                # Null percentage on all columns
                if not self.check_null_percentage(df, f"silver.{name}"):
                    all_passed = False
        
        # Circuit breaker: fail if any checks failed
        if not all_passed:
            print("\n" + "=" * 60)
            print("CIRCUIT BREAKER TRIPPED - DATA QUALITY CHECKS FAILED")
            print("=" * 60)
            for check in self.failed_checks:
                print(f"  ✗ {check}")
            print("=" * 60)
            raise DataQualityException(
                f"Data quality validation failed with {len(self.failed_checks)} errors. "
                "Pipeline stopped to prevent loading corrupted data."
            )
        
        print("\n" + "=" * 60)
        print("DATA QUALITY VALIDATION PASSED")
        print("=" * 60)
        return True


def run_quality_checks(
    spark: SparkSession,
    config: Dict[str, Any],
    **datasets: Dict[str, DataFrame]
) -> bool:
    """
    Convenience function to run data quality checks with circuit breaker.
    
    Args:
        spark: SparkSession
        config: Pipeline configuration
        **datasets: Keyword arguments for bronze_* and silver_* DataFrames
        
    Returns:
        True if validation passes
        
    Raises:
        DataQualityException: If validation fails
    """
    breaker = CircuitBreaker(config)
    
    bronze_datasets = {k.replace('bronze_', ''): v for k, v in datasets.items() if k.startswith('bronze_')}
    silver_datasets = {k.replace('silver_', ''): v for k, v in datasets.items() if k.startswith('silver_')}
    
    return breaker.validate_all(spark, config, bronze_datasets, silver_datasets)
