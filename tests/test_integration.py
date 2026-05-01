"""
Integration Tests for Data Pipeline

Tests the complete pipeline flow with mock data.
Uses pytest-spark for SparkSession fixtures.
"""

import pytest
import tempfile
import shutil
import os

# Skip tests if pyspark not available
pytest.importorskip("pyspark")

from pyspark.sql import SparkSession
from pipeline.utils import load_config, create_spark_session, get_output_path


@pytest.fixture(scope="module")
def spark():
    """Create a SparkSession for testing."""
    spark = SparkSession.builder \
        .appName("PipelineIntegrationTest") \
        .master("local[2]") \
        .config("spark.sql.shuffle.partitions", "2") \
        .config("spark.sql.adaptive.enabled", "false") \
        .getOrCreate()
    
    yield spark
    
    spark.stop()


@pytest.fixture
def test_config():
    """Create a test configuration."""
    test_dir = tempfile.mkdtemp()
    
    config = {
        'input': {
            'accounts': {
                'path': os.path.join(os.path.dirname(__file__), 'mock_data', 'accounts.csv'),
                'format': 'csv',
                'options': {'header': 'true'},
                'source_system': 'test_accounts'
            },
            'customers': {
                'path': os.path.join(os.path.dirname(__file__), 'mock_data', 'customers.csv'),
                'format': 'csv',
                'options': {'header': 'true'},
                'source_system': 'test_customers'
            },
            'transactions': {
                'path': os.path.join(os.path.dirname(__file__), 'mock_data', 'transactions.jsonl'),
                'format': 'json',
                'options': {'multiline': 'false'},
                'source_system': 'test_transactions'
            }
        },
        'output': {
            'bronze': {'base_path': os.path.join(test_dir, 'bronze'), 'format': 'parquet', 'mode': 'overwrite'},
            'silver': {'base_path': os.path.join(test_dir, 'silver'), 'format': 'parquet', 'mode': 'overwrite'},
            'gold': {'base_path': os.path.join(test_dir, 'gold'), 'format': 'parquet', 'mode': 'overwrite'}
        },
        'spark': {
            'app_name': 'TestPipeline',
            'sql_shuffle_partitions': 2
        },
        'data_quality': {
            'max_null_percentage': 0.50,
            'min_row_count': 1,
            'deduplication': {
                'accounts_key': 'account_id',
                'customers_key': 'customer_id',
                'transactions_key': 'transaction_id'
            }
        }
    }
    
    yield config
    
    # Cleanup
    shutil.rmtree(test_dir, ignore_errors=True)


class TestPipelineFlow:
    """Integration tests for the complete pipeline flow."""
    
    def test_bronze_ingestion_creates_files(self, spark, test_config):
        """Test that Bronze ingestion creates output files."""
        from pipeline.ingest import ingest_dataset, read_csv_source, read_json_source
        
        bronze_path = test_config['output']['bronze']['base_path']
        
        # Ingest accounts
        df_accounts = ingest_dataset(spark, test_config, 'accounts', read_csv_source, 'test_batch_001')
        assert df_accounts is not None
        
        # Verify output path exists
        assert os.path.exists(os.path.join(bronze_path, 'accounts'))
    
    def test_audit_columns_added(self, spark, test_config):
        """Test that audit metadata columns are added."""
        from pipeline.ingest import ingest_dataset, read_csv_source
        
        df_accounts = ingest_dataset(spark, test_config, 'accounts', read_csv_source, 'test_batch_002')
        
        # Check audit columns exist
        assert '_ingestion_timestamp' in df_accounts.columns
        assert '_source_system' in df_accounts.columns
        assert '_batch_id' in df_accounts.columns
        assert '_source_file_name' in df_accounts.columns
    
    def test_batch_id_tracking(self, spark, test_config):
        """Test that batch_id is correctly tracked across datasets."""
        from pipeline.ingest import ingest_dataset, read_csv_source
        
        batch_id = 'test_batch_003'
        
        df_accounts = ingest_dataset(spark, test_config, 'accounts', read_csv_source, batch_id)
        
        # Verify batch_id is in the data
        batch_ids = [row._batch_id for row in df_accounts.select('_batch_id').distinct().collect()]
        assert batch_id in batch_ids
    
    def test_silver_transformation_deduplicates(self, spark, test_config):
        """Test that Silver layer deduplicates records."""
        from pipeline.transform import deduplicate_records
        from pyspark.sql import Row
        
        # Create test data with duplicates
        data = [
            Row(account_id='ACC001', balance=100.00),
            Row(account_id='ACC001', balance=200.00),  # Duplicate key
            Row(account_id='ACC002', balance=300.00)
        ]
        df = spark.createDataFrame(data)
        
        # Deduplicate
        df_deduped = deduplicate_records(df, 'account_id')
        
        # Should have 2 records (one duplicate removed)
        assert df_deduped.count() == 2
    
    def test_silver_standardization(self, spark, test_config):
        """Test that data standardization works correctly."""
        from pipeline.transform import standardize_string_column
        from pyspark.sql import Row
        
        # Create test data with inconsistent case
        data = [
            Row(account_id='  ACC001  ', account_type='CHECKING'),
            Row(account_id='acc002', account_type='  savings  ')
        ]
        df = spark.createDataFrame(data)
        
        # Standardize
        df_std = standardize_string_column(df, 'account_type', case='lower')
        
        # Verify standardization
        types = [row.account_type for row in df_std.select('account_type').collect()]
        assert 'checking' in types
        assert 'savings' in types
    
    def test_gold_dimension_creation(self, spark, test_config):
        """Test that Gold dimension tables are created correctly."""
        from pipeline.provision import create_dimension_accounts
        from pyspark.sql import Row
        
        # Create mock silver accounts data
        data = [
            Row(account_id='ACC001', customer_id='CUST001', account_type='checking', balance=1000.00, created_date='2024-01-15'),
            Row(account_id='ACC002', customer_id='CUST002', account_type='savings', balance=2000.00, created_date='2024-02-20')
        ]
        df_silver = spark.createDataFrame(data)
        
        # Create dimension
        df_dim = create_dimension_accounts(df_silver)
        
        # Verify dimension structure
        assert 'account_sk' in df_dim.columns
        assert 'effective_date' in df_dim.columns
        assert 'is_current' in df_dim.columns
        assert df_dim.count() == 2
    
    def test_circuit_breaker_stops_pipeline(self, spark, test_config):
        """Test that circuit breaker stops pipeline on quality failures."""
        from pipeline.data_quality import CircuitBreaker, DataQualityException
        from pyspark.sql import Row
        
        # Create config with strict thresholds
        strict_config = test_config.copy()
        strict_config['data_quality'] = {'max_null_percentage': 0.01, 'min_row_count': 1000}
        
        breaker = CircuitBreaker(strict_config)
        
        # Create small DataFrame that will fail row count check
        data = [Row(id='1', value='test')]
        df = spark.createDataFrame(data)
        
        # Should fail row count check
        result = breaker.check_row_count(df, 'test_dataset')
        assert result is False
        
        # Circuit breaker should have recorded the failure
        assert len(breaker.failed_checks) > 0


class TestConfiguration:
    """Tests for configuration handling."""
    
    def test_config_file_loading(self):
        """Test that configuration file loads correctly."""
        config_path = os.path.join(
            os.path.dirname(os.path.dirname(__file__)),
            'config',
            'pipeline_config.yaml'
        )
        
        if os.path.exists(config_path):
            config = load_config(config_path)
            
            # Verify expected structure
            assert 'input' in config
            assert 'output' in config
            assert 'spark' in config
            
            # Verify Spark optimizations are set
            assert config['spark']['sql_shuffle_partitions'] == 4
    
    def test_required_input_datasets(self):
        """Test that all required input datasets are configured."""
        config_path = os.path.join(
            os.path.dirname(os.path.dirname(__file__)),
            'config',
            'pipeline_config.yaml'
        )
        
        if os.path.exists(config_path):
            config = load_config(config_path)
            
            required_datasets = ['accounts', 'transactions', 'customers']
            for dataset in required_datasets:
                assert dataset in config['input'], f"Missing dataset: {dataset}"
                assert 'path' in config['input'][dataset]
                assert 'format' in config['input'][dataset]


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
