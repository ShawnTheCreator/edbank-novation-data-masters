"""
Unit Tests for Utils Module

Tests configuration loading, batch ID generation, and helper functions.
"""

import pytest
import tempfile
import os
from unittest.mock import Mock, patch, mock_open

from pipeline.utils import (
    load_config, generate_batch_id, get_input_config,
    get_output_path
)


class TestLoadConfig:
    """Test cases for configuration loading."""
    
    def test_load_valid_config(self):
        """Test loading a valid YAML configuration file."""
        yaml_content = """
input:
  accounts:
    path: "/data/input/accounts.csv"
    format: "csv"

output:
  bronze:
    base_path: "/data/output/bronze"
    format: "delta"

spark:
  app_name: "TestPipeline"
  sql_shuffle_partitions: 4
"""
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            f.write(yaml_content)
            temp_path = f.name
        
        try:
            config = load_config(temp_path)
            
            assert config['input']['accounts']['path'] == "/data/input/accounts.csv"
            assert config['input']['accounts']['format'] == "csv"
            assert config['output']['bronze']['base_path'] == "/data/output/bronze"
            assert config['spark']['sql_shuffle_partitions'] == 4
        finally:
            os.unlink(temp_path)
    
    def test_load_config_file_not_found(self):
        """Test handling of missing configuration file."""
        with pytest.raises(FileNotFoundError):
            load_config("/nonexistent/path/config.yaml")


class TestGenerateBatchId:
    """Test cases for batch ID generation."""
    
    def test_batch_id_format(self):
        """Test that batch ID follows expected format."""
        batch_id = generate_batch_id()
        
        # Should contain timestamp and UUID components
        assert '_' in batch_id
        parts = batch_id.split('_')
        assert len(parts) >= 2  # timestamp and UUID
        
        # Timestamp part should be numeric (YYYYMMDD_HHMMSS)
        timestamp_parts = parts[0].split('_')
        assert timestamp_parts[0].isdigit()  # Date
    
    def test_batch_id_uniqueness(self):
        """Test that batch IDs are unique."""
        batch_ids = [generate_batch_id() for _ in range(10)]
        
        assert len(set(batch_ids)) == len(batch_ids)  # All unique


class TestGetInputConfig:
    """Test cases for input configuration retrieval."""
    
    def test_get_existing_dataset_config(self):
        """Test retrieving configuration for existing dataset."""
        config = {
            'input': {
                'accounts': {'path': '/data/accounts.csv', 'format': 'csv'},
                'transactions': {'path': '/data/transactions.jsonl', 'format': 'json'}
            }
        }
        
        result = get_input_config(config, 'accounts')
        
        assert result['path'] == '/data/accounts.csv'
        assert result['format'] == 'csv'
    
    def test_get_nonexistent_dataset_config(self):
        """Test retrieving configuration for non-existent dataset."""
        config = {'input': {}}
        
        result = get_input_config(config, 'nonexistent')
        
        assert result == {}


class TestGetOutputPath:
    """Test cases for output path generation."""
    
    def test_get_layer_path(self):
        """Test getting path for a layer without dataset."""
        config = {
            'output': {
                'bronze': {'base_path': '/data/output/bronze'},
                'silver': {'base_path': '/data/output/silver'}
            }
        }
        
        result = get_output_path(config, 'bronze')
        
        assert result == '/data/output/bronze'
    
    def test_get_layer_dataset_path(self):
        """Test getting path for a layer with dataset."""
        config = {
            'output': {
                'bronze': {'base_path': '/data/output/bronze'}
            }
        }
        
        result = get_output_path(config, 'bronze', 'accounts')
        
        assert result == '/data/output/bronze/accounts'
    
    def test_get_path_with_default(self):
        """Test getting path uses default when not configured."""
        config = {'output': {}}
        
        result = get_output_path(config, 'gold', 'fact_transactions')
        
        assert result == '/data/output/gold/fact_transactions'


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
