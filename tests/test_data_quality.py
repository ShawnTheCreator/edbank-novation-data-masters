"""
Unit Tests for Data Quality Module

Tests the Circuit Breaker pattern and data quality validation functions.
Uses mock DataFrames to verify behavior without requiring Spark cluster.
"""

import pytest
from unittest.mock import Mock, MagicMock
from pipeline.data_quality import CircuitBreaker, DataQualityException


class TestCircuitBreaker:
    """Test cases for CircuitBreaker class."""
    
    def test_check_null_percentage_passes(self):
        """Test that acceptable null percentages pass validation."""
        config = {'data_quality': {'max_null_percentage': 0.50}}
        breaker = CircuitBreaker(config)
        
        # Mock DataFrame with 20% nulls (below 50% threshold)
        mock_df = Mock()
        mock_df.columns = ['col1', 'col2']
        mock_df.count.return_value = 100
        mock_df.filter.return_value.count.return_value = 20  # 20% nulls
        
        result = breaker.check_null_percentage(mock_df, 'test_dataset', ['col1'])
        
        assert result is True
        assert len(breaker.failed_checks) == 0
    
    def test_check_null_percentage_fails(self):
        """Test that excessive null percentages fail validation."""
        config = {'data_quality': {'max_null_percentage': 0.30}}
        breaker = CircuitBreaker(config)
        
        # Mock DataFrame with 50% nulls (above 30% threshold)
        mock_df = Mock()
        mock_df.columns = ['col1']
        mock_df.count.return_value = 100
        mock_df.filter.return_value.count.return_value = 50  # 50% nulls
        
        result = breaker.check_null_percentage(mock_df, 'test_dataset', ['col1'])
        
        assert result is False
        assert len(breaker.failed_checks) == 1
    
    def test_check_row_count_passes(self):
        """Test that sufficient row counts pass validation."""
        config = {'data_quality': {'min_row_count': 10}}
        breaker = CircuitBreaker(config)
        
        mock_df = Mock()
        mock_df.count.return_value = 100
        
        result = breaker.check_row_count(mock_df, 'test_dataset')
        
        assert result is True
    
    def test_check_row_count_fails(self):
        """Test that insufficient row counts fail validation."""
        config = {'data_quality': {'min_row_count': 100}}
        breaker = CircuitBreaker(config)
        
        mock_df = Mock()
        mock_df.count.return_value = 5
        
        result = breaker.check_row_count(mock_df, 'test_dataset')
        
        assert result is False
        assert len(breaker.failed_checks) == 1
    
    def test_check_primary_key_uniqueness_passes(self):
        """Test that unique primary keys pass validation."""
        config = {}
        breaker = CircuitBreaker(config)
        
        mock_df = Mock()
        mock_df.columns = ['id', 'name']
        mock_df.count.return_value = 100
        mock_df.select.return_value.distinct.return_value.count.return_value = 100
        
        result = breaker.check_primary_key_uniqueness(mock_df, 'test_dataset', 'id')
        
        assert result is True
    
    def test_check_primary_key_uniqueness_fails(self):
        """Test that duplicate primary keys fail validation."""
        config = {}
        breaker = CircuitBreaker(config)
        
        mock_df = Mock()
        mock_df.columns = ['id', 'name']
        mock_df.count.return_value = 100
        mock_df.select.return_value.distinct.return_value.count.return_value = 90  # 10 duplicates
        
        result = breaker.check_primary_key_uniqueness(mock_df, 'test_dataset', 'id')
        
        assert result is False
        assert len(breaker.failed_checks) == 1
        assert 'duplicate' in breaker.failed_checks[0].lower()


class TestDataQualityException:
    """Test cases for DataQualityException."""
    
    def test_exception_message(self):
        """Test that exception carries correct message."""
        msg = "Data quality check failed"
        exc = DataQualityException(msg)
        
        assert str(exc) == msg
    
    def test_exception_is_catchable(self):
        """Test that exception can be caught properly."""
        try:
            raise DataQualityException("test error")
        except DataQualityException as e:
            assert str(e) == "test error"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
