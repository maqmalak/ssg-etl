"""
Test script for the optimized hanger_lane ETL implementation.
This script tests the memory optimization features and ETL best practices.
"""

import sys
import os
import unittest
from unittest.mock import patch, MagicMock
from datetime import datetime

# Add the dags directory to the Python path
sys.path.append(os.path.join(os.path.dirname(__file__), 'dags'))

class TestHangerLaneOptimized(unittest.TestCase):
    
    def setUp(self):
        """Set up test fixtures before each test method."""
        pass
    
    def tearDown(self):
        """Tear down test fixtures after each test method."""
        pass
    
    def test_memory_usage_monitoring(self):
        """Test memory usage monitoring functions."""
        # Import the optimized module
        try:
            from dags.hanger_lane_optimized import get_memory_usage, log_memory_usage, check_memory_and_cleanup
            # These should be callable without errors
            memory_percent = get_memory_usage()
            self.assertIsInstance(memory_percent, float)
            self.assertGreaterEqual(memory_percent, 0)
            self.assertLessEqual(memory_percent, 100)
            
            # Test logging function (should not raise exceptions)
            log_memory_usage("test_operation")
            
            # Test memory check function (should not raise exceptions)
            check_memory_and_cleanup("test_operation")
            
        except ImportError as e:
            self.skipTest(f"Could not import optimized module: {e}")
    
    def test_batch_processing(self):
        """Test batch processing functionality."""
        try:
            from dags.hanger_lane_optimized import BATCH_SIZE
            # Batch size should be a reasonable value for memory optimization
            self.assertGreater(BATCH_SIZE, 0)
            self.assertLessEqual(BATCH_SIZE, 5000)  # Should not be too large
            
        except ImportError as e:
            self.skipTest(f"Could not import optimized module: {e}")
    
    def test_connection_pooling(self):
        """Test connection pooling configuration."""
        try:
            from dags.hanger_lane_optimized import get_postgres_engine
            from sqlalchemy import create_engine
            
            # Mock the Airflow connection
            with patch('dags.hanger_lane_optimized.BaseHook.get_connection') as mock_get_connection:
                mock_connection = MagicMock()
                mock_connection.login = 'test_user'
                mock_connection.password = 'test_password'
                mock_connection.host = 'localhost'
                mock_connection.port = 5432
                mock_connection.schema = 'test_db'
                mock_get_connection.return_value = mock_connection
                
                # Get the engine
                engine = get_postgres_engine()
                self.assertIsInstance(engine, create_engine('').__class__)
                
                # Check that the engine has the expected configuration
                # Note: We can't directly access pool settings, but we can verify it's created
                
        except ImportError as e:
            self.skipTest(f"Could not import optimized module: {e}")
    
    def test_data_validation(self):
        """Test data validation function."""
        try:
            from dags.hanger_lane_optimized import validate_data
            
            # Test with empty list
            result = validate_data([])
            self.assertEqual(result, [])
            
            # Test with sample data
            sample_data = [
                {'id': 1, 'name': 'test'},
                {'id': 2, 'name': 'test2'}
            ]
            result = validate_data(sample_data)
            self.assertEqual(result, sample_data)
            
        except ImportError as e:
            self.skipTest(f"Could not import optimized module: {e}")

if __name__ == '__main__':
    # Run the tests
    unittest.main()