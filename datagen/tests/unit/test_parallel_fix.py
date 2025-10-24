"""
Test that the parallel fact generation fix works correctly.

This test verifies that switching from ProcessPoolExecutor to ThreadPoolExecutor
resolves the pickling error that occurred when using progress callbacks.
"""

import sys
import unittest
from unittest.mock import MagicMock, patch

# Add the src directory to the path
sys.path.insert(0, "src")


class TestParallelGenerationFix(unittest.TestCase):
    """Test that parallel fact generation uses ThreadPoolExecutor."""

    def test_threadpool_import(self):
        """Verify that ThreadPoolExecutor is imported instead of ProcessPoolExecutor."""
        from retail_datagen.generators import fact_generator

        # Check that ThreadPoolExecutor is imported
        self.assertTrue(hasattr(fact_generator, "ThreadPoolExecutor"))

        # ProcessPoolExecutor should not be imported
        self.assertFalse(hasattr(fact_generator, "ProcessPoolExecutor"))

    @patch("retail_datagen.generators.fact_generator.ThreadPoolExecutor")
    def test_parallel_uses_threadpool(self, mock_executor_class):
        """Verify that parallel generation uses ThreadPoolExecutor."""
        from retail_datagen.generators.fact_generator import FactDataGenerator
        from retail_datagen.config.models import RetailConfig
        from datetime import datetime

        # Create a minimal test config
        config = RetailConfig(
            seed=42,
            volume={
                "stores": 2,
                "dcs": 1,
                "trucks": 2,
                "total_customers": 10,
                "customers_per_day": 2,
                "items_per_ticket_mean": 4.2,
            },
            paths={
                "dict": "data/dictionaries",
                "master": "data/master",
                "facts": "data/facts",
            },
            realtime={"emit_interval_ms": 500, "burst": 100, "azure_connection_string": ""},
            stream={"hub": "test-hub"},
        )

        # Create mock executor instance
        mock_executor = MagicMock()
        mock_executor.__enter__ = MagicMock(return_value=mock_executor)
        mock_executor.__exit__ = MagicMock(return_value=None)
        mock_executor_class.return_value = mock_executor

        # Create generator with mocked master data
        generator = FactDataGenerator(config)
        generator.stores = [MagicMock()]
        generator.customers = [MagicMock()]
        generator.products = [MagicMock()]
        generator.dcs = [MagicMock()]
        generator.geographies = [MagicMock()]

        # Mock the progress callback (this would cause pickling error with ProcessPoolExecutor)
        generator._progress_callback = MagicMock()

        # Try to generate with parallel=True
        # This should work without pickling errors
        try:
            with patch.object(generator, "_generate_and_export_day", return_value=({}, 0)):
                with patch.object(generator, "load_master_data"):
                    # Just test that we can create the executor without errors
                    generator.generate_historical_data(
                        start_date=datetime(2024, 1, 1),
                        end_date=datetime(2024, 1, 1),
                        parallel=True,
                    )

            # Verify ThreadPoolExecutor was called
            mock_executor_class.assert_called_once()

            # The max_workers should be capped at 8
            call_args = mock_executor_class.call_args
            if call_args and call_args[1]:  # Check kwargs
                max_workers = call_args[1].get("max_workers", 8)
                self.assertLessEqual(max_workers, 8)

        except Exception as e:
            self.fail(f"Parallel generation failed with error: {e}")

    def test_docstring_updated(self):
        """Verify that the docstring was updated to reflect thread-safe behavior."""
        from retail_datagen.generators.fact_generator import FactDataGenerator

        # Check that the docstring mentions threads, not pickling
        docstring = FactDataGenerator._generate_and_export_day.__doc__
        self.assertIn("thread", docstring.lower())
        self.assertIn("thread-safe", docstring.lower())
        # Should not mention picklable anymore
        self.assertNotIn("picklable", docstring.lower())


if __name__ == "__main__":
    unittest.main()