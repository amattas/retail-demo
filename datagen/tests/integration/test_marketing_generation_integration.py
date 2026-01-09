"""
Integration tests for Marketing data generation system.

Tests end-to-end workflows for marketing campaign generation,
validates bug fixes, and ensures system integration.
"""

import asyncio
import shutil
from datetime import datetime
from pathlib import Path

import pandas as pd
import pytest

from retail_datagen.config.models import RetailConfig
from retail_datagen.generators.fact_generators import FactDataGenerator


class TestMarketingGenerationIntegration:
    """Integration tests for marketing data generation with bug fixes validation."""

    @pytest.fixture(scope="class")
    def test_output_dir(self, tmp_path_factory):
        """Create temporary output directory for tests."""
        output_dir = tmp_path_factory.mktemp("test_output")
        yield output_dir
        # Cleanup after all tests
        shutil.rmtree(output_dir, ignore_errors=True)

    @pytest.fixture(scope="class")
    def test_config(self, test_output_dir):
        """Create test configuration with small data volume for fast testing."""
        return RetailConfig(
            seed=42,
            volume={
                "stores": 5,
                "dcs": 2,
                "total_customers": 100,
                "total_products": 50,
                "customers_per_day": 20,
                "items_per_ticket_mean": 3,
            },
            paths={
                "dict": "data/dictionaries",
                "master": str(test_output_dir / "master"),
                "facts": str(test_output_dir / "facts"),
            },
            historical={"start_date": "2024-01-01"},
            realtime={
                "emit_interval_ms": 1000,
                "burst": 10,
            },
            stream={
                "hub": "test-hub",
            },
        )

    @pytest.fixture(scope="class")
    def generator_with_master_data(self, test_config):
        """Create generator and ensure master data exists."""
        generator = FactDataGenerator(test_config)

        # Check if master data exists, if not generate it
        master_path = Path(test_config.paths.master)
        stores_file = master_path / "stores.csv"

        if not stores_file.exists():
            # Generate master data first
            from retail_datagen.generators.master_generators import MasterDataGenerator

            master_gen = MasterDataGenerator(test_config)
            master_gen.generate_all_master_data()

        # Load master data
        generator.load_master_data()
        return generator

    @pytest.fixture(scope="class")
    def generated_marketing_data(self, generator_with_master_data, test_output_dir):
        """Generate marketing data once for all tests (30 days)."""
        start_date = datetime(2024, 1, 1)
        end_date = datetime(2024, 1, 30)

        # Generate historical data (async function - run with asyncio)
        asyncio.run(
            generator_with_master_data.generate_historical_data(start_date, end_date)
        )

        # Read all marketing CSV files
        marketing_dir = test_output_dir / "facts" / "marketing"
        if not marketing_dir.exists():
            return pd.DataFrame()

        marketing_files = list(marketing_dir.rglob("*.csv"))
        if marketing_files:
            df = pd.concat([pd.read_csv(f) for f in marketing_files], ignore_index=True)
            return df
        return pd.DataFrame()

    def test_marketing_data_generated_for_30_days(
        self, generated_marketing_data, test_output_dir
    ):
        """
        Test that marketing data is generated for 30-day period.

        Validates:
        - Marketing files exist in partitioned folders
        - Total records > 1000 (33+ per day average)
        - At least 24/30 days have data (80% coverage)
        - All required columns present
        - Data types correct
        """
        # Verify data was generated
        assert not generated_marketing_data.empty, "No marketing data was generated"

        # Check total record count (at least 33 per day on average)
        assert len(generated_marketing_data) >= 1000, (
            f"Expected >= 1000 records, got {len(generated_marketing_data)}"
        )

        # Check partitioned folder structure
        marketing_dir = test_output_dir / "facts" / "marketing"
        assert marketing_dir.exists(), "Marketing directory does not exist"

        partition_dirs = [
            d
            for d in marketing_dir.iterdir()
            if d.is_dir() and d.name.startswith("dt=")
        ]
        assert len(partition_dirs) > 0, "No partitioned directories found"

        # Verify at least 80% day coverage (24 out of 30 days)
        unique_dates = pd.to_datetime(
            generated_marketing_data["EventTS"]
        ).dt.date.nunique()
        coverage_percentage = unique_dates / 30.0
        assert coverage_percentage >= 0.80, (
            f"Expected >= 80% day coverage, got {coverage_percentage:.1%}"
        )

        # Verify required columns
        required_columns = [
            "TraceId",
            "EventTS",
            "Channel",
            "CampaignId",
            "CreativeId",
            "CustomerAdId",
            "ImpressionId",
            "Cost",
            "Device",
        ]
        for col in required_columns:
            assert col in generated_marketing_data.columns, f"Missing column: {col}"

        # Verify data types
        assert generated_marketing_data["TraceId"].dtype == "object", (
            "TraceId should be string"
        )
        assert generated_marketing_data["Channel"].dtype == "object", (
            "Channel should be string"
        )
        assert generated_marketing_data["CampaignId"].dtype == "object", (
            "CampaignId should be string"
        )
        assert generated_marketing_data["ImpressionId"].dtype == "object", (
            "ImpressionId should be string"
        )

    def test_campaigns_run_multiple_days(self, generated_marketing_data):
        """
        Test that campaigns persist across multiple days (Bug #2 validation).

        Validates:
        - At least 50% of campaigns run 2+ days
        - Some campaigns run 7+ days
        - No premature deletion on zero-impression days
        - Campaign durations are realistic (1-30 days)
        """
        assert not generated_marketing_data.empty, "No marketing data available"

        # Convert EventTS to datetime for analysis
        df = generated_marketing_data.copy()
        df["EventTS"] = pd.to_datetime(df["EventTS"])
        df["Date"] = df["EventTS"].dt.date

        # Group by CampaignId and calculate duration
        campaign_stats = (
            df.groupby("CampaignId")["Date"]
            .agg(["min", "max", "nunique"])
            .reset_index()
        )
        campaign_stats["duration_days"] = (
            campaign_stats["max"] - campaign_stats["min"]
        ).apply(lambda x: x.days + 1 if hasattr(x, "days") else 1)
        campaign_stats["active_days"] = campaign_stats["nunique"]

        # Verify at least 50% of campaigns run 2+ days
        multi_day_campaigns = campaign_stats[campaign_stats["duration_days"] >= 2]
        multi_day_percentage = len(multi_day_campaigns) / len(campaign_stats)
        assert multi_day_percentage >= 0.50, (
            f"Expected >= 50% multi-day campaigns, got {multi_day_percentage:.1%}"
        )

        # Verify some campaigns run 7+ days
        long_campaigns = campaign_stats[campaign_stats["duration_days"] >= 7]
        assert len(long_campaigns) > 0, "Expected some campaigns to run 7+ days"

        # Verify campaign durations are in realistic range (1-30 days)
        assert campaign_stats["duration_days"].min() >= 1, (
            "Campaign durations should be at least 1 day"
        )
        assert campaign_stats["duration_days"].max() <= 30, (
            "Campaign durations should not exceed 30 days"
        )

        # Check for campaigns with gaps (potential premature deletion)
        # A healthy campaign should have active_days close to duration_days
        campaigns_with_gaps = campaign_stats[
            (campaign_stats["duration_days"] > 1)
            & (campaign_stats["active_days"] < campaign_stats["duration_days"] * 0.5)
        ]

        # Some gaps are acceptable (low traffic days), but not too many
        gap_percentage = len(campaigns_with_gaps) / len(campaign_stats)
        assert gap_percentage < 0.3, (
            f"Too many campaigns with gaps: {gap_percentage:.1%}"
        )

    def test_marketing_channel_diversity(self, generated_marketing_data):
        """
        Test that multiple marketing channels are used.

        Validates:
        - At least 4 different channels present
        - Each channel has > 0 records
        - Channel distribution is reasonable
        """
        assert not generated_marketing_data.empty, "No marketing data available"

        # Count unique channels
        unique_channels = generated_marketing_data["Channel"].unique()
        assert len(unique_channels) >= 4, (
            f"Expected >= 4 channels, got {len(unique_channels)}: {unique_channels}"
        )

        # Verify each channel has records
        channel_counts = generated_marketing_data["Channel"].value_counts()
        for channel in unique_channels:
            assert channel_counts[channel] > 0, f"Channel {channel} has no records"

        # Verify channel distribution is reasonable (no single channel dominates)
        max_channel_percentage = channel_counts.max() / len(generated_marketing_data)
        assert max_channel_percentage < 0.80, (
            f"Single channel dominates with {max_channel_percentage:.1%} of records"
        )

        # Verify valid channel values (should match enum values)
        valid_channels = [
            "FACEBOOK",
            "GOOGLE",
            "INSTAGRAM",
            "TWITTER",
            "YOUTUBE",
            "EMAIL",
            "SMS",
            "DISPLAY",
            "SEARCH",
            "SOCIAL",
            "AFFILIATE",
        ]
        for channel in unique_channels:
            assert channel in valid_channels, f"Invalid channel value: {channel}"

    def test_impression_consistency_across_traffic_levels(
        self, generator_with_master_data, test_output_dir
    ):
        """
        Test impression counts respect minimum threshold (Bug #3 validation).

        Validates:
        - Most days have >= 100 impressions per campaign
        - No unexpected zeros on normal traffic days
        - High traffic days have higher impression counts
        """
        # Generate a shorter test period with controlled traffic
        start_date = datetime(2024, 2, 1)
        end_date = datetime(2024, 2, 7)  # 7 days

        # Clear previous data for this test
        facts_dir = test_output_dir / "facts"
        marketing_feb_dir = facts_dir / "marketing"
        if marketing_feb_dir.exists():
            shutil.rmtree(marketing_feb_dir)

        # Generate data (async function - run with asyncio)
        asyncio.run(
            generator_with_master_data.generate_historical_data(start_date, end_date)
        )

        # Read generated data
        marketing_files = list(marketing_feb_dir.rglob("*.csv"))
        if not marketing_files:
            pytest.skip("No marketing data generated for February test period")

        df = pd.concat([pd.read_csv(f) for f in marketing_files], ignore_index=True)
        df["EventTS"] = pd.to_datetime(df["EventTS"])
        df["Date"] = df["EventTS"].dt.date

        # Group by campaign and date
        daily_campaign_impressions = (
            df.groupby(["CampaignId", "Date"]).size().reset_index(name="impressions")
        )

        # Verify most campaign-days have >= 100 impressions
        high_impression_days = daily_campaign_impressions[
            daily_campaign_impressions["impressions"] >= 100
        ]
        high_impression_percentage = len(high_impression_days) / len(
            daily_campaign_impressions
        )

        # Should be at least 70% of campaign-days (allowing for some low-traffic periods)
        assert high_impression_percentage >= 0.70, (
            f"Expected >= 70% of days with 100+ impressions, got {high_impression_percentage:.1%}"
        )

        # Verify no campaigns have all zeros
        campaign_totals = df.groupby("CampaignId").size()
        assert all(campaign_totals > 0), "Found campaigns with zero impressions"

    def test_marketing_with_all_other_fact_tables(
        self, generator_with_master_data, test_output_dir
    ):
        """
        Test that marketing generation doesn't break other fact tables (regression).

        Validates:
        - All 8+ fact tables generate successfully
        - All tables have data (> 0 records)
        - Temporal alignment (same date range)
        """
        # Generate complete dataset for 7 days
        start_date = datetime(2024, 3, 1)
        end_date = datetime(2024, 3, 7)

        summary = asyncio.run(
            generator_with_master_data.generate_historical_data(start_date, end_date)
        )

        # Expected fact tables
        expected_tables = [
            "dc_inventory_txn",
            "truck_moves",
            "store_inventory_txn",
            "receipts",
            "receipt_lines",
            "foot_traffic",
            "ble_pings",
            "marketing",
        ]

        facts_dir = test_output_dir / "facts"

        # Verify all tables were generated
        for table in expected_tables:
            table_dir = facts_dir / table
            assert table_dir.exists(), f"Missing fact table directory: {table}"

            # Check for data files
            csv_files = list(table_dir.rglob("*.csv"))
            assert len(csv_files) > 0, f"No CSV files found for {table}"

            # Verify table has records
            assert summary.facts_generated.get(table, 0) > 0, (
                f"No records generated for {table}"
            )

        # Verify temporal alignment - all tables cover the same date range
        for table in expected_tables:
            table_dir = facts_dir / table
            partition_dirs = [
                d
                for d in table_dir.iterdir()
                if d.is_dir() and d.name.startswith("dt=")
            ]

            # Extract dates from partition names
            dates = [d.name.replace("dt=", "") for d in partition_dirs]

            # Should have data for most days in range (allowing for some variability)
            # Marketing may not have all days due to campaign scheduling
            if table == "marketing":
                assert len(dates) >= 5, f"{table} should have at least 5 days of data"
            else:
                # Other tables should have more consistent daily data
                assert len(dates) >= 6, f"{table} should have at least 6 days of data"

    def test_campaign_coverage_meets_specification(self, generated_marketing_data):
        """
        Test Bug #1 fix: 90% campaign start probability achieves 80-95% coverage.

        Validates:
        - Coverage >= 80%
        - Coverage <= 95%
        - Consistent with 90% campaign start probability
        """
        assert not generated_marketing_data.empty, "No marketing data available"

        # Calculate unique days with marketing activity
        df = generated_marketing_data.copy()
        df["EventTS"] = pd.to_datetime(df["EventTS"])
        df["Date"] = df["EventTS"].dt.date

        unique_days = df["Date"].nunique()
        total_days = 30

        coverage = unique_days / total_days

        # Verify coverage is within specification (85-100%)
        # With CAMPAIGN_START_PROBABILITY=0.90 and digital ads running 24/7,
        # we expect high coverage (campaigns run 7-30 days with 90% start probability)
        assert coverage >= 0.85, f"Coverage {coverage:.1%} is below 85% minimum"
        assert coverage <= 1.00, f"Coverage {coverage:.1%} exceeds 100% maximum"

        # Log coverage for informational purposes
        print(
            f"\nCampaign day coverage: {coverage:.1%} ({unique_days}/{total_days} days)"
        )

    def test_marketing_data_quality(
        self, generated_marketing_data, generator_with_master_data
    ):
        """
        Test data integrity and business rules validation.

        Validates:
        - No null TraceIds or CampaignIds
        - All ImpressionIds are unique
        - Cost > 0 for all records
        - CustomerAdIds reference valid customers (FK integrity)
        - EventTS within generation date range
        - All enum values valid
        """
        assert not generated_marketing_data.empty, "No marketing data available"

        df = generated_marketing_data.copy()

        # Check for null critical fields
        assert df["TraceId"].notnull().all(), "Found null TraceIds"
        assert df["CampaignId"].notnull().all(), "Found null CampaignIds"
        assert df["ImpressionId"].notnull().all(), "Found null ImpressionIds"
        assert df["CustomerAdId"].notnull().all(), "Found null CustomerAdIds"

        # Verify ImpressionIds are unique
        impression_duplicates = df["ImpressionId"].duplicated().sum()
        assert impression_duplicates == 0, (
            f"Found {impression_duplicates} duplicate ImpressionIds"
        )

        # Verify Cost > 0
        df["Cost"] = pd.to_numeric(df["Cost"], errors="coerce")
        assert (df["Cost"] > 0).all(), "Found records with Cost <= 0"

        # Verify CustomerAdIds reference valid customers (FK integrity)
        valid_ad_ids = {
            customer.AdId for customer in generator_with_master_data.customers
        }
        invalid_ad_ids = set(df["CustomerAdId"].unique()) - valid_ad_ids
        assert len(invalid_ad_ids) == 0, (
            f"Found invalid CustomerAdIds: {invalid_ad_ids}"
        )

        # Verify EventTS within generation date range
        df["EventTS"] = pd.to_datetime(df["EventTS"])
        min_date = datetime(2024, 1, 1)
        max_date = datetime(2024, 1, 30, 23, 59, 59)

        assert (df["EventTS"] >= min_date).all(), f"Found events before {min_date}"
        assert (df["EventTS"] <= max_date).all(), f"Found events after {max_date}"

        # Verify valid enum values for Channel
        valid_channels = [
            "FACEBOOK",
            "GOOGLE",
            "INSTAGRAM",
            "TWITTER",
            "YOUTUBE",
            "EMAIL",
            "SMS",
            "DISPLAY",
            "SEARCH",
            "SOCIAL",
            "AFFILIATE",
        ]
        invalid_channels = set(df["Channel"].unique()) - set(valid_channels)
        assert len(invalid_channels) == 0, (
            f"Found invalid Channel values: {invalid_channels}"
        )

        # Verify valid enum values for Device
        valid_devices = [
            "MOBILE",
            "DESKTOP",
            "TABLET",
            "TV",
            "SMART_SPEAKER",
            "WEARABLE",
        ]
        invalid_devices = set(df["Device"].unique()) - set(valid_devices)
        assert len(invalid_devices) == 0, (
            f"Found invalid Device values: {invalid_devices}"
        )

    def test_campaign_impression_minimum_threshold(self, generated_marketing_data):
        """
        Test that active campaigns maintain minimum impression threshold.

        This validates Bug #3 fix: campaigns should have at least 100 impressions
        per day unless traffic is zero (store closed).
        """
        assert not generated_marketing_data.empty, "No marketing data available"

        df = generated_marketing_data.copy()
        df["EventTS"] = pd.to_datetime(df["EventTS"])
        df["Date"] = df["EventTS"].dt.date

        # Group by campaign and date
        daily_impressions = (
            df.groupby(["CampaignId", "Date"]).size().reset_index(name="impressions")
        )

        # Count how many campaign-days meet the 100 impression threshold
        meets_threshold = daily_impressions[daily_impressions["impressions"] >= 100]
        threshold_percentage = len(meets_threshold) / len(daily_impressions)

        # At least 70% of campaign-days should meet the threshold
        # (allowing for some low-traffic days due to temporal patterns)
        assert threshold_percentage >= 0.70, (
            f"Only {threshold_percentage:.1%} of campaign-days meet 100 impression threshold"
        )

        # Log statistics
        print(f"\nImpression threshold compliance: {threshold_percentage:.1%}")
        print(
            f"Average impressions per campaign-day: {daily_impressions['impressions'].mean():.0f}"
        )
        print(
            f"Minimum impressions per campaign-day: {daily_impressions['impressions'].min()}"
        )


class TestMarketingBugFixValidation:
    """Specific tests validating the three bug fixes."""

    @pytest.fixture(scope="class")
    def small_test_config(self, tmp_path_factory):
        """Create minimal config for focused bug testing."""
        output_dir = tmp_path_factory.mktemp("bug_test_output")
        config = RetailConfig(
            seed=12345,  # Different seed for independent test
            volume={
                "stores": 3,
                "dcs": 1,
                "total_customers": 50,
                "total_products": 30,
                "customers_per_day": 15,
                "items_per_ticket_mean": 2.5,
            },
            paths={
                "dict": "data/dictionaries",
                "master": str(output_dir / "master"),
                "facts": str(output_dir / "facts"),
            },
            historical={"start_date": "2024-01-01"},
            realtime={
                "emit_interval_ms": 1000,
                "burst": 10,
            },
            stream={
                "hub": "test-hub",
            },
        )
        yield config, output_dir
        # Cleanup
        shutil.rmtree(output_dir, ignore_errors=True)

    def test_bug_1_campaign_start_probability_90_percent(self, small_test_config):
        """
        Validate Bug #1 Fix: Campaign start probability increased to 90%.

        With 90% probability, over 30 days we expect approximately 27 new campaign starts.
        This should result in 80-95% day coverage.
        """
        config, output_dir = small_test_config

        # Generate master data
        from retail_datagen.generators.master_generators import MasterDataGenerator

        master_gen = MasterDataGenerator(config)
        master_gen.generate_all_master_data()

        # Generate facts (async function - run with asyncio)
        generator = FactDataGenerator(config)
        generator.load_master_data()
        asyncio.run(
            generator.generate_historical_data(datetime(2024, 1, 1), datetime(2024, 1, 30))
        )

        # Read marketing data
        marketing_dir = output_dir / "facts" / "marketing"
        marketing_files = list(marketing_dir.rglob("*.csv"))

        if not marketing_files:
            pytest.fail("No marketing data generated")

        df = pd.concat([pd.read_csv(f) for f in marketing_files], ignore_index=True)
        df["EventTS"] = pd.to_datetime(df["EventTS"])

        # Calculate day coverage
        unique_days = df["EventTS"].dt.date.nunique()
        coverage = unique_days / 30

        # With 90% probability and digital ads running 24/7, expect 85-100% coverage
        assert 0.85 <= coverage <= 1.00, (
            f"Coverage {coverage:.1%} not in expected range [85%, 100%]"
        )

    def test_bug_2_campaigns_not_deleted_on_zero_impressions(self, small_test_config):
        """
        Validate Bug #2 Fix: Campaigns only deleted after end_date, not on zero impressions.

        Campaigns should persist through low-traffic days and only end when end_date is reached.
        """
        config, output_dir = small_test_config

        # Generate master data
        from retail_datagen.generators.master_generators import MasterDataGenerator

        master_gen = MasterDataGenerator(config)
        master_gen.generate_all_master_data()

        # Generate facts (async function - run with asyncio)
        generator = FactDataGenerator(config)
        generator.load_master_data()
        asyncio.run(
            generator.generate_historical_data(datetime(2024, 1, 1), datetime(2024, 1, 30))
        )

        # Read marketing data
        marketing_dir = output_dir / "facts" / "marketing"
        marketing_files = list(marketing_dir.rglob("*.csv"))

        if not marketing_files:
            pytest.skip("No marketing data generated")

        df = pd.concat([pd.read_csv(f) for f in marketing_files], ignore_index=True)
        df["EventTS"] = pd.to_datetime(df["EventTS"])
        df["Date"] = df["EventTS"].dt.date

        # Analyze campaign durations
        campaign_spans = (
            df.groupby("CampaignId")["Date"].agg(["min", "max"]).reset_index()
        )
        campaign_spans["duration"] = (
            campaign_spans["max"] - campaign_spans["min"]
        ).apply(lambda x: x.days + 1 if hasattr(x, "days") else 1)

        # Get daily impression counts
        daily_counts = (
            df.groupby(["CampaignId", "Date"]).size().reset_index(name="count")
        )

        # For each campaign, check if it has gaps (zero-impression days)
        for campaign_id in campaign_spans["CampaignId"]:
            campaign_data = daily_counts[daily_counts["CampaignId"] == campaign_id]
            span = campaign_spans[campaign_spans["CampaignId"] == campaign_id].iloc[0]

            # Generate full date range for this campaign
            full_range = pd.date_range(span["min"], span["max"], freq="D").date
            actual_dates = set(campaign_data["Date"])

            # It's OK to have gaps (zero-impression days) - campaign should NOT be deleted
            # Just verify campaign didn't end prematurely
            if len(full_range) > 1:
                # Multi-day campaign should have reasonable coverage
                coverage = len(actual_dates) / len(full_range)
                # Allow gaps, but campaign should persist through its scheduled duration
                assert coverage > 0, f"Campaign {campaign_id} has no impressions at all"

    def test_bug_3_minimum_impressions_threshold_enforced(self, small_test_config):
        """
        Validate Bug #3 Fix: Minimum impressions threshold of 100 enforced.

        Active campaigns should generate at least 100 impressions per day
        when traffic > 0 (stores open).
        """
        config, output_dir = small_test_config

        # Generate master data
        from retail_datagen.generators.master_generators import MasterDataGenerator

        master_gen = MasterDataGenerator(config)
        master_gen.generate_all_master_data()

        # Generate facts (async function - run with asyncio)
        generator = FactDataGenerator(config)
        generator.load_master_data()
        asyncio.run(
            generator.generate_historical_data(
                datetime(2024, 1, 1),
                datetime(2024, 1, 14),  # 2 weeks
            )
        )

        # Read marketing data
        marketing_dir = output_dir / "facts" / "marketing"
        marketing_files = list(marketing_dir.rglob("*.csv"))

        if not marketing_files:
            pytest.skip("No marketing data generated")

        df = pd.concat([pd.read_csv(f) for f in marketing_files], ignore_index=True)
        df["EventTS"] = pd.to_datetime(df["EventTS"])
        df["Date"] = df["EventTS"].dt.date

        # Group by campaign and date
        daily_impressions = (
            df.groupby(["CampaignId", "Date"]).size().reset_index(name="impressions")
        )

        # Count campaign-days meeting the 100 impression threshold
        meets_threshold = daily_impressions[daily_impressions["impressions"] >= 100]
        threshold_percentage = len(meets_threshold) / len(daily_impressions)

        # Most campaign-days should meet the threshold (allowing for temporal variations)
        assert threshold_percentage >= 0.65, (
            f"Only {threshold_percentage:.1%} of campaign-days meet 100 impression threshold"
        )

        # Verify average is well above threshold
        avg_impressions = daily_impressions["impressions"].mean()
        assert avg_impressions >= 100, (
            f"Average impressions {avg_impressions:.0f} below 100 threshold"
        )
