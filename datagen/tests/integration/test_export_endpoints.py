"""
Integration tests for data export API endpoints.

This module tests the complete export workflow from API request to file generation,
including master data export, fact data export, progress tracking, and error scenarios.

Tests verify:
- End-to-end export operations (API → Service → Database → Files)
- Both CSV and Parquet formats
- Date range filtering for fact tables
- Partitioned file structure
- Real-time progress tracking
- Error handling and validation
- File content verification
"""

import asyncio
import time
from datetime import date, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from uuid import uuid4

import pandas as pd
import pytest
from fastapi.testclient import TestClient
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine

from retail_datagen.config.models import RetailConfig
from retail_datagen.db.engine import get_retail_engine
from retail_datagen.db.models.base import Base
from retail_datagen.db.models.facts import (
    BLEPing,
    DCInventoryTransaction,
    FootTraffic,
    MarketingImpression,
    OnlineOrder,
    Receipt,
    ReceiptLine,
    StoreInventoryTransaction,
    TruckMove,
)
from retail_datagen.db.models.master import (
    Customer,
    DistributionCenter,
    Geography,
    Product,
    Store,
    Truck,
)
from retail_datagen.db.session import get_retail_session
from retail_datagen.main import app
from retail_datagen.shared.dependencies import _task_status, get_config


# ================================
# FIXTURES
# ================================


@pytest.fixture
def temp_export_dir(tmp_path):
    """Create temporary directory structure for exports."""
    master_dir = tmp_path / "master"
    facts_dir = tmp_path / "facts"

    master_dir.mkdir(parents=True)
    facts_dir.mkdir(parents=True)

    return tmp_path


@pytest.fixture
def test_retail_db(temp_export_dir):
    """
    Create in-memory test database with sample master and fact data.

    Creates a complete test dataset with:
    - 2 geographies (for FK relationships)
    - 3 stores
    - 2 distribution centers
    - 2 trucks
    - 10 customers
    - 20 products
    - Fact data spanning 3 dates with realistic relationships
    """
    # Create async engine for in-memory SQLite
    engine = create_async_engine(
        "sqlite+aiosqlite:///:memory:",
        echo=False,
        future=True,
    )

    # Helper to run async code
    async def setup_database():
        # Create all tables
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)

        # Populate with test data
        async with AsyncSession(engine, expire_on_commit=False) as session:
            # Create geographies (2 different states for diversity)
            geo1 = Geography(
            geography_id=1,
            city="Springfield",
            state="IL",
            postal_code="62701",
            district="Central",
            region="Midwest",
        )
        geo2 = Geography(
            geography_id=2,
            city="Riverside",
            state="CA",
            postal_code="92501",
            district="Inland Empire",
            region="West",
        )
        session.add_all([geo1, geo2])

        # Create stores (linked to geographies)
        store1 = Store(
            store_id=1,
            store_number="ST001",
            address="123 Main St, Springfield, IL 62701",
            geography_id=1,
        )
        store2 = Store(
            store_id=2,
            store_number="ST002",
            address="456 Oak Ave, Riverside, CA 92501",
            geography_id=2,
        )
        store3 = Store(
            store_id=3,
            store_number="ST003",
            address="789 Pine Rd, Springfield, IL 62702",
            geography_id=1,
        )
        session.add_all([store1, store2, store3])

        # Create distribution centers
        dc1 = DistributionCenter(
            dc_id=1,
            dc_number="DC001",
            address="100 Warehouse Dr, Springfield, IL 62701",
            geography_id=1,
        )
        dc2 = DistributionCenter(
            dc_id=2,
            dc_number="DC002",
            address="200 Industrial Blvd, Riverside, CA 92501",
            geography_id=2,
        )
        session.add_all([dc1, dc2])

        # Create trucks
        truck1 = Truck(truck_id=1, license_plate="TRK001", refrigeration=False, dc_id=1)
        truck2 = Truck(truck_id=2, license_plate="TRK002", refrigeration=True, dc_id=2)
        session.add_all([truck1, truck2])

        # Create customers
        customers = []
        for i in range(1, 11):
            customer = Customer(
                customer_id=i,
                first_name=f"FirstName{i}",
                last_name=f"LastName{i}",
                address=f"{100 + i} Test St, Springfield, IL 62701",
                geography_id=1 if i % 2 == 0 else 2,
                loyalty_card=f"LC{str(i).zfill(9)}",
                phone=f"555-{str(i).zfill(3)}-{str(i * 100).zfill(4)}",
                ble_id=f"BLE{str(i).zfill(6)}",
                ad_id=f"AD{str(i).zfill(6)}",
            )
            customers.append(customer)
        session.add_all(customers)

        # Create products
        products = []
        for i in range(1, 21):
            base_price = float(10.0 * i)
            cost = base_price * 0.60
            msrp = base_price * 1.10
            product = Product(
                product_id=i,
                product_name=f"Product {i}",
                brand=f"Brand{i % 5}",
                company=f"Company{i % 3}",
                department=f"Dept{i % 4}",
                category=f"Cat{i % 8}",
                subcategory=f"SubCat{i}",
                cost=cost,
                msrp=msrp,
                sale_price=base_price,
                requires_refrigeration=(i % 3 == 0),
                launch_date=date(2024, 1, 1),
            )
            products.append(product)
        session.add_all(products)

        await session.commit()

        # Create fact data spanning 3 dates
        base_date = date(2024, 1, 1)
        for day_offset in range(3):
            current_date = base_date + timedelta(days=day_offset)
            base_ts = datetime.combine(current_date, datetime.min.time())

            # DC inventory transactions (inbound shipments)
            for dc_id in [1, 2]:
                for product_id in range(1, 11):
                    dc_txn = DCInventoryTransaction(
                        dc_id=dc_id,
                        product_id=product_id,
                        event_ts=base_ts + timedelta(hours=8),
                        txn_type="receipt",
                        quantity=100 * (day_offset + 1),
                        balance=100 * (day_offset + 1),
                    )
                    session.add(dc_txn)

            # Truck moves (DC to store)
            for truck_id in [1, 2]:
                truck_move = TruckMove(
                    truck_id=truck_id,
                    dc_id=truck_id,  # DC 1 → Truck 1, DC 2 → Truck 2
                    store_id=truck_id,  # Truck 1 → Store 1, Truck 2 → Store 2
                    product_id=1,  # Some product
                    event_ts=base_ts + timedelta(hours=10),
                    status="DELIVERED",
                    shipment_id=f"SHIP{day_offset:03d}{truck_id}",
                    eta=base_ts + timedelta(hours=14),
                    etd=base_ts + timedelta(hours=15),
                )
                session.add(truck_move)

            # Store inventory transactions (truck deliveries)
            for store_id in [1, 2, 3]:
                for product_id in range(1, 11):
                    store_txn = StoreInventoryTransaction(
                        store_id=store_id,
                        product_id=product_id,
                        event_ts=base_ts + timedelta(hours=15),
                        txn_type="delivery",
                        quantity=50 * (day_offset + 1),
                        balance=50 * (day_offset + 1),
                    )
                    session.add(store_txn)

            # Receipts and receipt lines (customer purchases)
            for receipt_num in range(1, 6):
                receipt_id_ext = f"RCP{current_date.strftime('%Y%m%d')}{receipt_num:03d}"
                subtotal = 0.0
                receipt = Receipt(
                    receipt_id_ext=receipt_id_ext,
                    store_id=(receipt_num % 3) + 1,
                    customer_id=(receipt_num % 10) + 1,
                    event_ts=base_ts + timedelta(hours=16 + receipt_num),
                    total_amount=0.0,  # Will update after lines
                    tax_amount=0.0,
                    discount_amount=0.0,
                    payment_method="credit_card" if receipt_num % 2 == 0 else "cash",
                )

                # Must add receipt first to get auto-generated receipt_id
                session.add(receipt)
                await session.flush()  # Get the receipt_id

                # Add 2-4 line items per receipt
                num_lines = 2 + (receipt_num % 3)
                for line_num in range(1, num_lines + 1):
                    product_id = ((receipt_num + line_num) % 20) + 1
                    qty = 1 + (line_num % 3)
                    unit_price = float(10.0 * product_id)
                    line_total = unit_price * qty
                    subtotal += line_total

                    line = ReceiptLine(
                        receipt_id=receipt.receipt_id,  # Use auto-generated FK
                        product_id=product_id,
                        quantity=qty,
                        unit_price=unit_price,
                        line_total=line_total,
                    )
                    session.add(line)

                # Update receipt totals
                receipt.total_amount = subtotal
                receipt.tax_amount = subtotal * 0.08
                receipt.total_amount = subtotal + receipt.tax_amount

            # Foot traffic
            for store_id in [1, 2, 3]:
                for hour in [9, 12, 15, 18]:
                    foot_traffic = FootTraffic(
                        store_id=store_id,
                        event_ts=base_ts + timedelta(hours=hour),
                        sensor_id=f"SENSOR{store_id:03d}",
                        zone="ENTRANCE" if hour < 12 else "CHECKOUT",
                        dwell_seconds=15 + (hour % 60),
                        count=10 + (hour % 20),
                    )
                    session.add(foot_traffic)

            # BLE pings
            for customer_id in range(1, 6):
                ble_ping = BLEPing(
                    store_id=(customer_id % 3) + 1,
                    event_ts=base_ts + timedelta(hours=14),
                    beacon_id=f"BEACON{customer_id:03d}",
                    customer_ble_id=f"BLE{str(customer_id).zfill(6)}",
                    rssi=-65 - (customer_id % 10),
                    zone="ELECTRONICS" if customer_id % 2 == 0 else "GROCERY",
                    customer_id=customer_id,
                )
                session.add(ble_ping)

            # Marketing events
            for customer_id in range(1, 6):
                marketing = MarketingImpression(
                    event_ts=base_ts + timedelta(hours=10),
                    campaign_id=f"CAMP{customer_id:03d}",
                    creative_id=f"CREATIVE{customer_id:03d}",
                    impression_id_ext=f"IMP{current_date.strftime('%Y%m%d')}{customer_id:03d}",
                    customer_ad_id=f"AD{str(customer_id).zfill(6)}",
                    channel="social" if customer_id % 2 == 0 else "display",
                    device="mobile" if customer_id % 2 == 0 else "desktop",
                    cost=float(0.25 * customer_id),
                    customer_id=customer_id,
                )
                session.add(marketing)

            # Online orders
            for order_num in range(1, 4):
                order = OnlineOrder(
                    customer_id=(order_num % 10) + 1,
                    product_id=(order_num % 20) + 1,
                    event_ts=base_ts + timedelta(hours=11 + order_num),
                    quantity=order_num,
                    total_amount=float(50.0 * order_num),
                    fulfillment_status="created",
                )
                session.add(order)

            await session.commit()

    # Run the async setup
    asyncio.run(setup_database())

    yield engine

    # Cleanup
    async def cleanup():
        await engine.dispose()

    asyncio.run(cleanup())


@pytest.fixture
def test_config(temp_export_dir):
    """Create test configuration with temporary directories."""
    config = RetailConfig(
        seed=42,
        volume={
            "stores": 3,
            "dcs": 2,
            "customers_per_day": 10,
            "items_per_ticket_mean": 3.0,
        },
        realtime={"emit_interval_ms": 500, "burst": 100},
        paths={
            "dictionaries": str(temp_export_dir / "dictionaries"),
            "master": str(temp_export_dir / "master"),
            "facts": str(temp_export_dir / "facts"),
        },
        stream={"hub": "test-retail-events"},
    )
    return config


@pytest.fixture
def client(test_config, test_retail_db):
    """
    Create FastAPI test client with dependency overrides.

    Overrides:
    - get_config: Use test configuration with temp directories
    - get_retail_session: Use in-memory test database
    """

    async def override_get_config():
        return test_config

    async def override_get_retail_session():
        async with AsyncSession(test_retail_db, expire_on_commit=False) as session:
            yield session

    # Override dependencies
    app.dependency_overrides[get_config] = override_get_config
    app.dependency_overrides[get_retail_session] = override_get_retail_session

    with TestClient(app) as test_client:
        yield test_client

    # Cleanup
    app.dependency_overrides.clear()


@pytest.fixture(autouse=True)
def cleanup_tasks():
    """Clean up task status before and after each test."""
    _task_status.clear()
    yield
    _task_status.clear()


# ================================
# HELPER FUNCTIONS
# ================================


def poll_until_complete(
    client: TestClient,
    task_id: str,
    max_polls: int = 30,
    poll_interval: float = 0.5,
    timeout: float = 15.0,
) -> dict:
    """
    Poll export status endpoint until task completes or times out.

    Args:
        client: FastAPI test client
        task_id: Export task ID to poll
        max_polls: Maximum number of polling attempts
        poll_interval: Seconds between polls
        timeout: Maximum total time to wait

    Returns:
        Final status response data

    Raises:
        TimeoutError: If task doesn't complete within timeout
    """
    start_time = time.time()

    for attempt in range(max_polls):
        # Check timeout
        if time.time() - start_time > timeout:
            raise TimeoutError(
                f"Export task {task_id} did not complete within {timeout}s"
            )

        # Poll status
        response = client.get(f"/api/export/status/{task_id}")
        assert response.status_code == 200, f"Status check failed: {response.text}"

        status_data = response.json()

        # Check if completed or failed
        if status_data["status"] == "completed":
            return status_data
        elif status_data["status"] == "failed":
            error_msg = status_data.get("error_message", "Unknown error")
            raise RuntimeError(f"Export task {task_id} failed: {error_msg}")

        # Wait before next poll
        time.sleep(poll_interval)

    raise TimeoutError(
        f"Export task {task_id} did not complete within {max_polls} polls"
    )


def verify_csv_content(file_path: Path, expected_min_rows: int = 1) -> pd.DataFrame:
    """
    Verify CSV file exists and contains expected data.

    Args:
        file_path: Path to CSV file
        expected_min_rows: Minimum expected row count

    Returns:
        Loaded DataFrame

    Raises:
        AssertionError: If file doesn't exist or has insufficient rows
    """
    assert file_path.exists(), f"CSV file not found: {file_path}"
    df = pd.read_csv(file_path)
    assert len(df) >= expected_min_rows, (
        f"CSV has {len(df)} rows, expected at least {expected_min_rows}"
    )
    return df


def verify_parquet_content(
    file_path: Path, expected_min_rows: int = 1
) -> pd.DataFrame:
    """
    Verify Parquet file exists and contains expected data.

    Args:
        file_path: Path to Parquet file
        expected_min_rows: Minimum expected row count

    Returns:
        Loaded DataFrame

    Raises:
        AssertionError: If file doesn't exist or has insufficient rows
    """
    assert file_path.exists(), f"Parquet file not found: {file_path}"
    df = pd.read_parquet(file_path)
    assert len(df) >= expected_min_rows, (
        f"Parquet has {len(df)} rows, expected at least {expected_min_rows}"
    )
    return df


# ================================
# MASTER DATA EXPORT TESTS
# ================================


def test_export_master_csv_all_tables(client, temp_export_dir):
    """Test exporting all master tables to CSV format."""
    # POST export request
    response = client.post(
        "/api/export/master",
        json={"format": "csv", "tables": "all"},
    )

    assert response.status_code == 200
    data = response.json()
    assert data["success"] is True
    assert "task_id" in data
    assert data["task_id"].startswith("export_master_")

    task_id = data["task_id"]

    # Poll until complete
    final_status = poll_until_complete(client, task_id)

    # Verify final status
    assert final_status["status"] == "completed"
    assert final_status["progress"] == 1.0
    assert final_status["total_files"] == 6
    assert len(final_status["files_written"]) == 6
    assert len(final_status["tables_completed"]) == 6

    # Verify all master table files exist
    master_dir = temp_export_dir / "master"
    expected_tables = [
        "geographies_master",
        "stores",
        "distribution_centers",
        "trucks",
        "customers",
        "products_master",
    ]

    for table_name in expected_tables:
        file_path = master_dir / f"{table_name}.csv"
        df = verify_csv_content(file_path, expected_min_rows=1)

        # Verify table-specific row counts match test data
        if table_name == "geographies_master":
            assert len(df) == 2
        elif table_name == "stores":
            assert len(df) == 3
        elif table_name == "distribution_centers":
            assert len(df) == 2
        elif table_name == "trucks":
            assert len(df) == 2
        elif table_name == "customers":
            assert len(df) == 10
        elif table_name == "products_master":
            assert len(df) == 20


def test_export_master_parquet_all_tables(client, temp_export_dir):
    """Test exporting all master tables to Parquet format."""
    # POST export request
    response = client.post(
        "/api/export/master",
        json={"format": "parquet", "tables": "all"},
    )

    assert response.status_code == 200
    task_id = response.json()["task_id"]

    # Poll until complete
    final_status = poll_until_complete(client, task_id)

    # Verify final status
    assert final_status["status"] == "completed"
    assert final_status["progress"] == 1.0
    assert final_status["total_files"] == 6

    # Verify all master table Parquet files exist
    master_dir = temp_export_dir / "master"
    expected_tables = [
        "geographies_master",
        "stores",
        "distribution_centers",
        "trucks",
        "customers",
        "products_master",
    ]

    for table_name in expected_tables:
        file_path = master_dir / f"{table_name}.parquet"
        df = verify_parquet_content(file_path, expected_min_rows=1)

        # Verify row counts
        if table_name == "products_master":
            assert len(df) == 20
        elif table_name == "customers":
            assert len(df) == 10


def test_export_master_specific_tables(client, temp_export_dir):
    """Test exporting specific master tables (not all)."""
    # Export only stores and customers
    response = client.post(
        "/api/export/master",
        json={"format": "csv", "tables": ["stores", "customers"]},
    )

    assert response.status_code == 200
    task_id = response.json()["task_id"]

    # Poll until complete
    final_status = poll_until_complete(client, task_id)

    # Verify only requested tables were exported
    assert final_status["status"] == "completed"
    assert final_status["total_files"] == 2

    master_dir = temp_export_dir / "master"

    # These should exist
    stores_file = master_dir / "stores.csv"
    customers_file = master_dir / "customers.csv"
    assert stores_file.exists()
    assert customers_file.exists()

    # These should NOT exist
    products_file = master_dir / "products_master.csv"
    assert not products_file.exists()


def test_export_master_progress_tracking(client):
    """Test that master export provides real-time progress updates."""
    # Start export
    response = client.post(
        "/api/export/master",
        json={"format": "csv", "tables": "all"},
    )
    task_id = response.json()["task_id"]

    # Poll and track progress
    progress_values = []
    max_polls = 20

    for _ in range(max_polls):
        status_response = client.get(f"/api/export/status/{task_id}")
        status_data = status_response.json()

        progress_values.append(status_data["progress"])

        # Verify progress fields exist
        assert "tables_completed" in status_data
        assert "tables_remaining" in status_data
        assert "current_table" in status_data

        if status_data["status"] == "completed":
            break

        time.sleep(0.3)

    # Verify progress increased monotonically
    assert progress_values[-1] == 1.0
    assert all(
        progress_values[i] <= progress_values[i + 1]
        for i in range(len(progress_values) - 1)
    )


# ================================
# FACT DATA EXPORT TESTS
# ================================


def test_export_facts_csv_all_tables(client, temp_export_dir):
    """Test exporting all fact tables to CSV format."""
    # POST export request
    response = client.post(
        "/api/export/facts",
        json={"format": "csv", "tables": "all"},
    )

    assert response.status_code == 200
    task_id = response.json()["task_id"]

    # Poll until complete
    final_status = poll_until_complete(client, task_id)

    # Verify final status
    assert final_status["status"] == "completed"
    assert final_status["progress"] == 1.0
    assert final_status["total_files"] > 0  # At least some partitions

    # Verify partitioned structure exists
    facts_dir = temp_export_dir / "facts"

    expected_tables = [
        "dc_inventory_txn",
        "truck_moves",
        "store_inventory_txn",
        "receipts",
        "receipt_lines",
        "foot_traffic",
        "ble_pings",
        "marketing",
        "online_orders",
    ]

    for table_name in expected_tables:
        table_dir = facts_dir / table_name
        assert table_dir.exists(), f"Table directory not found: {table_dir}"

        # Verify partition directories exist (dt=YYYY-MM-DD)
        partition_dirs = list(table_dir.glob("dt=*"))
        assert len(partition_dirs) > 0, f"No partitions found for {table_name}"


def test_export_facts_parquet_with_date_filter(client, temp_export_dir):
    """Test exporting fact tables with date range filtering in Parquet format."""
    # Export only first 2 days of test data
    start_date = "2024-01-01"
    end_date = "2024-01-02"

    response = client.post(
        "/api/export/facts",
        json={
            "format": "parquet",
            "tables": "all",
            "start_date": start_date,
            "end_date": end_date,
        },
    )

    assert response.status_code == 200
    task_id = response.json()["task_id"]

    # Poll until complete
    final_status = poll_until_complete(client, task_id)

    assert final_status["status"] == "completed"

    # Verify only partitions within date range exist
    facts_dir = temp_export_dir / "facts"

    # Check receipts as representative table
    receipts_dir = facts_dir / "receipts"
    partition_dirs = sorted(receipts_dir.glob("dt=*"))

    # Should have 2 partitions (2024-01-01 and 2024-01-02)
    assert len(partition_dirs) == 2

    partition_dates = [p.name.replace("dt=", "") for p in partition_dirs]
    assert "2024-01-01" in partition_dates
    assert "2024-01-02" in partition_dates
    assert "2024-01-03" not in partition_dates


def test_export_facts_partitioned_file_structure(client, temp_export_dir):
    """Test that fact exports create correct partitioned directory structure."""
    response = client.post(
        "/api/export/facts",
        json={
            "format": "csv",
            "tables": ["receipts"],
            "start_date": "2024-01-01",
            "end_date": "2024-01-01",
        },
    )

    task_id = response.json()["task_id"]
    final_status = poll_until_complete(client, task_id)

    assert final_status["status"] == "completed"

    # Verify partition structure: facts/receipts/dt=2024-01-01/receipts_2024-01-01.csv
    facts_dir = temp_export_dir / "facts"
    partition_file = (
        facts_dir / "receipts" / "dt=2024-01-01" / "receipts_2024-01-01.csv"
    )

    assert partition_file.exists()

    # Verify file contains data
    df = verify_csv_content(partition_file, expected_min_rows=1)
    assert len(df) > 0


def test_export_facts_verify_partition_dates(client, temp_export_dir):
    """Test that exported fact data matches the requested date range."""
    # Export specific date range
    response = client.post(
        "/api/export/facts",
        json={
            "format": "parquet",
            "tables": ["receipts", "receipt_lines"],
            "start_date": "2024-01-02",
            "end_date": "2024-01-03",
        },
    )

    task_id = response.json()["task_id"]
    final_status = poll_until_complete(client, task_id)

    assert final_status["status"] == "completed"

    # Check that partitions match date filter
    facts_dir = temp_export_dir / "facts"

    for table in ["receipts", "receipt_lines"]:
        table_dir = facts_dir / table
        partition_dirs = sorted(table_dir.glob("dt=*"))

        partition_dates = [p.name.replace("dt=", "") for p in partition_dirs]

        # Should only have 2024-01-02 and 2024-01-03
        assert "2024-01-01" not in partition_dates
        assert "2024-01-02" in partition_dates
        assert "2024-01-03" in partition_dates


def test_export_facts_specific_tables(client, temp_export_dir):
    """Test exporting specific fact tables (not all)."""
    # Export only receipts and receipt_lines
    response = client.post(
        "/api/export/facts",
        json={
            "format": "csv",
            "tables": ["receipts", "receipt_lines"],
        },
    )

    task_id = response.json()["task_id"]
    final_status = poll_until_complete(client, task_id)

    assert final_status["status"] == "completed"

    facts_dir = temp_export_dir / "facts"

    # These should exist
    assert (facts_dir / "receipts").exists()
    assert (facts_dir / "receipt_lines").exists()

    # These should NOT exist
    assert not (facts_dir / "foot_traffic").exists()
    assert not (facts_dir / "marketing").exists()


# ================================
# ERROR SCENARIO TESTS
# ================================


def test_export_invalid_format(client):
    """Test that invalid format returns 422 validation error."""
    response = client.post(
        "/api/export/master",
        json={"format": "json", "tables": "all"},  # json is not valid
    )

    assert response.status_code == 422  # Validation error


def test_export_invalid_master_table_names(client):
    """Test that invalid master table names return 400 error."""
    response = client.post(
        "/api/export/master",
        json={"format": "csv", "tables": ["invalid_table", "fake_table"]},
    )

    assert response.status_code == 400
    error_detail = response.json()["detail"]
    assert "Invalid master table names" in error_detail


def test_export_invalid_fact_table_names(client):
    """Test that invalid fact table names return 400 error."""
    response = client.post(
        "/api/export/facts",
        json={"format": "csv", "tables": ["not_a_table"]},
    )

    assert response.status_code == 400
    error_detail = response.json()["detail"]
    assert "Invalid facts table names" in error_detail


def test_export_status_invalid_task_id(client):
    """Test that querying non-existent task returns 404."""
    response = client.get("/api/export/status/nonexistent_task_id_12345")

    assert response.status_code == 404
    error_detail = response.json()["detail"]
    assert "not found" in error_detail.lower()


def test_export_invalid_date_range(client):
    """Test that end_date before start_date returns validation error."""
    response = client.post(
        "/api/export/facts",
        json={
            "format": "csv",
            "tables": "all",
            "start_date": "2024-01-31",
            "end_date": "2024-01-01",  # Before start_date
        },
    )

    assert response.status_code == 422  # Validation error


# ================================
# FILE CONTENT VERIFICATION TESTS
# ================================


def test_export_csv_content_matches_database(client, temp_export_dir, test_retail_db):
    """Test that exported CSV content matches database records."""
    # Export stores table
    response = client.post(
        "/api/export/master",
        json={"format": "csv", "tables": ["stores"]},
    )

    task_id = response.json()["task_id"]
    poll_until_complete(client, task_id)

    # Read exported CSV
    stores_csv = temp_export_dir / "master" / "stores.csv"
    df = pd.read_csv(stores_csv)

    # Verify row count matches database
    assert len(df) == 3

    # Verify specific data
    assert "ST001" in df["StoreNumber"].values
    assert "ST002" in df["StoreNumber"].values
    assert "ST003" in df["StoreNumber"].values


def test_export_parquet_content_matches_database(
    client, temp_export_dir, test_retail_db
):
    """Test that exported Parquet content matches database records."""
    # Export products table
    response = client.post(
        "/api/export/master",
        json={"format": "parquet", "tables": ["products_master"]},
    )

    task_id = response.json()["task_id"]
    poll_until_complete(client, task_id)

    # Read exported Parquet
    products_parquet = temp_export_dir / "master" / "products_master.parquet"
    df = pd.read_parquet(products_parquet)

    # Verify row count
    assert len(df) == 20

    # Verify column presence
    assert "ProductName" in df.columns
    assert "Brand" in df.columns
    assert "Cost" in df.columns
    assert "MSRP" in df.columns
    assert "SalePrice" in df.columns


def test_export_fact_data_content_verification(client, temp_export_dir):
    """Test that exported fact data contains expected records."""
    # Export receipts for specific date
    response = client.post(
        "/api/export/facts",
        json={
            "format": "csv",
            "tables": ["receipts"],
            "start_date": "2024-01-01",
            "end_date": "2024-01-01",
        },
    )

    task_id = response.json()["task_id"]
    poll_until_complete(client, task_id)

    # Read exported partition
    partition_file = (
        temp_export_dir / "facts" / "receipts" / "dt=2024-01-01" / "receipts_2024-01-01.csv"
    )
    df = pd.read_csv(partition_file)

    # Verify receipts for 2024-01-01 (should have 5 receipts based on test data)
    assert len(df) == 5

    # Verify required columns exist
    assert "ReceiptId" in df.columns
    assert "StoreID" in df.columns
    assert "CustomerID" in df.columns
    assert "Subtotal" in df.columns
    assert "Tax" in df.columns
    assert "Total" in df.columns
    assert "TenderType" in df.columns


# ================================
# PROGRESS TRACKING TESTS
# ================================


def test_export_status_response_structure(client):
    """Test that export status response includes all expected fields."""
    response = client.post(
        "/api/export/master",
        json={"format": "csv", "tables": ["stores"]},
    )

    task_id = response.json()["task_id"]

    # Get status
    status_response = client.get(f"/api/export/status/{task_id}")
    assert status_response.status_code == 200

    status_data = status_response.json()

    # Verify required fields
    assert "task_id" in status_data
    assert "status" in status_data
    assert "progress" in status_data
    assert "message" in status_data
    assert "tables_completed" in status_data
    assert "tables_remaining" in status_data
    assert "current_table" in status_data
    assert "started_at" in status_data


def test_export_tables_completed_grows(client):
    """Test that tables_completed list grows during export."""
    response = client.post(
        "/api/export/master",
        json={"format": "csv", "tables": "all"},
    )

    task_id = response.json()["task_id"]

    # Track tables_completed over time
    tables_completed_history = []

    for _ in range(20):
        status_response = client.get(f"/api/export/status/{task_id}")
        status_data = status_response.json()

        if status_data.get("tables_completed"):
            tables_completed_history.append(len(status_data["tables_completed"]))

        if status_data["status"] == "completed":
            break

        time.sleep(0.3)

    # Final count should be 6 (all master tables)
    assert tables_completed_history[-1] == 6


def test_export_final_status_has_results(client, temp_export_dir):
    """Test that completed export status includes result metadata."""
    response = client.post(
        "/api/export/master",
        json={"format": "csv", "tables": ["stores", "customers"]},
    )

    task_id = response.json()["task_id"]
    final_status = poll_until_complete(client, task_id)

    # Verify result fields are populated
    assert final_status["total_files"] == 2
    assert final_status["files_written"] is not None
    assert len(final_status["files_written"]) == 2
    assert final_status["output_directory"] is not None

    # Verify total_rows is present for CSV
    assert "total_rows" in final_status


# ================================
# ADDITIONAL EDGE CASE TESTS
# ================================


def test_export_formats_endpoint(client):
    """Test GET /api/export/formats returns supported formats."""
    response = client.get("/api/export/formats")

    assert response.status_code == 200
    data = response.json()

    assert "formats" in data
    formats = data["formats"]

    # Verify CSV and Parquet are listed
    format_names = [f["name"] for f in formats]
    assert "csv" in format_names
    assert "parquet" in format_names


def test_export_concurrent_requests(client, temp_export_dir):
    """Test that multiple concurrent export requests can run independently."""
    # Start two exports simultaneously
    response1 = client.post(
        "/api/export/master",
        json={"format": "csv", "tables": ["stores"]},
    )

    response2 = client.post(
        "/api/export/master",
        json={"format": "parquet", "tables": ["customers"]},
    )

    assert response1.status_code == 200
    assert response2.status_code == 200

    task_id1 = response1.json()["task_id"]
    task_id2 = response2.json()["task_id"]

    # Task IDs should be different
    assert task_id1 != task_id2

    # Both should complete successfully
    final_status1 = poll_until_complete(client, task_id1)
    final_status2 = poll_until_complete(client, task_id2)

    assert final_status1["status"] == "completed"
    assert final_status2["status"] == "completed"


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
