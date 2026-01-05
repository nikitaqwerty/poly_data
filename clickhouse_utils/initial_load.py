"""Initial ETL to load existing CSV data into ClickHouse"""

import os
import polars as pl
from datetime import datetime
from clickhouse_utils.config import get_client, CLICKHOUSE_DATABASE
from clickhouse_utils.schema import initialize_schema, get_table_count


def load_markets(
    csv_file: str = "markets.csv",
    missing_csv: str = "missing_markets.csv",
    batch_size: int = 10000,
):
    """
    Load markets data from CSV files into ClickHouse

    Args:
        csv_file: Path to main markets CSV file
        missing_csv: Path to missing markets CSV file
        batch_size: Number of rows to insert per batch
    """
    print("\n" + "=" * 60)
    print("📊 Loading Markets Data")
    print("=" * 60)

    client = get_client()

    # Check existing count
    existing_count = get_table_count("markets")
    print(f"Current rows in ClickHouse: {existing_count:,}")

    # Load CSV files
    dfs = []

    if os.path.exists(csv_file):
        print(f"📂 Reading {csv_file}...")
        df = pl.read_csv(
            csv_file,
            schema_overrides={
                "token1": pl.Utf8,
                "token2": pl.Utf8,
                "event_slug": pl.Utf8,
                "tags": pl.Utf8,
            },
        )
        dfs.append(df)
        print(f"   Loaded {len(df):,} rows")

    if os.path.exists(missing_csv):
        print(f"📂 Reading {missing_csv}...")
        df = pl.read_csv(
            missing_csv,
            schema_overrides={
                "token1": pl.Utf8,
                "token2": pl.Utf8,
                "event_slug": pl.Utf8,
                "tags": pl.Utf8,
            },
        )
        dfs.append(df)
        print(f"   Loaded {len(df):,} rows")

    if not dfs:
        print("⚠️  No CSV files found!")
        return

    # Combine and deduplicate
    df = pl.concat(dfs).unique(subset=["id"], keep="first").sort("createdAt")
    print(f"\nTotal unique markets: {len(df):,}")

    if existing_count >= len(df):
        print("✓ All data already loaded")
        return

    # Convert datetime columns
    df = df.with_columns(
        [
            pl.col("createdAt")
            .str.to_datetime(time_zone="UTC", strict=False)
            .alias("createdAt"),
            pl.when(pl.col("closedTime").is_not_null() & (pl.col("closedTime") != ""))
            .then(pl.col("closedTime").str.to_datetime(time_zone="UTC", strict=False))
            .otherwise(None)
            .alias("closedTime"),
        ]
    )

    # Ensure boolean column
    df = df.with_columns([pl.col("neg_risk").cast(pl.Boolean)])

    # Parse tags from semicolon-separated string to array
    # Handle missing or empty tags gracefully
    df = df.with_columns(
        [
            pl.when(pl.col("tags").is_not_null() & (pl.col("tags") != ""))
            .then(pl.col("tags").str.split(";"))
            .otherwise(pl.lit([]))
            .alias("tags")
        ]
    )

    # Convert to list of tuples for insertion
    print(f"\n⚙️  Inserting {len(df):,} rows in batches of {batch_size:,}...")

    total_inserted = 0
    for i in range(0, len(df), batch_size):
        batch = df[i : i + batch_size]

        # Convert to Python types for insertion
        data = [
            (
                row["createdAt"],
                str(row["id"]) if row["id"] is not None else "",
                str(row["question"]) if row["question"] is not None else "",
                str(row["answer1"]) if row["answer1"] is not None else "",
                str(row["answer2"]) if row["answer2"] is not None else "",
                row["neg_risk"],
                str(row["market_slug"]) if row["market_slug"] is not None else "",
                str(row["token1"]) if row["token1"] is not None else "",
                str(row["token2"]) if row["token2"] is not None else "",
                str(row["condition_id"]) if row["condition_id"] is not None else "",
                float(row["volume"]) if row["volume"] else 0.0,
                str(row["ticker"]) if row["ticker"] is not None else "",
                row["closedTime"],
                str(row["event_slug"]) if row["event_slug"] is not None else "",
                row["tags"] if row["tags"] is not None else [],
            )
            for row in batch.iter_rows(named=True)
        ]

        client.insert(
            f"{CLICKHOUSE_DATABASE}.markets",
            data,
            column_names=[
                "createdAt",
                "id",
                "question",
                "answer1",
                "answer2",
                "neg_risk",
                "market_slug",
                "token1",
                "token2",
                "condition_id",
                "volume",
                "ticker",
                "closedTime",
                "event_slug",
                "tags",
            ],
        )

        total_inserted += len(data)
        print(f"   Inserted {total_inserted:,} / {len(df):,} rows", end="\r")

    print(f"\n✅ Successfully loaded {total_inserted:,} markets")


def load_order_filled(
    csv_file: str = "goldsky/orderFilled.csv", batch_size: int = 50000
):
    """
    Load order filled events from CSV into ClickHouse

    Args:
        csv_file: Path to orderFilled CSV file
        batch_size: Number of rows to insert per batch
    """
    print("\n" + "=" * 60)
    print("📊 Loading Order Filled Events")
    print("=" * 60)

    if not os.path.exists(csv_file):
        print(f"⚠️  File not found: {csv_file}")
        return

    client = get_client()

    # Check existing count
    existing_count = get_table_count("order_filled")
    print(f"Current rows in ClickHouse: {existing_count:,}")

    print(f"📂 Reading {csv_file}...")

    # Use scan_csv for memory efficiency with large files
    df = pl.scan_csv(
        csv_file,
        schema_overrides={
            "takerAssetId": pl.Utf8,
            "makerAssetId": pl.Utf8,
        },
    ).collect(streaming=True)

    print(f"   Loaded {len(df):,} rows from CSV")

    if existing_count >= len(df):
        print("✓ All data already loaded")
        return

    # Convert timestamp from Unix epoch to datetime
    df = df.with_columns(
        [pl.from_epoch(pl.col("timestamp"), time_unit="s").alias("timestamp")]
    )

    print(f"\n⚙️  Inserting {len(df):,} rows in batches of {batch_size:,}...")

    total_inserted = 0
    for i in range(0, len(df), batch_size):
        batch = df[i : i + batch_size]

        # Convert to Python types
        data = [
            (
                row["timestamp"],
                str(row["maker"]) if row["maker"] is not None else "",
                str(row["makerAssetId"]) if row["makerAssetId"] is not None else "",
                int(row["makerAmountFilled"]),
                str(row["taker"]) if row["taker"] is not None else "",
                str(row["takerAssetId"]) if row["takerAssetId"] is not None else "",
                int(row["takerAmountFilled"]),
                (
                    str(row["transactionHash"])
                    if row["transactionHash"] is not None
                    else ""
                ),
            )
            for row in batch.iter_rows(named=True)
        ]

        client.insert(
            f"{CLICKHOUSE_DATABASE}.order_filled",
            data,
            column_names=[
                "timestamp",
                "maker",
                "makerAssetId",
                "makerAmountFilled",
                "taker",
                "takerAssetId",
                "takerAmountFilled",
                "transactionHash",
            ],
        )

        total_inserted += len(data)
        print(f"   Inserted {total_inserted:,} / {len(df):,} rows", end="\r")

    print(f"\n✅ Successfully loaded {total_inserted:,} order filled events")


def load_trades(csv_file: str = "processed/trades.csv", batch_size: int = 50000):
    """
    Load processed trades from CSV into ClickHouse

    Args:
        csv_file: Path to trades CSV file
        batch_size: Number of rows to insert per batch
    """
    print("\n" + "=" * 60)
    print("📊 Loading Processed Trades")
    print("=" * 60)

    if not os.path.exists(csv_file):
        print(f"⚠️  File not found: {csv_file}")
        return

    client = get_client()

    # Check existing count
    existing_count = get_table_count("trades")
    print(f"Current rows in ClickHouse: {existing_count:,}")

    print(f"📂 Reading {csv_file}...")

    # Use scan_csv for memory efficiency
    df = pl.scan_csv(csv_file).collect(streaming=True)

    print(f"   Loaded {len(df):,} rows from CSV")

    if existing_count >= len(df):
        print("✓ All data already loaded")
        return

    # Convert timestamp column
    df = df.with_columns(
        [
            pl.col("timestamp")
            .str.to_datetime(time_zone="UTC", strict=False)
            .alias("timestamp")
        ]
    )

    print(f"\n⚙️  Inserting {len(df):,} rows in batches of {batch_size:,}...")

    total_inserted = 0
    for i in range(0, len(df), batch_size):
        batch = df[i : i + batch_size]

        # Convert to Python types
        data = [
            (
                row["timestamp"],
                str(row["market_id"]) if row["market_id"] is not None else "",
                str(row["maker"]) if row["maker"] is not None else "",
                str(row["taker"]) if row["taker"] is not None else "",
                str(row["nonusdc_side"]) if row["nonusdc_side"] is not None else "",
                (
                    str(row["maker_direction"])
                    if row["maker_direction"] is not None
                    else ""
                ),
                (
                    str(row["taker_direction"])
                    if row["taker_direction"] is not None
                    else ""
                ),
                float(row["price"]),
                float(row["usd_amount"]),
                float(row["token_amount"]),
                (
                    str(row["transactionHash"])
                    if row["transactionHash"] is not None
                    else ""
                ),
            )
            for row in batch.iter_rows(named=True)
        ]

        client.insert(
            f"{CLICKHOUSE_DATABASE}.trades",
            data,
            column_names=[
                "timestamp",
                "market_id",
                "maker",
                "taker",
                "nonusdc_side",
                "maker_direction",
                "taker_direction",
                "price",
                "usd_amount",
                "token_amount",
                "transactionHash",
            ],
        )

        total_inserted += len(data)
        print(f"   Inserted {total_inserted:,} / {len(df):,} rows", end="\r")

    print(f"\n✅ Successfully loaded {total_inserted:,} trades")


def run_initial_load():
    """
    Run the complete initial load process
    """
    print("\n" + "=" * 60)
    print("🚀 Starting Initial Data Load to ClickHouse")
    print("=" * 60)

    start_time = datetime.now()

    # Initialize schema first
    initialize_schema()

    # Load data in order
    load_markets()
    load_order_filled()
    load_trades()

    # Final summary
    print("\n" + "=" * 60)
    print("📈 Final Summary")
    print("=" * 60)

    for table in ["markets", "order_filled", "trades"]:
        count = get_table_count(table)
        print(f"  {table}: {count:,} rows")

    elapsed = datetime.now() - start_time
    print(f"\n⏱️  Total time: {elapsed}")
    print("=" * 60)
    print("✅ Initial load complete!")
    print("=" * 60)


if __name__ == "__main__":
    run_initial_load()
