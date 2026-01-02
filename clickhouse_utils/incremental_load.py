"""Incremental ETL to load new data into ClickHouse"""

import os
import polars as pl
from datetime import datetime
from clickhouse_utils.config import get_client, CLICKHOUSE_DATABASE
from clickhouse_utils.schema import get_table_count, get_last_trade_info
from poly_utils.utils import get_markets


def load_new_markets(batch_size: int = 10000):
    """
    Load any new markets from CSV that aren't in ClickHouse yet

    Args:
        batch_size: Number of rows to insert per batch
    """
    print("\n📊 Checking for new markets...")

    client = get_client()

    # Get existing market IDs from ClickHouse
    result = client.query(f"SELECT id FROM {CLICKHOUSE_DATABASE}.markets")
    existing_ids = set(row[0] for row in result.result_rows)
    print(f"   Existing markets in ClickHouse: {len(existing_ids):,}")

    # Load markets from CSV
    markets_df = get_markets()

    if len(markets_df) == 0:
        print("   No markets in CSV files")
        return 0

    print(f"   Markets in CSV files: {len(markets_df):,}")

    # Filter to only new markets
    new_markets = markets_df.filter(~pl.col("id").is_in(existing_ids))

    if len(new_markets) == 0:
        print("   ✓ No new markets to load")
        return 0

    print(f"   Found {len(new_markets):,} new markets to insert")

    # Rename 'id' back to 'id' if get_markets renamed it
    if "market_id" in new_markets.columns:
        new_markets = new_markets.rename({"market_id": "id"})

    # Convert datetime columns
    new_markets = new_markets.with_columns(
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
    new_markets = new_markets.with_columns([pl.col("neg_risk").cast(pl.Boolean)])

    # Insert in batches
    total_inserted = 0
    for i in range(0, len(new_markets), batch_size):
        batch = new_markets[i : i + batch_size]

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
            ],
        )

        total_inserted += len(data)
        print(f"   Inserted {total_inserted:,} / {len(new_markets):,} rows", end="\r")

    print(f"\n   ✅ Loaded {total_inserted:,} new markets")
    return total_inserted


def load_new_order_filled(
    csv_file: str = "goldsky/orderFilled.csv", batch_size: int = 50000
):
    """
    Load new order filled events from CSV into ClickHouse
    Only loads rows that come after the last timestamp in ClickHouse

    Args:
        csv_file: Path to orderFilled CSV file
        batch_size: Number of rows to insert per batch
    """
    print("\n📊 Checking for new order filled events...")

    if not os.path.exists(csv_file):
        print(f"   ⚠️  File not found: {csv_file}")
        return 0

    client = get_client()

    # Get the count and last timestamp from ClickHouse
    ch_count = get_table_count("order_filled")
    print(f"   Current rows in ClickHouse: {ch_count:,}")

    # Read CSV to check total rows
    df = pl.scan_csv(
        csv_file,
        schema_overrides={
            "takerAssetId": pl.Utf8,
            "makerAssetId": pl.Utf8,
        },
    ).collect(streaming=True)

    print(f"   Total rows in CSV: {len(df):,}")

    if ch_count >= len(df):
        print("   ✓ No new order filled events to load")
        return 0

    # Take only rows after what we have in ClickHouse
    new_rows = df[ch_count:]
    print(f"   Found {len(new_rows):,} new rows to insert")

    # Convert timestamp
    new_rows = new_rows.with_columns(
        [pl.from_epoch(pl.col("timestamp"), time_unit="s").alias("timestamp")]
    )

    # Insert in batches
    total_inserted = 0
    for i in range(0, len(new_rows), batch_size):
        batch = new_rows[i : i + batch_size]

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
        print(f"   Inserted {total_inserted:,} / {len(new_rows):,} rows", end="\r")

    print(f"\n   ✅ Loaded {total_inserted:,} new order filled events")
    return total_inserted


def load_new_trades(csv_file: str = "processed/trades.csv", batch_size: int = 50000):
    """
    Load new processed trades from CSV into ClickHouse
    Resumes from the last trade that was loaded

    Args:
        csv_file: Path to trades CSV file
        batch_size: Number of rows to insert per batch
    """
    print("\n📊 Checking for new trades...")

    if not os.path.exists(csv_file):
        print(f"   ⚠️  File not found: {csv_file}")
        return 0

    client = get_client()

    # Get last trade info from ClickHouse
    last_trade = get_last_trade_info()

    ch_count = get_table_count("trades")
    print(f"   Current rows in ClickHouse: {ch_count:,}")

    if last_trade:
        print(f"   Last trade timestamp: {last_trade['timestamp']}")
        print(f"   Last trade hash: {last_trade['transactionHash'][:16]}...")

    # Read CSV
    df = pl.scan_csv(csv_file).collect(streaming=True)
    print(f"   Total rows in CSV: {len(df):,}")

    # Convert timestamp
    df = df.with_columns(
        [
            pl.col("timestamp")
            .str.to_datetime(time_zone="UTC", strict=False)
            .alias("timestamp")
        ]
    )

    # If we have data in ClickHouse, filter to only new rows
    if last_trade and "timestamp" in last_trade:
        # Add row index
        df = df.with_row_index()

        # Find rows matching the last trade
        same_timestamp = df.filter(pl.col("timestamp") == last_trade["timestamp"])
        same_timestamp = same_timestamp.filter(
            (pl.col("transactionHash") == last_trade["transactionHash"])
            & (pl.col("maker") == last_trade["maker"])
            & (pl.col("taker") == last_trade["taker"])
        )

        if len(same_timestamp) > 0:
            last_index = same_timestamp.row(0)[0]
            new_rows = df.filter(pl.col("index") > last_index).drop("index")
        else:
            # If we can't find exact match, take everything after timestamp
            new_rows = df.filter(pl.col("timestamp") > last_trade["timestamp"]).drop(
                "index"
            )
    else:
        # No data in ClickHouse, load everything
        if "index" in df.columns:
            new_rows = df.drop("index")
        else:
            new_rows = df

    if len(new_rows) == 0:
        print("   ✓ No new trades to load")
        return 0

    print(f"   Found {len(new_rows):,} new rows to insert")

    # Insert in batches
    total_inserted = 0
    for i in range(0, len(new_rows), batch_size):
        batch = new_rows[i : i + batch_size]

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
        print(f"   Inserted {total_inserted:,} / {len(new_rows):,} rows", end="\r")

    print(f"\n   ✅ Loaded {total_inserted:,} new trades")
    return total_inserted


def run_incremental_load():
    """
    Run the incremental load process for all tables
    This should be run after update_all.py completes
    """
    print("\n" + "=" * 60)
    print("🔄 Starting Incremental Load to ClickHouse")
    print("=" * 60)

    start_time = datetime.now()

    # Load new data for each table
    markets_added = load_new_markets()
    orders_added = load_new_order_filled()
    trades_added = load_new_trades()

    # Final summary
    print("\n" + "=" * 60)
    print("📈 Incremental Load Summary")
    print("=" * 60)
    print(f"  New markets loaded: {markets_added:,}")
    print(f"  New order events loaded: {orders_added:,}")
    print(f"  New trades loaded: {trades_added:,}")
    print()

    print("Current table totals:")
    for table in ["markets", "order_filled", "trades"]:
        count = get_table_count(table)
        print(f"  {table}: {count:,} rows")

    elapsed = datetime.now() - start_time
    print(f"\n⏱️  Total time: {elapsed}")
    print("=" * 60)
    print("✅ Incremental load complete!")
    print("=" * 60)


if __name__ == "__main__":
    run_incremental_load()
