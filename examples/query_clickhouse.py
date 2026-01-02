"""
Example script for querying ClickHouse data

This demonstrates various queries you can run against the Polymarket data in ClickHouse
"""

from clickhouse_utils.config import get_client
import pandas as pd


def print_query_results(title, query, limit=None):
    """Helper function to run a query and print results"""
    print("\n" + "=" * 70)
    print(f"📊 {title}")
    print("=" * 70)

    client = get_client()
    result = client.query(query)

    # Convert QueryResult to pandas DataFrame
    df = pd.DataFrame(result.result_rows, columns=result.column_names)

    if limit and len(df) > limit:
        df = df.head(limit)

    print(df.to_string(index=False))
    print()


def main():
    """Run example queries"""

    print("\n" + "=" * 70)
    print("🔍 Polymarket ClickHouse Query Examples")
    print("=" * 70)

    # Query 1: Table row counts and sizes
    print_query_results(
        "Table Statistics",
        """
        SELECT 
            name as table_name,
            formatReadableQuantity(total_rows) as rows,
            formatReadableSize(total_bytes) as size
        FROM system.tables
        WHERE database = 'polymarket'
        ORDER BY total_bytes DESC
        """,
    )

    # Query 2: Top markets by volume
    print_query_results(
        "Top 10 Markets by Trading Volume",
        """
        SELECT 
            m.question,
            count() as trades,
            round(sum(t.usd_amount), 2) as total_volume,
            round(avg(t.price), 4) as avg_price
        FROM polymarket.trades t
        JOIN polymarket.markets m ON t.market_id = m.id
        GROUP BY m.question
        ORDER BY total_volume DESC
        LIMIT 10
        """,
    )

    # Query 3: Top traders
    print_query_results(
        "Top 20 Traders by Volume (excluding platform wallets)",
        """
        SELECT 
            maker as trader,
            count() as trades,
            round(sum(usd_amount), 2) as total_volume,
            round(avg(usd_amount), 2) as avg_trade_size
        FROM polymarket.trades
        WHERE maker NOT IN ('0xc5d563a36ae78145c45a50134d48a1215220f80a', '0x4bfb41d5b3570defd03c39a9a4d8de6bd8b8982e')
        GROUP BY trader
        ORDER BY total_volume DESC
        LIMIT 20
        """,
    )

    # Query 4: Recent trading activity
    print_query_results(
        "Trading Activity - Last 30 Days",
        """
        SELECT 
            toStartOfDay(timestamp) as day,
            count() as trades,
            round(sum(usd_amount), 2) as volume,
            uniq(maker) as unique_traders
        FROM polymarket.trades
        WHERE timestamp >= now() - INTERVAL 30 DAY
        GROUP BY day
        ORDER BY day DESC
        """,
        limit=30,
    )

    # Query 5: Buy vs Sell breakdown
    print_query_results(
        "Buy vs Sell Direction (Last 7 Days)",
        """
        SELECT 
            taker_direction,
            count() as trades,
            round(sum(usd_amount), 2) as volume,
            round(avg(price), 4) as avg_price
        FROM polymarket.trades
        WHERE timestamp >= now() - INTERVAL 7 DAY
        GROUP BY taker_direction
        """,
    )

    # Query 6: Largest single trades
    print_query_results(
        "Top 10 Largest Trades",
        """
        SELECT 
            timestamp,
            m.question,
            round(usd_amount, 2) as usd_amount,
            round(price, 4) as price,
            taker_direction
        FROM polymarket.trades t
        JOIN polymarket.markets m ON t.market_id = m.id
        ORDER BY usd_amount DESC
        LIMIT 10
        """,
    )

    print("=" * 70)
    print("✅ Query examples complete!")
    print("=" * 70)


if __name__ == "__main__":
    main()
