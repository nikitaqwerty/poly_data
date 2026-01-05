import os
import pandas as pd
from gql import gql, Client
from gql.transport.requests import RequestsHTTPTransport
from flatten_json import flatten
from datetime import datetime, timezone
import subprocess
import time
import argparse
from multiprocessing import Pool, Manager, Lock
from functools import partial

# Global runtime timestamp - set once when program starts
RUNTIME_TIMESTAMP = datetime.now().strftime("%Y%m%d_%H%M%S")

# Columns to save
COLUMNS_TO_SAVE = [
    "timestamp",
    "maker",
    "makerAssetId",
    "makerAmountFilled",
    "taker",
    "takerAssetId",
    "takerAmountFilled",
    "transactionHash",
]

QUERY_URL = "https://api.goldsky.com/api/public/project_cl6mb8i9h0003e201j6li0diw/subgraphs/orderbook-subgraph/0.0.1/gn"

if not os.path.isdir("goldsky"):
    os.mkdir("goldsky")


def get_latest_timestamp():
    """Get the latest timestamp from orderFilled.csv, or 0 if file doesn't exist"""
    cache_file = "goldsky/orderFilled.csv"

    if not os.path.isfile(cache_file):
        print("No existing file found, starting from beginning of time (timestamp 0)")
        return 0

    try:
        # Use tail to get the last line efficiently
        result = subprocess.run(
            ["tail", "-n", "1", cache_file], capture_output=True, text=True, check=True
        )
        last_line = result.stdout.strip()
        if last_line:
            # Get header to find timestamp column index
            header_result = subprocess.run(
                ["head", "-n", "1", cache_file],
                capture_output=True,
                text=True,
                check=True,
            )
            headers = header_result.stdout.strip().split(",")

            if "timestamp" in headers:
                timestamp_index = headers.index("timestamp")
                values = last_line.split(",")
                if len(values) > timestamp_index:
                    last_timestamp = int(values[timestamp_index])
                    readable_time = datetime.fromtimestamp(
                        last_timestamp, tz=timezone.utc
                    ).strftime("%Y-%m-%d %H:%M:%S UTC")
                    print(f"Resuming from timestamp {last_timestamp} ({readable_time})")
                    return last_timestamp
    except Exception as e:
        print(f"Error reading latest file with tail: {e}")
        # Fallback to pandas
        try:
            df = pd.read_csv(cache_file)
            if len(df) > 0 and "timestamp" in df.columns:
                last_timestamp = df.iloc[-1]["timestamp"]
                readable_time = datetime.fromtimestamp(
                    int(last_timestamp), tz=timezone.utc
                ).strftime("%Y-%m-%d %H:%M:%S UTC")
                print(f"Resuming from timestamp {last_timestamp} ({readable_time})")
                return int(last_timestamp)
        except Exception as e2:
            print(f"Error reading with pandas: {e2}")

    # Fallback to beginning of time
    print("Falling back to beginning of time (timestamp 0)")
    return 0


def get_max_timestamp():
    """Query the API to get the maximum timestamp available"""
    q_string = """query MyQuery {
                    orderFilledEvents(orderBy: timestamp 
                                         orderDirection: desc
                                         first: 1) {
                        timestamp
                    }
                }
            """

    query = gql(q_string)
    transport = RequestsHTTPTransport(url=QUERY_URL, verify=True, retries=3)
    client = Client(transport=transport)

    max_retries = 5
    for attempt in range(max_retries):
        try:
            res = client.execute(query)
            if res["orderFilledEvents"] and len(res["orderFilledEvents"]) > 0:
                max_ts = int(res["orderFilledEvents"][0]["timestamp"])
                readable_time = datetime.fromtimestamp(
                    max_ts, tz=timezone.utc
                ).strftime("%Y-%m-%d %H:%M:%S UTC")
                print(f"Maximum timestamp in database: {max_ts} ({readable_time})")
                return max_ts
            return None
        except Exception as e:
            print(
                f"Error getting max timestamp (attempt {attempt+1}/{max_retries}): {e}"
            )
            if attempt < max_retries - 1:
                time.sleep(5)

    return None


def scrape_range(worker_id, start_ts, end_ts, at_once=1000):
    """Scrape orderFilledEvents within a specific timestamp range"""
    worker_output_file = f"goldsky/orderFilled_worker_{worker_id}.csv"

    print(f"Worker {worker_id}: Processing range {start_ts} to {end_ts}")
    print(
        f"  Start: {datetime.fromtimestamp(start_ts, tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}"
    )
    print(
        f"  End: {datetime.fromtimestamp(end_ts, tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}"
    )

    last_value = start_ts
    count = 0
    total_records = 0

    # Remove worker file if it exists from previous run
    if os.path.isfile(worker_output_file):
        os.remove(worker_output_file)

    while True:
        # Query with both lower and upper bounds
        q_string = (
            """query MyQuery {
                        orderFilledEvents(orderBy: timestamp 
                                             first: """
            + str(at_once)
            + '''
                                             where: {timestamp_gt: "'''
            + str(last_value)
            + '''", timestamp_lte: "'''
            + str(end_ts)
            + """"}) {
                            fee
                            id
                            maker
                            makerAmountFilled
                            makerAssetId
                            orderHash
                            taker
                            takerAmountFilled
                            takerAssetId
                            timestamp
                            transactionHash
                        }
                    }
                """
        )

        query = gql(q_string)
        transport = RequestsHTTPTransport(url=QUERY_URL, verify=True, retries=3)
        client = Client(transport=transport)

        try:
            res = client.execute(query)
        except Exception as e:
            print(f"Worker {worker_id}: Query error: {e}")
            print(f"Worker {worker_id}: Retrying in 5 seconds...")
            time.sleep(5)
            continue

        if not res["orderFilledEvents"] or len(res["orderFilledEvents"]) == 0:
            print(f"Worker {worker_id}: No more data in range")
            break

        df = pd.DataFrame([flatten(x) for x in res["orderFilledEvents"]]).reset_index(
            drop=True
        )

        # Sort by timestamp and update last_value
        df = df.sort_values("timestamp", ascending=True).reset_index(drop=True)
        last_value = df.iloc[-1]["timestamp"]

        readable_time = datetime.fromtimestamp(
            int(last_value), tz=timezone.utc
        ).strftime("%Y-%m-%d %H:%M:%S UTC")
        print(
            f"Worker {worker_id}: Batch {count + 1}: Last timestamp {last_value} ({readable_time}), Records: {len(df)}"
        )

        count += 1
        total_records += len(df)

        # Remove duplicates
        df = df.drop_duplicates()

        # Filter to only the columns we want to save
        df_to_save = df[COLUMNS_TO_SAVE].copy()

        # Save to worker's file
        if os.path.isfile(worker_output_file):
            df_to_save.to_csv(worker_output_file, index=None, mode="a", header=None)
        else:
            df_to_save.to_csv(worker_output_file, index=None)

        if len(df) < at_once:
            break

    print(f"Worker {worker_id}: Finished. Total records: {total_records}")
    return worker_id, total_records


def merge_worker_files(n_workers):
    """Merge all worker files into the main output file"""
    output_file = "goldsky/orderFilled.csv"
    print(f"\nMerging {n_workers} worker files into {output_file}")

    # Check if main file exists to determine if we need header
    file_exists = os.path.isfile(output_file)

    total_merged = 0
    for worker_id in range(n_workers):
        worker_file = f"goldsky/orderFilled_worker_{worker_id}.csv"

        if not os.path.isfile(worker_file):
            print(f"Warning: Worker file {worker_file} not found, skipping")
            continue

        # Read worker file
        try:
            df = pd.read_csv(worker_file)
            if len(df) > 0:
                # Append to main file
                if file_exists:
                    df.to_csv(output_file, index=None, mode="a", header=None)
                else:
                    df.to_csv(output_file, index=None)
                    file_exists = True  # Now it exists

                print(f"Merged {len(df)} records from worker {worker_id}")
                total_merged += len(df)

            # Clean up worker file
            os.remove(worker_file)
        except Exception as e:
            print(f"Error merging worker {worker_id} file: {e}")

    print(f"Total records merged: {total_merged}")

    # Sort the final file by timestamp
    if os.path.isfile(output_file):
        print("Sorting final file by timestamp...")
        try:
            df = pd.read_csv(output_file)
            df = df.sort_values("timestamp", ascending=True).reset_index(drop=True)
            # Remove any duplicates that might exist across worker boundaries
            df = df.drop_duplicates(
                subset=["transactionHash", "timestamp"], keep="first"
            )
            df.to_csv(output_file, index=None)
            print(f"Final file sorted and deduplicated. Total records: {len(df)}")
        except Exception as e:
            print(f"Error sorting final file: {e}")


def update_goldsky_parallel(n_workers=4):
    """Run parallelized scraping for orderFilledEvents"""
    print(f"\n{'='*50}")
    print(f"Starting PARALLEL scrape of orderFilledEvents")
    print(f"Number of workers: {n_workers}")
    print(f"Runtime: {RUNTIME_TIMESTAMP}")
    print(f"{'='*50}\n")

    # Get the starting point (where we left off)
    start_ts = get_latest_timestamp()

    # Get the maximum timestamp available
    max_ts = get_max_timestamp()

    if max_ts is None:
        print("Could not determine max timestamp. Exiting.")
        return

    if start_ts >= max_ts:
        print(
            f"Already up to date! Start timestamp ({start_ts}) >= max timestamp ({max_ts})"
        )
        return

    # Calculate timestamp range per worker
    total_range = max_ts - start_ts
    range_per_worker = total_range // n_workers

    print(f"\nTimestamp range to process: {total_range} seconds")
    print(f"Range per worker: ~{range_per_worker} seconds")
    print(f"  (~{range_per_worker/3600:.1f} hours per worker)\n")

    # Create work assignments
    work_assignments = []
    for i in range(n_workers):
        worker_start = start_ts + (i * range_per_worker)
        if i == n_workers - 1:
            # Last worker gets everything up to max
            worker_end = max_ts
        else:
            worker_end = start_ts + ((i + 1) * range_per_worker)

        work_assignments.append((i, worker_start, worker_end))

    # Print work assignments
    print("Work assignments:")
    for worker_id, worker_start, worker_end in work_assignments:
        start_str = datetime.fromtimestamp(worker_start, tz=timezone.utc).strftime(
            "%Y-%m-%d %H:%M:%S UTC"
        )
        end_str = datetime.fromtimestamp(worker_end, tz=timezone.utc).strftime(
            "%Y-%m-%d %H:%M:%S UTC"
        )
        print(f"  Worker {worker_id}: {start_str} to {end_str}")
    print()

    # Run workers in parallel
    try:
        with Pool(processes=n_workers) as pool:
            results = pool.starmap(scrape_range, work_assignments)

        print("\n" + "=" * 50)
        print("All workers completed!")
        print("=" * 50)

        total_records = sum(r[1] for r in results)
        print(f"Total records collected: {total_records}")

        # Merge all worker files
        merge_worker_files(n_workers)

        print(f"\nSuccessfully completed parallel scraping!")

    except Exception as e:
        print(f"Error during parallel scraping: {str(e)}")
        import traceback

        traceback.print_exc()


def main():
    parser = argparse.ArgumentParser(
        description="Parallel scraping of Goldsky orderFilledEvents"
    )
    parser.add_argument(
        "-n",
        "--workers",
        type=int,
        default=4,
        help="Number of parallel workers (default: 4)",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=1000,
        help="Number of records to fetch per batch (default: 1000)",
    )

    args = parser.parse_args()

    if args.workers < 1:
        print("Error: Number of workers must be at least 1")
        return

    update_goldsky_parallel(n_workers=args.workers)


if __name__ == "__main__":
    main()
