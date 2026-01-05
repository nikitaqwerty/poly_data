"""
Script to backfill event_slug and tags for existing markets in CSV.
This script reads the existing markets.csv and updates rows that have empty
event_slug or tags fields by fetching data from the Polymarket API.
"""

import requests
import csv
import time
from typing import List, Dict, Tuple
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
from tqdm import tqdm


def fetch_event_tags(
    event_slug: str, event_cache: Dict, cache_lock: Lock, session: requests.Session
) -> List[str]:
    """
    Fetch tags for a given event slug from the /events endpoint.
    Uses caching to avoid duplicate API calls.

    Args:
        event_slug: The slug of the event to fetch
        event_cache: Dictionary to cache event data
        cache_lock: Lock for thread-safe cache access
        session: Requests session for connection pooling

    Returns:
        List of tag labels for the event
    """
    # Check cache with lock
    with cache_lock:
        if event_slug in event_cache:
            return event_cache[event_slug]

    if not event_slug:
        with cache_lock:
            event_cache[event_slug] = []
        return []

    try:
        url = f"https://gamma-api.polymarket.com/events?slug={event_slug}"
        response = session.get(url, timeout=10)

        if response.status_code == 200:
            events = response.json()
            if events and len(events) > 0:
                event = events[0]
                tags = event.get("tags", [])
                tag_labels = [tag.get("label", "") for tag in tags if tag.get("label")]
                with cache_lock:
                    event_cache[event_slug] = tag_labels
                return tag_labels
    except Exception as e:
        tqdm.write(f"Error fetching event tags for {event_slug}: {e}")

    with cache_lock:
        event_cache[event_slug] = []
    return []


def fetch_market_data(market_slug: str, session: requests.Session) -> Dict:
    """
    Fetch full market data from the /markets endpoint to get event_slug.

    Args:
        market_slug: The slug of the market to fetch
        session: Requests session for connection pooling

    Returns:
        Dictionary with event_slug and other market data
    """
    try:
        url = f"https://gamma-api.polymarket.com/markets/{market_slug}"
        response = session.get(url, timeout=10)

        if response.status_code == 200:
            market = response.json()
            event_slug = ""
            ticker = ""

            if market.get("events") and len(market.get("events", [])) > 0:
                event_slug = market["events"][0].get("slug", "")
                ticker = market["events"][0].get("ticker", "")

            return {
                "event_slug": event_slug,
                "ticker": ticker,
            }
    except Exception as e:
        tqdm.write(f"Error fetching market data for {market_slug}: {e}")

    return {"event_slug": "", "ticker": ""}


def process_row(
    idx: int,
    row: Dict,
    event_cache: Dict,
    cache_lock: Lock,
    session: requests.Session,
) -> Tuple[int, Dict, bool, bool]:
    """
    Process a single row to fetch missing event_slug and tags.

    Args:
        idx: Row index
        row: Row data dictionary
        event_cache: Shared event cache
        cache_lock: Lock for thread-safe cache access
        session: Requests session for connection pooling

    Returns:
        Tuple of (idx, updated_row, was_updated, already_had_data)
    """
    try:
        ticker = row.get("ticker", "").strip()
        event_slug = row.get("event_slug", "").strip()
        tags = row.get("tags", "").strip()

        # Track if this row already has data
        has_data = bool(event_slug and tags)

        # If both fields are already populated, skip
        if event_slug and tags:
            return (idx, row, False, True)

        updated = False

        # Use ticker as event_slug if event_slug is missing
        # (ticker column already contains the event slug)
        if not event_slug and ticker:
            event_slug = ticker
            row["event_slug"] = event_slug
            updated = True

        # Fetch tags if we have event_slug but no tags
        if event_slug and not tags:
            tags_list = fetch_event_tags(event_slug, event_cache, cache_lock, session)

            if tags_list:
                tags_str = ";".join(tags_list)
                row["tags"] = tags_str
                updated = True

            # Small delay to avoid rate limiting
            time.sleep(0.05)

        return (idx, row, updated, has_data)

    except Exception as e:
        tqdm.write(f"Error processing row {idx}: {e}")
        return (idx, row, False, False)


def backfill_event_tags(
    input_csv: str = "markets.csv",
    output_csv: str = "markets_updated.csv",
    start_from: int = 0,
    batch_checkpoint: int = 100,
    max_workers: int = 10,
):
    """
    Backfill event_slug and tags for existing markets using parallel processing.

    Args:
        input_csv: Input CSV file with existing data
        output_csv: Output CSV file with updated data
        start_from: Row number to start from (0-indexed, for resuming)
        batch_checkpoint: Save progress every N rows
        max_workers: Number of parallel threads to use
    """
    if not os.path.exists(input_csv):
        print(f"Error: {input_csv} not found!")
        return

    event_cache = {}
    cache_lock = Lock()
    rows_updated = 0
    rows_with_data = 0

    # Read all rows from input CSV
    print(f"Reading {input_csv}...")
    with open(input_csv, "r", encoding="utf-8") as infile:
        reader = csv.DictReader(infile)
        headers = reader.fieldnames
        all_rows = list(reader)

    total_rows = len(all_rows)
    print(f"Found {total_rows} rows to process (starting from row {start_from})")
    print(f"Using {max_workers} parallel workers")

    # Check if output file exists to resume
    if os.path.exists(output_csv) and start_from > 0:
        mode = "a"
        print(f"Resuming from row {start_from}...")
    else:
        mode = "w"
        print(f"Creating new output file: {output_csv}")

    with open(output_csv, mode, newline="", encoding="utf-8") as outfile:
        writer = csv.DictWriter(outfile, fieldnames=headers)

        # Write header only if creating new file
        if mode == "w":
            writer.writeheader()

            # If starting from a later row, write all previous rows unchanged
            if start_from > 0:
                for row in all_rows[:start_from]:
                    writer.writerow(row)
                print(f"Wrote first {start_from} rows unchanged")

        # Prepare rows to process
        rows_to_process = [
            (idx, all_rows[idx]) for idx in range(start_from, total_rows)
        ]

        # Store results to maintain order
        results = {}
        next_idx_to_write = start_from

        # Process rows in parallel with progress bar
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Create a session for each worker
            sessions = [requests.Session() for _ in range(max_workers)]
            session_idx = 0

            # Submit all tasks
            future_to_idx = {}
            for idx, row in rows_to_process:
                session = sessions[session_idx % max_workers]
                session_idx += 1
                future = executor.submit(
                    process_row, idx, row.copy(), event_cache, cache_lock, session
                )
                future_to_idx[future] = idx

            # Process completed tasks with progress bar
            with tqdm(
                total=len(rows_to_process), desc="Processing rows", unit="row"
            ) as pbar:
                for future in as_completed(future_to_idx):
                    idx, updated_row, was_updated, had_data = future.result()

                    # Store result
                    results[idx] = updated_row

                    # Update counters
                    if was_updated:
                        rows_updated += 1
                    if had_data:
                        rows_with_data += 1

                    # Write results in order
                    while next_idx_to_write in results:
                        writer.writerow(results[next_idx_to_write])
                        del results[next_idx_to_write]
                        next_idx_to_write += 1

                        # Periodic flush
                        if (next_idx_to_write - start_from) % batch_checkpoint == 0:
                            outfile.flush()

                    # Update progress bar
                    pbar.update(1)
                    pbar.set_postfix(
                        {"updated": rows_updated, "had_data": rows_with_data}
                    )

            # Close sessions
            for session in sessions:
                session.close()

    print(f"\nCompleted!")
    print(f"Total rows processed: {total_rows - start_from}")
    print(f"Rows updated: {rows_updated}")
    print(f"Rows that already had data: {rows_with_data}")
    print(f"Output saved to: {output_csv}")

    # If successful, optionally replace the original file
    if input_csv != output_csv:
        print(f"\nTo replace the original file, run:")
        print(f"  mv {output_csv} {input_csv}")


if __name__ == "__main__":
    import sys

    # Allow starting from a specific row (useful for resuming)
    start_row = 0
    max_workers = 10

    if len(sys.argv) > 1:
        try:
            start_row = int(sys.argv[1])
            print(f"Starting from row {start_row}")
        except ValueError:
            print(f"Invalid start row: {sys.argv[1]}, starting from 0")

    if len(sys.argv) > 2:
        try:
            max_workers = int(sys.argv[2])
            print(f"Using {max_workers} workers")
        except ValueError:
            print(f"Invalid number of workers: {sys.argv[2]}, using default 10")

    backfill_event_tags(
        input_csv="markets.csv",
        output_csv="markets_updated.csv",
        start_from=start_row,
        batch_checkpoint=100,
        max_workers=max_workers,
    )
