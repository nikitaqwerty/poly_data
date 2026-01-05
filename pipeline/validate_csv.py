#!/usr/bin/env python3
"""
CSV Validator

Validates CSV files before loading into ClickHouse
"""

import csv
import sys
from pathlib import Path
from datetime import datetime

# Required columns for each CSV type
MARKETS_REQUIRED_COLUMNS = {
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
    "event_slug",
    "closedTime",
    "tags",
}

TRADES_REQUIRED_COLUMNS = {
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
}


def validate_datetime(value: str, field_name: str) -> tuple[bool, str]:
    """Validate datetime string"""
    if not value:
        return True, ""  # Nullable is ok

    try:
        datetime.fromisoformat(value.replace("Z", "+00:00"))
        return True, ""
    except Exception as e:
        return False, f"Invalid datetime format in {field_name}: {value} ({e})"


def validate_float(value: str, field_name: str) -> tuple[bool, str]:
    """Validate float value"""
    if not value:
        return True, ""

    try:
        float(value)
        return True, ""
    except Exception as e:
        return False, f"Invalid float in {field_name}: {value} ({e})"


def validate_boolean(value: str, field_name: str) -> tuple[bool, str]:
    """Validate boolean value"""
    if not value:
        return True, ""

    valid_values = {"true", "false", "1", "0", "yes", "no"}
    if value.lower() not in valid_values:
        return False, f"Invalid boolean in {field_name}: {value} (expected true/false/1/0/yes/no)"

    return True, ""


def validate_markets_csv(csv_path: str, max_rows: int = 100) -> tuple[bool, list]:
    """
    Validate markets CSV file

    Args:
        csv_path: Path to CSV file
        max_rows: Maximum number of rows to validate (0 = all)

    Returns:
        Tuple of (is_valid, list of errors)
    """
    csv_file = Path(csv_path)
    if not csv_file.exists():
        return False, [f"File not found: {csv_path}"]

    errors = []

    try:
        with open(csv_file, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)

            # Check headers
            headers = set(reader.fieldnames or [])
            missing_cols = MARKETS_REQUIRED_COLUMNS - headers
            if missing_cols:
                errors.append(f"Missing required columns: {', '.join(missing_cols)}")
                return False, errors

            # Validate rows
            row_count = 0
            for row_num, row in enumerate(reader, start=1):
                if max_rows > 0 and row_num > max_rows:
                    break

                row_count += 1

                # Validate createdAt
                valid, error = validate_datetime(row.get("createdAt", ""), "createdAt")
                if not valid:
                    errors.append(f"Row {row_num}: {error}")

                # Validate closedTime (nullable)
                valid, error = validate_datetime(row.get("closedTime", ""), "closedTime")
                if not valid:
                    errors.append(f"Row {row_num}: {error}")

                # Validate volume
                valid, error = validate_float(row.get("volume", ""), "volume")
                if not valid:
                    errors.append(f"Row {row_num}: {error}")

                # Validate neg_risk
                valid, error = validate_boolean(row.get("neg_risk", ""), "neg_risk")
                if not valid:
                    errors.append(f"Row {row_num}: {error}")

            if row_count == 0:
                errors.append("CSV file is empty (no data rows)")

    except Exception as e:
        errors.append(f"Error reading CSV: {e}")
        return False, errors

    return len(errors) == 0, errors


def validate_trades_csv(csv_path: str, max_rows: int = 100) -> tuple[bool, list]:
    """
    Validate trades CSV file

    Args:
        csv_path: Path to CSV file
        max_rows: Maximum number of rows to validate (0 = all)

    Returns:
        Tuple of (is_valid, list of errors)
    """
    csv_file = Path(csv_path)
    if not csv_file.exists():
        return False, [f"File not found: {csv_path}"]

    errors = []

    try:
        with open(csv_file, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)

            # Check headers
            headers = set(reader.fieldnames or [])
            missing_cols = TRADES_REQUIRED_COLUMNS - headers
            if missing_cols:
                errors.append(f"Missing required columns: {', '.join(missing_cols)}")
                return False, errors

            # Validate rows
            row_count = 0
            for row_num, row in enumerate(reader, start=1):
                if max_rows > 0 and row_num > max_rows:
                    break

                row_count += 1

                # Validate timestamp
                valid, error = validate_datetime(row.get("timestamp", ""), "timestamp")
                if not valid:
                    errors.append(f"Row {row_num}: {error}")

                # Validate price
                valid, error = validate_float(row.get("price", ""), "price")
                if not valid:
                    errors.append(f"Row {row_num}: {error}")

                # Validate usd_amount
                valid, error = validate_float(row.get("usd_amount", ""), "usd_amount")
                if not valid:
                    errors.append(f"Row {row_num}: {error}")

                # Validate token_amount
                valid, error = validate_float(row.get("token_amount", ""), "token_amount")
                if not valid:
                    errors.append(f"Row {row_num}: {error}")

            if row_count == 0:
                errors.append("CSV file is empty (no data rows)")

    except Exception as e:
        errors.append(f"Error reading CSV: {e}")
        return False, errors

    return len(errors) == 0, errors


def main():
    """Main entry point"""
    import argparse

    parser = argparse.ArgumentParser(description="Validate CSV files before loading")
    parser.add_argument(
        "csv_type",
        choices=["markets", "trades"],
        help="Type of CSV file to validate",
    )
    parser.add_argument("csv_path", help="Path to CSV file")
    parser.add_argument(
        "--max-rows",
        type=int,
        default=100,
        help="Maximum number of rows to validate (0 = all, default: 100)",
    )

    args = parser.parse_args()

    print("=" * 60)
    print(f"📋 Validating {args.csv_type} CSV: {args.csv_path}")
    print("=" * 60)

    if args.csv_type == "markets":
        is_valid, errors = validate_markets_csv(args.csv_path, args.max_rows)
    else:
        is_valid, errors = validate_trades_csv(args.csv_path, args.max_rows)

    if is_valid:
        print("✅ CSV file is valid!")
        print(f"   Checked first {args.max_rows} rows (use --max-rows 0 to check all)")
    else:
        print("❌ CSV file has errors:")
        for error in errors[:20]:  # Show first 20 errors
            print(f"   • {error}")

        if len(errors) > 20:
            print(f"   ... and {len(errors) - 20} more errors")

        sys.exit(1)


if __name__ == "__main__":
    main()
