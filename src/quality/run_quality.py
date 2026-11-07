"""Run the project's small set of completeness and format checks."""

import argparse
import json
from datetime import datetime
from pathlib import Path

import pandas as pd

try:
    from .checks import check_dataframe
except ImportError:  # Supports direct execution: python src/quality/run_quality.py
    from checks import check_dataframe

ROOT = Path(__file__).resolve().parents[2]
DATA = ROOT / "data"
REPORTS = DATA / "quality_reports"

# Keep expectations here so they are easy to read and change.
DATASETS = {
    "silver.pos_transactions": {
        "path": DATA / "silver" / "pos_transactions",
        "columns": ["transaction_id", "line_number", "store_id", "product_id", "quantity",
                    "unit_price_kes", "timestamp", "transaction_timestamp", "transaction_date",
                    "year_month", "total_amount_kes", "real_amount_kes"],
        "required": ["transaction_id", "line_number", "store_id", "product_id", "quantity",
                     "unit_price_kes", "total_amount_kes", "real_amount_kes"],
        "types": {"line_number": "integer", "quantity": "number", "unit_price_kes": "number",
                  "total_amount_kes": "number", "real_amount_kes": "number",
                  "transaction_date": "date", "timestamp": "date"},
    },
    "gold.store_daily_sales": {
        "path": DATA / "gold" / "gold_store_daily_sales",
        "columns": ["store_id", "store_name", "store_county", "transaction_date", "year_month",
                    "total_transactions", "total_items_sold", "nominal_revenue_kes",
                    "real_revenue_kes", "avg_basket_value_kes", "inflation_loss_kes"],
        "required": ["store_id", "transaction_date", "nominal_revenue_kes", "total_transactions"],
        "types": {"transaction_date": "date", "total_transactions": "integer",
                  "total_items_sold": "integer", "nominal_revenue_kes": "number",
                  "real_revenue_kes": "number", "avg_basket_value_kes": "number"},
    },
    "silver.kamis_prices": {
        "path": DATA / "silver" / "kamis_prices",
        "columns": ["commodity_name", "market_name", "county_name", "market_date", "year_month",
                    "wholesale_kes", "retail_kes", "supply_volume_kg"],
        "required": ["commodity_name", "market_name", "market_date", "wholesale_kes", "retail_kes"],
        "types": {"market_date": "date", "wholesale_kes": "number", "retail_kes": "number",
                  "supply_volume_kg": "number"},
    },
    "silver.knbs_cpi": {
        "path": DATA / "silver" / "knbs_cpi",
        "columns": ["year_month", "cpi_index", "deflation_multiplier", "yoy_inflation_pct", "mom_inflation_pct"],
        "required": ["year_month", "cpi_index", "deflation_multiplier"],
        "types": {"cpi_index": "number", "deflation_multiplier": "number",
                  "yoy_inflation_pct": "number", "mom_inflation_pct": "number"},
    },
}


def load_dataset(path):
    if not path.exists():
        raise FileNotFoundError(f"Dataset not found: {path}")
    if path.is_dir() or path.suffix == ".parquet":
        return pd.read_parquet(path)
    if path.suffix == ".csv":
        return pd.read_csv(path)
    raise ValueError(f"Unsupported dataset format: {path}")


def main():
    parser = argparse.ArgumentParser(description="Simple completeness and format checks")
    parser.add_argument("--dataset", choices=["all", *DATASETS], default="all")
    parser.add_argument("--max-missing", type=float, default=1.0,
                        help="Maximum missing percentage for required columns (default: 1)")
    parser.add_argument("--no-save", action="store_true")
    args = parser.parse_args()

    names = list(DATASETS) if args.dataset == "all" else [args.dataset]
    results = []
    for name in names:
        spec = DATASETS[name]
        try:
            df = load_dataset(spec["path"])
            results.extend(check_dataframe(df, name, spec["columns"], spec["required"],
                                           args.max_missing, spec["types"]))
        except Exception as error:
            results.append({"dataset": name, "check": "load_dataset", "column": None,
                            "status": "FAIL", "message": str(error)})

    for item in results:
        col = f" [{item['column']}]" if item["column"] else ""
        print(f"{item['status']}: {item['dataset']}{col} - {item['message']}")

    failed = sum(item["status"] == "FAIL" for item in results)
    print(f"\nChecks: {len(results)} | Passed: {len(results) - failed} | Failed: {failed}")
    if not args.no_save:
        REPORTS.mkdir(parents=True, exist_ok=True)
        output = REPORTS / f"quality_{datetime.now():%Y%m%d_%H%M%S}.json"
        output.write_text(json.dumps(results, indent=2), encoding="utf-8")
        print(f"Report saved to: {output}")
    if failed:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
