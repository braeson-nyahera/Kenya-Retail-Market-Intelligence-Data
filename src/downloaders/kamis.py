#!/usr/bin/env python3
from __future__ import annotations

import argparse
import re
import time
from io import StringIO
from pathlib import Path

import pandas as pd
import requests
from bs4 import BeautifulSoup

BASE_URL = "https://kamis.kilimo.go.ke/site/market"


def _norm(s: str) -> str:
    s = s.strip().lower()
    s = re.sub(r"\s+", " ", s)
    return s


def _fetch_product_catalog(session: requests.Session, timeout: int) -> pd.DataFrame:
    """Fetch the product catalog from the KAMIS market page."""
    response = session.get(BASE_URL, timeout=timeout)
    response.raise_for_status()
    soup = BeautifulSoup(response.text, "html.parser")

    product_select = soup.find("select", attrs={"name": "product"})
    if product_select is None:
        for select in soup.find_all("select"):
            if select.find(string=re.compile(r"Dry\s+Maize", re.I)):
                product_select = select
                break

    if product_select is None:
        raise RuntimeError(
            "Could not locate the product dropdown on the KAMIS market page."
        )

    rows = []
    for option in product_select.find_all("option"):
        value = (option.get("value") or "").strip()
        name = re.sub(r"\s+", " ", (option.get_text() or "").strip())
        if not value or not value.isdigit():
            continue
        rows.append({"product_id": int(value), "product_name": name})

    if not rows:
        raise RuntimeError("Parsed the product dropdown but found no numeric option values.")

    return pd.DataFrame(rows).sort_values("product_id").reset_index(drop=True)


def _resolve_product_id(products_df: pd.DataFrame, requested_name: str) -> int:
    """Resolve a product name such as 'Dry Maize' to the matching KAMIS product_id."""
    target = _norm(requested_name)
    products_df = products_df.copy()
    products_df["key"] = products_df["product_name"].map(_norm)

    exact = products_df[products_df["key"] == target]
    if len(exact) == 1:
        return int(exact.iloc[0]["product_id"])
    if len(exact) > 1:
        ids = exact["product_id"].tolist()
        raise ValueError(f"Ambiguous product name '{requested_name}'. Matches multiple ids: {ids}")

    contains = products_df[products_df["key"].str.contains(re.escape(target), na=False)]
    if len(contains) == 1:
        return int(contains.iloc[0]["product_id"])
    if len(contains) > 1:
        sample = contains[["product_id", "product_name"]].head(10).to_dict(orient="records")
        raise ValueError(
            f"Ambiguous product '{requested_name}'. Did you mean one of: {sample} (showing up to 10)"
        )

    sample = products_df[["product_id", "product_name"]].head(30).to_dict(orient="records")
    raise ValueError(
        f"Unknown product '{requested_name}'. Check KAMIS dropdown. First options: {sample} (showing 30)"
    )


def _read_market_table(html: str) -> pd.DataFrame:
    """Parse the main table from the KAMIS HTML response."""
    try:
        tables = pd.read_html(StringIO(html), flavor="bs4")
    except ValueError:
        return pd.DataFrame()

    if not tables:
        return pd.DataFrame()

    for table in tables:
        cols = [str(column).strip().lower() for column in table.columns]
        if "commodity" in cols and "county" in cols and "date" in cols:
            return table

    return tables[-1]


def run_kamis(products: list[str] | None = None, *, out_dir: str | Path = "output/kamis", per_page: int = 3000, max_offsets: int = 1000, timeout: int = 30, sleep_seconds: float = 1.0, force: bool = False) -> None:
    """Download KAMIS market data as CSV files for one or more products."""
    products = products or ["Dry Maize"]
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    session = requests.Session()
    catalog_path = out_dir / "_products.csv"
    catalog = _fetch_product_catalog(session, timeout=timeout)
    catalog.to_csv(catalog_path, index=False)
    print(f"Saved product catalog to {catalog_path} (n={len(catalog)})")

    for product_name in products:
        product_id = _resolve_product_id(catalog, product_name)
        slug = re.sub(r"[^a-z0-9]+", "_", _norm(product_name)).strip("_")
        out_path = out_dir / f"kamis_product{product_id}_{slug}_perpage{per_page}.csv"
        if out_path.exists() and not force:
            print(f"Skipping existing file: {out_path}")
            continue

        print(f"Downloading product='{product_name}' id={product_id} per_page={per_page}")
        chunks: list[pd.DataFrame] = []

        for i in range(max_offsets):
            offset = i * per_page
            url = f"{BASE_URL}/{offset}" if offset > 0 else BASE_URL
            params = {"product": product_id, "per_page": per_page}

            try:
                response = session.get(url, params=params, timeout=timeout)
                response.raise_for_status()
                df = _read_market_table(response.text)

                if df.empty:
                    print(f"No rows at offset={offset}; stopping.")
                    break

                df["product_id"] = product_id
                df["product_name"] = product_name
                df["offset"] = offset
                chunks.append(df)
                print(f"offset={offset} rows={len(df)}")
                time.sleep(sleep_seconds)

                if len(df) < per_page:
                    break
            except Exception as exc:  # pragma: no cover - runtime network failure path
                print(f"offset={offset} error: {exc}")
                break

        if not chunks:
            print(f"No data found for product='{product_name}' (id={product_id})")
            continue

        combined = pd.concat(chunks, ignore_index=True)
        key_cols = [column for column in ["Commodity", "Classification", "Market", "County", "Date"] if column in combined.columns]
        if key_cols:
            combined = combined.drop_duplicates(subset=key_cols + ["Wholesale", "Retail"], keep="last")
        combined.to_csv(out_path, index=False)
        print(f"Saved {out_path} rows={len(combined)}")


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Download KAMIS market data to CSV files.")
    parser.add_argument("--product", action="append", default=[], help="Product name to download, repeatable. Defaults to 'Dry Maize'.")
    parser.add_argument("--per-page", type=int, default=3000, help="Rows per page to request from KAMIS.")
    parser.add_argument("--max-offsets", type=int, default=1000, help="Maximum pagination offsets before stopping.")
    parser.add_argument("--timeout", type=int, default=30, help="HTTP timeout in seconds.")
    parser.add_argument("--sleep", type=float, default=1.0, help="Seconds to wait between requests.")
    parser.add_argument("--out-dir", type=Path, default=Path("output/kamis"), help="Directory where CSV files are written.")
    parser.add_argument("--force", action="store_true", help="Overwrite existing output files.")
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    products = args.product or ["Dry Maize"]
    run_kamis(
        products=products,
        out_dir=args.out_dir,
        per_page=args.per_page,
        max_offsets=args.max_offsets,
        timeout=args.timeout,
        sleep_seconds=args.sleep,
        force=args.force,
    )


if __name__ == "__main__":
    main()
