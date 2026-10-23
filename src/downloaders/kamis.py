#!/usr/bin/env python3
import re
import time
from io import StringIO
from pathlib import Path
import pandas as pd
import requests
from bs4 import BeautifulSoup

BASE_URL = "https://kamis.kilimo.go.ke/site/market"
OUT_DIR = Path("../../data/kamis")
OUT_DIR.mkdir(parents=True, exist_ok=True)
# Define the single master output file path
MASTER_CSV_PATH = OUT_DIR / "kamis_combined_products.csv"

# Shared HTTP Session setup
session = requests.Session()
session.headers.update({
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
})

def fetch_all_products() -> list[dict]:
    """Scrape the product selector dropdown to get all available items."""
    print("Fetching product catalog...")
    response = session.get(BASE_URL, timeout=30)
    response.raise_for_status()
    soup = BeautifulSoup(response.text, "html.parser")
    
    product_select = soup.find("select", attrs={"name": "product"})
    products = []
    
    if product_select:
        for option in product_select.find_all("option"):
            val = (option.get("value") or "").strip()
            name = (option.get_text() or "").strip()
            if val.isdigit():
                products.append({"id": int(val), "name": re.sub(r"\s+", " ", name)})
                
    print(f"Found {len(products)} products on KAMIS.")
    return products

def read_market_table(html: str) -> pd.DataFrame:
    """Extract and standardize the price data table from response HTML."""
    try:
        tables = pd.read_html(StringIO(html), flavor="bs4")
        for table in tables:
            # Enforce lowercase columns immediately to capture price strings safely
            table.columns = [str(col).strip().lower() for col in table.columns]
            if "commodity" in table.columns and ("wholesale" in table.columns or "retail" in table.columns):
                return table
    except ValueError:
        pass
    return pd.DataFrame()

def download_product_data(product_id: int, product_name: str, per_page: int = 1000):
    """Scrape the first 1000 records for a unique product and append to a master CSV file."""
    print(f"\nProcessing: {product_name} (ID: {product_id})")
    
    url = BASE_URL
    try:
        response = session.get(url, params={"product": product_id, "per_page": per_page}, timeout=30)
        response.raise_for_status()
        df = read_market_table(response.text)
        
        if df.empty:
            print(f"  No available records found for {product_name}.")
            return
            
        # Enforce the strict 1000 row cap
        if len(df) > per_page:
            df = df.head(per_page)
            
        df["scraped_id"] = product_id
        df["scraped_name"] = product_name
        
        # Load existing data if file exists to drop duplicates across the whole dataset
        if MASTER_CSV_PATH.exists():
            try:
                existing_df = pd.read_csv(MASTER_CSV_PATH)
                combined = pd.concat([existing_df, df], ignore_index=True)
            except Exception:
                # Fallback if the file is corrupted or empty
                combined = df
        else:
            combined = df

        # Deduplicate using your business logic columns
        key_cols = [c for c in ["commodity", "classification", "market", "county", "date"] if c in combined.columns]
        price_cols = [c for c in ["wholesale", "retail"] if c in combined.columns]
        
        if key_cols and price_cols:
            combined = combined.drop_duplicates(subset=key_cols + price_cols, keep="last")
            
        # Overwrite the spreadsheet with the fresh combined dataset
        combined.to_csv(MASTER_CSV_PATH, index=False)
        print(f"  Successfully updated master CSV. Total records now: {len(combined)}")
        
    except Exception as e:
        print(f"  -> Error tracking product {product_id}: {e}")



def main():
    # 1. Fetch the complete item collection 
    catalog = fetch_all_products()
    if not catalog:
        print("Failed to pull products. Exiting.")
        return
        
    # Save a master register copy for reference
    pd.DataFrame(catalog).to_csv(OUT_DIR / "_master_catalog.csv", index=False)
    
    # 2. Sequential collection iteration for every available crop/commodity
    for item in catalog:
        download_product_data(item["id"], item["name"])

if __name__ == "__main__":
    main()
