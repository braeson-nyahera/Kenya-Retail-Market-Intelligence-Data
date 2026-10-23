import os
import csv
import json
import random
from pathlib import Path
from datetime import datetime, timedelta

# CONFIGURATION & SEEDING
SEED = 42
random.seed(SEED)
OUT_DIR = Path("../../data/synthetic")
OUT_DIR.mkdir(parents=True, exist_ok=True)

VOLUME = {
    "stores": random.randint(20, 30),
    "suppliers": random.randint(10, 30),
    "products": random.randint(150, 300),
    "pos_lines": 250000
}

DIRT_RATES = {
    "duplicate": 0.005,    # 0.5% duplicate rows
    "null_field": 0.005,   # 0.5% null values in critical fields
    "negative_qty": 0.005  # 0.5% negative quantities or prices
}

# Ensure output directory exists
os.makedirs("output", exist_ok=True)

dirt_manifest = []

def log_dirt(entity, issue_type, identifier, details=""):
    dirt_manifest.append({
        "entity": entity,
        "issue_type": issue_type,
        "identifier": identifier,
        "details": details,
        "timestamp": datetime.now().isoformat()
    })

# MOCK KAMIS BASE DATA
# Simulating the data/kamis/ historical data structure if files aren't parsed
MOCK_KAMIS_COMMODITIES = [
    {"commodity": "Dry Maize", "category": "Cereals", "base_price_kes": 80},
    {"commodity": "Green Maize", "category": "Cereals", "base_price_kes": 50},
    {"commodity": "Finger Millet", "category": "Cereals", "base_price_kes": 120},
    {"commodity": "Sorghum", "category": "Cereals", "base_price_kes": 90},
    {"commodity": "Rosecoco Beans", "category": "Pulses", "base_price_kes": 140},
    {"commodity": "Mwitemania Beans", "category": "Pulses", "base_price_kes": 130},
    {"commodity": "Red Irish Potatoes", "category": "Tubers", "base_price_kes": 60},
    {"commodity": "Cabbages", "category": "Vegetables", "base_price_kes": 40},
    {"commodity": "Tomatoes", "category": "Vegetables", "base_price_kes": 90},
    {"commodity": "Onions Dry", "category": "Vegetables", "base_price_kes": 110},
]

# ENTITY GENERATORS
def generate_suppliers():
    suppliers = []
    for i in range(1, VOLUME["suppliers"] + 1):
        sup_id = f"SUP-{i:03d}"
        suppliers.append({
            "supplier_id": sup_id,
            "supplier_name": f"Supplier Elite Ltd {i}",
            "contact_email": f"info@supplier{i}.co.ke",
            "region": random.choice(["Nairobi", "Mombasa", "Kisumu", "Nakuru", "Eldoret"])
        })
    return suppliers

def generate_stores():
    stores = []
    regions = ["Nairobi", "Central", "Coast", "Rift Valley", "Nyanza", "Western"]
    for i in range(1, VOLUME["stores"] + 1):
        store_id = f"STR-{i:03d}"
        stores.append({
            "store_id": store_id,
            "store_name": f"MegaStore Branch {i}",
            "county": random.choice(regions),
            "is_active": True
        })
    return stores

def generate_products(suppliers):
    products = []
    prod_to_commodity_map = []
    
    for i in range(1, VOLUME["products"] + 1):
        prod_id = f"PRD-{i:04d}"
        kamis_ref = random.choice(MOCK_KAMIS_COMMODITIES)
        
        # Inject standard price variations based on KAMIS base price
        base_price = round(kamis_ref["base_price_kes"] * random.uniform(0.85, 1.25), 2)
        
        # Dirt Injection: Negative Price
        if random.random() < DIRT_RATES["negative_qty"]:
            base_price = -base_price
            log_dirt("products", "NEGATIVE_PRICE", prod_id, "Base price generated as negative")

        product = {
            "product_id": prod_id,
            "product_name": f"Premium Packaged {kamis_ref['commodity']} {random.choice(['1kg', '2kg', '500g'])}",
            "supplier_id": random.choice(suppliers)["supplier_id"],
            "base_price_kes": base_price
        }
        
        # Dirt Injection: Null values in product fields
        if random.random() < DIRT_RATES["null_field"]:
            product["supplier_id"] = None
            log_dirt("products", "NULL_SUPPLIER", prod_id, "Supplier ID set to null")

        products.append(product)
        
        prod_to_commodity_map.append({
            "product_id": prod_id,
            "kamis_commodity_name": kamis_ref["commodity"],
            "kamis_category": kamis_ref["category"]
        })
        
    return products, prod_to_commodity_map


# HISTORICAL POS TRANSACTION GENERATOR
def emit_pos_line(tx_id, line_no, stores, products, current_date, emitter_func):
    store = random.choice(stores)
    product = random.choice(products)
    
    qty = random.randint(1, 10)
    unit_price = product["base_price_kes"]
    
    # Dirt Injection: Negative Quantity
    if random.random() < DIRT_RATES["negative_qty"]:
        qty = -qty
        log_dirt("pos_lines", "NEGATIVE_QTY", tx_id, f"Line {line_no} set to negative quantity")
        
    # Dirt Injection: Missing product association
    p_id = product["product_id"]
    if random.random() < DIRT_RATES["null_field"]:
        p_id = None
        log_dirt("pos_lines", "NULL_PRODUCT", tx_id, f"Line {line_no} product_id set to null")

    line_item = {
        "transaction_id": tx_id,
        "line_number": line_no,
        "store_id": store["store_id"],
        "product_id": p_id,
        "quantity": qty,
        "unit_price_kes": unit_price,
        "timestamp": current_date.strftime("%Y-%m-%dT%H:%M:%S")
    }
    
    emitter_func(line_item)
    
    # Dirt Injection: Duplicate Row Emission
    if random.random() < DIRT_RATES["duplicate"]:
        emitter_func(line_item)
        log_dirt("pos_lines", "DUPLICATE_ROW", tx_id, f"Duplicate emitted for line {line_no}")

# EMISSION STRATEGIES (BATCH & STREAM SKELETON)
class DataWriter:
    def __init__(self, mode="file", filepath=None):
        self.mode = mode
        self.filepath = filepath
        self.file = None
        self.writer = None

    def open(self):
        if self.mode == "file" and self.filepath:
            self.file = open(self.filepath, "w", newline="", encoding="utf-8")
            self.writer = csv.DictWriter(self.file, fieldnames=["transaction_id", "line_number", "store_id", "product_id", "quantity", "unit_price_kes", "timestamp"])
            self.writer.writeheader()

    def emit(self, record):
        if self.mode == "file" and self.writer:
            self.writer.writerow(record)
        elif self.mode == "stream":
            # Skeleton for Milestone 3 Kafka integration
            # kafka_producer.send('pos-transactions', value=json.dumps(record).encode('utf-8'))
            pass

    def close(self):
        if self.file:
            self.file.close()

# MAIN EXECUTION PIPELINE
if __name__ == "__main__":
    print("Generating Master Data Dimensions...")
    suppliers = generate_suppliers()
    stores = generate_stores()
    products, item_map = generate_products(suppliers)

    # Save static dimension entities to CSV
    for entity_name, data in [("suppliers", suppliers), ("stores", stores), ("products", products), ("product_commodity_map", item_map)]:
        with open(OUT_DIR / f"{entity_name}.csv", "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=data[0].keys())
            writer.writeheader()
            writer.writerows(data)

    print(f"Generating {VOLUME['pos_lines']} Historical POS Lines...")
    
    # Choose emission mode here: "file" or "stream"
    writer = DataWriter(mode="file", filepath=OUT_DIR / "historical_pos_lines.csv")
    writer.open()

    start_date = datetime(2026, 1, 1)
    tx_counter = 100001
    
    for _ in range(VOLUME["pos_lines"]):
        tx_id = f"TXN-{tx_counter}"
        lines_in_tx = random.choices([1, 2, 3, 4], weights=[40, 30, 20, 10])[0]
        current_date = start_date + timedelta(seconds=random.randint(0, 31536000)) # random distribution across a year
        
        for line_no in range(1, lines_in_tx + 1):
            emit_pos_line(tx_id, line_no, stores, products, current_date, writer.emit)
            
        tx_counter += 1

    writer.close()

    # Save dirt manifest for Milestone 6 audit checks
    with open(OUT_DIR / "dirt_manifest.json", "w", encoding="utf-8") as f:
        json.dump(dirt_manifest, f, indent=4)

    print("\nGeneration Completed Successfully!")
    print(f"Generated: {len(stores)} stores, {len(products)} products, {len(suppliers)} suppliers.")
    print(f"Injected anomalies logged in output/dirt_manifest.json (Total clean-up targets: {len(dirt_manifest)})")
