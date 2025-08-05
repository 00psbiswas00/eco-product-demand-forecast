import pandas as pd
import sqlite3
from openfoodfacts import API, APIVersion, Country, Flavor, Environment

# Script to fetch eco-product data from OpenFoodFacts and OpenBeautyFacts APIs,
# normalize results, and store them into SQLite for later analysis.

# Mapping of dataset flavor to corresponding SQLite table names
flavor_table: dict[Flavor,str]={
    Flavor.off: "openfoodfacts_raw",
    Flavor.obf: "openbeautyfacts_raw"
}

# Keywords per flavor: separate queries for Food (off) and Beauty (obf)
kw_map: dict[Flavor, list[str]] = {
    Flavor.off: [
        "organic food",
        "plant-based",
        "organic snacks",
        "beverages",
        "eco"
    ],
    Flavor.obf: [
        "natural skincare",
        "organic shampoo",
        "organic cosmetics"
    ]
}


def fetch_openfoodfacts(api: API, query: str, page_size: int = 50) -> pd.DataFrame:
    """Fetch product data from the given API flavor for a query and return as DataFrame"""
    
    # Attempt to fetch products from API using text search
    try:
        results = api.product.text_search(query, page_size=page_size)
        products = results.get("products", []) if results else []
    except Exception as e:
        # Handle any errors during API call gracefully
        print(f"Error fetching data for query '{query}': {e}")
        products = []
    
    # Normalize product data into a list of dictionaries
    records=[]
    for p in products:
        records.append(
            {
                "product_name": p.get("product_name"),
                "brand": p.get("brands"),
                "categories": p.get("categories"),
                "labels": p.get("labels"),
                "packaging": p.get("packaging"),
                "nutriscore_grade": p.get("nutriscore_grade"),
                "ecoscore_grade": p.get("ecoscore_grade"),
                "url": p.get("url"),
            }
        )
    return pd.DataFrame(records)


def store_to_sqlite(df: pd.DataFrame, db_path="data/raw/products.db", table="products_raw"):
    """Store DataFrame into SQLite under the given table"""
    if df.empty:
        print(f" No data to store in {table}")
        return
    con = sqlite3.connect(db_path)
    df.to_sql(table, con, if_exists="append", index=False)
    con.close()
    print(f"✅ Stored {len(df)} rows into {table}")


if __name__ == "__main__":
    # Loop through each flavor and fetch data for relevant queries
    for flavor, table_name in flavor_table.items():
        print(f"\nFetching data for flavor: {flavor} -> table: {table_name}")
    
        # Create API object for the current flavor
        api = API(
            user_agent="eco-demand",
            country=Country.world,
            flavor=flavor,
            version=APIVersion.v2,
            environment=Environment.org,
        )
        
        for query in kw_map[flavor]:
            df = fetch_openfoodfacts(api, query, page_size=50)
            # Preview first rows before storing
            print(df.head(2))
            store_to_sqlite(df, table=table_name)
