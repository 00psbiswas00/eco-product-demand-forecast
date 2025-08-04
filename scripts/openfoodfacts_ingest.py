import pandas as pd
import sqlite3
from openfoodfacts import API, APIVersion, Country, Flavor, Environment

flavor_table={
    Flavor.off: "openfoodfacts_raw",
    Flavor.obf: "openbeautyfacts_raw"
}

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
    
    try:
        results = api.product.text_search(query, page_size=page_size)
        products = results.get("products", []) if results else []
    except Exception as e:
        print(f"Error fetching data for query '{query}': {e}")
        products = []
    
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
    if df.empty:
        print(f" No data to store in {table}")
        return
    con = sqlite3.connect(db_path)
    df.to_sql(table, con, if_exists="append", index=False)
    con.close()
    print(f"✅ Stored {len(df)} rows into {table}")


if __name__ == "__main__":
    for flavor, table_name in flavor_table.items():
        print(f"\nFetching data for flavor: {flavor} -> table: {table_name}")
    
        #create API object
        api = API(
            user_agent="eco-demand",
            country=Country.world,
            flavor=flavor,
            version=APIVersion.v2,
            environment=Environment.org,
        )
        
        for query in kw_map[flavor]:
            df = fetch_openfoodfacts(api, query, page_size=50)
            print(df.head(2))
            store_to_sqlite(df, table=table_name)
