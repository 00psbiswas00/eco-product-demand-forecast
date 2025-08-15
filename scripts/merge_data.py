import pandas as pd 
import sqlite3
from config.db_config import PROCESSED_PRODUCTS_DB, PROCESSED_TRENDS_DB, OPENFOODFACTS_PROCESSED_TABLE, OPENBEAUTYFACTS_PROCESSED_TABLE, PROCESSED_TRENDS_TABLE, FINAL_db, FINAL_TABLE





def data_merge() -> pd.DataFrame:
    with sqlite3.connect(PROCESSED_TRENDS_DB) as trend_db_con:
        trends_df= pd.read_sql(f'SELECT * FROM {PROCESSED_TRENDS_TABLE}', trend_db_con)


    with sqlite3.connect(PROCESSED_PRODUCTS_DB) as product_db_con:
        beauty_product_df= pd.read_sql(f'SELECT * FROM {OPENBEAUTYFACTS_PROCESSED_TABLE}', product_db_con)
        food_product_df=pd.read_sql(f'SELECT * FROM {OPENFOODFACTS_PROCESSED_TABLE}', product_db_con)


    
    # Ensure columns match before concatenation
    if list(beauty_product_df.columns) != list(food_product_df.columns):
        raise ValueError("Column mismatch between beauty and food product tables.")

    # Append into one unified DataFrame
    products_df = pd.concat([beauty_product_df, food_product_df], ignore_index=True)

    # Drop rows where 'product_name' or 'brand' are empty or whitespace only
    products_df = products_df[
        (products_df['product_name'].str.strip() != '') & 
        (products_df['brand'].str.strip() != '')
    ]

    # Merge products with trends
    merged_df = pd.merge(products_df, trends_df, left_on="sustainability_tags", right_on="keyword", how="inner")
    return merged_df



def store_to_sqlite(df: pd.DataFrame, db_path: str = FINAL_db, table: str = FINAL_TABLE) -> None:
    """Store DataFrame into SQLite under the given table"""

    if df.empty:
        print(f" No data to store in {table}")
        return
    
    with sqlite3.connect(db_path) as con:
        df.to_sql(table, con, if_exists="replace", index=False)
    print(f"Data successfully stored in {table}.")


def main() -> None:
    merged_df = data_merge()
    store_to_sqlite(merged_df)

if __name__ == "__main__":
    main()