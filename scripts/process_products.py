import pandas as pd
import sqlite3
from config.db_config import RAW_PRODUCTS_DB, PROCESSED_PRODUCTS_DB,OPENFOODFACTS_RAW_TABLE, OPENBEAUTYFACTS_RAW_TABLE, OPENBEAUTYFACTS_PROCESSED_TABLE, OPENFOODFACTS_PROCESSED_TABLE

# List of raw product tables to be processed
raw_table: list[str]=[OPENFOODFACTS_RAW_TABLE, OPENBEAUTYFACTS_RAW_TABLE]
processed_table:list[str]=[OPENFOODFACTS_PROCESSED_TABLE,OPENBEAUTYFACTS_PROCESSED_TABLE]

def clean_product_data(table:str)->pd.DataFrame:
    """
    Cleans product data from a given SQLite table.

    Parameters:
    table (str): Name of the table to read from.

    Returns:
    pd.DataFrame: Cleaned DataFrame with necessary fields filled and duplicates removed.
    """
    with sqlite3.connect(RAW_PRODUCTS_DB) as con:
        df = pd.read_sql(f'SELECT * FROM {table};', con)
    df=df.dropna(subset=['product_name','url'])
    df = df[(df['product_name'].str.strip() != '') & (df['brand'].str.strip() != '')]
    for col in ['brand','categories','labels','packaging','ecoscore_grade','nutriscore_grade']:
        if col in df.columns:
            df[col] = df[col].fillna('unknown')
    # Standardize key text fields for better tagging accuracy
    for col in ['categories', 'labels', 'packaging']:
        if col in df.columns:
            df[col] = (
                df[col].str.lower()
                .str.replace(r"[>\-;|/\\]", ",", regex=True)  # unify separators
                .str.replace(r"[a-z]{2}:", "", regex=True)    # remove lang prefixes like 'en:'
                .str.split(',')
                .apply(lambda items: ', '.join([i.strip() for i in items if i.strip() and i.strip() != 'unknown'])
                       if isinstance(items, list) else '')
                .replace('', 'unknown')
            )
    df=df.drop_duplicates(subset=['url'])
    return df


def store_to_sqlite(df: pd.DataFrame, db_path=PROCESSED_PRODUCTS_DB, table="products_processed")-> None:
    """
    Stores a cleaned DataFrame into a SQLite database.

    Parameters:
    df (pd.DataFrame): The cleaned DataFrame to store.
    db_path (str): Path to the SQLite database.
    table (str): Table name to store the data in.
    """
    if df.empty:
        print(f'No data to store in {table}')
        return
    with sqlite3.connect(db_path) as con:
        df.to_sql(table, con, if_exists="replace", index=False)
    print(f"✅ Stored {len(df)} rows into {table}")


def main():
    for raw, processed in zip(raw_table, processed_table):
        df_clean = clean_product_data(raw)
        store_to_sqlite(df_clean, table=processed)

# Process and store each raw table
if __name__=='__main__':
    main()