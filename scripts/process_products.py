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
    con= sqlite3.connect(RAW_PRODUCTS_DB)
    df=pd.read_sql(f'SELECT * FROM {table};', con)
    con.close()
    df=df.dropna(subset=['product_name','url'])
    for col in ['brand','categories','labels','packaging','ecoscore_grade','nutriscore_grade']:
        if col in df.columns:
            df[col] = df[col].fillna('unknown')
    df=df.drop_duplicates(subset=['url'])

    #standarize the data
    if 'categories' in df.columns:
        df['categories']= (df['categories'].str.lower()
                                        .str.replace(r"[>\-;|/\\]", ",", regex=True)
                                        .str.split(',')
                                        .apply(lambda cat:', '.join([i.strip() for i in cat if i.strip()] if isinstance(cat, list) else [])))
                                         

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
    con = sqlite3.connect(db_path)
    df.to_sql(table, con, if_exists="replace", index=False)
    con.close()
    print(f"✅ Stored {len(df)} rows into {table}")


def main():
    for raw, processed in zip(raw_table, processed_table):
        df_clean = clean_product_data(raw)
        store_to_sqlite(df_clean, table=processed)

# Process and store each raw table
if __name__=='__main__':
    main()