import pandas as pd
import sqlite3

raw_table: list[str]=['openfoodfacts_raw', 'openbeautyfacts_raw']

def clean_prodct_data(table:str)->pd.DataFrame:
    con= sqlite3.connect('data/raw/products.db')
    df=pd.read_sql(f'SELECT * FROM {table};', con)
    print(df.head(2))
    print('Null before cleanign\n')
    print(df.isna().sum()/df.shape[0]*100)
    df=df.dropna(subset=['product_name','url'])
    for col in ['brand','categories','labels','packaging','ecoscore_grade','nutriscore_grade']:
        if col in df.columns:
            df[col] = df[col].fillna('unknown')
    print('Null after cleanign\n')
    print(df.isna().sum()/df.shape[0]*100)

    print('duplicate after drop null\n')
    print(df.duplicated(subset=['url']).sum())
    df=df.drop_duplicates(subset=['url'])
    print('duplicate after drop null\n')
    print(df.duplicated(subset=['url']).sum())

    return df


def store_to_sqlite(df: pd.DataFrame, db_path="data/processed/eco_products.db", table="products_processed"):
    if df.empty:
        print(f'No data to store in {table}')
        return
    con = sqlite3.connect(db_path)
    df.to_sql(table, con, if_exists="append", index=False)
    con.close()
    print(f"✅ Stored {len(df)} rows into {table}")


if __name__=='__main__':
    for table in raw_table:
        df_clean = clean_prodct_data(table)
        processed_table = table.replace("_raw", "_processed")
        store_to_sqlite(df_clean, table=processed_table)