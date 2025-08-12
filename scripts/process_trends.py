import pandas as pd
import sqlite3
from config.db_config import RAW_TRENDS_DB, RAW_TRENDS_TABLE, PROCESSED_TRENDS_DB, PROCESSED_TRENDS_TABLE



def clean_trend_data(table:str) -> pd.DataFrame:
    con= sqlite3.connect(RAW_TRENDS_DB)
    df=pd.read_sql(f'SELECT * FROM {table}', con)
    print(df.head(5))
    con.close()
    df=df.reset_index()
    df= df.melt(id_vars=['date'], var_name='keyword', value_name='value')
    df['date'] = pd.to_datetime(df['date'], errors='coerce')
    df['date'] = df['date'].dt.strftime("%Y-%m-%d")
    df = df.drop_duplicates(subset=['date', 'keyword'])
    df['value'] = df['value'].fillna(0).astype(int)
    df = df.sort_values(['keyword', 'date']).reset_index(drop=True)

    return df

def store_to_sqlite(df: pd.DataFrame, db_path=PROCESSED_TRENDS_DB, table=PROCESSED_TRENDS_TABLE)-> None:
    if df.empty:
        print(f'No data to store in {table}')
        return
    
    con= sqlite3.connect(db_path)
    df.to_sql(table,con, if_exists='replace', index=False)
    con.close()
    print(f"✅ Stored {len(df)} rows into {table}")


def main():
    df= clean_trend_data(RAW_TRENDS_TABLE)
    store_to_sqlite(df)



if __name__=='__main__':
    main()

