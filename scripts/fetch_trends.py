from pytrends.request import TrendReq
import pandas as pd
import sqlite3
import time
from config.trends_config import TRENDS_KEYWORDS, TRENDS_BATCH_SIZE, TRENDS_TIMEFRAME
from config.db_config import RAW_TRENDS_DB, RAW_TRENDS_TABLE


"""
Fetches Google Trends data for the given list of keywords.

Notes:
- Only 5 keywords can be fetched per request; batching is used to handle larger lists.
- Retrieves the last 12 months of interest-over-time data.
- Combines all batches into a single DataFrame and returns it.
"""
def fetch_trends_data(kw_list: list[str]) -> pd.DataFrame:
    pytrends = TrendReq(hl='en-US', tz=330)
    all_data: list[pd.DataFrame] = []
    for i in range(0, len(kw_list), TRENDS_BATCH_SIZE):
        batch = kw_list[i:i + TRENDS_BATCH_SIZE]
        try:
            pytrends.build_payload(batch, timeframe=TRENDS_TIMEFRAME)
            time.sleep(2)
            data = pytrends.interest_over_time()
            if data.empty:
                print(f"⚠️ No data for batch {batch}, skipping...")
                continue #continue to next batch
            all_data.append(data)
        except Exception as e:
            print(f"❌ Error fetching batch {batch}: {e}")
            continue
    return pd.concat(all_data) #marge all batch into dataframe
        

"""
Cleans and reshapes the raw Google Trends data into a Bronze-layer DataFrame.

Steps performed:
- Reset index to make date a column.
- Reshape from wide to long format (date | keyword | value).
- Format dates as ISO strings (YYYY-MM-DD).
- Remove duplicate rows by date and keyword.
- Replace null values in 'value' with 0 and convert to integer.
- Sort the data by keyword and date for consistency.

Returns:
    pd.DataFrame: A cleaned DataFrame ready for storage in the Bronze layer.
"""

def bronze_layer() -> pd.DataFrame:
    raw_df=fetch_trends_data(TRENDS_KEYWORDS)
    raw_df=raw_df.reset_index()
    raw_df= raw_df.melt(id_vars=['date'], var_name='keyword', value_name='value')
    raw_df['date'] = raw_df['date'].dt.strftime("%Y-%m-%d")
    raw_df= raw_df.drop_duplicates(subset=['date', 'keyword'])
    raw_df['value']= raw_df['value'].fillna(0).astype(int)
    raw_df=raw_df.sort_values(['keyword', 'date'])
    return raw_df


def store_to_sqlite(df:pd.DataFrame, db_path: str = RAW_TRENDS_DB, table: str =RAW_TRENDS_TABLE) -> None:
    con= sqlite3.connect(db_path)
    df.to_sql(table, con, if_exists='replace', index=False)
    con.close()



if __name__=="__main__":
    # Run the Bronze layer ingestion and print a preview of the results.
    df: pd.DataFrame = bronze_layer()
    store_to_sqlite(df)
