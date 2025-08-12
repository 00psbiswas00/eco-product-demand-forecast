from config.sustainability_tags import SUSTAINABILITY_TAGS
from config.db_config import PROCESSED_PRODUCTS_DB, OPENFOODFACTS_PROCESSED_TABLE,OPENBEAUTYFACTS_PROCESSED_TABLE
import sqlite3
import pandas as pd



def map_tags(df:pd.DataFrame)-> pd.DataFrame:
    # Combine relevant columns into one searchable string
    search_series= df[['categories', 'labels', 'packaging']].fillna('').apply(lambda row: ' '.join(row), axis=1)
    
    # For each tag category, check if any keyword is in the row text
    df['sustainability_tags']= search_series.apply(
        lambda text: [
            tag for tag, keywords in SUSTAINABILITY_TAGS.items()
                     if any(kw in text for kw in keywords)
        ]
    )

    # Convert list of tags to a comma-separated string for DB storage
    df['sustainability_tags'] = df['sustainability_tags'].apply(
        lambda tags: ', '.join(tags) if isinstance(tags, list) else ''
    )
    return df


def add_susutainabily_tag()->None:
    con=sqlite3.connect(PROCESSED_PRODUCTS_DB)
    openbeautyfacts_df= pd.read_sql(f'SELECT * FROM {OPENBEAUTYFACTS_PROCESSED_TABLE}', con)
    openfoodfacts_df=pd.read_sql(f'SELECT * FROM {OPENFOODFACTS_PROCESSED_TABLE}', con)
    
    
    openbeautyfacts_df=map_tags(openbeautyfacts_df)
    openfoodfacts_df=map_tags(openfoodfacts_df)

    openbeautyfacts_df.to_sql(OPENBEAUTYFACTS_PROCESSED_TABLE, con, if_exists="replace", index=False)
    openfoodfacts_df.to_sql(OPENFOODFACTS_PROCESSED_TABLE, con, if_exists="replace", index=False)
    con.close()

def main():
    add_susutainabily_tag()


if __name__=="__main__":
    main()
