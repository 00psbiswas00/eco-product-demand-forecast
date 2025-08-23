import pandas as pd
import sqlite3
import numpy as np
from sklearn.linear_model import LinearRegression
from config.db_config import PROCESSED_TRENDS_DB, PROCESSED_TRENDS_TABLE, FORECAST_TRENDS_DB, FORECAST_TRENDS_TABLE
from config.forecasting_config import FORECAST_HORIZON, MIN_POINTS_PER_KEYWORD




def load_trends_df()-> pd.DataFrame:
    with sqlite3.connect(PROCESSED_TRENDS_DB) as con:
        df=pd.read_sql(f'SELECT * from {PROCESSED_TRENDS_TABLE}', con)
        df['date']=pd.to_datetime(df['date'])
        df.sort_values(by=['keyword','date'], inplace= True)
    return df[['date', 'keyword', 'value']]

def add_time_index(df: pd.DataFrame) -> pd.DataFrame:
    """Add a sequential time index per keyword for regression forecasting."""
    df = df.copy()
    df["t"] = df.groupby("keyword").cumcount()
    return df

def forecast_one_keyword(g:pd.DataFrame,horizon:int, min_points:int):
    
    g.sort_values(by='t', inplace= True)
    n=len(g)
    if n< min_points:
        return None
    
    X= g[['t']].to_numpy()
    y=g['value'].to_numpy()
    model= LinearRegression().fit(X,y)


    # future timeline (weekly)
    t_future= np.arange(n, n+horizon).reshape(-1,1)
    start_next_week=g['date'].iloc[-1]+pd.Timedelta(weeks=1)
    future_dates=pd.date_range(start=start_next_week, periods= horizon, freq='W')

    yhat=model.predict(t_future)
    yhat= np.clip(yhat, 0, None) # to avoid negatives just in case

    return pd.DataFrame(
        {
            "keyword": g["keyword"].iloc[0],
            "date": future_dates,
            "forecast": yhat,
            "n_obs": n,
            "slope": float(model.coef_[0]),
            "intercept": float(model.intercept_),
        }
    )


def fit_and_forecast(df:pd.DataFrame, horizon: int, min_points:int)-> pd.DataFrame:
    results=[]

    for kw, g in df.groupby('keyword', sort= False):
        fc = forecast_one_keyword(g, horizon, min_points)
        if fc is not None:
            results.append(fc)
        else:
            print(f"[forecast] skipped {kw}: n<{min_points}")
    if not results:
        return pd.DataFrame(columns=["keyword","date","forecast","n_obs","slope","intercept"])
    return pd.concat(results, ignore_index=True)

def store_forecast(fc: pd.DataFrame) -> None:
    with sqlite3.connect(FORECAST_TRENDS_DB) as con:
        fc.to_sql(FORECAST_TRENDS_TABLE, con, if_exists="replace", index=False)
        print(f"✅ stored {len(fc)} forecast rows → {FORECAST_TRENDS_DB}:{FORECAST_TRENDS_TABLE}")

def main():
    df = load_trends_df()
    df = add_time_index(df)
    fc = fit_and_forecast(df, horizon=FORECAST_HORIZON, min_points=MIN_POINTS_PER_KEYWORD)
    store_forecast(fc)

if __name__ == "__main__":
    main()