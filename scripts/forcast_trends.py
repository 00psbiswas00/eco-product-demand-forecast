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
        
        # --- Filtering dead/flat keywords ---
        df = (
            df.groupby("keyword")
            .filter(lambda g: (g["value"] > 0)
            .sum() >= MIN_POINTS_PER_KEYWORD and g["value"].nunique() > 1)
        )

    return df[['date', 'keyword', 'value']]

def add_time_index(df: pd.DataFrame) -> pd.DataFrame:
    """Add a sequential time index per keyword for regression forecasting."""
    df = df.copy()
    df["t"] = df.groupby("keyword").cumcount()
    return df

def add_features(df: pd.DataFrame, max_lag: int = 3, rolling_windows: list = [3, 7]) -> pd.DataFrame:
    df = df.copy()
    # Add lag features
    for lag in range(1, max_lag + 1):
        df[f'lag_{lag}'] = df['value'].shift(lag)
    # Add rolling mean features
    for window in rolling_windows:
        df[f'roll_mean_{window}'] = df['value'].shift(1).rolling(window=window, min_periods=1).mean()
    return df

def forecast_one_keyword(g:pd.DataFrame,horizon:int, min_points:int):
    g = g.sort_values(by='t').copy()
    n = len(g)
    if n < min_points:
        return None

    horizon = min(horizon, n // 2)
    
    # Add features
    g = add_features(g)
    g = g.dropna()
    if len(g) < min_points:
        return None

    feature_cols = ['t'] + [col for col in g.columns if col.startswith('lag_') or col.startswith('roll_mean_')]
    X = g[feature_cols].to_numpy()
    y = g['value'].to_numpy()
    model = LinearRegression().fit(X, y)

    # Prepare for recursive forecasting
    last_row = g.iloc[-1].copy()
    forecasts = []
    current_values = list(g['value'].values)
    max_lag = max([int(col.split('_')[1]) for col in feature_cols if col.startswith('lag_')] or [0])
    rolling_windows = [int(col.split('_')[2]) for col in feature_cols if col.startswith('roll_mean_')]
    rolling_windows = list(set(rolling_windows)) if rolling_windows else []

    for step in range(1, horizon + 1):
        t_future = last_row['t'] + step

        # Build features for prediction
        feature_dict = {'t': t_future}
        # Lags
        for lag in range(1, max_lag + 1):
            if lag <= len(current_values):
                feature_dict[f'lag_{lag}'] = current_values[-lag]
            else:
                feature_dict[f'lag_{lag}'] = np.nan
        # Rolling means
        for window in rolling_windows:
            if len(current_values) >= window:
                feature_dict[f'roll_mean_{window}'] = np.mean(current_values[-window:])
            else:
                feature_dict[f'roll_mean_{window}'] = np.mean(current_values)
        # If any NaN in features, replace with zero
        for k, v in feature_dict.items():
            if pd.isna(v):
                feature_dict[k] = 0.0

        X_pred = np.array([feature_dict[col] for col in feature_cols]).reshape(1, -1)
        y_pred = model.predict(X_pred)[0]
        y_pred = max(y_pred, 0)  # clip to zero

        forecasts.append(y_pred)
        current_values.append(y_pred)

    start_next_week = g['date'].iloc[-1] + pd.Timedelta(weeks=1)
    future_dates = pd.date_range(start=start_next_week, periods=horizon, freq='W')

    return pd.DataFrame(
        {
            "keyword": g["keyword"].iloc[0],
            "date": future_dates,
            "forecast": forecasts,
            "n_obs": n,
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
        return pd.DataFrame(columns=["keyword","date","forecast","n_obs"])
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