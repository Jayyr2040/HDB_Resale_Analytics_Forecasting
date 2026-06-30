# resale_flat_dagster/resale_flat_dagster/assets/ml_preprocessor.py
import pandas as pd
import numpy as np

def build_production_features(df, rpi_dict=None):
    """Processes final production warehouse rows using exact BigQuery schema column names."""
    df = df.copy()
    
    # 1. Timeline Sorting (Using your official warehouse column: transaction_month)
    df['month_date'] = pd.to_datetime(df['transaction_month'] + "-01", errors='coerce')
    df = df.sort_values('month_date').reset_index(drop=True)
    
    # 2. Extract Calendar Numerical Tokens
    df['transaction_year'] = df['month_date'].dt.year
    df['transaction_month'] = df['month_date'].dt.month
    
    # 3. Handle Storey Non-Linear Mappings
    df["floor_level"] = df["storey_range"].str.split(" ").str[0].astype(float)
    
    # 4. MOP Lease Shock Waves (Using your official warehouse column: remaining_lease_years)
    df['remaining_lease_numeric'] = pd.to_numeric(df['remaining_lease_years'], errors='coerce').fillna(85.0)
    df['lease_commence_date'] = pd.to_numeric(df['lease_commence_date'], errors='coerce').fillna(df['transaction_year'] - (99.0 - df['remaining_lease_numeric']))
    
    # 5. Granular Micro Lags Calculation [Town + Flat Type]
   # 🌟 FIXED: Added chronological sorting to guarantee safe, sequential shifting!
    town_type_monthly = (
        df.groupby(['month_date', 'town', 'flat_type'])['resale_price']
        .mean()
        .reset_index()
        .sort_values('month_date')
    )

    town_type_monthly = town_type_monthly.rename(columns={'resale_price': 'grid_avg_price'})
    
    # Generate your leakage-proof shifting momentum anchors
    town_type_monthly['lag_1'] = town_type_monthly.groupby(['town', 'flat_type'])['grid_avg_price'].shift(1)
    town_type_monthly['lag_12'] = town_type_monthly.groupby(['town', 'flat_type'])['grid_avg_price'].shift(12)
    
    # Add a 3-month moving average of past months to trace trajectory velocity
    town_type_monthly['roll_mean_3'] = (
        town_type_monthly.groupby(['town', 'flat_type'])['lag_1']
        .transform(lambda x: x.rolling(3, min_periods=1).mean())
    )

    # Impute initial historical boundary gaps using the global dataset median
    global_median = df['resale_price'].median()
    for col in ['lag_1', 'lag_12', 'roll_mean_3']:
        town_type_monthly[col] = town_type_monthly[col].fillna(global_median)
    
    # Left-merge your clean time-series columns back into the master dataset
    df = pd.merge(df, town_type_monthly.drop(columns=['grid_avg_price']), on=['month_date', 'town', 'flat_type'], how='left')
    
    # 6. Safe Proximity Conversions & Outlier Handlers
    proximity_metrics = [
        "dist_to_closest_mrt_km", "dist_to_closest_lrt_km", "dist_to_closest_shopping_mall_km",
        "dist_to_closest_primary_school_km", "min_distance_to_regional_hub_km"
    ]
    for col in proximity_metrics:
        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(df[col].median())
        
    
    return df
