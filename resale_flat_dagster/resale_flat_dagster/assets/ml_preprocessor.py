# resale_flat_dagster/resale_flat_dagster/assets/ml_preprocessor.py
import pandas as pd
import numpy as np

def build_production_features(df, rpi_dict):
    """Processes final production warehouse rows using exact BigQuery schema column names."""
    df = df.copy()
    
    # 1. Timeline Sorting (Using your official warehouse column: transaction_month)
    df['month_date'] = pd.to_datetime(df['transaction_month'])
    df = df.sort_values('month_date').reset_index(drop=True)
    
    df['year'] = df['month_date'].dt.year
    df['quarter'] = df['month_date'].dt.quarter
    df['quarter_string'] = df['year'].astype(str) + "-Q" + df['quarter'].astype(str)
    
    # 2. Map & Lag RPI Macro Indicators
    df['rpi_current'] = df['quarter_string'].apply(lambda x: rpi_dict.get(x, 204.1))
    df['rpi_lag_1'] = df.groupby('town')['rpi_current'].shift(1).fillna(df['rpi_current'].median())
    
    # 3. Handle Storey Non-Linear Mappings
    df["floor_level"] = df["storey_range"].str.split(" ").str[0].astype(float)
    df["floor_squared"] = df["floor_level"] ** 2
    
    # 4. MOP Lease Shock Waves (Using your official warehouse column: remaining_lease_years)
    df['remaining_lease_numeric'] = df['remaining_lease_years'].astype(float)
    df['lease_commence_year'] = df['month_date'].dt.year - (99 - df['remaining_lease_numeric'].astype(int))
    df['mop_window_flush'] = ((df['month_date'].dt.year - df['lease_commence_year'] >= 5) & 
                              (df['month_date'].dt.year - df['lease_commence_year'] <= 8)).astype(int)
    
    # 5. Granular Micro Lags Calculation [Town + Flat Type]
    town_type_monthly = df.groupby(['month_date', 'town', 'flat_type'])['resale_price'].mean().reset_index()
    town_type_monthly = town_type_monthly.rename(columns={'resale_price': 'grid_avg_price'})
    
    town_type_monthly['lag_1_month'] = town_type_monthly.groupby(['town', 'flat_type'])['grid_avg_price'].shift(1)
    town_type_monthly['lag_12_month'] = town_type_monthly.groupby(['town', 'flat_type'])['grid_avg_price'].shift(12)
    
    global_median = df['resale_price'].median()
    town_type_monthly['lag_1_month'] = town_type_monthly['lag_1_month'].fillna(global_median)
    town_type_monthly['lag_12_month'] = town_type_monthly['lag_12_month'].fillna(global_median)
    
    df = pd.merge(df, town_type_monthly.drop(columns=['grid_avg_price']), on=['month_date', 'town', 'flat_type'], how='left')
    
    df['transaction_year'] = df['month_date'].dt.year
    df['transaction_month_num'] = df['month_date'].dt.month
    
    # 6. Safe Proximity Conversions & Outlier Fillna Handlers
    proximity_metrics = [
        "dist_to_closest_mrt_km", "dist_to_closest_lrt_km", "dist_to_closest_shopping_mall_km",
        "dist_to_closest_primary_school_km", "min_distance_to_regional_hub_km"
    ]
    for col in proximity_metrics:
        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(df[col].median())
        
    # 7. Safe Lease Date Conversion
    df['lease_commence_date'] = pd.to_numeric(df['lease_commence_date'], errors='coerce').fillna(df['lease_commence_year'])
    
    return df
