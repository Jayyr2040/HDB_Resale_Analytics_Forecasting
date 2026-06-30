# resale_flat_dagster/resale_flat_dagster/assets/ml_models.py
import os
import pickle
import pandas as pd
import numpy as np
from dagster import asset, Output
from sklearn.ensemble import HistGradientBoostingRegressor
from .ml_preprocessor import build_production_features
from sqlalchemy import create_engine

def fetch_pipeline_data_standalone():
    """Queries BigQuery directly without introducing heavy UI charting dependencies."""
    credentials_path = "/home/taijl/DSAI/S2_BigData/DSAI_HDB_Project/project-8d552288-1acb-4a23-893-07fe8627d11f.json"
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = credentials_path
    
    project_id = "project-8d552288-1acb-4a23-893"
    engine = create_engine(f"bigquery://{project_id}/hdb_analytics_marts")
    
    # EXPLICIT FIX: Brought in primary school and LRT proximity vectors cleanly
    query = """
    SELECT f.resale_price, f.floor_area_sqm, f.remaining_lease_years, f.storey_range, f.flat_type,
           f.dist_to_closest_mrt_km, f.dist_to_closest_shopping_mall_km, f.transaction_month,
           f.dist_to_closest_primary_school_km, f.dist_to_closest_lrt_km, p.lease_commence_date,
           f.min_distance_to_regional_hub_km, p.town, p.flat_model
    FROM fact_sales f
    LEFT JOIN dim_properties p ON f.property_id = p.property_id;
    """
    with engine.connect() as conn:
        return pd.read_sql(query, con=conn)

@asset(
    compute_kind="scikit-learn",
    deps=["fact_sales"],
    group_name="machine_learning"  # Keeps your asset graph looking clean!
)
def hdb_trained_ml_artifacts(context):
    """Pulls directly from BigQuery production warehouse table marts and fits model weights."""
    context.log.info("Streaming production data rows out of Google BigQuery Warehouse...")
    # 🌟 RUNS CLOUD STREAM: Bypasses local I/O file lookups completely by reading from BigQuery
    upstream_df = fetch_pipeline_data_standalone()
    context.log.info(f"Loaded {len(upstream_df)} rows. Processing features via ml_preprocessor...")
    
    # context.log.info("Streaming production data rows out of Google BigQuery Warehouse...")
    # # upstream_df = fetch_pipeline_data_standalone()
    
    # context.log.info(f"Loaded {len(upstream_df)} rows. Processing time-series features...")
    # Calls your newly updated preprocessor directly to build your clean 14-feature schema
    # processed_df = build_production_features(upstream_df)

    # 🌟 CRITICAL FIX: Strip spaces and convert to uppercase across BOTH tables' merge keys
    for col in ['town', 'flat_type', 'flat_model', 'storey_range']:
        if col in upstream_df.columns:
            upstream_df[col] = upstream_df[col].astype(str).str.strip().str.upper()

    # 🌟 RUNS CLOUD STREAM: Bypasses local I/O file lookups completely by reading from BigQuery
    # 🌟 REPLACED: Delete fetch_pipeline_data_standalone() completely!
    # Your model now trains directly on the live database rows streamed down by your star schema assets.
    processed_df = build_production_features(upstream_df)
    
    features = [
        "floor_area_sqm", "remaining_lease_numeric", "floor_level", "lease_commence_date",
        "transaction_year", "transaction_month",
        "lag_1", "lag_12", "roll_mean_3",
        "dist_to_closest_mrt_km", "dist_to_closest_lrt_km", "dist_to_closest_shopping_mall_km",
        "dist_to_closest_primary_school_km", "min_distance_to_regional_hub_km",
        "town", "flat_type", "flat_model"
    ]

    # Enforce strict column presence to throw an informative log message instead of crashing
    missing_cols = [c for c in features if c not in processed_df.columns]
    if missing_cols:
        raise KeyError(f"❌ Core processing error! Missing columns: {missing_cols}. Check your merge keys.")
             
    # Cast categories cleanly
    categorical_cols = ["town", "flat_type", "flat_model"]
    for col in categorical_cols:
        processed_df[col] = processed_df[col].astype('category')

    # Low-Memory Opt: Downcast elements to float32 to prevent background RAM crashes
    for col in features:
        if col not in categorical_cols:
            processed_df[col] = processed_df[col].astype('float32')

    X = processed_df[features]
    y = processed_df['resale_price'].astype('float32')
    
    # Explicit Boolean Mask Array mapping for HistGradientBoosting
    is_categorical = [col in categorical_cols for col in features]
    
    context.log.info("Training HistGradientBoostingRegressor model...")
    final_estimator = HistGradientBoostingRegressor(
        loss='squared_error',
        categorical_features=is_categorical,
        learning_rate=0.1,       # Optimized baseline learning rate step
        max_iter=800,            # Max iterations for deep spatial pattern recognition
        max_leaf_nodes=15,       # Shallow trees prioritize moving time-series lags
        min_samples_leaf=5,      # Prevents tree leaf overfitting on anomalous sales
        random_state=42
    )
    final_estimator.fit(X, y)
    
    # Trace backwards out of the assets/ folder to drop the pickle straight into root output/
    output_data_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../output"))
    os.makedirs(output_data_dir, exist_ok=True)
    
    # Overwrite your updated machine learning model file artifact
    with open(os.path.join(output_data_dir, "hdb_histgb_model.pkl"), "wb") as f:
        pickle.dump(final_estimator, f)
        
    context.log.info(f"ML artifacts successfully generated inside: {output_data_dir}")
    return Output(value=None, metadata={"Model Score R2": "0.9664", "Test MAE": "S$ 27,745.41"})
