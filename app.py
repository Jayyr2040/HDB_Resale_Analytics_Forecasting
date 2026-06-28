import os
import plotly.express as px
import pandas as pd
import streamlit as st
from sqlalchemy import create_engine
from google.oauth2 import service_account
from dotenv import load_dotenv
import json
import pickle
import numpy as np

# =====================================================================================
# 1. APPLICATION SETUP & PAGE CONFIGURATION
# =====================================================================================

st.set_page_config(
    page_title = "HDB Resale Enterprise Analytics",
    page_icon = "🏢",
    layout = 'wide',
    initial_sidebar_state = "expanded"
)

# Custom Executive Header Styled for Presentation Deck & Alignment
st.title("🏢 HDB Resale Analytics Portal")
st.caption("Data Engineering Core Pipeline | Live Production Environment ")
st.markdown("---")

# =====================================================================================
# 2. DATA WAREHOUSE CONNECTION & CACHING LAYER (SQLALCHEMY BIGQUERY Engine)
# =====================================================================================

@st.cache_resource
def get_bigquery_engine():
    """
    Establishes and caches a long-lived database engine link to Google BigQuery.
    Looks for local workspace credentials file, falls back to Streamlit Secrets in Cloud.
    """
    # credentials_path = "/home/geekytan/Documents/ntu_dsai/DSAI_HDB_Project/geekytan-bigquery-c5c23def44f5.json"
    # Prefer an environment variable pointing to your service account JSON.
    # Example: export GOOGLE_APPLICATION_CREDENTIALS=/path/to/key.json
    load_dotenv()
    credentials_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")

    # if os.path.exists(credentials_path):
    #     os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = credentials_path
    # elif "bigquery_credentials" in st.secrets:
    #     # For deployment security on Streamlit Community Cloud
    #     import json
    #     with open("gcp_key.json", "w") as f:
    #         json.dump(dict(st.secrets["bigquery_credentials"]), f)
    #     os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "gcp_key.json"

    # PROJECT_ID = "project-8d552288-1acb-4a23-893"
    # connection_string = f"bigquery://{PROJECT_ID}/hdb_analytics_marts"
    # return create_engine(connection_string)

    # 28/5 #########################################################################
    if credentials_path and os.path.exists(credentials_path):
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = credentials_path
        project_id = os.getenv("GOOGLE_PROJECT_ID")
        connection_string = f"bigquery://{project_id}/hdb_analytics_marts"
        return create_engine(connection_string)
        
    # 2. Cloud Environment Path via Streamlit Secrets (Fixes your Timeout Error)
    elif "bigquery_credentials" in st.secrets:
        # Load your secrets dict
        creds_info = dict(st.secrets["bigquery_credentials"])
        project_id = creds_info.get("project_id")
        
        # Stringify the JSON credentials dictionary to pass into the engine configuration
        creds_json_string = json.dumps(creds_info)
        
        # Pass the raw JSON string configuration directly as a dialect parameter string
        connection_string = f"bigquery://{project_id}/hdb_analytics_marts"
        return create_engine(connection_string, credentials_info=creds_info)
        
    else:
        raise ValueError("No BigQuery credentials found locally or in Streamlit Secrets.")
    ################ 28/5 #####################################################


@st.cache_data(ttl=3600)  # Cache invalidates auto-refreshing once every hour
def fetch_analytics_mart_data(limit_years=None):
    """Streams analytics tables directly out of BigQuery, reading keys dynamically from cloud or local."""
    import json
    from google.oauth2 import service_account
    
    project_id = "project-8d552288-1acb-4a23-893"
    
    # 1. CLOUD LAYER CHECK: Detect if hosted live inside Streamlit Community Cloud
    if "gcp_service_account" in st.secrets:
        # Reconstruct dict from Streamlit secrets encrypted keys vault
        gcp_dict = dict(st.secrets["gcp_service_account"])
        credentials = service_account.Credentials.from_service_account_info(gcp_dict)
        engine = create_engine(f"bigquery://{project_id}/hdb_analytics_marts", credentials_info=gcp_dict)
        
    # 2. LOCAL DEV FALLBACK: Read from local storage workspace folder path if on your computer
    else:
        local_path = "/home/taijl/DSAI/S2_BigData/DSAI_HDB_Project/project-8d552288-1acb-4a23-893-07fe8627d11f.json"
        if os.path.exists(local_path):
            os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = local_path
            engine = create_engine(f"bigquery://{project_id}/hdb_analytics_marts")
        else:
            st.error("Failed to fetch infrastructure layers from Cloud Warehouse: No BigQuery credentials found locally or in Streamlit Secrets.")
            return pd.DataFrame()
            

    # Base query extracting from fact and dimension table joined together
    base_query = """
     SELECT 
        f.resale_price,
        f.floor_area_sqm,
        f.price_per_sqm,
        f.price_per_sqft,
        f.remaining_lease_years,
        f.min_distance_to_regional_hub_km,
        f.dist_to_closest_shopping_mall_km,
        f.dist_to_closest_primary_school_km,
        f.dist_to_closest_mrt_km,
        f.dist_to_closest_lrt_km,
        f.transaction_month,
        f.storey_range,
        f.flat_type,
        p.town,
        p.flat_model,
        p.street_name, 
        p.block
    FROM fact_sales f
    LEFT JOIN dim_properties p ON f.property_id = p.property_id
    """

    if limit_years:
        # Server-side calculation partition pruning (current year anchor 2026)
        cutoff_year = 2026 - limit_years
        # Fix: Appends '-01' to your 'YYYY-MM' strings to make them clear YYYY-MM-DD objects
        query = f"""
        {base_query} 
        WHERE EXTRACT(YEAR FROM PARSE_DATE('%Y-%m-%d', CONCAT(f.transaction_month, '-01'))) >= {cutoff_year};
        """
    else:
        query = base_query

    with engine.connect() as conn:
        df = pd.read_sql(query,con=conn)

    if "transaction_month" in df.columns:
        df['transaction_month'] = pd.to_datetime(df['transaction_month'])
    return df

# ==============================================================================
# 2.5 ML MODEL CACHED LOADING LAYER
# ==============================================================================
@st.cache_resource
def load_ml_forecasting_assets():
    """Loads binary pkl assets compiled by your automated Dagster asset pipeline."""
    # Matches your project's local /output structure perfectly
    model_path = os.path.join("output", "hdb_histgb_model.pkl")
    grid_path = os.path.join("output", "hdb_lookup_grid.pkl")
    
    try:
        with open(model_path, "rb") as f:
            model = pickle.load(f)
        with open(grid_path, "rb") as f:
            grid = pickle.load(f)
        return model, grid
    except FileNotFoundError:
        return None, None

hgb_model, hdb_lookup_grid = load_ml_forecasting_assets()

# ==============================================================================
# 3. SIDEBAR NAVIGATION & DATA LAKE WINDOW CONTROL 
# ==============================================================================
st.sidebar.header("🎛️ Operational Parameters")

# Time-Window query optimization control interface
time_window_choice = st.sidebar.selectbox(
    label = "Warehouse Query Scope Dpeth:",
    options=[
        "1 Year (Most Recent)",
        "2 years",
        "3 Years",
        "4 Years",
        "All Historical Data"
    ],
    index=0, # Keep default to 1 year
    help = "Trims query paritition on BigQuery side to conserve data scan"
)

# Parse dropdown choice into function variables
if "All" in time_window_choice:
    years_to_pull = None
    st.sidebar.caption("🌐 Scanning complete master repository matrix framework.")
else:
    years_to_pull = int(time_window_choice.split()[0])
    cutoff_target = 2026 - years_to_pull
    st.sidebar.caption(f"📅 Scraped partitions bound: Jan 1, {cutoff_target} ➔ Present")

# Execute background extraction safely
with st.spinner("Streaming analytical matrices from BigQuery..."):
    try:
        master_df = fetch_analytics_mart_data(limit_years=years_to_pull)
    except Exception as e:
        st.error(f"Failed to fetch infrastructure layers from Cloud Warehouse: {e}")
        st.stop()

st.sidebar.markdown("---")
st.sidebar.header("🎯 Dimensional Filters")

# Town multi-select filtering
all_towns = sorted(master_df['town'].dropna().unique())
selected_towns = st.sidebar.multiselect("Target Townships",all_towns, default=all_towns[:3])

# Flat type configuration filter
all_flat_types = sorted(master_df["flat_type"].dropna().unique())
selected_flat_types = st.sidebar.multiselect("Property unit Tiers", all_flat_types,default=all_flat_types)

# Lease Duration Range Slider
min_lease = int(master_df['remaining_lease_years'].min())
max_lease = int(master_df["remaining_lease_years"].max())
selected_lease_range = st.sidebar.slider("Remaining Lease Boundary (Years)", min_lease,max_lease, (min_lease,max_lease))

# Apply combined workspace constraints to active runtime dataframe
filtered_df = master_df[
    (master_df['town'].isin(selected_towns)) & 
    (master_df['flat_type'].isin(selected_flat_types)) &
    (master_df['remaining_lease_years'].between(selected_lease_range[0],selected_lease_range[1])) 
]

# ==============================================================================
# 4. EXECUTIVE EXECUTIVE KPI FLASH-CARDS LAYER (CEO/CMO Focus)
# ==============================================================================
st.subheader("📊 Dynamic Market Capital Performance Metrics")
kpi_col1,kpi_col2,kpi_col3,kpi_col4 = st.columns(4)

if not filtered_df.empty:
    with kpi_col1:
        st.metric(label="Active Dataset Footprint", value=f"{len(filtered_df):,} Rows")
    with kpi_col2:
        st.metric(label="Average Valuation Pricing", value=f"${filtered_df['resale_price'].mean():,.0f}")
    with kpi_col3:
        st.metric(label="Mean Unit Value (per PSF)", value=f"${filtered_df['price_per_sqft'].mean():,.2f}")
    with kpi_col4:
        st.metric(label="Transit Vector Proximity", value=f"{filtered_df['dist_to_closest_mrt_km'].mean():,.2f} km")
else:
    st.warning("Empty matrix match. Adjust dimensional boundary filters on the left sidebar.")

st.markdown("---")

# ==============================================================================
# 5. ENTERPRISE MULTI-TAB ANALYTICS MATRIX INTERFACE
# ==============================================================================
# Update your multi-tab definition line to include a 4th index:
tab1, tab2, tab3, tab4 = st.tabs([
    "📈 Temporal Price Trajectories",
    "🧭 Geospatial Proximity Elasticity",
    "🏢 Vertical Storey Premium Index",
    "🔮 Time-Aware ML Price Forecaster"  # <-- Paste this 4th item here
])

# ---- TAB 1: FINANCIAL TRENDS TIMELINE ----
with tab1:
    st.subheader("Financial Progression Vector Over Time")
    if not filtered_df.empty:
        trend_data = filtered_df.groupby(["transaction_month","flat_type"])["resale_price"].mean().reset_index()
        fig_line = px.line(
            trend_data, x="transaction_month", y="resale_price", color="flat_type",
            markers=True,
            labels={"transaction_month": "Timeline Period", "resale_price": "Mean Resale Price ($)"},
            title="Chronological Mean Asset Valuation Trends Across Selection Matrix"
        )
        st.plotly_chart(fig_line, use_container_width=True)
    else:
        st.caption("No temporal components to map.")

# ---- TAB 2: GEOSPATIAL PROXIMITY ANALYTICS ----
with tab2:
    st.subheader("Proximity Impact on Asset Capitalization")
    if not filtered_df.empty:
        col_left, col_right = st.columns(2)

        with col_left:
            fig_mrt = px.scatter(
                filtered_df, x="dist_to_closest_mrt_km", y="resale_price", color="town",
                size="floor_area_sqm", hover_data=["flat_type", "remaining_lease_years"],
                title="Valuation Variance Driven by Core Mass Rapid Transit (MRT) Proximity Grid"
            )
            st.plotly_chart(fig_mrt, use_container_width=True)

        with col_right:
            fig_hub = px.scatter(
                filtered_df, x="min_distance_to_regional_hub_km", y="resale_price", color="town",
                trendline="ols",
                title="Price Degradation Curve vs. URA Polycentric Masterplan Commercial Hub Centers"
            )
            st.plotly_chart(fig_hub, use_container_width=True)

        st.markdown("---")
        st.subheader("Primary School Proximity Analysis (MOE P1 Registration Zones)")
        st.caption("Relevant for buyers with children under 12, or newly married couples planning to start a family.")

        col_sch_left, col_sch_right = st.columns(2)

        with col_sch_left:
            fig_school_scatter = px.scatter(
                filtered_df,
                x="dist_to_closest_primary_school_km",
                y="resale_price",
                color="town",
                trendline="ols",
                hover_data=["flat_type", "remaining_lease_years"],
                labels={
                    "dist_to_closest_primary_school_km": "Distance to Closest Primary School (km)",
                    "resale_price": "Resale Price ($)"
                },
                title="Resale Price vs. Distance to Closest Primary School"
            )
            st.plotly_chart(fig_school_scatter, use_container_width=True)

        with col_sch_right:
            # Bin by distance into MOE P1 priority zone bands
            def school_distance_band(d):
                if d < 0.5:
                    return "< 0.5 km"
                elif d < 1.0:
                    return "0.5 – 1 km"
                elif d < 2.0:
                    return "1 – 2 km"
                else:
                    return "> 2 km"

            school_df = filtered_df.copy()
            school_df["school_distance_band"] = school_df["dist_to_closest_primary_school_km"].apply(school_distance_band)

            band_order = ["< 0.5 km", "0.5 – 1 km", "1 – 2 km", "> 2 km"]
            school_band_summary = (
                school_df.groupby("school_distance_band")["resale_price"]
                .mean()
                .reindex(band_order)
                .reset_index()
            )
            school_band_summary.columns = ["Distance to Closest Primary School", "Average Resale Price ($)"]

            fig_school_bar = px.bar(
                school_band_summary,
                x="Distance to Closest Primary School",
                y="Average Resale Price ($)",
                text_auto=".2s",
                color="Distance to Closest Primary School",
                color_discrete_sequence=px.colors.sequential.Greens_r[1:5],
                title="Average Resale Price by MOE P1 Priority Distance Band"
            )
            fig_school_bar.update_traces(textposition="outside")
            fig_school_bar.update_layout(showlegend=False)
            st.plotly_chart(fig_school_bar, use_container_width=True)
        
        # Malls proximity analysis
        st.markdown("---")
        st.subheader("Shopping Malls Proximity Analysis")
        
        # Render the Plotly scatter plot directly in the main layout (1 column)
        fig_malls_scatter = px.scatter(
            filtered_df,
            x="dist_to_closest_shopping_mall_km",
            y="resale_price",
            color="town",
            trendline="ols",
            hover_data=["flat_type", "remaining_lease_years"],
            labels={
                "dist_to_closest_shopping_mall_km": "Distance to Closest Shopping Mall (km)",
                "resale_price": "Resale Price ($)"
            },
            title="Resale Price vs. Distance to Closest Shopping Mall"
        )
        
        st.plotly_chart(fig_malls_scatter, use_container_width=True)

        

    else:
        st.caption("No vector parameters to isolate.")

# ---- TAB 3: STOREY HIGH VERTICAL GRAPH ANALYSIS ----
with tab3:
    st.subheader("Vertical Pricing Premium Evaluation Index")
    if not filtered_df.empty:
        sorted_storey = sorted(filtered_df["storey_range"].dropna().unique())
        
        chart_view = st.radio(
            "Visualization Granularity Profile:", 
            ["Aggregated Trend Matrix (Clean Presentation)", "Full Dispersion Boxplot (Detailed Structural Validation)"],
            horizontal=True,
            key="storey_view_filter" # Added explicit key for state stability
        )
        
        if chart_view == "Aggregated Trend Matrix (Clean Presentation)":
            floor_grouped = filtered_df.groupby(["storey_range", "town"])["price_per_sqm"].mean().reset_index()
            
            fig_floor = px.line(
                floor_grouped, 
                x="storey_range", 
                y="price_per_sqm", 
                color="town",
                category_orders={"storey_range": sorted_storey}, 
                markers=True,
                labels={"storey_range": "Structural Storey Tier Range", "price_per_sqm": "Avg Price / SQM ($)"},
                title="Vertical Storey Level Pricing Trajectories across Townships"
            )
            # Fix: Ensure categorical line vectors connect properly across string coordinates
            fig_floor.update_traces(connectgaps=True)
        else:
            fig_floor = px.box(
                filtered_df, 
                x="storey_range", 
                y="price_per_sqm", 
                color="town",
                category_orders={"storey_range": sorted_storey},
                labels={"storey_range": "Structural Storey Tier Range", "price_per_sqm": "Price / SQM ($)"},
                title="Valuation Data Spread Profiles broken down by Storey Height Blocks"
            )
        
        # FIX: Override the st.tabs container width bug by specifying an explicit pixel height
        fig_floor.update_layout(height=500, margin=dict(l=20, r=20, t=40, b=20))
        st.plotly_chart(fig_floor, use_container_width=True)
        
    else:
        st.warning("No data available for the selected filters.")

# ==============================================================================
# ---- TAB 4: PRODUCTION ML ENGINE WITH AUTOMATIC OVERRIDE STATES ----
# ==============================================================================
with tab4:
    st.subheader("🔮 Predictive Fair Valuation Baseline Engine")
    
    if hgb_model is None or hdb_lookup_grid is None:
        st.warning("⚠️ **Predictive Module Offline:** Please materialize your assets in Dagster first.")
    else:
        st.markdown("Select the property address. The engine will auto-populate historical timeline attributes, which you can override manually.")
        st.markdown("---")
        
        ml_col1, ml_col2 = st.columns(2)
        
        with ml_col1:
            st.markdown("##### 📍 Address & Location Profile")
            all_warehouse_towns = sorted(master_df['town'].dropna().unique())
            input_town = st.selectbox("Target Town Location", all_warehouse_towns, key="ml_town")
            
            town_filtered_df = master_df[master_df['town'] == input_town]
            available_streets = sorted(town_filtered_df['street_name'].dropna().unique()) if not town_filtered_df.empty else []
            input_street = st.selectbox("Select Street Name", available_streets, key="ml_street")
            
            street_filtered_df = town_filtered_df[town_filtered_df['street_name'] == input_street] if available_streets else pd.DataFrame()
            available_blocks = sorted(street_filtered_df['block'].dropna().unique(), key=lambda x: str(x)) if not street_filtered_df.empty else []
            input_block = st.selectbox("Select Block Number", available_blocks, key="ml_block")
            
            all_warehouse_types = sorted(master_df['flat_type'].dropna().unique())
            input_flat_type = st.selectbox("Flat Unit Size Type", all_warehouse_types, key="ml_type")
            
            all_warehouse_models = sorted(master_df['flat_model'].dropna().unique())
            input_flat_model = st.selectbox("Architectural Flat Model Variant", all_warehouse_models, key="ml_model")

        # --- BACKGROUND LOOKUP SELECTION INTERCEPTOR ---
        # Look up the actual record *before* rendering the numeric inputs to set the default values
        matched_subset = master_df[
            (master_df['town'] == input_town) & 
            (master_df['street_name'] == input_street) & 
            (master_df['block'] == input_block)
        ]
        if matched_subset.empty:
            matched_subset = master_df[(master_df['town'] == input_town) & (master_df['flat_type'] == input_flat_type)]

        if not matched_subset.empty:
            latest_real_record = matched_subset.sort_values('transaction_month', ascending=False).iloc[0]
            db_lease = float(latest_real_record["remaining_lease_years"]) if "remaining_lease_years" in latest_real_record else 85.0
            db_area = float(latest_real_record["floor_area_sqm"]) if "floor_area_sqm" in latest_real_record else 95.0
            db_floor = int(latest_real_record["storey_range"].split(" ")[0]) if ("storey_range" in latest_real_record and "TO" in str(latest_real_record["storey_range"])) else 8
        else:
            db_lease, db_area, db_floor = 85.0, 95.0, 8

        with ml_col2:
            st.markdown("##### 📏 Physical Layout Parameters (Auto-Populated)")
            # Passing the database value as the baseline default value parameter
            input_area = st.number_input("Floor Area Space (Square Meters)", min_value=30.0, max_value=170.0, value=db_area, step=1.0)
            input_floor = st.number_input("Storey Level Height (Exact Floor Number)", min_value=1, max_value=50, value=db_floor, step=1)
            
            # The remaining lease slider automatically snaps to the database value but can be adjusted manually
            input_lease = st.slider(
            "Remaining Lease Life Duration (Years)", 
            min_value=40.0, 
            max_value=99.0, 
            value=db_lease, 
            step=0.5,
            key=f"lease_slider_{input_town}_{input_street}_{input_block}" # <-- Paste this unique key here
            )
      
        st.markdown("---")
        # Scroll down to your button handler inside Tab 4 of your app.py file:
        if st.button("🚀 Calculate Live Future Valuation Frame", use_container_width=True):
            with st.spinner("Executing model prediction loop..."):
                
                # Pull latest database entry for the proximity constants matching the address
                if not matched_subset.empty:
                    latest_real_record = matched_subset.sort_values('transaction_month', ascending=False).iloc[0]
                    real_mrt    = float(latest_real_record["dist_to_closest_mrt_km"])
                    real_lrt    = float(latest_real_record["dist_to_closest_lrt_km"])
                    real_mall   = float(latest_real_record["dist_to_closest_shopping_mall_km"])
                    real_school = float(latest_real_record["dist_to_closest_primary_school_km"])
                    real_hub    = float(latest_real_record["min_distance_to_regional_hub_km"])
                else:
                    real_mrt, real_lrt, real_mall, real_school, real_hub = 0.55, 1.20, 0.65, 0.35, 4.50

                # DYNAMIC LINK: Calculate lease commence year based directly on the slider's active choice
                real_lease_commence = int(2026 - (99 - float(input_lease)))
                floor_squared = float(input_floor ** 2)
                mop_window_flush = 1 if (2026 - real_lease_commence >= 5 and 2026 - real_lease_commence <= 8) else 0
                
                # Pull baseline
                baseline_lag_1 = hdb_lookup_grid.get((input_town, input_flat_type), 540000.0)
                if isinstance(baseline_lag_1, dict):
                    baseline_lag_1 = baseline_lag_1.get("lag_1_month", 540000.0)

                # Assemble your 19-feature payload matrix
                prediction_payload = pd.DataFrame([{
                    "floor_area_sqm": float(input_area),
                    "remaining_lease_numeric": float(input_lease),
                    "floor_level": float(input_floor),
                    "floor_squared": floor_squared,
                    "lease_commence_date": float(real_lease_commence),
                    "lag_1_month": float(baseline_lag_1),
                    "lag_12_month": float(baseline_lag_1 * 0.92),
                    "rpi_lag_1": 204.1,                  
                    "mop_window_flush": int(mop_window_flush),
                    "transaction_year": 2026,            
                    "transaction_month_num": 6,          
                    "dist_to_closest_mrt_km": real_mrt,
                    "dist_to_closest_lrt_km": real_lrt,
                    "dist_to_closest_shopping_mall_km": real_mall,
                    "dist_to_closest_primary_school_km": real_school,
                    "min_distance_to_regional_hub_km": real_hub,
                    "town": input_town,
                    "flat_type": input_flat_type,
                    "flat_model": input_flat_model
                }])
                
                for col in prediction_payload.columns:
                    if col not in ["town", "flat_type", "flat_model"]:
                        prediction_payload[col] = prediction_payload[col].astype('float32')
                for categorical_column in ["town", "flat_type", "flat_model"]:
                    prediction_payload[categorical_column] = prediction_payload[categorical_column].astype('category')
                    
                # E. Run prediction and target reconstruction
                predicted_stationary_delta = hgb_model.predict(prediction_payload)
                final_calculated_valuation = float(predicted_stationary_delta[0] + baseline_lag_1)
                
                # F. Display output cards
                st.markdown("### 🔑 Valuation Matrix Analysis Result")
                res_col1, res_col2 = st.columns(2)
                with res_col1:
                    st.success(f"#### Estimated Fair Market Value\n## **S${final_calculated_valuation:,.2f}**")
                with res_col2:
                    st.info(f"#### Local Sub-Market Moving Baseline\n## **S${baseline_lag_1:,.0f}**")
                    st.caption(f"Based on real database records for **Blk {input_block} {input_street}**.")



# ==============================================================================
# 6. RAW ARCHIVE DATASET DATA FRAME CHECKBOX INSPECTION
# ==============================================================================
st.markdown("---")
if st.checkbox("🔍 Enable Audit-Trail View for Active Segment Rows Engine"):
    st.markdown("### Raw Warehouse Analytics Mart Extraction Feed")
    st.dataframe(filtered_df)
