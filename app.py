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
st.title("🏢 HDB Resale Analytics & Forecasting Portal")
st.caption("Data Engineering Core Pipeline | Live Prediction Environment ")
st.markdown("---")

# --- PRODUCTION ARTIFACT PATH RESOLUTION ENGINE ---
# Dynamically establishes root paths for model deployment on Streamlit Cloud
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
model_path = os.path.join(BASE_DIR, "output", "hdb_histgb_model.pkl")

hgb_model = None

# Securely read serialized model binaries out of repository root space
if os.path.exists(model_path):
    with open(model_path, "rb") as f:
        hgb_model = pickle.load(f)
    
else:
    # Local fallback for notebook directory alignment backtracks
    local_m_path = os.path.abspath(os.path.join(BASE_DIR, "../../../output/hdb_histgb_model.pkl"))
    if os.path.exists(local_m_path):
        with open(local_m_path, "rb") as f:
            hgb_model = pickle.load(f)

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
        
    # 2. STREAMLIT CLOUD ENVIRONMENT INTEGRATION LAYER
    # Uses safe attribute extraction to pull credentials without TOML section formatting failures
    elif hasattr(st, "secrets") and "bigquery_credentials" in st.secrets:
        # Extract secret parameters natively using standard mapping routines
        creds_info = dict(st.secrets["bigquery_credentials"])

        # If Streamlit proxies it as a custom Secrets object, downcast it cleanly to a dictionary
        if hasattr(creds_info, "to_dict"):
            creds_info = creds_info.to_dict()
        else:
            creds_info = dict(creds_info)

        project_id = creds_info.get("project_id")
        
        # Stringify the JSON credentials dictionary to pass into the engine configuration
        # creds_json_string = json.dumps(creds_info)
        
        # Pass the raw JSON string configuration directly as a dialect parameter string
        connection_string = f"bigquery://{project_id}/hdb_analytics_marts"
        return create_engine(connection_string, credentials_info=creds_info)
        
    else:
        raise ValueError("No BigQuery credentials found locally or in Streamlit Secrets.")
    ################ 28/5 #####################################################


@st.cache_data(ttl=3600)  # Cache invalidates auto-refreshing once every hour
def fetch_analytics_mart_data(limit_years=None):
    """Streams analytics tables directly out of BigQuery, reading keys dynamically from cloud or local."""
 
    try:
        # INDUSTRIAL REFACTOR: Leverage your working engine connection directly
        engine = get_bigquery_engine()
    except Exception as e:
        st.error(f"Data Pipeline Engine Connection Error: {str(e)}")
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
    # """Loads binary pkl assets compiled by your automated Dagster asset pipeline."""
    # # Matches your project's local /output structure perfectly
    # m_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "output", "hdb_histgb_model.pkl")
    # try:
    #     with open(m_path, "rb") as f:
    #         model = pickle.load(f)
    # except FileNotFoundError:
    #     return None

    """Bulletproof relative locator for both local machines and Streamlit Cloud."""
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    
    # Use relative lookups that work anywhere from the git root
    possible_paths = [
        os.path.join(BASE_DIR, "output", "hdb_histgb_model.pkl"),
        os.path.join(BASE_DIR, "resale_flat_dagster", "output", "hdb_histgb_model.pkl")
    ]
    
    for path in possible_paths:
        if os.path.exists(path):
            with open(path, "rb") as f:
                return pickle.load(f)
    return None

hgb_model = load_ml_forecasting_assets()

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

# =====================================================================================
# HIGH-PERFORMANCE GLOBAL DIMENSIONAL DICTIONARY ENGINE (PLACED SAFELY OUTSIDE LOOPS)
# =====================================================================================
# Open app.py -> Go to Section 3 around Line 220:

@st.cache_data(ttl=86400)
def compile_global_building_dictionary():
    """Streams dimensions from BigQuery and dynamically derives time-series lags in RAM."""
    try:
        global_raw_df = fetch_analytics_mart_data(limit_years=None)
        if not global_raw_df.empty:
            dim_df = global_raw_df[['town', 'street_name', 'block', 'flat_type', 'flat_model', 'floor_area_sqm', 'remaining_lease_years', 'transaction_month']].copy()
            
            # Clean text arrays to eliminate trailing whitespaces or casing bugs
            for col in ['town', 'street_name', 'block', 'flat_type', 'flat_model']:
                dim_df[col] = dim_df[col].astype(str).str.strip().str.upper()
            
            dim_df['floor_area_sqm'] = pd.to_numeric(dim_df['floor_area_sqm'], errors='coerce').astype(float).fillna(0.0)
            
            # 🌟 INCORPORATED PERFECTLY: Your sorting line happens right here at line 239!
            dim_df = dim_df.sort_values('transaction_month', ascending=False)
            
            # Parse dates and numerical leasehold variables natively
            dim_df["month_date"] = pd.to_datetime(dim_df["transaction_month"] + "-01", errors='coerce')
            dim_df["remaining_lease_numeric"] = pd.to_numeric(dim_df["remaining_lease_years"], errors='coerce').fillna(85.0)
            
              # ⚡ ACCELERATED TIME-SERIES MAP GENERATOR
            # grid_prices = global_raw_df.groupby(['transaction_month', 'town', 'flat_type'])['resale_price'].mean().reset_index()
            # grid_prices['month_date'] = pd.to_datetime(grid_prices['transaction_month'] + "-01", errors='coerce')
            
            grid_prices = global_raw_df.copy()
            if 'transaction_month' in grid_prices.columns:
                grid_prices['month_date'] = pd.to_datetime(grid_prices['transaction_month'])
            grid_prices = grid_prices.groupby(['month_date', 'town', 'flat_type'])['resale_price'].mean().reset_index()

            grid_prices['lag_1'] = grid_prices.groupby(['town', 'flat_type'])['resale_price'].shift(1)
            grid_prices['lag_12'] = grid_prices.groupby(['town', 'flat_type'])['resale_price'].shift(12)
            grid_prices['roll_mean_3'] = grid_prices.groupby(['town', 'flat_type'])['lag_1'].transform(lambda x: x.rolling(3, min_periods=1).mean())
            
            global_median_val = global_raw_df["resale_price"].median()
            for lag_col in ['lag_1', 'lag_12', 'roll_mean_3']:
                grid_prices[lag_col] = grid_prices[lag_col].fillna(global_median_val)
            
            # Left-merge your clean time-series metrics back into the sorted dim_df
            dim_df = pd.merge(dim_df, grid_prices[['month_date', 'town', 'flat_type', 'lag_1', 'lag_12', 'roll_mean_3']], on=['month_date', 'town', 'flat_type'], how='left')

             # Group by flat_type composite indexes and aggregate features natively
            grouped = dim_df.groupby(['town', 'street_name', 'block', 'flat_type']).agg({
                "flat_model": "first",
                "floor_area_sqm": "first",
                "remaining_lease_numeric": "first",
                "transaction_month": "first",
                "lag_1": "first",
                "lag_12": "first",
                "roll_mean_3": "first",
                "storey_range": "first"
            })
            return grouped.to_dict(orient="index")
    except Exception:
        pass
    return {}


# --- EXECUTE BACKGROUND DATA EXTRACTION SAFELY ---
with st.spinner("Streaming active transactional matrices from BigQuery..."):
    try:
        master_df = fetch_analytics_mart_data(limit_years=years_to_pull)
        
        # FIXED: Assign the compiled dictionary to st.session_state using our clean, un-nested keys function
        if "global_building_specs" not in st.session_state:
            st.session_state["global_building_specs"] = compile_global_building_dictionary()
            
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
# ---- TAB 4: PRODUCTION PRICE FORECASTER (COMPLETE PROD ENGINE FIXED) ----
# ==============================================================================
with tab4:
    st.subheader("🔮 Predictive Fair Valuation Baseline Engine")
    
    if hgb_model is None:
        st.warning("⚠️ **Predictive Module Offline:** Please materialize your assets in Dagster first.")
    else:
        st.markdown("Select property parameters below. The system extracts pipeline contexts dynamically.")
        st.markdown("---")
        
        # ==============================================================================
        # PHASE 1: RENDER THE LIVE PRIMARY ADDRESS DROPDOWNS (WITH CUSTOM OVERRIDES)
        # ==============================================================================
        ml_col1, ml_col2 = st.columns(2)
        
        with ml_col1:
            st.markdown("##### 📍 Address & Location Profile")
            all_warehouse_towns = sorted(master_df['town'].dropna().unique()) if not master_df.empty else ["ANG MO KIO"]
            input_town = st.selectbox("Target Town Location", all_warehouse_towns, key="ml_town")
            
            # --- STREET OVERRIDE LAYOUT ---
            all_town_streets = sorted(master_df[master_df['town'] == input_town]['street_name'].dropna().unique()) if not master_df.empty else []
            street_options = ["➕ Type Custom Street Name..."] + all_town_streets
            selected_street_ui = st.selectbox("Street Name", street_options, index=1 if len(street_options) > 1 else 0, key="ml_street_select")
            
            # Conditionally render raw text input box if user selects custom options
            if selected_street_ui == "➕ Type Custom Street Name...":
                selected_street_ui = st.text_input("Type Custom Street Name", value="", placeholder="e.g. BISHAN ST 24", key="ml_street_custom").strip().upper()

            # --- BLOCK OVERRIDE LAYOUT ---
            # Filter blocks using the resolved street name variable
            filtered_blocks_df = master_df[(master_df['town'] == input_town) & (master_df['street_name'] == selected_street_ui)] if not master_df.empty else pd.DataFrame()
            all_street_blocks = sorted(filtered_blocks_df['block'].dropna().unique(), key=lambda x: str(x)) if not filtered_blocks_df.empty else []
            block_options = ["➕ Type Custom Block Number..."] + all_street_blocks
            selected_block_ui = st.selectbox("Block Number", block_options, index=1 if len(block_options) > 1 else 0, key="ml_block_select")
            
            if selected_block_ui == "➕ Type Custom Block Number...":
                selected_block_ui = st.text_input("Type Custom Block Number", value="", placeholder="e.g. 273A", key="ml_block_custom").strip().upper()
            
            # --- FLAT SIZE CONFIGURATION ---
            all_warehouse_types = sorted(master_df['flat_type'].dropna().unique()) if not master_df.empty else ["3 ROOM"]
            input_flat_type = st.selectbox("Flat Unit Size Type", all_warehouse_types, key="ml_type")

        # ==============================================================================
        # PHASE 2: REAL-TIME TRANSACTION LOOKUP FOR HISTORICAL DEFAULTS (LIVE AGGREGATION)
        # ==============================================================================
        match_town = str(input_town).strip().upper()
        match_street = str(selected_street_ui).strip().upper()
        match_block = str(selected_block_ui).strip().upper()
        
        match_type_hyphen = str(input_flat_type).strip().upper().replace(" ", "-")
        match_type_space = str(input_flat_type).strip().upper().replace("-", " ")

        # Cache Invalidation Engine: Clears out stale session calculations on selection changes
        current_property_fingerprint = f"{match_town}_{match_street}_{match_block}_{match_type_hyphen}"
        if "last_selected_property" not in st.session_state:
            st.session_state["last_selected_property"] = current_property_fingerprint
            
        if st.session_state["last_selected_property"] != current_property_fingerprint:
            st.session_state["calc_fair_value"] = None
            st.session_state["calc_baseline"] = None
            st.session_state["last_selected_property"] = current_property_fingerprint

        # Query master_df using strict segment isolation parameters
        match_rows = master_df[
            (master_df['town'].astype(str).str.strip().str.upper() == match_town) & 
            (master_df['street_name'].astype(str).str.strip().str.upper() == match_street) & 
            (master_df['block'].astype(str).str.strip().str.upper() == match_block) &
            ((master_df['flat_type'].astype(str).str.strip().str.upper() == match_type_hyphen) |
             (master_df['flat_type'].astype(str).str.strip().str.upper() == match_type_space))
        ]
        
        global_median_price = float(master_df['resale_price'].median()) if not master_df.empty else 540000.0
        
        if not match_rows.empty:
            # Forces robust index sequence reset to ensure .iloc extracts records flawlessly
            sorted_match = match_rows.sort_values(by='transaction_month', ascending=False).reset_index(drop=True)
            latest_rec = sorted_match.iloc[0]
            
            calc_area = float(latest_rec.get("floor_area_sqm", 120.0))
            calc_floor_str = str(latest_rec.get("storey_range", "22 TO 24"))
            calc_lease = pd.to_numeric(latest_rec.get("remaining_lease_years"), errors='coerce')
            calc_model = str(latest_rec.get("flat_model", "DBSS")).strip().upper()
            
            # 🌟 SELF-CONTAINED TIME SERIES COMPULATION ENGINE (BYPASSES EMPTY BIGQUERY COLUMNS)
            # Pull town-level trends dynamically from master_df records in memory
            historical_town_series = master_df[
                (master_df['town'].astype(str).str.strip().str.upper() == match_town) &
                ((master_df['flat_type'].astype(str).str.strip().str.upper() == match_type_hyphen) |
                 (master_df['flat_type'].astype(str).str.strip().str.upper() == match_type_space))
            ].sort_values('transaction_month', ascending=True)

            if len(historical_town_series) > 0:
                # Group by months natively to derive true time lags
                monthly_averages = historical_town_series.groupby('transaction_month')['resale_price'].mean().reset_index()
                
                # Assign lag vectors shifting elements backwards chronologically
                monthly_averages['lag_1'] = monthly_averages['resale_price'].shift(1)
                monthly_averages['lag_12'] = monthly_averages['resale_price'].shift(12)
                monthly_averages['roll_mean_3'] = monthly_averages['lag_1'].rolling(window=3, min_periods=1).mean()
                
                # Map back to the most recent transaction month context
                target_month = str(latest_rec.get("transaction_month"))
                target_node = monthly_averages[monthly_averages['transaction_month'] == target_month]
                
                if not target_node.empty:
                    baseline_lag_1 = float(target_node['lag_1'].fillna(global_median_price).iloc[0])
                    baseline_lag_12 = float(target_node['lag_12'].fillna(global_median_price).iloc[0])
                    baseline_roll_3 = float(target_node['roll_mean_3'].fillna(global_median_price).iloc[0])
                else:
                    baseline_lag_1, baseline_lag_12, baseline_roll_3 = global_median_price, global_median_price, global_median_price
            else:
                baseline_lag_1, baseline_lag_12, baseline_roll_3 = global_median_price, global_median_price, global_median_price
        else:
            # Fallback level 2: Widen to town-level segment averages to keep pricing realistic
            calc_floor_str, calc_lease, calc_model = "04 TO 06", 85.0, "MODEL A"
            calc_area = 65.0 if "3" in match_type_hyphen else (95.0 if "4" in match_type_hyphen else 120.0)
            
            regional_df = master_df[
                (master_df['town'].astype(str).str.strip().str.upper() == match_town) & 
                ((master_df['flat_type'].astype(str).str.strip().str.upper() == match_type_hyphen) |
                 (master_df['flat_type'].astype(str).str.strip().str.upper() == match_type_space))
            ]
            if not regional_df.empty:
                baseline_lag_1 = float(regional_df['resale_price'].median())
                baseline_lag_12 = baseline_lag_1
                baseline_roll_3 = baseline_lag_1
            else:
                baseline_lag_1, baseline_lag_12, baseline_roll_3 = global_median_price, global_median_price, global_median_price

        try:
            # Safely parse numeric floor lists (e.g., "22 TO 24" -> lower bound 22)
            parsed_floor_numeric = float(calc_floor_str.strip().split()[0])
        except Exception:
            parsed_floor_numeric = 22.0




        # Calculate infrastructure metrics natively on every single screen layout refresh
        spatial_df = master_df[
            (master_df['town'].astype(str).str.strip().str.upper() == match_town) & 
            (master_df['street_name'].astype(str).str.strip().str.upper() == match_street) & 
            (master_df['block'].astype(str).str.strip().str.upper() == match_block)
        ]
        if spatial_df.empty:
            spatial_df = master_df[
                (master_df['town'].astype(str).str.strip().str.upper() == match_town) & 
                ((master_df['flat_type'].astype(str).str.strip().str.upper() == match_type_hyphen) | 
                 (master_df['flat_type'].astype(str).str.strip().str.upper() == match_type_space))
            ]
        
        if not spatial_df.empty:
            base_rec = spatial_df.sort_values('transaction_month', ascending=False).iloc[0]
            mrt = float(base_rec.get("dist_to_closest_mrt_km", 0.55))
            lrt = float(base_rec.get("dist_to_closest_lrt_km", 1.20))
            mall = float(base_rec.get("dist_to_closest_shopping_mall_km", 0.65))
            school = float(base_rec.get("dist_to_closest_primary_school_km", 0.35))
            hub = float(base_rec.get("min_distance_to_regional_hub_km", 4.50))
        else:
            mrt, lrt, mall, school, hub = 0.55, 1.20, 0.65, 0.35, 4.50

        # ==============================================================================
        # PHASE 3: RENDER PHYSICAL CONTROLS WITH COMPOSITE FRESHNESS KEYS
        # ==============================================================================
        with ml_col2:
            st.markdown("##### 📏 Physical Layout Parameters (Auto-Populated)")
            widget_suffix = f"{match_town}_{match_street}_{match_block}_{match_type_hyphen}".replace(" ", "-")
            
            input_area = st.number_input("Floor Area Space (Square Meters)", min_value=15.0, max_value=250.0, value=float(calc_area), key=f"area_{widget_suffix}")
            input_floor = st.number_input("Storey Level Height (Numeric Level)", min_value=1.0, max_value=60.0, value=float(parsed_floor_numeric), key=f"floor_{widget_suffix}")
            input_lease = st.slider("Remaining Lease Duration (Years)", min_value=30.0, max_value=99.0, value=float(np.nan_to_num(calc_lease, nan=85.0)), step=0.5, key=f"lease_{widget_suffix}")

        all_warehouse_models = sorted(list(set([str(m).strip().upper() for m in master_df['flat_model'].dropna().unique() if "ROOM" not in str(m).upper()])))
        if not all_warehouse_models:
            all_warehouse_models = ["NEW GENERATION", "IMPROVED", "MODEL A"]
        try:
            default_model_index = all_warehouse_models.index(calc_model)
        except ValueError:
            default_model_index = 0
        
        with ml_col1:
            input_flat_model = st.selectbox("Architectural Flat Model Variant", all_warehouse_models, index=default_model_index, key=f"model_{widget_suffix}")

        st.markdown("---")
        
        # Initialize session state tracking spaces
        if "calc_fair_value" not in st.session_state:
            st.session_state["calc_fair_value"] = None
        if "calc_baseline" not in st.session_state:
            st.session_state["calc_baseline"] = None

        # ==============================================================================
        # PHASE 4: EXECUTE MODEL PREDICTION ON CLICK (WITH PERSISTENT SESSION STATE)
        # ==============================================================================
        if st.button("🚀 Calculate Live Future Valuation Frame", use_container_width=True):
        # st.info("🔄 Running active pipeline feature transformations and inference calculations...")
            with st.spinner("Extracting parameters and running inference model..."):
                
                real_lease_commence = float(2026.0 - (99.0 - float(input_lease)))
                # 1. DYNAMIC TIME EXTRACTION (ADD THIS RIGHT HERE)
                if not match_rows.empty:
                    # Parse the most recent historical transaction month available for this block
                    latest_tx_date = pd.to_datetime(match_rows['transaction_month'].max())
                    dynamic_year = float(latest_tx_date.year)
                    dynamic_month = float(latest_tx_date.month)
                else:
                    # Fallback parameters if evaluating a completely new address structure
                    dynamic_year = 2026.0
                    dynamic_month = 6.0

                # 2. UNIFIED PAYLOAD ASSEMBLY
                # Build clean inference DataFrame payload matching your exact 17 feature matrices layout
                prediction_payload = pd.DataFrame([{
                    "floor_area_sqm": float(input_area),
                    "remaining_lease_numeric": float(input_lease),
                    "floor_level": float(input_floor),
                    "lease_commence_date": float(real_lease_commence),
                    "transaction_year": dynamic_year,     # FIXED: Uses your dynamic calculation variable
                    "transaction_month": dynamic_month,   # FIXED: Uses your dynamic calculation variable
                    "lag_1": float(baseline_lag_1),
                    "lag_12": float(baseline_lag_12),
                    "roll_mean_3": float(baseline_roll_3),
                    "dist_to_closest_mrt_km": float(mrt),
                    "dist_to_closest_lrt_km": float(lrt),
                    "dist_to_closest_shopping_mall_km": float(mall),
                    "dist_to_closest_primary_school_km": float(school),
                    "min_distance_to_regional_hub_km": float(hub),
                    "town": str(input_town).strip().upper(),
                    "flat_type": str(input_flat_type).strip().upper(), 
                    "flat_model": str(input_flat_model).strip().upper()
                }])
                
                categorical_cols = ["town", "flat_type", "flat_model"]
                for col in prediction_payload.columns:
                    if col in categorical_cols:
                        prediction_payload[col] = prediction_payload[col].astype('category')
                    else:
                        prediction_payload[col] = prediction_payload[col].astype('float32')

                ordered_features = [
                    "floor_area_sqm", "remaining_lease_numeric", "floor_level", "lease_commence_date",
                    "transaction_year", "transaction_month", "lag_1", "lag_12", "roll_mean_3",
                    "dist_to_closest_mrt_km", "dist_to_closest_lrt_km", "dist_to_closest_shopping_mall_km",
                    "dist_to_closest_primary_school_km", "min_distance_to_regional_hub_km",
                    "town", "flat_type", "flat_model"
                ]
                
                # Execute direct price prediction evaluation run cleanly
                predicted_valuation = hgb_model.predict(prediction_payload[ordered_features])
                st.session_state["calc_fair_value"] = float(predicted_valuation[0])
                st.session_state["calc_baseline"] = float(baseline_lag_1)

        # ==============================================================================
        # PHASE 5: RENDER METRIC SCOREBOARD CARDS (READS FROM STATE)
        # ==============================================================================
        # Pull values out of state variables dynamically or use realistic initial markers
        display_valuation = st.session_state["calc_fair_value"] if st.session_state["calc_fair_value"] is not None else float(baseline_lag_1)
        display_baseline = st.session_state["calc_baseline"] if st.session_state["calc_baseline"] is not None else float(baseline_lag_1)

        st.markdown("### 🔑 Valuation Matrix Analysis Result")
        res_col1, res_col2 = st.columns(2)
        with res_col1:
            st.success(f"#### Estimated Fair Market Value\n## **S${display_valuation:,.2f}**")
        with res_col2:
            st.info(f"#### Local Sub-Market Moving Baseline\n## **S${display_baseline:,.0f}**")
            st.caption(f"Based on real database records for **Blk {selected_block_ui} {selected_street_ui}**.")

        st.markdown("---")
        st.markdown("##### 📍 Active Geospatial Proximity Asset Footprint Matrix")
        prox_col1, prox_col2, prox_col3, prox_col4, prox_col5 = st.columns(5)
        with prox_col1: 
            st.metric(label="🚇 Nearest MRT Station", value=f"{mrt:.2f} km")
        with prox_col2: 
            st.metric(label="🚝 Closest LRT Node", value=f"{lrt:.2f} km" if lrt < 10.0 else "N/A")
        with prox_col3: 
            st.metric(label="🛍️ Shopping Mall Hub", value=f"{mall:.2f} km")
        with prox_col4: 
            st.metric(label="🏫 Primary School", value=f"{school:.2f} km")
        with prox_col5: 
            st.metric(label="🏙️ Regional Core Hub", value=f"{hub:.2f} km")

        # ==============================================================================
        # PHASE 6: RAW ARCHIVE DATASET DATA FRAME CHECKBOX INSPECTION
        # ==============================================================================
        st.markdown("---")
        if st.checkbox("🔍 Enable Audit-Trail View for Active Segment Rows Engine"):
            st.markdown("### Raw Warehouse Analytics Mart Extraction Feed")
            audit_df = master_df[
                (master_df['town'] == input_town) & 
                (master_df['street_name'] == selected_street_ui) & 
                (master_df['block'] == selected_block_ui)
            ]
            if audit_df.empty:
                audit_df = master_df[master_df['town'] == input_town]
                
            st.dataframe(audit_df.sort_values(by="transaction_month", ascending=False), use_container_width=True)


