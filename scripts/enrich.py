import os
import json
import time
import io
import pandas as pd
import numpy as np
import requests
from dotenv import load_dotenv
from pprint import pprint
from datetime import datetime
import pyarrow as pa
import pyarrow.parquet as pq
from google.cloud import bigquery #27/5- from parquet to bigquery for orchestration consideration
from google.oauth2 import service_account #27/5- from parquet to bigquery for orchestration consideration

def run_enrichment_pipeline(): #Code for dagster
    """
    Dagster implementation: 
    Executes the ingestion, cleaning, and distance to MRT/hub
    Loads results into 'project-8d552288-1acb-4a23-893.hdb_raw_staging.raw_enriched_transactions'.
    """
    SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))  #Code for dagster
    coords_cache_path = os.path.join(SCRIPT_DIR, "coords_cache.json") #Code for dagster
    mrt_cache_path = os.path.join(SCRIPT_DIR, "mrt_lrt_cache.json") #Code for dagster
    schools_cache_path = os.path.join(SCRIPT_DIR, "primary_schools_cache.json") #Code for dagster
    shopping_malls_cache_path = os.path.join(SCRIPT_DIR, "shopping_malls_cache.json") #Code for dagster
    output_path = os.path.join(SCRIPT_DIR, "enriched_hdb_resale.parquet") #Code for dagster
    
    print("🚀 Starting the HDB Resale Data Enrichment Pipeline...") #Code for dagster

    load_dotenv()

    print("""
    # ===============================================================================================================================
    # STEP 1: Connect to Data.gov.sg Collection Endpoint & Extract Dataset IDs
    # ===============================================================================================================================
    """)

    # 1 Query the HDB resale price API and read the structure
    print("Connecting to Data.gov.sg V2 Collection API...")
    collection_id = 189          
    collection_url = f"https://api-production.data.gov.sg/v2/public/api/collections/{collection_id}/metadata?withDatasetMetadata=true"
            
    response = requests.get(collection_url)
    # print(response.json())

    # 2 store the Json dictionary payload
    meta_res = response.json()
    pprint(meta_res) # see the structure -> collectionMetadata under data, chidldatasets

    # Extract the data set, note meed to set withDatasetMetadata in the above url
    dataset_list = meta_res['data']['datasetMetadata']

    # 3. Find latest data set, name, ID for download step
    print("\n--- Scanning and Parsing Timeframes ---")
    latest_date = None
    latest_dataset = None

    for ds in dataset_list:

        # 1. Clean the text string by cutting off the timezone suffix (e.g., "2024-04-08T00:00:00+08:00")
        clean_date_str = ds['coverageEnd'].split("+")[0]

        # 2. Parse the string directly into an official Python Datetime Object
        parsed_date = datetime.strptime(clean_date_str, "%Y-%m-%dT%H:%M:%S")

        print(f"📦 ID: {ds['datasetId']} ➔ Coverage Ends: {parsed_date.strftime('%B %d, %Y')}")

        # 3. Chronologically track and isolate the most recent date object
        if latest_date is None or parsed_date > latest_date:
            latest_date = parsed_date
            latest_dataset = ds
        
    latest_id = latest_dataset["datasetId"]
    print("=====================================================================")
    print(f"🎯 Data set name: {latest_dataset['name']}")
    print(f"   Dataset ID: {latest_id}")
    print(f"   Coverage End Date:          {latest_date.strftime('%Y-%m-%d')}")
    print("=====================================================================")   

    print("""
    # ===============================================================================================================================
    # STEP 2: Execute Data.gov.sg Official Download Handshake (Initiate & Poll)
    # ===============================================================================================================================
    """)

    print(f"\nInitiating download for the latest data segment...")
    initiate_url = f"https://api-open.data.gov.sg/v1/public/api/datasets/{latest_id}/initiate-download"
    init_res = requests.get(initiate_url).json()
    poll_url = f"https://api-open.data.gov.sg/v1/public/api/datasets/{latest_id}/poll-download"
    poll_res = requests.get(poll_url).json()
    final_download_url = poll_res["data"]["url"]
    print(f"✅ Download link resolved! Streaming rows from cloud storage...")

    #Pull only the latest time-slice directly into a single dataframe
    df = pd.read_csv(final_download_url)
    print(f"🚀 Loaded {len(df):,} latest active transaction records into memory.")
    print(df.head(5))

    print("""
    # ===============================================================================================================================
    # STEP 3: Request OneMap Temporary 3-Day Token (PLACED HERE)
    # ===============================================================================================================================
    """)

    # print("\n🔑 Requesting temporary 3-day access token from OneMap API... ")
    # login_url = "https://www.onemap.gov.sg/api/auth/post/getToken"
    # payload = {
    #               "email": os.getenv('ONEMAP_EMAIL'),
    #               "password": os.getenv('ONEMAP_PASSWORD')
    #             }

    # token_res = requests.post(login_url, json=payload).json()
    # access_token_1 = token_res.get("access_token")
    def get_current_onemap_token():
        """
        Automated credential authentication wrapper for the OneMap gateway engine.
        """
        print("🔑 Authenticating developer credentials with OneMap Security Gateway...")
        login_url = "https://www.onemap.gov.sg/api/auth/post/getToken"

        # Securely retrieve constants from your root `.env` system environment file
        email = os.getenv("ONEMAP_EMAIL")
        password = os.getenv("ONEMAP_PASSWORD")

        if not email or not password:
            raise ValueError(
                "❌ Missing Credentials! Ensure ONEMAP_EMAIL and ONEMAP_PASSWORD "
                "are defined inside your workspace root `.env` configuration file."
            )
        
        payload = {
            "email" : email,
            "password" : password
        }

        try:
            response_obj = requests.post(login_url, json=payload)
            if response_obj.status_code != 200:
                print(f"❌ Handshake Denied: Status {response_obj.status_code}")
                return None
            
            response = response_obj.json()
            token = response.get("access_token")
            print("✅ Handshake successful. Valid 72-hour API token generated.")
            return token
        
        except Exception as e:
            print(f"❌ Security gateway transmission failed: {e}")
            return None

    access_token_1 = get_current_onemap_token()
    headers = {"Authorization": access_token_1} 
    # print("   ✅ Token successfully generated and loaded into pipeline.")
    print("Your Token:", access_token_1)

    print("""
    # ===============================================================================================================================
    # STEP 4: OneMap Incremental Geocoding Cache Engine
    # ===============================================================================================================================
    """)

    df["full_address"] = df["block"] + " " + df["street_name"]
    print(df.head(5))
    unique_addresses = df["full_address"].unique()

    # Smart Incremental Cache Check for Monthly Updates Optimization
    # if os.path.exists("scripts/coords_cache.json"):   # dagster
    #     with open("scripts/coords_cache.json", "r") as f: #dagster
    if os.path.exists(coords_cache_path):
        with open(coords_cache_path, "r") as f:
            cache = json.load(f)
    else:
        cache = {}

    # headers = {"Authorization": token}
    new_geocodes = 0
    if access_token_1:
        print("   Scanning and geocoding missing properties...")
        for i, addr in enumerate(unique_addresses):
            if addr in cache:
                continue
            
            # 🔴 ADD THIS PROGRESS INDICATOR BLOCK DIRECTLY HERE:
            if i % 100 == 0:
                print(f"   Processed {i}/{len(unique_addresses)} addresses... (New geocodes found: {new_geocodes})")

            search_url = f"https://www.onemap.gov.sg/api/common/elastic/search?searchVal={addr}&returnGeom=Y&getAddrDetails=Y&pageNum=1"
            try:
                    res = requests.get(search_url, headers=headers).json()
                    if res.get("results") and len(res["results"]) > 0:
                        result_node = res["results"][0] # Grab the first search result match
                        
                        cache[addr] = {
                            "lat": float(result_node["LATITUDE"]),
                            "lon": float(result_node["LONGITUDE"]),
                            "x": float(result_node["X"]),   # ◄ ADD THIS: SVY21 X coordinate (metres)
                            "y": float(result_node["Y"])    # ◄ ADD THIS: SVY21 Y coordinate (metres)
                        }
                        new_geocodes += 1
            except:
                    # keeps your pipeline running smootjhlly if an address fails
                    continue
                
            if new_geocodes % 50 == 0 and new_geocodes > 0:
                    time.sleep(0.2)

        # with open('scripts/coords_cache.json', "w") as f: # dagster
        with open(coords_cache_path, "w") as f:
            json.dump(cache, f, indent=4)
        print(f"   Cache update complete. Added {new_geocodes} new address mappings.")

    # Map coordinates from cache directly back into the dataframe rows
    df["lat"] = df["full_address"].map(lambda x: cache.get(x, {}).get("lat", np.nan))
    df["lon"] = df["full_address"].map(lambda x: cache.get(x, {}).get("lon", np.nan))
    df["x"] = df["full_address"].map(lambda x: cache.get(x, {}).get("x", np.nan)) # ◄ NEW
    df["y"] = df["full_address"].map(lambda x: cache.get(x, {}).get("y", np.nan)) # ◄ NEW

    # Drop rows that failed to geocode to prevent math errors
    df = df.dropna(subset=["lat", "lon"]).copy()
    df = df.dropna(subset=["x", "y"]).copy()
    print(df.head(5))

    # output_path = "scripts/enriched_hdb_resale.parquet" # dagster

    print("""
    # ===============================================================================================================================
    # STEP 5: Polycentric Matrix Math Engine 
    # ===============================================================================================================================
    """)
    # UPDATED GOVT POLICY ECONOMIC NODES (URA Master Plan Hierarchy)

    # lat and lon hub version
    # hubs = {
    #     "Jurong_Lake_District": (1.3329, 103.7436),
    #     "Tampines_Regional_Centre": (1.3530, 103.9452),
    #     "Woodlands_Regional_Centre": (1.4368, 103.7865),
    #     "Paya_Lebar_Central": (1.3182, 103.8931),
    #     "One_North_Hub": (1.3002, 103.7915),
    #     "Punggol_Digital_District": (1.4115, 103.9015),
    #     "Bishan_Sub_Regional_Centre": (1.3508, 103.8491),
    #     "Seletar_Aerospace_Park": (1.4168, 103.8668),
    #     "Changi_Business_Park": (1.3338, 103.9671),
    #     "International_Business_Park": (1.3315, 103.7512)
    # }

    # --- REVISED HUBS DICTIONARY (SVY21 METRES) ---
    hubs = {
        "Jurong_Lake_District": (18033.4, 35359.6),
        "Tampines_Regional_Centre": (41243.5, 36980.2),
        "Woodlands_Regional_Centre": (22802.1, 44710.8),
        "Paya_Lebar_Central": (34820.6, 34105.9),
        "One_North_Hub": (23348.9, 31711.2),
        "Punggol_Digital_District": (35720.4, 41920.5),
        "Bishan_Sub_Regional_Centre": (29718.3, 37280.4),
        "Seletar_Aerospace_Park": (31740.1, 42510.9),
        "Changi_Business_Park": (43720.8, 34810.1),
        "International_Business_Park": (18810.2, 35120.7)
    }

    # --- AUTHORITATIVE NON-"SHOPPING" RETAIL NODES (SVY21 METRES) ---
    # --- AUTHORITATIVE NON-"SHOPPING" RETAIL NODES (CORRECTED SVY21 METRES) ---
    FIXED_MALLS = [
        {"mall_name": "Amk Hub", "mall_x": 29184.22, "mall_y": 39105.82}, 
        {"mall_name": "Ion Orchard", "mall_x": 27807.51, "mall_y": 32395.71},
        {"mall_name": "Ngee Ann City", "mall_x": 27931.33, "mall_y": 32247.16},
        {"mall_name": "Vivocity", "mall_x": 26458.74, "mall_y": 29013.91},
        {"mall_name": "Plaza Singapura", "mall_x": 29088.22, "mall_y": 32881.54},
        {"mall_name": "Bugis Junction", "mall_x": 30200.70, "mall_y": 32857.73},
        {"mall_name": "Nex", "mall_x": 32306.21, "mall_y": 36743.79},
        {"mall_name": "Jem", "mall_x": 17972.14, "mall_y": 35431.11},
        {"mall_name": "Westgate", "mall_x": 17871.91, "mall_y": 35572.82},
        {"mall_name": "Tampines Mall", "mall_x": 41280.55, "mall_y": 36802.40}
    ]


    ###############################################
    # Fetch Primary School from OneMap API or cache
    ###############################################
    def fetch_primary_schools_svy21(token):
    # Define a tracking path inside your working repository
        # cache_file = "scripts/primary_schools_cache.json" # dagster
        cache_file = schools_cache_path
        
        # ─── STEP A: CHECK LOCAL STORAGE CACHE WITH 30-DAY TTL ───
        # 30 days in seconds = 30 * 24 * 60 * 60 = 2,592,000 seconds
        EXPIRATION_PERIOD_SECONDS = 2592000

        if os.path.exists(cache_file):
            # Calculate how many seconds ago the file was modified
            file_age_seconds = time.time() - os.path.getmtime(cache_file)

            if file_age_seconds < EXPIRATION_PERIOD_SECONDS:
                print("\n💾 Loading Primary School data from local JSON storage cache...")
                try:
                    with open(cache_file, "r") as f:
                        cached_data = json.load(f)
                    df_sch = pd.DataFrame(cached_data)
                    print(f"✔ Cache hit. Loaded {len(df_sch)} official primary schools instantly.")
                    return df_sch
                except Exception as e:
                    print(f"⚠ Cache corrupted, falling back to network request: {e}")
            
            else:
                days_old = round(file_age_seconds / (24 * 3600), 1)
                print(f"\n⏳ School cache file exists but is outdated ({days_old} days old). Triggering auto-refresh...")

        # ─── STEP B: FALLBACK TO LIVE NETWORK CALLS ───
        print("\n🌐 Cache miss! Downloading Primary School dataset from official OneMap API...")

        schools = []
        current_page = 1
        total_pages = 1
        headers = {"Authorization": token} 
        
        while current_page <= total_pages:
            # Construct the URL with the active page number variable    
            url = f"https://www.onemap.gov.sg/api/common/elastic/search?searchVal=PRIMARY%20SCHOOL&returnGeom=Y&getAddrDetails=Y&pageNum={current_page}"   
        
            try:
                response = requests.get(url, headers=headers)
            
                # Guard check: Ensure the server returned a valid 200 OK code
                if response.status_code != 200:
                    print(f"❌ Server Error on Page {current_page}: Received status code {response.status_code}")
                    break
                
                res = response.json()
                # pprint(res)
        
                # Update total pages dynamically from the API's first metadata response
                if current_page == 1:
                    total_pages = int(res.get("totalNumPages", 1))
                    print(f"Total pages to retrieve: {total_pages}")

                # Extract items from results dictionary array
                items = res.get("results", [])
                for item in items:
                    name = item.get("SEARCHVAL", "").upper()

                    # 2. Establish strict string exclusions for student care, enrichment, and preschools
                    is_student_care = "STUDENT CARE" in name or "ENRICHMENT" in name or "PRESCHOOL" in name

                    # Verify that it is an official Primary School asset
                    if "PRIMARY SCHOOL" in name and not is_student_care:
                        schools.append({
                            "school_name": name.title(),
                            "sch_x": float(item["X"]),
                            "sch_y": float(item["Y"])
                        })

                print(f"Processed Page {current_page}/{total_pages}...")
                current_page += 1
                time.sleep(0.2)  # Short pause to satisfy OneMap rate-limiting rules

            except Exception as e:
                print(f"❌ Connection or JSON parsing failed on page {current_page}: {e}")
                break

        df_sch = pd.DataFrame(schools)
        print(f"✅ Filter complete. Isolated {len(df_sch)} official primary schools.")

        # ─── STEP C: SAVE DOWNLOADED OBJECT TO CACHE ───
        if not df_sch.empty:
            try:
                os.makedirs(os.path.dirname(cache_file), exist_ok=True)
                # Convert dataframe records to a clean dictionary list object for tracking
                with open(cache_file, "w") as f:
                    json.dump(df_sch.to_dict(orient="records"), f, indent=4)
                print(f"💾 Successfully saved schools collection cache to: {cache_file}")
            except Exception as e:
                print(f"⚠ Failed to save local backup file: {e}")
                
        return df_sch



    # def haversine_np(lon1, lat1, lon2, lat2):
    #     lon1, lat1, lon2, lat2 = map(np.radians, [lon1, lat1, lon2, lat2])
    #     dlon, dlat = lon2 - lon1, lat2 - lat1
    #     a = np.sin(dlat/2.0)**2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon/2.0)**2
    #     return 6371.0 * 2.0 * np.arcsin(np.sqrt(a))

    # # Compute distance to all 10 hubs in parallel using NumPy matrix broadcasting
    # for hub_name, coords in hubs.items():
    #     df[f"dist_{hub_name}"] = haversine_np(df["lon"], df["lat"], coords[1], coords[0])

    # # Capture the closest commercial node distance and identify which hub it is
    # hub_cols = [f"dist_{name}" for name in hubs.keys()]
    # df["min_distance_to_regional_hub_km"] = df[hub_cols].min(axis=1)
    # df["closest_regional_hub_name"] = df[hub_cols].idxmin(axis=1).str.replace("dist_", "")

    # df.to_parquet(output_path, engine="pyarrow", index=False)
    # print("Polycentric distance matrix calculations completed.")
    # print(df.tail(5))

    #####################
    #haversine_vectorized method using lat and lon
    #######################

    # def haversine_vectorized(lon1, lat1, lon2, lat2):
    #     """
    #     Computes distance array using Haversine formula for maximum execution speed.
    #     """
    #     # Convert degrees to radians
    #     lon1, lat1, lon2, lat2 = map(np.radians, [lon1, lat1, lon2, lat2])
        
    #     dlon = lon2 - lon1
    #     dlat = lat2 - lat1
        
    #     a = np.sin(dlat/2.0)**2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon/2.0)**2
    #     c = 2.0 * np.arcsin(np.sqrt(a))
        
    #     km = 6367.0 * c  # Earth radius in kilometers
    #     return km

    ###############################################
    # Fetch MRT/LRT from OneMap API or cache
    ###############################################
    def fetch_MRT_LRT_svy21(token):
    # Define a tracking path inside your working repository
        # cache_file = "scripts/mrt_lrt_cache.json" # dagster
        cache_file = mrt_cache_path
        
        # ─── STEP A: CHECK LOCAL STORAGE CACHE WITH 30-DAY TTL ───
        # 30 days in seconds = 30 * 24 * 60 * 60 = 2,592,000 seconds
        EXPIRATION_PERIOD_SECONDS = 2592000

        if os.path.exists(cache_file):
            # Calculate how many seconds ago the file was modifled
            file_age_seconds = time.time() - os.path.getmtime(cache_file)

            if file_age_seconds < EXPIRATION_PERIOD_SECONDS:            
                print("\n💾 Loading MRT/LRT data from local JSON storage cache...")
                try:
                    with open(cache_file, "r") as f:
                        cached_data = json.load(f)
                    df_stations = pd.DataFrame(cached_data)
                    print(f"✔ Cache hit. Loaded {len(df_stations)} MRT/LRT instantly.")
                    return df_stations
                except Exception as e:
                    print(f"⚠ Cache corrupted, falling back to network request: {e}")

            else:
                days_old = round(file_age_seconds / (24*3600),1)
                print(f"\n⏳ Cache file exists but is outdated ({days_old} days old). Triggering auto-refresh...")

        # ─── STEP B: FALLBACK TO LIVE NETWORK CALLS ───
        print("\n🌐 Cache miss! Downloading MRT/LRT dataset from official OneMap API...")

        search_queries = ["MRT STATION", "LRT STATION"]
        stations_dict = {}
        headers = {"Authorization": token} 

        for query in search_queries:
            encoded_query = query.replace(" ", "%20")
            current_page = 1
            total_pages = 1
        
            print(f"🛰 Scanning endpoint layer for: {query}")

            while current_page <= total_pages:
                # Construct the URL with the active page number variable    
                url = f"https://www.onemap.gov.sg/api/common/elastic/search?searchVal={encoded_query}&returnGeom=Y&getAddrDetails=Y&pageNum={current_page}"   
            
                try:
                    response = requests.get(url, headers=headers)
                
                    # Guard check: Ensure the server returned a valid 200 OK code
                    if response.status_code != 200:
                        print(f"❌ Server Error on Page {current_page}: Received status code {response.status_code}")
                        break
                    
                    res = response.json()
                    # pprint(res)
            
                    # Update total pages dynamically from the API's first metadata response
                    if current_page == 1:
                        total_pages = int(res.get("totalNumPages", 1))
                        print(f"Total pages to retrieve: {total_pages}")

                    # Extract items from results dictionary array
                    items = res.get("results", [])
                    for item in items:
                        name = item.get("SEARCHVAL", "").upper()

                        # 2. Establish strict string exclusions for student care, enrichment, and preschools
                        # is_student_care = "STUDENT CARE" in name or "ENRICHMENT" in name or "PRESCHOOL" in name

                        # 🌟 EXCLUSION FLAGS: Identify exits, bus terminals, and depots
                        is_station = "MRT STATION" in name or "LRT STATION" in name
                        is_noise = "EXIT" in name or "BUS INTERCHANGE" in name or "DEPOT" in name
                        # Verify that it is an official Primary School asset
                        if is_station and not is_noise:
                            # Title case the name cleanly (e.g., "Ang Mo Kio Mrt Station")
                            clean_name = name.strip().title()

                            stations_dict[clean_name] = {
                                "station_name": clean_name,
                                "station_x": float(item["X"]),
                                "station_y": float(item["Y"])
                            }

                    print(f"Processed Page {current_page}/{total_pages}...")
                    current_page += 1
                    time.sleep(0.2)  # Short pause to satisfy OneMap rate-limiting rules

                except Exception as e:
                    print(f"❌ Connection or JSON parsing failed on page {current_page}: {e}")
                    break

        df_stations = pd.DataFrame(list(stations_dict.values()))
        print(f"✅ Filter complete. Isolated {len(df_stations)} MRT/LRT stations.")

        # ─── STEP C: SAVE DOWNLOADED OBJECT TO CACHE ───
        if not df_stations.empty:
            try:
                os.makedirs(os.path.dirname(cache_file), exist_ok=True)
                # Convert dataframe records to a clean dictionary list object for tracking
                with open(cache_file, "w") as f:
                    json.dump(df_stations.to_dict(orient="records"), f, indent=4)
                print(f"💾 Successfully saved schools collection cache to: {cache_file}")
            except Exception as e:
                print(f"⚠ Failed to save local backup file: {e}")
                
        return df_stations


    def fetch_shopping_malls_svy21(token):
        """
        Automated Themes API ingestion engine with 30-Day TTL caching layer 
        built exclusively to map Singapore's shopping mall network infrastructure.
        """
        # ─── STEP A: CHECK LOCAL STORAGE CACHE WITH 30-DAY TTL ───
        # 30 days in seconds = 30 * 24 * 60 * 60 = 2,592,000 seconds
        # cache_file = "scripts/shopping_malls_cache.json" # dagster
        cache_file = shopping_malls_cache_path
        EXPIRATION_PERIOD_SECONDS = 2592000 # 30 Days

        if os.path.exists(cache_file):
            file_age_seconds = time.time() - os.path.getmtime(cache_file)
            
            if file_age_seconds < EXPIRATION_PERIOD_SECONDS:            
                print("\n💾 Loading Shopping mall data from local JSON storage cache...")
                try:
                    with open(cache_file, "r") as f:
                        cached_data = json.load(f)
                    df_mall = pd.DataFrame(cached_data)
                    print(f"✔ Cache hit. Loaded {len(df_mall)} shopping mall instantly.")
                    return df_mall
                except Exception as e:
                    print(f"⚠ Cache corrupted, falling back to network request: {e}")

            else:
                days_old = round(file_age_seconds / (24*3600),1)
                print(f"\n⏳ Cache file exists but is outdated ({days_old} days old). Triggering auto-refresh...")

        # ─── STEP B: FALLBACK TO LIVE NETWORK CALLS ───
        print("\n🌐 Cache miss! Requesting shopping_mall records from OneMap Themes API...")
        shopping_malls = []
        current_page = 1
        total_pages = 1
        headers = {"Authorization": token} 
        
        while current_page <= total_pages:
            # Construct the URL with the active page number variable    
            url = f"https://www.onemap.gov.sg/api/common/elastic/search?searchVal=SHOPPING&returnGeom=Y&getAddrDetails=Y&pageNum={current_page}"   
        
            try:
                response = requests.get(url, headers=headers)
            
                # Guard check: Ensure the server returned a valid 200 OK code
                if response.status_code != 200:
                    print(f"❌ Server Error on Page {current_page}: Received status code {response.status_code}")
                    break
                
                res = response.json()
                # pprint(res)
        
                # Update total pages dynamically from the API's first metadata response
                if current_page == 1:
                    total_pages = int(res.get("totalNumPages", 1))
                    print(f"Total pages to retrieve: {total_pages}")

                # Extract items from results dictionary array
                items = res.get("results", [])
                for item in items:
                    name = item.get("SEARCHVAL", "").upper()

                    # 2. Establish strict string exclusions for student care, enrichment, and preschools
                    # is_student_care = "STUDENT CARE" in name or "ENRICHMENT" in name or "PRESCHOOL" in name

                    # Verify that it is an official Primary School asset
                    # if "PRIMARY SCHOOL" in name and not is_student_care:
                    if "SHOPPING" in name:
                        shopping_malls.append({
                            "mall_name": name.title(),
                            "mall_x": float(item["X"]),
                            "mall_y": float(item["Y"])
                        })

                print(f"Processed Page {current_page}/{total_pages}...")
                current_page += 1
                time.sleep(0.2)  # Short pause to satisfy OneMap rate-limiting rules

            except Exception as e:
                print(f"❌ Connection or JSON parsing failed on page {current_page}: {e}")
                break

        # seen_mall_names = set()
        # final_aligned_malls = []

        # # 1. First ingest your hardcoded custom coordinate additions
        # for fixed_mall in FIXED_MALLS:
        #     norm_name = fixed_mall["mall_name"].strip().upper()
        #     if norm_name not in seen_mall_names:
        #         seen_mall_names.add(norm_name)
        #         final_aligned_malls.append({
        #             "mall_name": fixed_mall["mall_name"].strip().title(),
        #             "mall_x": float(fixed_mall["mall_x"]),
        #             "mall_y": float(fixed_mall["mall_y"])
        #         })

        # # 2. Append the API results only if they are not already in your fixed list
        # for network_mall in shopping_malls:
        #     norm_name = network_mall["mall_name"].strip().upper()
        #     if norm_name not in seen_mall_names:
        #         seen_mall_names.add(norm_name)
        #         final_aligned_malls.append({
        #             "mall_name": network_mall["mall_name"].strip().title(),
        #             "mall_x": float(network_mall["mall_x"]),
        #             "mall_y": float(network_mall["mall_y"])
        #         })    
        
        
        # [Insert this directly inside fetch_shopping_malls_svy21, after Step B's while loop completes]
        df_mall = pd.DataFrame(shopping_malls)
            
        # --- FIXED INJECTION METHOD ---
        print("\nInjecting fixed non-'shopping' destination nodes into baseline framework...")
        df_fixed = pd.DataFrame(FIXED_MALLS)
            
        # Deduplicate to prevent overlapping duplicate values if your keyword script also catches them
        if not df_mall.empty:
            df_mall = pd.concat([df_mall, df_fixed], ignore_index=True)
            df_mall = df_mall.drop_duplicates(subset=["mall_name"]).reset_index(drop=True)
        else:
            df_mall = df_fixed

        print(f"✅ Filter complete. Isolated {len(df_mall)} shopping mall.")

        # ─── STEP C: SAVE DOWNLOADED OBJECT TO CACHE ───
        if not df_mall.empty:
            try:
                os.makedirs(os.path.dirname(cache_file), exist_ok=True)
                # Convert dataframe records to a clean dictionary list object for tracking
                with open(cache_file, "w") as f:
                    json.dump(df_mall.to_dict(orient="records"), f, indent=4)
                print(f"💾 Successfully saved shopping mall collection cache to: {cache_file}")
            except Exception as e:
                print(f"⚠ Failed to save local backup file: {e}")
                
        return df_mall


    # =====================================================================
    # PHASE 1: HUB DISTANCE TRACKING MATRIX
    # =====================================================================
    def pythahorean_vectorized(x1, y1, x2, y2):
        """
        Computes straight-line distance in kilometres using SVY21 flat-plane grid
        coordinates. Leverages Numpy beoadcasting for fast execution across large data sets.
        """

        dx = x2 - x1
        dy = y2 - y1

        # Hypotenuse math (gives metres), divided by 1000 to return km
        return np.sqrt(dx**2 + dy**2) / 1000.0


    print("\n📐 Computing distances to URA Master Plan Economic Hubs...")

    # # Calculate vectorized distances for every hub in the dictionary
    # for hub_name, (hub_lat, hub_lon) in hubs.items():
    #     col_name = f"dist_to_{hub_name.lower()}"
    #     df[col_name] = haversine_vectorized(df["lon"], df["lat"], hub_lon, hub_lat)

    # Compute straight-line distances using the flat cartesian coorindates
    for hub_name, (hub_x, hub_y) in hubs.items():
        col_name = f"dist_to_{hub_name.lower()}"
        df[col_name] = pythahorean_vectorized(df["x"], df["y"], hub_x, hub_y)

    # Calculate proximity to the absolute closest economic hub
    dist_cols = [f"dist_to_{hub_name.lower()}" for hub_name in hubs.keys()]
    df["min_distance_to_regional_hub_km"] = df[dist_cols].min(axis=1)
    df["closest_regional_hub_name"] = df[dist_cols].idxmin(axis=1).str.replace("dist_", "")

    print("   ✅ Commercial Hub straight-line matrix calculations complete!")
    print(df[["full_address", "min_distance_to_regional_hub_km", "closest_regional_hub_name"] + dist_cols[:]].head(5))
    print(df[["full_address", "min_distance_to_regional_hub_km", "closest_regional_hub_name"] + dist_cols[:]].tail(5))


    # =====================================================================
    # PHASE 1: PRIMARY SCHOOL DISTANCE TRACKING MATRIX
    # =====================================================================
    print("\n📐 Executing straight-line matrix calculations for Primary Schools...")

    df_schools = fetch_primary_schools_svy21(access_token_1)

    if not df_schools.empty:
        # Convert your 231k HDB rows into arrays for vector math
        # 1. Extract inputs into clean, flat NumPy arrays
        hdb_x = df["x"].to_numpy() # Shape: (231770)
        hdb_y = df["y"].to_numpy() 

        # Convert your primary school coordinates into arrays
        sch_x = df_schools["sch_x"].to_numpy()     # Shape: (NumPrimarySchools,)
        sch_y = df_schools["sch_y"].to_numpy()     # Shape: (NumPrimarySchools,)
        sch_names = df_schools["school_name"].to_numpy()

        # 2. Instantiate blank pre-allocated storage structures for speed
        total_rows = len(df)
        schools_1km = np.zeros(total_rows, dtype=int)
        schools_2km = np.zeros(total_rows, dtype=int)
        closest_names = []
        closest_distances = np.zeros(total_rows, dtype=float)

        # 3. Process in chunks of 20,000 rows to completely eliminate OOM crashes
        chunk_size = 20000
        print(f"Processing calculations in blocks of {chunk_size} HDB rows...")

        for i in range(0, total_rows,chunk_size):
            end_idx = min(i + chunk_size, total_rows)

            # Reshape coordinates for broadcasting: Shape (Chunk_Rows, 1)
            chunk_x = hdb_x[i:end_idx][:, np.newaxis]
            chunk_y = hdb_y[i:end_idx][:, np.newaxis]

            # Broadcast subtraction: (Chunk_rows, 1) - (1, 197) -> Shape (Chunk_Rows, 197)
            dx_sch = chunk_x - sch_x
            dy_sch = chunk_y - sch_y
            dist_matrix_sch = np.sqrt(dx_sch**2 + dy_sch**2)

            schools_1km[i:end_idx] = np.sum(dist_matrix_sch <= 1000.0, axis = 1)
            schools_2km[i:end_idx] = np.sum(dist_matrix_sch <= 2000.0, axis = 1)

            # Pull min index map positions and exact distance values
            min_idx = np.argmin(dist_matrix_sch, axis=1)
            closest_names.extend(sch_names[min_idx])
            closest_distances[i:end_idx] = np.min(dist_matrix_sch, axis=1) / 1000.0

    # 4. Save results back into your primary dataframe
        df["primary_schools_within_1km"] = schools_1km
        df["primary_schools_within_2km"] = schools_2km
        df["closest_primary_school_name"] = closest_names
        df["dist_to_closest_primary_school_km"] = closest_distances

        print("Primary School proximity calculations successfully completed! ✅")
        print(df[["full_address", "primary_schools_within_1km", "primary_schools_within_2km", "closest_primary_school_name"]].head())
        print(df[["full_address", "primary_schools_within_1km", "primary_schools_within_2km", "closest_primary_school_name"]].tail())
    else:
        print("⚠ Calculation skipped: Primary School DataFrame is empty.")

        # # Run flat-plane matrix geometry calculations (metres)
        # dx_sch = hdb_x - sch_x
        # dy_sch = hdb_y - sch_y
        # dist_matrix_sch = np.sqrt(dx_sch**2 + dy_sch**2)

        # # Compute official MOE distance counts across the matrix columns
        # df["primary_schools_within_1km"] = np.sum(dist_matrix_sch <= 1000.0, axis=1)
        # df["primary_schools_within_2km"] = np.sum(dist_matrix_sch <= 2000.0, axis=1)

        # # Extract the absolute closest school name and distance
        # closest_idx = np.argmin(dist_matrix_sch, axis=1)
        # df["closest_primary_school_name"] = df_schools["school_name"].iloc[closest_idx].values
        # df["dist_to_closest_primary_school_km"] = np.min(dist_matrix_sch, axis=1) / 1000.0

        # print("Primary School proximity calculations successfully completed! ✅")
        # print(df[["full_address", "primary_schools_within_1km", "primary_schools_within_2km", "closest_primary_school_name"]].head())

    # =====================================================================
    # PHASE 1: MAJOR TRANSPORT NODES DISTANCE TRACKING MATRIX (MRT,LRT)
    # =====================================================================
    print("\n📐 Executing straight-line matrix calculations for MRT/LRT...")

    df_train_stations = fetch_MRT_LRT_svy21(access_token_1)
    print(df_train_stations.head(5))

    if not df_train_stations.empty:
        # Separate between MRT and LRT
        is_LRT_mask = df_train_stations['station_name'].str.contains("Lrt|LRT",case=False, na=False)

        df_LRT = df_train_stations[is_LRT_mask].copy()
        df_MRT = df_train_stations[~is_LRT_mask].copy()

        print(f"Isolated: {len(df_MRT)} MRT Stations vs. {len(df_LRT)} LRT Stations.")

        # Convert your 231k HDB rows into arrays for vector math
        # 1. Extract inputs into clean, flat NumPy arrays
        hdb_x = df["x"].to_numpy() # Shape: (231770)
        hdb_y = df["y"].to_numpy() 

        # Extract mrt arrays
        mrt_x = df_MRT["station_x"].to_numpy()    
        mrt_y = df_MRT["station_y"].to_numpy()     
        mrt_names = df_MRT["station_name"].to_numpy()

        # Extract lrt arrays
        lrt_x = df_LRT["station_x"].to_numpy()    
        lrt_y = df_LRT["station_y"].to_numpy()     
        lrt_names = df_LRT["station_name"].to_numpy()

        # 2. Instantiate blank pre-allocated storage structures for speed
        total_rows = len(df)
        mrt_within_500m = np.zeros(total_rows, dtype=int)
        mrt_within_1km = np.zeros(total_rows, dtype=int)
        closest_mrt_names = []
        closest_mrt_distances = np.zeros(total_rows, dtype=float)

        lrt_within_500m = np.zeros(total_rows, dtype=int)
        closest_lrt_names = []
        closest_lrt_distances = np.zeros(total_rows, dtype=float)

        # 3. Process in chunks of 20,000 rows to completely eliminate OOM crashes
        chunk_size = 20000
        print(f"Processing calculations in blocks of {chunk_size} HDB rows...")

        for i in range(0, total_rows,chunk_size):
            end_idx = min(i + chunk_size, total_rows)

            # Reshape coordinates for broadcasting: Shape (Chunk_Rows, 1)
            chunk_x = hdb_x[i:end_idx][:, np.newaxis]
            chunk_y = hdb_y[i:end_idx][:, np.newaxis]

            # ---- COMPUTE MRT METRICS ONLY ----
            
            # Broadcast subtraction: (Chunk_rows, 1) - (1, 197) -> Shape (Chunk_Rows, 197)
            if len(mrt_names) > 0: 
                dx_mrt = chunk_x - mrt_x
                dy_mrt = chunk_y - mrt_y
                dist_mrt = np.sqrt(dx_mrt**2 + dy_mrt**2)

                mrt_within_500m[i:end_idx] = np.sum(dist_mrt <= 500.0, axis = 1)
                mrt_within_1km[i:end_idx] = np.sum(dist_mrt <= 1000.0, axis = 1)

                # Pull min index map positions and exact distance values
                min_mrt_idx = np.argmin(dist_mrt, axis=1)
                closest_mrt_names.extend(mrt_names[min_mrt_idx])
                closest_mrt_distances[i:end_idx] = np.min(dist_mrt, axis=1) / 1000.0

            # ---- COMPUTE LRT METRICS ONLY ----
            if len(lrt_names) > 0: 
                dx_lrt = chunk_x - lrt_x
                dy_lrt = chunk_y - lrt_y
                dist_lrt = np.sqrt(dx_lrt**2 + dy_lrt**2)

                lrt_within_500m[i:end_idx] = np.sum(dist_lrt <= 500.0, axis = 1)
            
                # Pull min index map positions and exact distance values
                min_lrt_idx = np.argmin(dist_lrt, axis=1)
                closest_lrt_names.extend(lrt_names[min_lrt_idx])
                closest_lrt_distances[i:end_idx] = np.min(dist_lrt, axis=1) / 1000.0 

    # 4. Save results back into your primary dataframe
        df["mrt_within_500m"] = mrt_within_500m
        df["mrt_within_1km"] = mrt_within_1km
        # Check if we actually captured any MRT names before binding
        if len(closest_mrt_names) == total_rows:
            df["closest_mrt_name"] = closest_mrt_names
            df["dist_to_closest_mrt_km"] = closest_mrt_distances
        else:
            df["closest_mrt_name"] = "None"
            df["dist_to_closest_mrt_km"] = np.nan
            
        df["lrt_within_500m"] = lrt_within_500m
        
        # Check if we actually captured any LRT names before binding
        if len(closest_lrt_names) == total_rows:
            df["closest_lrt_name"] = closest_lrt_names
            df["dist_to_closest_lrt_km"] = closest_lrt_distances
        else:
            df["closest_lrt_name"] = "None"
            df["dist_to_closest_lrt_km"] = np.nan

        print("MRT and LRT separated proximity calculations successfully completed! 🚇✅")
        print(df[["full_address", "mrt_within_500m", "lrt_within_500m", "closest_mrt_name", "closest_lrt_name"]].head())
    else:
        print("⚠ Calculation skipped: Could not load the transit station elements.")


    # =====================================================================
    # PHASE 4: COMMERCIAL LIFESTYLE PROXIMITY MATRIX (SHOPPING MALLS ONLY)
    # =====================================================================
    print("\n📐 Executing straight-line matrix calculations for Shopping Malls...")

    df_malls = fetch_shopping_malls_svy21(access_token_1)

    if not df_malls.empty:
        hdb_x = df["x"].to_numpy()
        hdb_y = df["y"].to_numpy()
        total_rows = len(df)
        chunk_size = 20000

        # Extract target array points
        mall_x = df_malls["mall_x"].to_numpy()
        mall_y = df_malls["mall_y"].to_numpy()
        mall_names = df_malls["mall_name"].to_numpy()

        # Pre-allocate feature columns for operational execution speed
        malls_within_500m = np.zeros(total_rows, dtype=int)
        malls_within_1km = np.zeros(total_rows, dtype=int)
        # closest_mall_names = []
        closest_mall_names = np.empty(total_rows, dtype=object) 
        closest_mall_distances = np.zeros(total_rows, dtype=float)

        print(f"Processing shopping mall allocations in blocks of {chunk_size} HDB rows...")
        for i in range(0, total_rows, chunk_size):
            end_idx = min(i + chunk_size, total_rows)
            chunk_x = hdb_x[i:end_idx][:, np.newaxis]
            chunk_y = hdb_y[i:end_idx][:, np.newaxis]
            # chunk_x = hdb_x[i:end_idx].reshape(-1, 1)
            # chunk_y = hdb_y[i:end_idx].reshape(-1, 1)

            # Cartesian broadcasting math (planar metric system calculations)
            # dx_m = chunk_x - mall_x.flatten()
            # dy_m = chunk_y - mall_y.flatten()
            dx_m = chunk_x - mall_x
            dy_m = chunk_y - mall_y
            dist_m = np.sqrt(dx_m**2 + dy_m**2)
            
            # Accumulate density maps based on SVY21 metric bounds (metres)
            malls_within_500m[i:end_idx] = np.sum(dist_m <= 500.0, axis=1)
            malls_within_1km[i:end_idx] = np.sum(dist_m <= 1000.0, axis=1)
            
            # Map nearest asset fields across rows
            min_m_idx = np.argmin(dist_m, axis=1)
            # closest_mall_names.extend((mall_names[min_m_idx]))
            closest_mall_names[i:end_idx] = mall_names[min_m_idx]
            closest_mall_distances[i:end_idx] = np.min(dist_m, axis=1) / 1000.0

        # Bind fields seamlessly back to central tracking database structure
        df["malls_within_500m"] = malls_within_500m
        df["malls_within_1km"] = malls_within_1km
        df["closest_shopping_mall_name"] = closest_mall_names
        df["dist_to_closest_shopping_mall_km"] = closest_mall_distances
        
        print("Shopping Mall matrix calculations completed successfully! 🛍️✅")
        print(df[["full_address", "malls_within_500m", "malls_within_1km", "closest_shopping_mall_name", "dist_to_closest_shopping_mall_km"]].head())
        print(df[["full_address", "malls_within_500m", "malls_within_1km", "closest_shopping_mall_name", "dist_to_closest_shopping_mall_km"]].tail())
        print(df[df["full_address"].str.contains("BISHAN", case=False, na=False)].head())
    else:
        print("⚠ Calculation skipped: Shopping Mall DataFrame could not be compiled.")




    print("""
    # ===============================================================================================================================
    # STEP 6: Overwrite Final Parquet to include Proximity Matrix Data
    # ===============================================================================================================================
    """)

    # 1. Original local file write for reference
    df.to_parquet(output_path, engine="pyarrow", index=False)

    print("\n=====================================================================")
    print(f"🎉 PIPELINE ENRICHMENT COMPLETE!")
    print(f"   Shape of final DataFrame: {df.shape[0]:,} rows x {df.shape[1]} columns")
    print(f"   Final Matrix Exported:    {output_path}")
    print("=====================================================================")

    # 2. Complete Bigquery setup and execution logic
    credentials_path = "/home/taijl/DSAI/S2_BigData/DSAI_HDB_Project/project-8d552288-1acb-4a23-893-07fe8627d11f.json"
    if not os.path.exists(credentials_path):
        raise FileNotFoundError(f"Missing Cloud credentials file at {credentials_path}")

    credentials = service_account.Credentials.from_service_account_file(credentials_path)

    PROJECT_ID = "project-8d552288-1acb-4a23-893"
    DATASET_ID = "hdb_raw_staging"
    client = bigquery.Client(credentials=credentials, project=PROJECT_ID)

    dataset_ref = bigquery.DatasetReference(PROJECT_ID,DATASET_ID)
    dataset = bigquery.Dataset(dataset_ref)
    dataset.location = "US"
    client.create_dataset(dataset, exists_ok = True)

    job_config = bigquery.LoadJobConfig(write_disposition = "WRITE_TRUNCATE")

    target_table = f"{PROJECT_ID}.{DATASET_ID}.raw_enriched_transactions"
    print(f"🚀 Ingesting duplicate stream to BigQuery: {target_table}...")

    job = client.load_table_from_dataframe(df, target_table, job_config = job_config)
    job.result()

    # 3. Clean UI Print logs reporting BOTH export locations
    print("\n==========================================================================")
    print("🎉 PIPELINE ENRICHMENT & COMPONENT INGESTION COMPLETE!")
    print(f"   Shape of final DataFrame: {df.shape[0]:,} rows x {df.shape[1]} columns")
    print(f"   Local Parquet Backup Saved:        {output_path}")
    print(f"   Cloud Warehouse Layer Ingested:    {target_table}")
    print("============================================================================")


    # Check final NA columns post-enrichment
    print("\nFinal Missing Value Check:")
    print(df.isna().sum())



    # =====================================================================
    # STEP 7: PARQUET INTEGRITY & CONTENT VERIFICATION
    # =====================================================================
    # print("\n🔍 Validating exported Parquet file against final memory DataFrame...")

    # try:
    #     # Read the exported matrix back into an independent test frame
    #     df_verify = pd.read_parquet(output_path, engine="pyarrow")
        
    #     # 1. Row/Column Shape Check
    #     if df.shape == df_verify.shape:
    #         print(f"✅ Shape matches perfectly! Rows: {df_verify.shape[0]}, Columns: {df_verify.shape[1]}")
    #     else:
    #         print(f"❌ Shape mismatch! Memory: {df.shape} vs Exported File: {df_verify.shape}")

    #     # 2. Complete Value & Data Type Matrix Check
    #     # pd.testing.assert_frame_equal will throw an error if a single cell or data type differs
    #     pd.testing.assert_frame_equal(df, df_verify, check_dtype=True)
    #     print("🏆 Content check passed! The exported Parquet file is an exact, pixel-perfect copy of the final DataFrame.")
        
    #     # 3. Print a quick sanity look at the engineered transport columns from the file
    #     transport_cols = ["full_address", "primary_schools_within_1km", "primary_schools_within_2km", "closest_primary_school_name", "mrt_within_500m", "lrt_within_500m", "closest_mrt_name", "malls_within_1km", "closest_shopping_mall_name", "dist_to_closest_shopping_mall_km"]
    #     available_cols = [c for c in transport_cols if c in df_verify.columns]
    #     if available_cols:
    #         print("\n👀 Preview of data read back directly from the Parquet file:")
    #         print(df_verify[available_cols].head())

    # except AssertionError as ae:
    #     print(f"❌ Verification failed! Data content or data types differ: {ae}")
    # except Exception as e:
    #     print(f"❌ Could not execute verification check: {e}")

    # Dagster implementation
    try:
        total_rows = len(df)
        print(f"✅ Successfully staged {total_rows} rows to BigQuery.")
        return total_rows
    except NameError:
        print("✅ Script complete.")
        return 0

# Dagster implementation - Backward compatibility wrapper
if __name__ == "__main__":
    run_enrichment_pipeline()
