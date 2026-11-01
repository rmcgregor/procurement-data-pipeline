
import requests
import duckdb
import pandas as pd

ONS_API_URL = "https://services1.arcgis.com/ESMARspQHYMw9BZ9/arcgis/rest/services/ONS_Postcode_Directory_%28August_2026%29_%28Hosted_Table%29/FeatureServer/0/query"

def format_postcode(postcode_key):
    return f"{postcode_key[:-3]} {postcode_key[-3:]}"

def get_connection():
    return duckdb.connect("md:procurement")

def get_buyer_postcodes(conn):
    return conn.execute("""
        SELECT DISTINCT postcode_key
        FROM dim_buyers
        WHERE postcode_key IS NOT NULL
    """).df()

def load_buyer_geography(buyer_postcodes):
    postcode_keys = buyer_postcodes["postcode_key"].tolist()

    formatted_postcodes = [
        format_postcode(postcode)
        for postcode in postcode_keys
    ]

    all_rows = []
    batch_size = 100

    for i in range(0, len(formatted_postcodes), batch_size):
        batch = formatted_postcodes[i:i + batch_size]

        postcode_list = ", ".join(
            f"'{postcode}'"
            for postcode in batch
        )

        params = {
            "where": f"pcds IN ({postcode_list})",
            "outFields": "pcds,ctry26cd,rgn26cd,lad26cd,itl25cd,lat,long",
            "returnGeometry": "false",
            "f": "json"
        }

        response = requests.get(
            ONS_API_URL,
            params=params,
            timeout=30
        )
        response.raise_for_status()

        data = response.json()

        rows = [
            feature["attributes"]
            for feature in data.get("features", [])
        ]

        all_rows.extend(rows)

    geography = pd.DataFrame(all_rows)

    geography["postcode_key"] = (
        geography["pcds"]
        .str.strip()
        .str.upper()
        .str.replace(r"[^A-Z0-9]", "", regex=True)
    )

    return geography

def load_geography_lookups(conn):
    country = conn.execute("""
        SELECT *
        FROM ref_country
    """).df()

    region = conn.execute("""
        SELECT *
        FROM ref_region
    """).df()

    lad = conn.execute("""
        SELECT *
        FROM ref_local_authority
    """).df()

    itl = conn.execute("""
        SELECT *
        FROM ref_itl
    """).df()

    return country, region, lad, itl

def enrich_geography(buyer_geography, country, region, lad, itl):
    geography = buyer_geography.merge(
        country,
        left_on="ctry26cd",
        right_on="country_code",
        how="left"
    )

    geography = geography.merge(
        region,
        left_on="rgn26cd",
        right_on="region_code",
        how="left"
    )

    geography = geography.merge(
        lad,
        left_on="lad26cd",
        right_on="local_authority_code",
        how="left"
    )

    geography = geography.merge(
        itl,
        left_on="itl25cd",
        right_on="local_area_code",
        how="left"
    )

    return geography

def prepare_geography_table(geography):
    geography_table = geography[
        [
            "postcode_key",
            "pcds",
            "country_code",
            "country_name",
            "region_code",
            "region_name",
            "local_authority_code",
            "local_authority_name",
            "itl1_code",
            "itl1_name",
            "itl2_code",
            "itl2_name",
            "itl3_code",
            "itl3_name",
            "lat",
            "long"
        ]
    ].rename(
        columns={
            "pcds": "postcode",
            "lat": "latitude",
            "long": "longitude"
        }
    )

    geography_table = geography_table.drop_duplicates(
        subset=["postcode_key"]
    )

    return geography_table

def load_geography(conn, geography_table):
    conn.register("geography_df", geography_table)

    conn.execute("""
        CREATE OR REPLACE TABLE ref_geography AS
        SELECT *
        FROM geography_df
    """)

    row_count = conn.execute("""
        SELECT COUNT(*)
        FROM ref_geography
    """).fetchone()[0]

    return row_count

def run_geography():
    conn = get_connection()
    
    buyer_postcodes = get_buyer_postcodes(conn)
    
    print(f"Distinct buyer postcodes: {len(buyer_postcodes)}")
        
    
    buyer_geography = load_buyer_geography(buyer_postcodes)
    
    print(f"Matched ONSPD postcodes: {len(buyer_geography)}")
        
    unmatched_postcodes = set(buyer_postcodes["postcode_key"]) - set(
        buyer_geography["postcode_key"]
    )
    
    print(f"Unmatched postcodes: {len(unmatched_postcodes)}")
    
    if unmatched_postcodes:
        print(f"Unmatched values: {sorted(unmatched_postcodes)}")
        
    country, region, lad, itl = load_geography_lookups(conn)
    
    geography = enrich_geography(
    buyer_geography,
    country,
    region,
    lad,
    itl
    )
    
    
    geography_table = prepare_geography_table(geography)
    
    rows_loaded = load_geography(conn, geography_table)
    
    print(f"Geography rows loaded: {rows_loaded}")
    
    conn.close()

if __name__ == "__main__":
    run_geography()