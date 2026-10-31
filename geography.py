import os

import duckdb
import pandas as pd


POSTCODE_DIR = r"C:\Users\rorym\Procurement\postcodes"
ONSPD_FILE = os.path.join(POSTCODE_DIR, "ONSPD_AUG_2026_UK.csv")

COUNTRY_FILE = os.path.join(
    POSTCODE_DIR,
    "CTRY Country names and codes UK as at 05_25.csv"
)

REGION_FILE = os.path.join(
    POSTCODE_DIR,
    "RGN Region names and codes EN as at 05_25.csv"
)

LAD_FILE = os.path.join(
    POSTCODE_DIR,
    "LAD Local Authority District names and codes UK as at 04_25.csv"
)

ITL_FILE = os.path.join(
    POSTCODE_DIR,
    "ITL125_ITL225_ITL325_LAU125_LAD24_UK_LU.csv"
)

def get_connection():
    return duckdb.connect("md:procurement")

def get_buyer_postcodes(conn):
    return conn.execute("""
        SELECT DISTINCT postcode_key
        FROM dim_buyers
        WHERE postcode_key IS NOT NULL
    """).df()

def load_buyer_geography(buyer_postcodes):
    onspd = pd.read_csv(
        ONSPD_FILE,
        usecols=[
            "pcds",
            "ctry26cd",
            "rgn26cd",
            "lad26cd",
            "itl25cd",
            "lat",
            "long"
        ]
    )

    onspd["postcode_key"] = (
        onspd["pcds"]
        .str.strip()
        .str.upper()
        .str.replace(r"[^A-Z0-9]", "", regex=True)
    )

    buyer_postcode_set = set(buyer_postcodes["postcode_key"])

    return onspd[
        onspd["postcode_key"].isin(buyer_postcode_set)
    ].copy()

def load_geography_lookups():
    country = pd.read_csv(COUNTRY_FILE)
    region = pd.read_csv(REGION_FILE)
    lad = pd.read_csv(LAD_FILE)
    itl = pd.read_csv(ITL_FILE)

    return country, region, lad, itl

def enrich_geography(buyer_geography, country, region, lad, itl):
    geography = buyer_geography.merge(
        country[["CTRY25CD", "CTRY25NM"]],
        left_on="ctry26cd",
        right_on="CTRY25CD",
        how="left"
    )

    geography = geography.merge(
        region[["RGN25CD", "RGN25NM"]],
        left_on="rgn26cd",
        right_on="RGN25CD",
        how="left"
    )

    geography = geography.merge(
        lad[["LAD25CD", "LAD25NM"]],
        left_on="lad26cd",
        right_on="LAD25CD",
        how="left"
    )

    itl_lookup = itl[
        [
            "LAU125CD",
            "ITL125CD", "ITL125NM",
            "ITL225CD", "ITL225NM",
            "ITL325CD", "ITL325NM"
        ]
    ].drop_duplicates()

    geography = geography.merge(
        itl_lookup,
        left_on="itl25cd",
        right_on="LAU125CD",
        how="left"
    )
    return geography

def prepare_geography_table(geography):
    geography_table = geography[
        [
            "postcode_key",
            "pcds",
            "ctry26cd",
            "CTRY25NM",
            "rgn26cd",
            "RGN25NM",
            "lad26cd",
            "LAD25NM",
            "ITL125CD",
            "ITL125NM",
            "ITL225CD",
            "ITL225NM",
            "ITL325CD",
            "ITL325NM",
            "lat",
            "long"
        ]
    ].rename(
        columns={
            "pcds": "postcode",
            "ctry26cd": "country_code",
            "CTRY25NM": "country_name",
            "rgn26cd": "region_code",
            "RGN25NM": "region_name",
            "lad26cd": "local_authority_code",
            "LAD25NM": "local_authority_name",
            "ITL125CD": "itl1_code",
            "ITL125NM": "itl1_name",
            "ITL225CD": "itl2_code",
            "ITL225NM": "itl2_name",
            "ITL325CD": "itl3_code",
            "ITL325NM": "itl3_name",
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

if __name__ == "__main__":
    conn = get_connection()

    buyer_postcodes = get_buyer_postcodes(conn)

    print(f"Distinct buyer postcodes: {len(buyer_postcodes)}")
    print(buyer_postcodes.head())

    buyer_geography = load_buyer_geography(buyer_postcodes)

    print(f"Matched ONSPD postcodes: {len(buyer_geography)}")
    print(buyer_geography.head())

    country, region, lad, itl = load_geography_lookups()

    print(f"Country lookup rows: {len(country)}")
    print(f"Region lookup rows: {len(region)}")
    print(f"LAD lookup rows: {len(lad)}")
    print(f"ITL lookup rows: {len(itl)}")

    geography = enrich_geography(
    buyer_geography,
    country,
    region,
    lad,
    itl
    )

    print(
    geography[
        [
            "pcds",
            "CTRY25NM",
            "RGN25NM",
            "LAD25NM",
            "ITL125NM",
            "ITL225NM",
            "ITL325NM"
        ]
    ].head(10)
    )

    geography_table = prepare_geography_table(geography)

    print(geography_table.head())
    print(geography_table.columns.tolist())

    rows_loaded = load_geography(conn, geography_table)

    print(f"Geography rows loaded: {rows_loaded}")

    conn.close()