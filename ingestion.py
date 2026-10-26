import duckdb
import requests
import json
import uuid
import time
from datetime import datetime, timedelta

## Refactoring the pipeline
def get_watermark():
    with duckdb.connect("md:procurement") as conn:

        result = conn.execute("""
            SELECT watermark_to
            FROM pipeline_runs
            WHERE pipeline_name = 'procurement_ingestion'
              AND status = 'SUCCESS'
            ORDER BY completed_at DESC
            LIMIT 1
        """).fetchone()

        if result is not None:
            watermark = result[0]
        else:
            watermark = conn.execute("""
                SELECT MAX(release_date)
                FROM raw_releases
            """).fetchone()[0]

    return watermark

def extract_releases(watermark_from, watermark_to):
    url = "https://www.find-tender.service.gov.uk/api/1.0/ocdsReleasePackages"

    params = {
    "updatedFrom": watermark_from.strftime("%Y-%m-%dT%H:%M:%S"),
    "updatedTo": watermark_to.strftime("%Y-%m-%dT%H:%M:%S")
    }
    
    incremental_releases = []
    page = 1

    while url:
        print(f"Downloading page {page}...")
        print("URL:", url)
        print("Params:", params)

        max_retries = 5
        retry_count = 0

        while retry_count < max_retries:

            response = requests.get(
                url,
                params=params,
                timeout=30
            )

            if response.status_code == 429:
                retry_after = int(response.headers.get("Retry-After", 10))

                print(
                    f"Rate limited. Waiting {retry_after} seconds "
                    f"before retrying..."
                )

                time.sleep(retry_after)
                retry_count += 1
                continue

            response.raise_for_status()
            break

        else:
            raise Exception(
                f"API request failed after {max_retries} retries"
            )
        incremental_data = response.json()
        incremental_releases.extend(incremental_data["releases"])
        
        
        url = incremental_data.get("links", {}).get("next")
        params = None
        page += 1
        
    return incremental_releases

def load_releases(releases):
    with duckdb.connect("md:procurement") as conn:
        before_count = conn.execute("""
        SELECT COUNT(*)
        FROM raw_releases
        """).fetchone()[0]
        processed = 0
        for release in releases:
            processed += 1
            conn.execute("""
            INSERT INTO raw_releases (
                release_id,
                ocid,
                release_date,
                tags,
                raw_json
            )
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT (release_id) DO NOTHING
            """, [
            release["id"],
            release["ocid"],
            release["date"],
            json.dumps(release["tag"]),
            json.dumps(release)
        ])
        after_count = conn.execute("""
        SELECT COUNT(*)
        FROM raw_releases
        """).fetchone()[0]
    inserted = after_count - before_count
    skipped = processed - inserted

    return processed, inserted, skipped

def fail_pipeline_run(run_id):
    with duckdb.connect("md:procurement") as conn:
        conn.execute("""
            UPDATE pipeline_runs
            SET
            completed_at = CURRENT_TIMESTAMP,
            status = 'FAILED'
            WHERE run_id = ?
            """, [
            run_id
            ])

def start_pipeline_run(watermark):
    with duckdb.connect("md:procurement") as conn:
        run_id = str(uuid.uuid4())
        conn.execute("""
            INSERT INTO pipeline_runs (
                run_id,
                pipeline_name,
                started_at,
                watermark_from,
                status
            )
            VALUES (?, ?, CURRENT_TIMESTAMP, ?, ?)
            """, [
            run_id,
            'procurement_ingestion',
            watermark,
            'RUNNING'
        ])
        return run_id
        

def complete_pipeline_run(
    run_id,
    watermark_to,
    rows_extracted,
    rows_inserted,
    rows_skipped
):
    with duckdb.connect("md:procurement") as conn:
        conn.execute("""
            UPDATE pipeline_runs
            SET
                completed_at = CURRENT_TIMESTAMP,
                watermark_to = ?,
                rows_extracted = ?,
                rows_inserted = ?,
                rows_skipped = ?,
                status = 'SUCCESS'
            WHERE run_id = ?
            """, [
            watermark_to,
            rows_extracted,
            rows_inserted,
            rows_skipped,
            run_id
        ])

def run_pipeline():

    watermark = get_watermark()
    print("Watermark from:", watermark)

    run_id = start_pipeline_run(watermark)
    print("Run ID:", run_id)

    try:
        # Start processing from the last successful watermark
        watermark_from = watermark

        # Fix the upper boundary at the start of this pipeline run
        current_time = datetime.now().astimezone()

        # Totals for the entire pipeline run
        total_extracted = 0
        total_inserted = 0
        total_skipped = 0

        # Process the catch-up period in one-day windows
        while watermark_from < current_time:

            watermark_to = min(
                watermark_from + timedelta(days=1),
                current_time
            )

            print(
                f"\nExtracting window: "
                f"{watermark_from} to {watermark_to}"
            )

            releases = extract_releases(
                watermark_from,
                watermark_to
            )

            processed, inserted, skipped = load_releases(releases)

            total_extracted += len(releases)
            total_inserted += inserted
            total_skipped += skipped

            print("Extracted:", len(releases))
            print("Processed:", processed)
            print("Inserted:", inserted)
            print("Skipped:", skipped)

            # Only move forward after this window has loaded successfully
            watermark_from = watermark_to

        # Record the whole run as successful
        complete_pipeline_run(
            run_id,
            watermark_from,
            total_extracted,
            total_inserted,
            total_skipped
        )

        print("\nPipeline completed successfully")
        print("Total extracted:", total_extracted)
        print("Total inserted:", total_inserted)
        print("Total skipped:", total_skipped)
        print("Watermark to:", watermark_from)

    except Exception:
        fail_pipeline_run(run_id)
        print("Pipeline failed!")
        raise

if __name__ == "__main__":
    run_pipeline()