import duckdb
import os
import resend

resend.api_key = os.environ["RESEND_API_KEY"]


def get_latest_run():
    con = duckdb.connect("md:procurement")

    result = con.execute("""
        SELECT
            run_id,
            status,
            started_at,
            completed_at,
            rows_extracted,
            rows_inserted,
            rows_skipped
        FROM pipeline_runs
        WHERE status = 'SUCCESS'
        ORDER BY completed_at DESC
        LIMIT 1
    """).fetchone()

    con.close()

    return {
    "run_id": result[0],
    "status": result[1],
    "started_at": result[2],
    "completed_at": result[3],
    "rows_extracted": result[4],
    "rows_inserted": result[5],
    "rows_skipped": result[6]
}

def get_recent_runs():
    con = duckdb.connect("md:procurement")

    results = con.execute("""
        SELECT
            CAST(completed_at AS DATE) AS run_date,
            SUM(rows_inserted) AS rows_inserted
        FROM pipeline_runs
        WHERE status = 'SUCCESS'
          AND rows_inserted IS NOT NULL
        GROUP BY CAST(completed_at AS DATE)
        ORDER BY run_date DESC
        LIMIT 8
    """).fetchall()

    con.close()

    return results

def get_data_freshness():
    con = duckdb.connect("md:procurement")

    result = con.execute("""
        SELECT MAX(release_date)
        FROM raw_releases
    """).fetchone()

    con.close()

    return result[0]

def generate_report():
    run = get_latest_run()
    latest_release = get_data_freshness()

    duration = run["completed_at"] - run["started_at"]
    data_age = run["started_at"] - latest_release
    data_age_hours = data_age.total_seconds() / 3600

    if data_age_hours <= 24:
        freshness_status = "HEALTHY"
    elif data_age_hours <= 48:
        freshness_status = "WARNING"
    else:
        freshness_status = "STALE"

    report = f"""
        PROCUREMENT PIPELINE REPORT

        PIPELINE
        Status:          {run["status"]}
        Started:         {run["started_at"].strftime("%d %b %Y %H:%M")}
        Duration:        {format_duration(duration)}

        DATA LOAD
        Rows extracted:  {run["rows_extracted"]}
        Rows inserted:   {run["rows_inserted"]}
        Rows skipped:    {run["rows_skipped"]}

        DATA HEALTH
        Latest release:  {latest_release.strftime("%d %b %Y %H:%M")}
        Data age:        {format_duration(data_age)}
        Freshness:       {freshness_status}
        """

    return report

def generate_failure_report(error):
    report = f"""
    PROCUREMENT PIPELINE ALERT

    Status:          FAILED

    ERROR
    {type(error).__name__}: {error}

    The procurement pipeline did not complete successfully.
    Check the Prefect flow logs for further details.
    """

    return report

def format_duration(duration):
    total_seconds = int(duration.total_seconds())

    hours, remainder = divmod(total_seconds, 3600)
    minutes, seconds = divmod(remainder, 60)

    if hours:
        return f"{hours}h {minutes}m {seconds}s"
    elif minutes:
        return f"{minutes}m {seconds}s"
    else:
        return f"{seconds}s"

def send_report(report, recipient):
    response = resend.Emails.send({
        "from": "Procurement Pipeline <onboarding@resend.dev>",
        "to": [recipient],
        "subject": "Procurement Pipeline Report",
        "text": report
    })

    return response


if __name__ == "__main__":
    report = generate_report()
    print(report)

    response = send_report(
        report,
        "rory.mcgregor@gmail.com"
    )

    print("Email response:", response)