import subprocess
import ingestion
import observability
from prefect import flow, task


def run_dbt():

    print("\n2. Running dbt models...")

    subprocess.run(
        ["dbt", "run", "--profiles-dir", "."],
        cwd="procurement_analytics",
        check=True
    )

def test_dbt():

    print("\n3. Running dbt tests...")

    subprocess.run(
        ["dbt", "test", "--profiles-dir", "."],
        cwd="procurement_analytics",
        check=True
    )

@task(retries=2, retry_delay_seconds=60)
def ingest_data():
    ingestion.run_pipeline()


@task
def build_dbt_models():
    run_dbt()


@task
def run_dbt_tests():
    test_dbt()

@task
def send_pipeline_report():
    report = observability.generate_report()

    observability.send_report(
        report,
        "rory.mcgregor@gmail.com"
    )

def notify_failure(flow, flow_run, state):
    print("\n*** PREFECT FAILURE HOOK CALLED ***")
    error = state.message or "Unknown pipeline error"

    report = observability.generate_failure_report(
        RuntimeError(error)
    )

    try:
        observability.send_report(
            report,
            "rory.mcgregor@gmail.com"
        )
        print("*** FAILURE EMAIL SENT ***")
    except Exception as email_error:
        print(f"*** FAILURE EMAIL ERROR: {email_error} ***")
        print(f"Failed to send failure notification: {email_error}")

@flow(
    name="Procurement Pipeline",
    on_failure=[notify_failure]
)
def run():

    print("Starting procurement pipeline")

    ingest_data()
    build_dbt_models()
    run_dbt_tests()

    send_pipeline_report()

    print("\nProcurement pipeline completed successfully")

if __name__ == "__main__":
    run()