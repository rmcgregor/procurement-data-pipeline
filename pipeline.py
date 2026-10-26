import subprocess
import ingestion
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

@flow(name="Procurement Pipeline")
def run():

    print("Starting procurement pipeline")

    ingest_data()

    build_dbt_models()

    run_dbt_tests()

    print("\nProcurement pipeline completed successfully")

if __name__ == "__main__":
    run()