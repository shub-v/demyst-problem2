import csv
from datetime import datetime, timedelta

import faker

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator

# Create an instance of the Faker class
fake = faker.Faker()


# Function to generate fake data in batches and save it as a CSV
def generate_big_data_csv(num_records, batch_size, filename):
    # Open the file in write mode
    with open(filename, mode="w", newline="", encoding="windows-1252") as file:
        writer = csv.writer(file)
        # Write the header row
        writer.writerow(["first_name", "last_name", "address", "date_of_birth"])

        # Generate data in batches
        for _ in range(0, num_records, batch_size):
            data = []
            # Generate a batch of fake data
            for _ in range(min(batch_size, num_records)):
                data.append(
                    [
                        fake.first_name(),
                        fake.last_name(),
                        fake.address().replace("\n", ", "),
                        fake.date_of_birth().strftime("%Y-%m-%d"),
                    ]
                )
            # Write the batch to the CSV file
            writer.writerows(data)

    return f"{filename} created successfully with {num_records} records."


# Default arguments for the Airflow DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Define the Airflow DAG
with DAG(
    "generate_fake_data",
    default_args=default_args,
    description="Generate fake data CSV in batches",
    schedule_interval=None,
    start_date=datetime(2024, 9, 3),
    catchup=False,
) as dag:
    # Dummy start task
    start_task = DummyOperator(task_id="start")

    # Task to generate the CSV file
    generate_csv_task = PythonOperator(
        task_id="generate_csv",
        python_callable=generate_big_data_csv,
        op_kwargs={
            "num_records": 100000,
            "batch_size": 10000,
            "filename": "/opt/airflow/data/big_fake_data.csv",
        },
    )

    # Dummy end task
    end_task = DummyOperator(task_id="end")

    # Set task dependencies
    start_task >> generate_csv_task >> end_task
