import csv
from datetime import datetime, timedelta

import faker
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator

from airflow import DAG

# Create an instance of the Faker class
fake = faker.Faker()


def generate_big_data_csv(num_records, batch_size, filename):
    """
    Generate fake data in batches and save it as a CSV file.

    Args:
        num_records (int): The total number of records to generate.
        batch_size (int): The number of records to generate in each batch.
        filename (str): The name of the CSV file to save the data.

    Returns:
        str: A message indicating the file creation status.
    """
    with open(filename, mode="w", newline="", encoding="windows-1252") as file:
        writer = csv.writer(file)
        writer.writerow(["first_name", "last_name", "address", "date_of_birth"])

        for _ in range(0, num_records, batch_size):
            data = []
            for _ in range(min(batch_size, num_records)):
                data.append(
                    [
                        fake.first_name(),
                        fake.last_name(),
                        fake.address().replace("\n", ", "),
                        fake.date_of_birth().strftime("%Y-%m-%d"),
                    ]
                )
            writer.writerows(data)

    return f"{filename} created successfully with {num_records} records."


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "generate_fake_data",
    default_args=default_args,
    description="Generate fake data CSV in batches",
    schedule_interval=None,
    start_date=datetime(2024, 9, 3),
    catchup=False,
) as dag:
    start_task = DummyOperator(task_id="start")

    generate_csv_task = PythonOperator(
        task_id="generate_csv",
        python_callable=generate_big_data_csv,
        op_kwargs={
            "num_records": 100000,
            "batch_size": 10000,
            "filename": "/opt/airflow/data/big_fake_data.csv",
        },
    )

    end_task = DummyOperator(task_id="end")

    start_task >> generate_csv_task >> end_task
