import csv
import hashlib
import logging
from datetime import datetime, timedelta
from typing import Dict, List

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

file_path: str = "/opt/airflow/data/big_fake_data.csv"
output_csv_file: str = "/opt/airflow/data/anonymized_large_data.csv"
chunk_size: int = 10000  # Adjust chunk size based on memory capacity


# Function to anonymize a given value using SHA-256
def anonymize(value: str) -> str:
    return hashlib.sha256(value.encode()).hexdigest()


# Function to read a chunk of data from a CSV file
def read_chunk(file_path: str, start_row: int, end_row: int) -> List[Dict[str, str]]:
    chunk_data = []
    with open(file_path, mode="r", encoding="windows-1252") as infile:
        reader = csv.DictReader(infile)
        for i, row in enumerate(reader):
            if i < start_row:
                continue
            if i >= end_row:
                break
            chunk_data.append(row)
    return chunk_data


# Function to write a chunk of data to a CSV file
def write_chunk(
    output_csv_file: str, chunk_data: List[Dict[str, str]], write_header: bool = False
) -> None:
    if not chunk_data:
        return  # Skip writing if chunk_data is empty
    with open(
        output_csv_file, mode="a", newline="", encoding="windows-1252"
    ) as outfile:
        writer = csv.DictWriter(outfile, fieldnames=chunk_data[0].keys())
        if write_header:
            writer.writeheader()
        writer.writerows(chunk_data)


# Function to process a chunk of data by anonymizing specific fields
def process_chunk(chunk_data: List[Dict[str, str]]) -> List[Dict[str, str]]:
    anonymized_data = []
    for row in chunk_data:
        row["first_name"] = anonymize(row["first_name"])
        row["last_name"] = anonymize(row["last_name"])
        row["address"] = anonymize(row["address"])
        anonymized_data.append(row)
    return anonymized_data


# Default arguments for the Airflow DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


# Function to calculate the number of chunks by reading the number of rows in the CSV
def calculate_num_chunks(file_path: str, chunk_size: int) -> int:
    with open(file_path, mode="r") as infile:
        reader = csv.reader(infile)
        total_rows = sum(1 for row in reader) - 1  # Exclude header
    num_chunks = (total_rows // chunk_size) + 1
    return num_chunks


num_chunks: int = calculate_num_chunks(file_path, chunk_size)


# Function to process and write a chunk of data
def process_and_write_chunk(
    file_path: str, chunk_number: int, chunk_size: int, output_csv_file: str
) -> None:
    start_row = chunk_number * chunk_size
    end_row = start_row + chunk_size
    chunk_data = read_chunk(file_path, start_row, end_row)
    anonymized_data = process_chunk(chunk_data)
    write_chunk(output_csv_file, anonymized_data, write_header=(chunk_number == 0))


# Define the Airflow DAG
with DAG(
    "anonymize_large_csv_chunked",
    default_args=default_args,
    description="Anonymize large CSV file in chunks",
    schedule_interval=None,
    start_date=datetime(2024, 9, 4),
    catchup=False,
) as dag:
    # Dummy start task
    start_task = DummyOperator(task_id="start")

    # Create a task group to process CSV in chunks
    with TaskGroup("process_chunks", tooltip="Process CSV in chunks") as process_chunks:
        for chunk_number in range(num_chunks):
            task = PythonOperator(
                task_id=f"process_chunk_{chunk_number}",
                python_callable=process_and_write_chunk,
                op_kwargs={
                    "file_path": file_path,
                    "chunk_number": chunk_number,
                    "chunk_size": chunk_size,
                    "output_csv_file": output_csv_file,
                },
            )
        # Dummy end task
    end_task = DummyOperator(task_id="end")

    # Set task dependencies
    start_task >> process_chunks >> end_task
