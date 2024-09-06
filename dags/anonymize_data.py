import csv
import hashlib
import logging
from datetime import datetime, timedelta
from typing import Dict, List

from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

from airflow import DAG

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

file_path: str = "/opt/airflow/data/big_fake_data.csv"
output_csv_file: str = "/opt/airflow/data/anonymized_large_data.csv"
chunk_size: int = 10000  # Adjust chunk size based on memory capacity


def anonymize(value: str) -> str:
    """
    Anonymize a given value using SHA-256.

    Args:
        value (str): The value to be anonymized.

    Returns:
        str: The anonymized value.
    """
    return hashlib.sha256(value.encode()).hexdigest()


def read_chunk(file_path: str, start_row: int, end_row: int) -> List[Dict[str, str]]:
    """
    Read a chunk of data from a CSV file.

    Args:
        file_path (str): The path to the CSV file.
        start_row (int): The starting row index.
        end_row (int): The ending row index.

    Returns:
        List[Dict[str, str]]: A list of dictionaries representing the chunk of data.
    """
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


def write_chunk(
    output_csv_file: str, chunk_data: List[Dict[str, str]], write_header: bool = False
) -> None:
    """
    Write a chunk of data to a CSV file.

    Args:
        output_csv_file (str): The path to the output CSV file.
        chunk_data (List[Dict[str, str]]): The chunk of data to be written.
        write_header (bool): Whether to write the header row. Defaults to False.
    """
    if not chunk_data:
        return  # Skip writing if chunk_data is empty
    with open(
        output_csv_file, mode="a", newline="", encoding="windows-1252"
    ) as outfile:
        writer = csv.DictWriter(outfile, fieldnames=chunk_data[0].keys())
        if write_header:
            writer.writeheader()
        writer.writerows(chunk_data)


def process_chunk(chunk_data: List[Dict[str, str]]) -> List[Dict[str, str]]:
    """
    Process a chunk of data by anonymizing specific fields.

    Args:
        chunk_data (List[Dict[str, str]]): The chunk of data to be processed.

    Returns:
        List[Dict[str, str]]: The processed chunk of data with anonymized fields.
    """
    anonymized_data = []
    for row in chunk_data:
        row["first_name"] = anonymize(row["first_name"])
        row["last_name"] = anonymize(row["last_name"])
        row["address"] = anonymize(row["address"])
        anonymized_data.append(row)
    return anonymized_data


def calculate_num_chunks(file_path: str, chunk_size: int) -> int:
    """
    Calculate the number of chunks by reading the number of rows in the CSV.

    Args:
        file_path (str): The path to the CSV file.
        chunk_size (int): The size of each chunk.

    Returns:
        int: The number of chunks.
    """
    with open(file_path, mode="r") as infile:
        reader = csv.reader(infile)
        total_rows = sum(1 for row in reader) - 1  # Exclude header
    num_chunks = (total_rows // chunk_size) + 1
    return num_chunks


def process_and_write_chunk(
    file_path: str, chunk_number: int, chunk_size: int, output_csv_file: str
) -> None:
    """
    Process and write a chunk of data.

    Args:
        file_path (str): The path to the input CSV file.
        chunk_number (int): The chunk number to be processed.
        chunk_size (int): The size of each chunk.
        output_csv_file (str): The path to the output CSV file.
    """
    start_row = chunk_number * chunk_size
    end_row = start_row + chunk_size
    chunk_data = read_chunk(file_path, start_row, end_row)
    anonymized_data = process_chunk(chunk_data)
    write_chunk(output_csv_file, anonymized_data, write_header=(chunk_number == 0))


# Default arguments for the Airflow DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

num_chunks: int = calculate_num_chunks(file_path, chunk_size)

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
