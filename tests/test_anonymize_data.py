import csv
import os
import tempfile

from dags.anonymize_data import anonymize, process_chunk, read_chunk, write_chunk


def test_read_chunk():
    # Create a temporary CSV file with test data
    with tempfile.NamedTemporaryFile(delete=False, mode="w", newline="") as temp_file:
        writer = csv.DictWriter(
            temp_file, fieldnames=["first_name", "last_name", "address"]
        )
        writer.writeheader()
        for i in range(20):
            writer.writerow(
                {
                    "first_name": f"First{i}",
                    "last_name": f"Last{i}",
                    "address": f"Address{i}",
                }
            )
        temp_file_path = temp_file.name

    # Test read_chunk function
    chunk_data = read_chunk(temp_file_path, start_row=5, end_row=10)
    assert len(chunk_data) == 5
    assert chunk_data[0]["first_name"] == "First5"
    assert chunk_data[-1]["first_name"] == "First9"

    # Clean up temporary file
    os.remove(temp_file_path)


def test_write_chunk():
    # Create a temporary CSV file
    with tempfile.NamedTemporaryFile(delete=False, mode="w", newline="") as temp_file:
        temp_file_path = temp_file.name

    # Test write_chunk function
    chunk_data = [{"first_name": "First1", "last_name": "Last1", "address": "Address1"}]
    write_chunk(temp_file_path, chunk_data, write_header=True)

    # Verify the written data
    with open(temp_file_path, mode="r", newline="") as file:
        reader = csv.DictReader(file)
        rows = list(reader)
        assert len(rows) == 1
        assert rows[0]["first_name"] == "First1"

    # Clean up temporary file
    os.remove(temp_file_path)


def test_process_chunk():
    # Test process_chunk function
    chunk_data = [{"first_name": "First1", "last_name": "Last1", "address": "Address1"}]
    anonymized_data = process_chunk(chunk_data)
    assert len(anonymized_data) == 1
    assert anonymized_data[0]["first_name"] != "First1"
    assert anonymized_data[0]["last_name"] != "Last1"
    assert anonymized_data[0]["address"] != "Address1"


def test_anonymize():
    # Test anonymize function
    original_value = "TestValue"
    anonymized_value = anonymize(original_value)
    assert anonymized_value != original_value
    assert len(anonymized_value) == 64  # SHA-256 hash length


# Run the tests
test_read_chunk()
test_write_chunk()
test_process_chunk()
test_anonymize()
