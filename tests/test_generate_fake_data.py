import csv

from dags.generate_fake_data import generate_big_data_csv


def test_generate_big_data_csv(tmp_path):
    # Define test parameters
    num_records = 100
    batch_size = 10
    filename = tmp_path / "test_fake_data.csv"

    # Call the function
    result = generate_big_data_csv(num_records, batch_size, filename)

    # Check the result message
    assert result == f"{filename} created successfully with {num_records} records."

    # Read the generated CSV file
    with open(filename, mode="r", newline="") as file:
        reader = csv.reader(file)
        rows = list(reader)

    # Check the header
    assert rows[0] == ["first_name", "last_name", "address", "date_of_birth"]

    # Check the number of records
    assert len(rows) == num_records + 1  # +1 for the header

    # Check that each row has 4 columns
    for row in rows[1:]:
        assert len(row) == 4
