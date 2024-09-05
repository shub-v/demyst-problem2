# Airflow Data Anonymization Project

This project uses Apache Airflow to anonymize large CSV files in chunks. The anonymization process involves hashing sensitive data fields to ensure privacy.

## Prerequisites

- Docker
- Docker Compose
- Python 3.12
- pip

## Installation

1. Clone the repository:
    ```sh
    git clone https://github.com/shub-v/demyst-problem2.git
    cd demyst-problem2
    ```

2. Start the data pipeline:
    ```sh
    make up
    make ci
    ```

3. Access the Airflow UI:
    - URL: [http://localhost:8080](http://localhost:8080)
    - Username: `airflow`
    - Password: `airflow`

## Project Structure

- **DAGs**: The DAGs for the project are defined in the `dags` directory.
  - **`dags/anonymize_csv.py`**: Contains the DAG and functions to anonymize large CSV files in chunks.
  - **`dags/generate_fake_data.py`**: Contains the DAG and functions to generate fake data and save it as a CSV file.
- **Makefile**: Contains various commands to manage the project.
- **Tests**: Located in the `tests` directory.

## Usage

- To generate a dataset with 100,000 rows, first run the `generate_fake_data` DAG. Once the data is generated, execute the `anonymize_large_csv_chunked` DAG to anonymize sensitive fields by hashing.

- If you prefer to work with a smaller sample, a CSV file containing 10 rows is already available at `data/big_fake_data.csv`.

## Makefile Commands

- **Setup Containers**:
    - `make up`: Set up and start the Airflow containers.
    - `make down`: Stop and remove the Airflow containers.
    - `make restart`: Restart the Airflow containers.
    - `make sh`: Open a shell in the Airflow webserver container.


- **Testing and Linting**:
    - `make pytest`: Run tests.
    - `make format`: Format code using Black.
    - `make isort`: Sort imports.
    - `make type`: Run type checks using mypy.
    - `make lint`: Run lint checks using flake8.
    - `make black`: Format code using Black.
    - `make ci`: Run all checks (isort, format, type, lint, black, pytest).
