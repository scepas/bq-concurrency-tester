# BigQuery Concurrency Tester

This application runs a concurrency test against Google BigQuery. It allows you to configure the number of concurrent queries, the duration of the test, and a weighted mix of SQL queries to execute.

## Setup

1.  **Install Dependencies:**

    ```bash
    pip install -r requirements.txt
    ```

2.  **Authenticate with Google Cloud:**

    Make sure you have the `gcloud` CLI installed and authenticated:

    ```bash
    gcloud auth application-default login
    ```

3.  **Configure the Test:**

    Edit the `config.yaml` file to set your desired test parameters:

    *   `project_id`: Your Google Cloud project ID.
    *   `concurrency`: The number of concurrent queries to run.
    *   `duration_seconds`: The duration of the test in seconds.
    *   `queries`: A list of SQL files and their execution percentage.

## Usage

Run the application from your terminal:

```bash
python main.py
```

The application will print the results of each query execution and a summary at the end of the test.
