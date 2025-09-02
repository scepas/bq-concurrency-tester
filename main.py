import argparse
import time
import yaml
import random
import numpy as np
from concurrent.futures import ThreadPoolExecutor, as_completed
from google.cloud import bigquery

def load_config(config_path):
    """Loads the YAML configuration file."""
    with open(config_path, 'r') as f:
        return yaml.safe_load(f)

def load_queries(query_config):
    """Loads the SQL queries from the specified files."""
    queries = []
    for item in query_config:
        with open(item['sql_file'], 'r') as f:
            queries.append({
                'sql': f.read(),
                'percentage': item['percentage']
            })
    return queries

def create_weighted_query_list(queries):
    """Creates a weighted list of queries for random selection."""
    weighted_list = []
    for query in queries:
        count = int(query['percentage'])
        weighted_list.extend([query['sql']] * count)
    return weighted_list

def run_query(client, sql):
    """Executes a single BigQuery query and returns the result."""
    job_config = bigquery.QueryJobConfig(use_query_cache=False)
    start_time = time.time()
    try:
        query_job = client.query(sql, job_config=job_config)
        results = query_job.result()  # Wait for the job to complete
        end_time = time.time()
        return {
            'success': True,
            'duration': end_time - start_time,
            'rows': results.total_rows
        }
    except Exception as e:
        end_time = time.time()
        return {
            'success': False,
            'duration': end_time - start_time,
            'error': str(e)
        }

def main():
    """Main function to run the concurrency test."""
    parser = argparse.ArgumentParser(description='BigQuery Concurrency Tester')
    parser.add_argument('--config', default='config.yaml', help='Path to the configuration file.')
    args = parser.parse_args()

    config = load_config(args.config)
    queries = load_queries(config['queries'])
    weighted_queries = create_weighted_query_list(queries)

    client = bigquery.Client(project=config['project_id'])

    print(f"Starting BigQuery concurrency test...")
    print(f"Project: {config['project_id']}")
    print(f"Concurrency: {config['concurrency']}")
    print(f"Duration: {config['duration_seconds']} seconds")
    print("-" * 30)

    total_queries = 0
    successful_queries = 0
    failed_queries = 0
    query_durations = []

    with ThreadPoolExecutor(max_workers=config['concurrency']) as executor:
        futures = []
        start_time = time.time()
        while time.time() - start_time < config['duration_seconds']:
            sql = random.choice(weighted_queries)
            futures.append(executor.submit(run_query, client, sql))
            total_queries += 1
            time.sleep(1 / config['concurrency']) # To avoid submitting all queries at once

        for future in as_completed(futures):
            result = future.result()
            if result['success']:
                successful_queries += 1
                query_durations.append(result['duration'])
                print(f"Query successful in {result['duration']:.2f}s, rows: {result['rows']}")
            else:
                failed_queries += 1
                print(f"Query failed in {result['duration']:.2f}s, error: {result['error']}")

    print("-" * 30)
    print("Test finished.")
    print(f"Total queries submitted: {total_queries}")
    print(f"Successful queries: {successful_queries}")
    print(f"Failed queries: {failed_queries}")

    if successful_queries > 0:
        avg_duration = np.mean(query_durations)
        p95_duration = np.percentile(query_durations, 95)
        p99_duration = np.percentile(query_durations, 99)
        throughput = successful_queries / config['duration_seconds']

        print(f"Average query duration: {avg_duration:.2f}s")
        print(f"P95 query duration: {p95_duration:.2f}s")
        print(f"P99 query duration: {p99_duration:.2f}s")
        print(f"Average throughput: {throughput:.2f} queries/sec")

if __name__ == '__main__':
    main()