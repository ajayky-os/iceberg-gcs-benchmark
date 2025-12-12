import argparse
import time
import uuid
from pathlib import Path
import json

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import (StringType, DoubleType, StructField,
                               StructType, TimestampType, BooleanType)
from datetime import datetime

class BenchmarkRunner:
    """
    A utility to run SQL benchmark queries (like TPC-DS, TPC-H) against a Spark cluster
    and log the execution results into an Apache Iceberg table.
    """

    def __init__(self, spark: SparkSession, results_table: str, catalog_name: str):
        """
        Initializes the BenchmarkRunner.

        :param spark: An active SparkSession.
        :param results_table: The fully qualified name of the Iceberg table for results
                              (e.g., 'my_catalog.db.benchmark_results').
        :param catalog_name: The name of the catalog to use.
        """
        self.spark = spark
        self.results_table_name = results_table
        self.catalog_name = catalog_name
        self.run_id = str(uuid.uuid4())
        print(f"Initialized new benchmark run with ID: {self.run_id}")

    def ensure_results_table_exists(self):
        """
        Creates the Iceberg table for storing benchmark results if it does not already exist.
        """
        print(f"Ensuring results table '{self.results_table_name}' exists...")
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {self.results_table_name} (
            run_id STRING,
            schema_size STRING,
            benchmark_type STRING,
            query_name STRING,
            execution_time_sec DOUBLE,
            status STRING,
            
                        error_message STRING,
                        metrics_json STRING,
                        analytics_core_enabled BOOLEAN,
                        timestamp TIMESTAMP
                    )
        USING iceberg
        PARTITIONED BY (days(timestamp), benchmark_type)
        """
        self.spark.sql(create_table_sql)
        print(f"Table '{self.results_table_name}' is ready.")

    def _log_result(self, db_name: str, benchmark_type: str, query_name: str, duration_sec: float,
                    status: str, error_msg: str = None, metrics_json: str = None, analytics_core_enabled: bool = False):
        """
        Logs a single query result to the Iceberg results table.
        """
        schema = StructType([
            StructField("run_id", StringType(), False),
            StructField("schema_size", StringType(), False),
            StructField("benchmark_type", StringType(), False),
            StructField("query_name", StringType(), False),
            StructField("execution_time_sec", DoubleType(), True),
            StructField("status", StringType(), False),
            StructField("error_message", StringType(), True),
            StructField("metrics_json", StringType(), True),
            StructField("analytics_core_enabled", BooleanType(), True),
            StructField("timestamp", TimestampType(), False)
        ])

        data = [(
            self.run_id,
            db_name,
            benchmark_type,
            query_name,
            duration_sec,
            status,
            error_msg,
            metrics_json,
            analytics_core_enabled,
            datetime.now()
        )]

        result_df = self.spark.createDataFrame(data, schema)

        # Using the DataFrame V2 API for appending to Iceberg is idiomatic
        result_df.writeTo(self.results_table_name).append()
        print(f"  -> Logged result for {query_name}: {status}")

    def run_benchmark(self, benchmark_name: str, queries_path: str, db_name: str):
        """
        Executes all .sql files in a given directory for a specific benchmark.

        :param benchmark_name: The name of the benchmark (e.g., 'TPC-DS', 'TPC-H').
        :param queries_path: The local path to the directory containing .sql query files.
        """
        query_dir = Path(queries_path)
        if not query_dir.is_dir():
            print(f"Warning: Directory not found, skipping benchmark: {queries_path}")
            return

        # Use the specified database for the queries
        self.spark.sql(f"USE {self.catalog_name}.{db_name}")
        print(f"\nSwitched to database: '{db_name}' for {benchmark_name} queries.")

        analytics_core_enabled = self.spark.conf.get("spark.sql.catalog.gcs_prod.gcs.analytics-core.enabled", "false").lower() == "true"

        sql_files = sorted(list(query_dir.glob('*.sql')))
        print(f"Found {len(sql_files)} queries for benchmark '{benchmark_name}' in '{queries_path}'.")

        for sql_file in sql_files:
            query_name = sql_file.name
            print(f"Running {benchmark_name} query: {query_name}...")
            query_sql = ""
            with open(sql_file, 'r') as f:
                query_sql = f.read()
            query_sql = query_sql.replace("${database}","gcs_prod").replace("${schema}", db_name)
            start_time = time.time()
            status = "SUCCESS"
            error_message = None
            duration = 0.0
            metrics_json = None
            try:
                print(f"Clearing Spark cache before running {benchmark_name}...")
                # self.spark.catalog.clearCache()
                start_time = time.time()
                # To force execution, we call an action. .collect() is simple.
                # For queries with large result sets, a more robust action is to write the output.
                # .write.format('noop') is a clean way to trigger execution without producing output.
                self.spark.sql(query_sql).write.format("noop").mode("overwrite").save()
            except Exception as e:
                status = "FAILED"
                error_message = str(e)[:2000] # Truncate long error messages
                print(f"  -> FAILED: {query_name}. Error: {error_message}")


            finally:
                end_time = time.time()
                duration = end_time - start_time
                self._log_result(db_name, benchmark_name, query_name, duration, status, error_message, metrics_json, analytics_core_enabled)



def main():
    parser = argparse.ArgumentParser(description="Run Spark SQL Benchmarks and log to Iceberg.")
    parser.add_argument("--tpcds-dir", required=True, help="Path to the directory with TPC-DS .sql queries.")
    parser.add_argument("--tpch-dir", required=True, help="Path to the directory with TPC-H .sql queries.")
    parser.add_argument("--tpcds-data-db", required=True, help="Database/schema where TPC data is located (e.g., tpcds_sf100).")
    parser.add_argument("--tpch-data-db", required=True, help="Database/schema where TPC data is located (e.g., tpcds_sf100).")
    parser.add_argument("--catalog-name", required=True, help="Name of the Iceberg catalog configured in Spark (e.g., gcs_catalog).")
    parser.add_argument("--results-db", required=True, help="Database/schema within the catalog to store results.")
    args = parser.parse_args()

    spark = (
        SparkSession.builder
        .appName("Iceberg Benchmark Runner")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .getOrCreate()
    )

    results_table_fqn = f"{args.catalog_name}.{args.results_db}.results"

    runner = BenchmarkRunner(spark, results_table_fqn, args.catalog_name)
    runner.ensure_results_table_exists()

    # Run TPC-DS queries
    for i in range(3):
        runner.run_benchmark("TPC-DS", args.tpcds_dir, args.tpcds_data_db)

    # Run TPC-H queries
    for i in range(3):
        runner.run_benchmark("TPC-H", args.tpch_dir, args.tpch_data_db)

    print("\nBenchmark run completed.")
    spark.stop()


if __name__ == "__main__":
    main()