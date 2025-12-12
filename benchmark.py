import argparse
import time
import uuid
from pathlib import Path
import json
import requests
import pprint

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import (StringType, DoubleType, StructField,
                               StructType, TimestampType, BooleanType, LongType)
from datetime import datetime
from sparkmeasure import StageMetrics


class MySparkListener:
    def onStageCompleted(self, stageCompleted):
        print(f"--- LISTENER: Stage Completed: {stageCompleted.stageInfo.stageId} ({stageCompleted.stageInfo.name}) ---")
        # Optional: Deeper inspection of stageInfo

    def onTaskEnd(self, taskEnd):
        # This can be very verbose
        # print(f"--- LISTENER: Task End: {taskEnd.taskInfo.taskId} in Stage {taskEnd.stageId} ---")
        pass

    def onJobEnd(self, jobEnd):
        print(f"--- LISTENER: Job End: {jobEnd.jobId} ---")

    def onApplicationEnd(self, applicationEnd):
        print(f"--- LISTENER: Application End ---")

    def onApplicationStart(self, applicationStart):
        print(f"--- LISTENER: Application Start ---")


def set_iceberg_logging(spark):
    print("--- Configuring Log4j 2 Programmatically ---")

    # 1. Get the Log4j 2 Context
    # We must use the specific Log4j 2 API, not the old Log4j 1 API
    LogManager = spark._jvm.org.apache.logging.log4j.LogManager
    Level = spark._jvm.org.apache.logging.log4j.Level
    ctx = LogManager.getContext(False)
    config = ctx.getConfiguration()

    # 2. Define the loggers you want to silence or enable
    # (Logger Name, Level)
    loggers_to_set = [
        ("org.apache.iceberg.gcp", Level.DEBUG),
        ("com.google.cloud.gcs", Level.DEBUG),
        # Optional: Set Root to WARN to prevent noise
        ("", Level.WARN)
    ]

    for logger_name, level in loggers_to_set:
        # Get or create the logger configuration
        loggerConfig = config.getLoggerConfig(logger_name)
        loggerConfig.setLevel(level)

        # CRITICAL: Link the logger to the Console Appender
        # Without this, the logger is "on" but has nowhere to print
        root = config.getRootLogger()
        for key in root.getAppenders().keySet():
            appender = root.getAppenders().get(key)
            loggerConfig.addAppender(appender, level, None)

        print(f"Set {logger_name} to {level}")

    # 3. Apply the changes
    ctx.updateLoggers(config)
    print("--- Log4j 2 Configuration Applied ---")

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
        self.results_buffer = []
        self.listener = MySparkListener()
        # try:
        #     # Ensure Java Spark Context is initialized
        #     print(f"--- SparkContext type: {type(self.spark.sparkContext)} ---")
        #     print(f"--- SparkContext dir: {dir(self.spark.sparkContext)} ---")
        #     _ = self.spark.sparkContext._jsc
        #     print("--- _jsc accessed ---")
        #     self.spark.sparkContext.addSparkListener(self.listener)
        #     print("--- Custom Spark Listener Added ---")
        # except Exception as e:
        #     print(f"  -> Error adding listener: {e}")
        print(f"Initialized new benchmark run with ID: {self.run_id}")

    def __del__(self):
        try:
            if self.spark and hasattr(self, 'listener') and self.listener:
                self.spark.sparkContext.removeSparkListener(self.listener)
                print("--- Custom Spark Listener Removed ---")
        except Exception as e:
            print(f"  -> Error removing listener: {e}")

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
                        client_type STRING,
                        planning_scan_time_ms BIGINT,
                        custom_scan_time_ms BIGINT,
                        timestamp TIMESTAMP
                    )
        USING iceberg
        """
        self.spark.sql(create_table_sql)
        print(f"Table '{self.results_table_name}' is ready.")


    def _log_result(self, db_name: str, benchmark_type: str, query_name: str, duration_sec: float,
                    status: str, error_msg: str = None, metrics_json: str = None, analytics_core_enabled: bool = False):
        """
        Buffers a single query result to be logged later.
        """
        client_type_config = self.spark.conf.get("spark.sql.catalog.gcs_prod.gcs.client.type", "HTTP_CLIENT")
        if client_type_config == "GRPC_CLIENT":
            client_type = "GRPC"
        else:
            client_type = "HTTP"

        data = (
            self.run_id,
            db_name,
            benchmark_type,
            query_name,
            duration_sec,
            status,
            error_msg,
            metrics_json,
            analytics_core_enabled,
            client_type,
            None,  # planning_scan_time_ms
            None,  # custom_scan_time_ms
            datetime.now()
        )
        self.results_buffer.append(data)
        print(f"  -> Buffered result for {query_name}: {status}")

    def flush_results(self):
        """
        Writes all buffered results to the Iceberg results table.
        """
        if not self.results_buffer:
            print("No results to flush.")
            return

        print(f"Flushing {len(self.results_buffer)} results to table {self.results_table_name}...")
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
            StructField("client_type", StringType(), True),
            StructField("planning_scan_time_ms", LongType(), True),
            StructField("custom_scan_time_ms", LongType(), True),
            StructField("timestamp", TimestampType(), False)
        ])

        result_df = self.spark.createDataFrame(self.results_buffer, schema)
        result_df.writeTo(self.results_table_name).append()
        print(f"  -> Flushed {len(self.results_buffer)} results.")
        self.results_buffer = []

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

        stagemetrics = StageMetrics(self.spark)
        analytics_core_enabled = self.spark.conf.get("spark.sql.catalog.gcs_prod.gcs.analytics-core.enabled", "false").lower() == "true"

        sql_files = sorted(list(query_dir.glob('*.sql')))
        print(f"Found {len(sql_files)} queries for benchmark '{benchmark_name}' in '{queries_path}'.")
        
        for sql_file in sql_files:
            self.spark.catalog.clearCache()
            query_name = sql_file.name
            print(f"Running {benchmark_name} query: {query_name}...")
            query_sql = ""
            with open(sql_file, 'r') as f:
                query_sql = f.read()
            query_sql = query_sql.replace("${database}",self.catalog_name).replace("${schema}", db_name)
            
            status = "SUCCESS"
            error_message = None
            metrics_json = None
            planning_scan_time_ms = 0
            custom_scan_time_ms = 0
            start_time = time.time()
            end_time = None
            app_id = self.spark.sparkContext.applicationId
            
            try:
                stagemetrics.begin()
                df = self.spark.sql(query_sql)
                df.write.format("noop").mode("overwrite").save()
                stagemetrics.end()
                end_time = time.time()

                metrics = stagemetrics.aggregate_stagemetrics()
                python_metrics = {k: v for k, v in metrics.items()}
                metrics_json = json.dumps(python_metrics)

            except Exception as e:
                status = "FAILED"
                error_message = str(e)[:2000] # Truncate long error messages
                print(f"  -> FAILED: {query_name}. Error: {error_message}")


            finally:
                if end_time is None: end_time = time.time()
                duration = end_time - start_time
                print(f"  -> Total duration in sec =: {duration}")

                self._log_result(db_name, benchmark_name, query_name, duration, status, error_message, metrics_json, analytics_core_enabled)
                
                # --- API Payload --- 
                if status == "SUCCESS":
                    try:
                        client_type = "HTTP"
                        if self.spark.conf.get("spark.sql.catalog.gcs_prod.gcs.client.type", "HTTP_CLIENT") == "GRPC_CLIENT":
                            client_type = "GRPC"
                        api_payload = {
                            "query_name": query_name,
                            "run_id": self.run_id,
                            "benchmark_type": benchmark_type,
                            "planning_scan_time_ms": None, # Metric not reliably available
                            "custom_scan_time_ms": None, # Metric not reliably available
                            "total_duration_sec": duration,
                            "analytics_core_enabled": analytics_core_enabled,
                            "client_type": client_type,
                            "schema_size": db_name
                        }
                        print(f"  -> API Payload (example): {api_payload}")
                    except Exception as e:
                        print(f"  -> ERROR sending to API: {e}")


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
        .config("spark.sql.debug.maxToStringFields", 1000)
        .config("spark.jars.packages", "org.grpc:grpc-alts:1.64.0,io.grpc:grpc-core:1.64.0")
        # --- Add History Server Configs ---
        .config("spark.eventLog.enabled", "true")
        .config("spark.eventLog.dir", "file:///tmp/spark-events")  # Example local path
        # Ensure your History Server also uses this path
        .config("spark.history.fs.logDirectory", "file:///tmp/spark-events") 
        # --- End History Server Configs ---
        .getOrCreate()
    )
    set_iceberg_logging(spark)

#     results_table_fqn = f"{args.catalog_name}.{args.results_db}.results"
    results_table_fqn = f"gcs_prod.{args.results_db}.results"

    runner = BenchmarkRunner(spark, results_table_fqn, args.catalog_name)
    runner.ensure_results_table_exists()

    # Run TPC-DS queries
    for i in range(1):
        runner.run_benchmark("TPC-DS", args.tpcds_dir, args.tpcds_data_db)

    # Run TPC-H queries

    for i in range(1):
        runner.run_benchmark("TPC-H", args.tpch_dir, args.tpch_data_db)

    runner.flush_results()
    print("\nBenchmark run completed.")
    spark.stop()


if __name__ == "__main__":
    main()