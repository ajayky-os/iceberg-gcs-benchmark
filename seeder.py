import argparse
import logging
import sys
import os
import subprocess
import shutil
import tempfile
from pyspark.sql import SparkSession

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def get_spark_session(args):
    """Initializes and returns a SparkSession configured for Iceberg and GCS."""
    spark_builder = SparkSession.builder \
        .appName(f"Iceberg {args.benchmark_type.upper()} Seeder - SF{args.scale_factor}") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config(f"spark.sql.catalog.{args.iceberg_catalog}", "org.apache.iceberg.spark.SparkCatalog") \
        .config(f"spark.sql.catalog.{args.iceberg_catalog}.type", "hadoop") \
        .config(f"spark.sql.catalog.{args.iceberg_catalog}.warehouse", f"gs://{args.bucket}/{args.iceberg_catalog}") \
        .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
        .config("spark.hadoop.fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS") \
        .config("spark.hadoop.google.cloud.auth.service.account.enable", "true")

    if args.spark_conf:
        for conf in args.spark_conf:
            key, value = conf.split('=', 1)
            spark_builder.config(key, value)
            logging.info(f"Added Spark config: {key}={value}")

    spark = spark_builder.getOrCreate()
    logging.info("SparkSession initialized.")
    return spark

def create_database(spark, args):
    """Creates the database if it doesn't exist."""
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {args.iceberg_catalog}.{args.database}")
    logging.info(f"Database {args.iceberg_catalog}.{args.database} ensured to exist.")

def get_tpcds_tables():
    return [
        "call_center", "catalog_page", "catalog_returns", "catalog_sales", "customer",
        "customer_address", "customer_demographics", "date_dim", "dbgen_version",
        "household_demographics", "income_band", "inventory", "item", "promotion",
        "reason", "ship_mode", "store", "store_returns", "store_sales",
        "time_dim", "warehouse", "web_page", "web_returns", "web_sales", "web_site"
    ]

def get_tpch_tables():
    return ["customer", "lineitem", "nation", "orders", "part", "partsupp", "region", "supplier"]

def get_tpch_table_code(table_name):
    codes = {
        "customer": "c", "lineitem": "L", "nation": "n", "orders": "O",
        "part": "P", "partsupp": "S", "region": "r", "supplier": "s"
    }
    return codes.get(table_name)

def get_partition_column(table_name, benchmark_type):
    if benchmark_type.lower() == 'tpcds':
        partition_map = {
            "store_sales": "ss_sold_date_sk", "store_returns": "sr_returned_date_sk",
            "catalog_sales": "cs_sold_date_sk", "catalog_returns": "cr_returned_date_sk",
            "web_sales": "ws_sold_date_sk", "web_returns": "wr_returned_date_sk",
            "inventory": "inv_date_sk",
        }
        return partition_map.get(table_name)
    elif benchmark_type.lower() == 'tpch':
        partition_map = {"lineitem": "l_shipdate", "orders": "o_orderdate"}
        return partition_map.get(table_name)
    return None

def run_command(command, cwd=None):
    logging.info(f"Running command: {' '.join(command)}")
    try:
        result = subprocess.run(command, check=True, capture_output=True, text=True, cwd=cwd)
        logging.info(f"Command stdout: {result.stdout}")
        if result.stderr:
            logging.warning(f"Command stderr: {result.stderr}")
    except subprocess.CalledProcessError as e:
        logging.error(f"Command failed: {e}")
        logging.error(f"Stdout: {e.stdout}")
        logging.error(f"Stderr: {e.stderr}")
        raise

def run_dsdgen(args, table, local_table_dir):
    os.makedirs(local_table_dir, exist_ok=True)
    cmd = [
        args.dsdgen_path,
        "-DIR", local_table_dir,
        "-SCALE", str(args.scale_factor),
        "-TABLE", table,
        "-PARALLEL", "4", # Example, adjust as needed
        "-TERMINATE", "N",
        "-FORCE",
    ]
    run_command(cmd)
    # dsdgen creates table_N.dat files, rename to {table}.dat
    for f in os.listdir(local_table_dir):
        if f.endswith(".dat"):
            os.rename(os.path.join(local_table_dir, f), os.path.join(local_table_dir, f"{table}.dat"))
            logging.info(f"Renamed {f} to {table}.dat")

def run_dbgen(args, table, local_table_dir):
    os.makedirs(local_table_dir, exist_ok=True)
    table_code = get_tpch_table_code(table)
    if not table_code:
        logging.warning(f"No dbgen table code found for {table}, skipping generation.")
        return

    cmd = [
        args.dbgen_path,
        "-s", str(args.scale_factor),
        "-T", table_code,
        "-f",
    ]
    # dbgen writes to current dir, so run it within local_table_dir
    run_command(cmd, cwd=local_table_dir)
    # dbgen names files like customer.tbl.1, rename to {table}.dat
    for f in os.listdir(local_table_dir):
        if f.startswith(table + ".tbl"):
             os.rename(os.path.join(local_table_dir, f), os.path.join(local_table_dir, f"{table}.dat"))
             logging.info(f"Renamed {f} to {table}.dat")

def upload_to_gcs(local_path, gcs_path):
    if not os.listdir(local_path):
        logging.warning(f"Local path {local_path} is empty, skipping upload to {gcs_path}")
        return
    cmd = ["gsutil", "-m", "cp", "-r", os.path.join(local_path, "*"), gcs_path + "/"]
    run_command(cmd)
    logging.info(f"Successfully uploaded from {local_path} to {gcs_path}")

def generate_and_load_data(spark, args):
    create_database(spark, args)
    benchmark_type = args.benchmark_type.lower()

    if benchmark_type == 'tpcds':
        tables = get_tpcds_tables()
        if not args.dsdgen_path: raise ValueError("--dsdgen-path is required for TPC-DS")
    elif benchmark_type == 'tpch':
        tables = get_tpch_tables()
        if not args.dbgen_path: raise ValueError("--dbgen-path is required for TPC-H")
    else:
        raise ValueError("Invalid benchmark_type.")

    gcs_data_path_base = f"gs://{args.bucket}/tmp/{benchmark_type}-data-sf{args.scale_factor}"
    local_tmp_base = os.path.join(args.local_tmp_dir, f"{benchmark_type}-data-sf{args.scale_factor}")

    logging.info(f"Starting data generation and load for {benchmark_type.upper()} SF{args.scale_factor}")

    for table in tables:
        logging.info(f"Processing table: {table}")
        local_table_dir = os.path.join(local_tmp_base, table)
        gcs_table_path = f"{gcs_data_path_base}/{table}"

        try:
            # 1. Generate raw data locally
            logging.info(f"Generating data for {table} in {local_table_dir}")
            if os.path.exists(local_table_dir): shutil.rmtree(local_table_dir)
            os.makedirs(local_table_dir)

            if benchmark_type == 'tpcds':
                run_dsdgen(args, table, local_table_dir)
            else: # tpch
                run_dbgen(args, table, local_table_dir)

            # 2. Upload to GCS
            logging.info(f"Uploading {table} data to {gcs_table_path}")
            upload_to_gcs(local_table_dir, gcs_table_path)

            # 3. Read data with Spark from GCS
            df = spark.read.format("csv") \
                .option("header", "false") \
                .option("inferSchema", "true") \
                .option("delimiter", "|") \
                .load(f"{gcs_table_path}/*.dat")
            logging.info(f"Schema inferred for {table}: {df.schema}")

            # 4. Create and load Iceberg table
            full_table_name = f"{args.iceberg_catalog}.{args.database}.{table}"
            writer = df.write.format("iceberg") \
                .mode("overwrite") \
                .option("write.format", args.file_format)

            partition_column = get_partition_column(table, benchmark_type)
            if args.partition_data and partition_column:
                writer = writer.partitionBy(partition_column)
                logging.info(f"Using partition column '{partition_column}' for table {table}.")
            else:
                logging.info(f"Not partitioning table {table}.")

            writer.saveAsTable(full_table_name)
            logging.info(f"Successfully created and loaded Iceberg table: {full_table_name}")

        except Exception as e:
            logging.error(f"Error processing table {table}: {e}", exc_info=True)
        finally:
            if os.path.exists(local_table_dir):
                shutil.rmtree(local_table_dir)
                logging.info(f"Cleaned up local tmp dir: {local_table_dir}")

def main():
    parser = argparse.ArgumentParser(description="TPC-DS/TPC-H Data Seeder for Iceberg on GCS")
    parser.add_argument("--scale-factor", type=str, required=True, help="Scale factor (e.g., 1, 10, 100)")
    parser.add_argument("--benchmark-type", type=str, required=True, choices=['tpcds', 'tpch'], help="Benchmark type")
    parser.add_argument("--iceberg-catalog", type=str, required=True, help="Iceberg catalog name")
    parser.add_argument("--database", type=str, required=True, help="Database name in Iceberg catalog")
    parser.add_argument("--bucket", type=str, required=True, help="GCS bucket for warehouse and tmp data")
    parser.add_argument("--partition-data", action='store_true', help="Partition Iceberg tables")
    parser.add_argument("--file-format", type=str, default="parquet", choices=['parquet', 'orc', 'avro'], help="Iceberg table file format")
    parser.add_argument("--dsdgen-path", type=str, help="Path to dsdgen executable (for TPC-DS)")
    parser.add_argument("--dbgen-path", type=str, help="Path to dbgen executable (for TPC-H)")
    parser.add_argument("--local-tmp-dir", type=str, default=tempfile.gettempdir(), help="Local temporary directory for data generation")
    parser.add_argument("--spark-conf", nargs='*', help="Additional Spark configurations in key=value format")

    args = parser.parse_args()
    logging.info(f"Starting seeder with args: {args}")

    spark = None
    try:
        spark = get_spark_session(args)
        generate_and_load_data(spark, args)
        logging.info("Seeder script completed successfully.")
    except Exception as e:
        logging.error(f"An error occurred: {e}", exc_info=True)
        sys.exit(1)
    finally:
        if spark:
            spark.stop()
            logging.info("SparkSession stopped.")

if __name__ == "__main__":
    main()