# Define environment variables for clarity
export SPARK_HOME=/opt/spark
export ICEBERG_SPARK_JAR="gs://ajayky-asia/jars/iceberg-spark-runtime-4.0_2.13-1.11.0-SNAPSHOT.jar,gs://ajayky-asia/jars/iceberg-gcp-bundle-1.11.0-SNAPSHOT.jar"

CATALOG_NAME="gcs_prod"
TPCDS_DATA_DB="tpcds_sf1"
TPCH_DATA_DB="tpch_sf1"
RESULTS_DB="fileio_results"

# The spark-submit command
$SPARK_HOME/bin/spark-submit \
  --master spark://10.182.0.17:7077 \
  --jars $ICEBERG_SPARK_JAR \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  --conf spark.sql.catalog.gcs_prod=org.apache.iceberg.spark.SparkCatalog  \
  --conf spark.sql.catalog.gcs_prod.type=hadoop  \
  --conf spark.sql.catalog.gcs_prod.warehouse=gs://ajayky-spark/hadoop-warehouse \
  --conf spark.sql.catalog.gcs_prod.hadoop.fs.AbstractFileSystem.gs.impl=com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS \
  --conf spark.hadoop.fs.AbstractFileSystem.gs.impl=com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS \
  --conf spark.sql.catalog.gcs_prod.io-impl=org.apache.iceberg.gcp.gcs.GCSFileIO \
  --conf spark.hadoop.hive.cli.print.header=true \
  --driver-memory 4g \
  --executor-memory 8g \
  --executor-cores 4 \
  --num-executors 10 \
  benchmark.py \
    --tpcds-dir /home/ajayky_google_com/iceberg-gcs-benchmark/queries/tpcds/ \
    --tpch-dir /home/ajayky_google_com/iceberg-gcs-benchmark/queries/tpch/ \
    --tpcds-data-db $TPCDS_DATA_DB \
    --tpch-data-db $TPCH_DATA_DB \
    --catalog-name $CATALOG_NAME \
    --results-db $RESULTS_DB