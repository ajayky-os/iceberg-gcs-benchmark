# Define environment variables for clarity
export SPARK_HOME=/opt/spark
export ICEBERG_SPARK_JAR="gs://ajayky-asia/jars/iceberg-spark-runtime-4.0_2.13-1.11.0-SNAPSHOT.jar,gs://ajayky-asia/jars/iceberg-gcp-bundle-1.11.0-SNAPSHOT.jar"

CATALOG_NAME="ajayky_biglake"
TPCDS_DATA_DB="tpcds_sf10"
TPCH_DATA_DB="tpch_sf10"
RESULTS_DB="benchmarks"
export POLARIS_URL="htttp://10.182.15.225:8181/api/catalog"
export CREDENTIALS="root:my-secret-password" # format: clientId:clientSecret
#export CREDENTIALS="8ea011b87b5e736d:a1ccc96791128c334c91164d19bb5bdd"
export CATALOG_NAME_IN_POLARIS="gcp_lakehouse"

# The spark-submit command
$SPARK_HOME/bin/spark-submit \
  --master spark://10.182.0.17:7077 \
  --jars $ICEBERG_SPARK_JAR \
  --packages ch.cern.sparkmeasure:spark-measure_2.13:0.27 \
  --conf spark.hadoop.hive.cli.print.header=true \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  --conf spark.sql.catalog.gcs_prod=org.apache.iceberg.spark.SparkCatalog  \
  --conf spark.sql.catalog.gcs_prod.type=hadoop  \
  --conf spark.sql.catalog.gcs_prod.warehouse=gs://ajayky-spark/hadoop-warehouse \
  --conf spark.sql.catalog.gcs_prod.hadoop.fs.AbstractFileSystem.gs.impl=com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS \
  --conf spark.hadoop.fs.AbstractFileSystem.gs.impl=com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS \
  --conf spark.sql.catalog.gcs_prod.io-impl=org.apache.iceberg.gcp.gcs.GCSFileIO \
  --conf spark.sql.catalog.gcs_prod.gcs.analytics-core.enabled=true \
  --conf spark.sql.iceberg.vectorization.enabled=true \
  --conf spark.sql.catalog.ajayky_biglake=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.ajayky_biglake.type=rest \
  --conf spark.sql.catalog.ajayky_biglake.rest.auth.type=oauth2 \
  --conf spark.sql.catalog.ajayky_biglake.uri=https://biglake.googleapis.com/iceberg/v1/restcatalog \
  --conf spark.sql.catalog.ajayky_biglake.header.x-goog-user-project=ajayky-test-project \
  --conf spark.sql.catalog.ajayky_biglake.header.X-Iceberg-Access-Delegation=vended-credentials \
  --conf spark.sql.catalog.ajayky_biglake.warehouse=gs://ajayky_biglake \
  --conf spark.sql.catalog.ajayky_biglake.rest.auth.type=org.apache.iceberg.gcp.auth.GoogleAuthManager \
  --conf spark.sql.catalog.ajayky_biglake.token-refresh-enabled=true \
  --conf spark.sql.catalog.ajayky_biglake.io-impl=org.apache.iceberg.gcp.gcs.GCSFileIO \
  --conf spark.sql.catalog.ajayky_biglake.gcs.analytics-core.enabled=true \
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

$SPARK_HOME/bin/spark-submit \
  --master spark://10.182.0.17:7077 \
  --jars $ICEBERG_SPARK_JAR \
  --packages ch.cern.sparkmeasure:spark-measure_2.13:0.27 \
  --conf spark.hadoop.hive.cli.print.header=true \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  --conf spark.sql.catalog.gcs_prod=org.apache.iceberg.spark.SparkCatalog  \
  --conf spark.sql.catalog.gcs_prod.type=hadoop  \
  --conf spark.sql.catalog.gcs_prod.warehouse=gs://ajayky-spark/hadoop-warehouse \
  --conf spark.sql.catalog.gcs_prod.hadoop.fs.AbstractFileSystem.gs.impl=com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS \
  --conf spark.hadoop.fs.AbstractFileSystem.gs.impl=com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS \
  --conf spark.sql.catalog.gcs_prod.io-impl=org.apache.iceberg.gcp.gcs.GCSFileIO \
  --conf spark.sql.iceberg.vectorization.enabled=true \
  --conf spark.hadoop.hive.cli.print.header=true \
--conf spark.sql.catalog.ajayky_biglake=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.ajayky_biglake.type=rest \
  --conf spark.sql.catalog.ajayky_biglake.rest.auth.type=oauth2 \
  --conf spark.sql.catalog.ajayky_biglake.uri=https://biglake.googleapis.com/iceberg/v1/restcatalog \
  --conf spark.sql.catalog.ajayky_biglake.header.x-goog-user-project=ajayky-test-project \
  --conf spark.sql.catalog.ajayky_biglake.header.X-Iceberg-Access-Delegation=vended-credentials \
  --conf spark.sql.catalog.ajayky_biglake.warehouse=gs://ajayky_biglake \
  --conf spark.sql.catalog.ajayky_biglake.rest.auth.type=org.apache.iceberg.gcp.auth.GoogleAuthManager \
  --conf spark.sql.catalog.ajayky_biglake.token-refresh-enabled=true \
  --conf spark.sql.catalog.ajayky_biglake.io-impl=org.apache.iceberg.gcp.gcs.GCSFileIO \
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


TPCDS_DATA_DB="tpcds_sf100"
TPCH_DATA_DB="tpch_sf100"

# The spark-submit command
$SPARK_HOME/bin/spark-submit \
  --master spark://10.182.0.17:7077 \
  --jars $ICEBERG_SPARK_JAR \
  --packages ch.cern.sparkmeasure:spark-measure_2.13:0.27 \
  --conf spark.hadoop.hive.cli.print.header=true \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  --conf spark.sql.catalog.gcs_prod=org.apache.iceberg.spark.SparkCatalog  \
  --conf spark.sql.catalog.gcs_prod.type=hadoop  \
  --conf spark.sql.catalog.gcs_prod.warehouse=gs://ajayky-spark/hadoop-warehouse \
  --conf spark.sql.catalog.gcs_prod.hadoop.fs.AbstractFileSystem.gs.impl=com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS \
  --conf spark.hadoop.fs.AbstractFileSystem.gs.impl=com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS \
  --conf spark.sql.catalog.gcs_prod.io-impl=org.apache.iceberg.gcp.gcs.GCSFileIO \
  --conf spark.sql.catalog.gcs_prod.gcs.analytics-core.enabled=true \
  --conf spark.sql.iceberg.vectorization.enabled=true \
  --conf spark.sql.catalog.ajayky_biglake=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.ajayky_biglake.type=rest \
  --conf spark.sql.catalog.ajayky_biglake.rest.auth.type=oauth2 \
  --conf spark.sql.catalog.ajayky_biglake.uri=https://biglake.googleapis.com/iceberg/v1/restcatalog \
  --conf spark.sql.catalog.ajayky_biglake.header.x-goog-user-project=ajayky-test-project \
  --conf spark.sql.catalog.ajayky_biglake.header.X-Iceberg-Access-Delegation=vended-credentials \
  --conf spark.sql.catalog.ajayky_biglake.warehouse=gs://ajayky_biglake \
  --conf spark.sql.catalog.ajayky_biglake.rest.auth.type=org.apache.iceberg.gcp.auth.GoogleAuthManager \
  --conf spark.sql.catalog.ajayky_biglake.token-refresh-enabled=true \
  --conf spark.sql.catalog.ajayky_biglake.io-impl=org.apache.iceberg.gcp.gcs.GCSFileIO \
  --conf spark.sql.catalog.ajayky_biglake.gcs.analytics-core.enabled=true \
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

$SPARK_HOME/bin/spark-submit \
  --master spark://10.182.0.17:7077 \
  --jars $ICEBERG_SPARK_JAR \
  --packages ch.cern.sparkmeasure:spark-measure_2.13:0.27 \
  --conf spark.hadoop.hive.cli.print.header=true \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  --conf spark.sql.catalog.gcs_prod=org.apache.iceberg.spark.SparkCatalog  \
  --conf spark.sql.catalog.gcs_prod.type=hadoop  \
  --conf spark.sql.catalog.gcs_prod.warehouse=gs://ajayky-spark/hadoop-warehouse \
  --conf spark.sql.catalog.gcs_prod.hadoop.fs.AbstractFileSystem.gs.impl=com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS \
  --conf spark.hadoop.fs.AbstractFileSystem.gs.impl=com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS \
  --conf spark.sql.catalog.gcs_prod.io-impl=org.apache.iceberg.gcp.gcs.GCSFileIO \
  --conf spark.sql.iceberg.vectorization.enabled=true \
  --conf spark.hadoop.hive.cli.print.header=true \
--conf spark.sql.catalog.ajayky_biglake=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.ajayky_biglake.type=rest \
  --conf spark.sql.catalog.ajayky_biglake.rest.auth.type=oauth2 \
  --conf spark.sql.catalog.ajayky_biglake.uri=https://biglake.googleapis.com/iceberg/v1/restcatalog \
  --conf spark.sql.catalog.ajayky_biglake.header.x-goog-user-project=ajayky-test-project \
  --conf spark.sql.catalog.ajayky_biglake.header.X-Iceberg-Access-Delegation=vended-credentials \
  --conf spark.sql.catalog.ajayky_biglake.warehouse=gs://ajayky_biglake \
  --conf spark.sql.catalog.ajayky_biglake.rest.auth.type=org.apache.iceberg.gcp.auth.GoogleAuthManager \
  --conf spark.sql.catalog.ajayky_biglake.token-refresh-enabled=true \
  --conf spark.sql.catalog.ajayky_biglake.io-impl=org.apache.iceberg.gcp.gcs.GCSFileIO \
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

TPCDS_DATA_DB="tpcds_sf1000"
TPCH_DATA_DB="tpch_sf1000"

# The spark-submit command
$SPARK_HOME/bin/spark-submit \
  --master spark://10.182.0.17:7077 \
  --jars $ICEBERG_SPARK_JAR \
  --packages ch.cern.sparkmeasure:spark-measure_2.13:0.27 \
  --conf spark.hadoop.hive.cli.print.header=true \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  --conf spark.sql.catalog.gcs_prod=org.apache.iceberg.spark.SparkCatalog  \
  --conf spark.sql.catalog.gcs_prod.type=hadoop  \
  --conf spark.sql.catalog.gcs_prod.warehouse=gs://ajayky-spark/hadoop-warehouse \
  --conf spark.sql.catalog.gcs_prod.hadoop.fs.AbstractFileSystem.gs.impl=com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS \
  --conf spark.hadoop.fs.AbstractFileSystem.gs.impl=com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS \
  --conf spark.sql.catalog.gcs_prod.io-impl=org.apache.iceberg.gcp.gcs.GCSFileIO \
  --conf spark.sql.catalog.gcs_prod.gcs.analytics-core.enabled=true \
  --conf spark.sql.iceberg.vectorization.enabled=true \
  --conf spark.sql.catalog.ajayky_biglake=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.ajayky_biglake.type=rest \
  --conf spark.sql.catalog.ajayky_biglake.rest.auth.type=oauth2 \
  --conf spark.sql.catalog.ajayky_biglake.uri=https://biglake.googleapis.com/iceberg/v1/restcatalog \
  --conf spark.sql.catalog.ajayky_biglake.header.x-goog-user-project=ajayky-test-project \
  --conf spark.sql.catalog.ajayky_biglake.header.X-Iceberg-Access-Delegation=vended-credentials \
  --conf spark.sql.catalog.ajayky_biglake.warehouse=gs://ajayky_biglake \
  --conf spark.sql.catalog.ajayky_biglake.rest.auth.type=org.apache.iceberg.gcp.auth.GoogleAuthManager \
  --conf spark.sql.catalog.ajayky_biglake.token-refresh-enabled=true \
  --conf spark.sql.catalog.ajayky_biglake.io-impl=org.apache.iceberg.gcp.gcs.GCSFileIO \
  --conf spark.sql.catalog.ajayky_biglake.gcs.analytics-core.enabled=true \
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

$SPARK_HOME/bin/spark-submit \
  --master spark://10.182.0.17:7077 \
  --jars $ICEBERG_SPARK_JAR \
  --packages ch.cern.sparkmeasure:spark-measure_2.13:0.27 \
  --conf spark.hadoop.hive.cli.print.header=true \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  --conf spark.sql.catalog.gcs_prod=org.apache.iceberg.spark.SparkCatalog  \
  --conf spark.sql.catalog.gcs_prod.type=hadoop  \
  --conf spark.sql.catalog.gcs_prod.warehouse=gs://ajayky-spark/hadoop-warehouse \
  --conf spark.sql.catalog.gcs_prod.hadoop.fs.AbstractFileSystem.gs.impl=com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS \
  --conf spark.hadoop.fs.AbstractFileSystem.gs.impl=com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS \
  --conf spark.sql.catalog.gcs_prod.io-impl=org.apache.iceberg.gcp.gcs.GCSFileIO \
  --conf spark.sql.iceberg.vectorization.enabled=true \
  --conf spark.hadoop.hive.cli.print.header=true \
--conf spark.sql.catalog.ajayky_biglake=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.ajayky_biglake.type=rest \
  --conf spark.sql.catalog.ajayky_biglake.rest.auth.type=oauth2 \
  --conf spark.sql.catalog.ajayky_biglake.uri=https://biglake.googleapis.com/iceberg/v1/restcatalog \
  --conf spark.sql.catalog.ajayky_biglake.header.x-goog-user-project=ajayky-test-project \
  --conf spark.sql.catalog.ajayky_biglake.header.X-Iceberg-Access-Delegation=vended-credentials \
  --conf spark.sql.catalog.ajayky_biglake.warehouse=gs://ajayky_biglake \
  --conf spark.sql.catalog.ajayky_biglake.rest.auth.type=org.apache.iceberg.gcp.auth.GoogleAuthManager \
  --conf spark.sql.catalog.ajayky_biglake.token-refresh-enabled=true \
  --conf spark.sql.catalog.ajayky_biglake.io-impl=org.apache.iceberg.gcp.gcs.GCSFileIO \
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