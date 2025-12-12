package com.google.cloud.gcs;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(
    name = "IcebergBenchmark",
    mixinStandardHelpOptions = true,
    version = "1.0",
    description = "Runs TPC-DS and TPC-H benchmarks against Iceberg on GCS.")
public class IcebergBenchmark implements Runnable {

  @Option(
      names = {"--tpcds-dir"},
      required = true,
      description = "TPC-DS query directory")
  private String tpcdsDir;

  @Option(
      names = {"--tpch-dir"},
      required = true,
      description = "TPC-H query directory")
  private String tpchDir;

  @Option(
      names = {"--tpcds-data-db"},
      required = true,
      description = "TPC-DS data database")
  private String tpcdsDataDb;

  @Option(
      names = {"--tpch-data-db"},
      required = true,
      description = "TPC-H data database")
  private String tpchDataDb;

  @Option(
      names = {"--catalog-name"},
      required = true,
      description = "Iceberg catalog name")
  private String catalogName;

  @Option(
      names = {"--results-db"},
      required = true,
      description = "Results database")
  private String resultsDb;

  private final String runId = UUID.randomUUID().toString();
  private final List<Map<String, Object>> resultsBuffer = new ArrayList<>();
  private SparkSession spark;
  private com.google.cloud.gcs.CustomMetricListener listener;
  private ObjectMapper mapper = new ObjectMapper();

  public static void main(String[] args) {
    int exitCode = new CommandLine(new IcebergBenchmark()).execute(args);
    System.exit(exitCode);
  }

  @Override
  public void run() {
    spark =
        SparkSession.builder()
            .appName("Iceberg GCS Benchmark Java")
            .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
            .config("spark.sql.debug.maxToStringFields", 1000)
            .getOrCreate();

    listener = new CustomMetricListener();
    spark.sparkContext().addSparkListener(listener);
    System.out.println("--- CustomMetricListener Registered ---");

    try {
      String resultsTableFqn = catalogName + "." + resultsDb + ".results";
      ensureResultsTableExists(resultsTableFqn);

      runBenchmark("TPC-DS", tpcdsDir, tpcdsDataDb, catalogName);
      runBenchmark("TPC-H", tpchDir, tpchDataDb, catalogName);

      flushResults(resultsTableFqn);
    } finally {
      spark.sparkContext().removeSparkListener(listener);
      spark.stop();
    }
  }

  private void ensureResultsTableExists(String tableName) {
    System.out.println("Ensuring results table '" + tableName + "' exists...");
    spark.sql(
        "CREATE TABLE IF NOT EXISTS "
            + tableName
            + " (\n"
            + "  run_id STRING,\n"
            + "  schema_size STRING,\n"
            + "  benchmark_type STRING,\n"
            + "  query_name STRING,\n"
            + "  execution_time_sec DOUBLE,\n"
            + "  status STRING,\n"
            + "  error_message STRING,\n"
            + "  metrics_json STRING,\n"
            + "  analytics_core_enabled BOOLEAN,\n"
            + "  client_type STRING,\n"
            + "  planning_scan_time_ms BIGINT,\n"
            + "  total_batch_scan_time_ms BIGINT,\n"
            + "  timestamp TIMESTAMP\n"
            + ") USING iceberg\n"
            + "PARTITIONED BY (days(timestamp))");
    System.out.println("Table '" + tableName + "' is ready.");
  }

  private void runBenchmark(
      String benchmarkName, String queriesPath, String dbName, String catalogName) {
    System.out.println(
        "\nRunning " + benchmarkName + " benchmark from " + queriesPath + " on " + dbName);
    spark.sql("USE " + catalogName + "." + dbName);

    Path queryDir = Paths.get(queriesPath);
    if (!Files.exists(queryDir)) {
      System.out.println("Warning: Query directory not found: " + queriesPath);
      return;
    }

    try {
      List<Path> sqlFiles =
          Files.list(queryDir)
              .filter(Files::isRegularFile)
              .filter(p -> p.toString().endsWith(".sql"))
              .sorted(Comparator.comparing(p -> p.getFileName().toString()))
              .collect(Collectors.toList());

      System.out.println("Found " + sqlFiles.size() + " queries for " + benchmarkName);

      boolean analyticsCoreEnabled =
          Boolean.parseBoolean(
              spark
                  .conf()
                  .get(
                      "spark.sql.catalog." + catalogName + ".gcs.analytics-core.enabled", "false"));
      String clientType = "HTTP";
      if ("GRPC_CLIENT"
          .equals(
              spark
                  .conf()
                  .get("spark.sql.catalog." + catalogName + ".gcs.client.type", "HTTP_CLIENT"))) {
        clientType = "GRPC";
      }

      for (Path sqlFile : sqlFiles) {
        String queryName = sqlFile.getFileName().toString();
        System.out.println("Running " + benchmarkName + " query: " + queryName);
        String querySql = new String(Files.readAllBytes(sqlFile));
        querySql = querySql.replace("${database}", catalogName).replace("${schema}", dbName);

        String status = "SUCCESS";
        String errorMessage = null;
        long startTime = System.nanoTime();

        listener.resetMetrics(); // Reset metrics for the current query

        try {
          spark.sql(querySql).write().format("noop").mode(SaveMode.Overwrite).save();
        } catch (Exception e) {
          status = "FAILED";
          errorMessage = e.toString().substring(0, Math.min(e.toString().length(), 2000));
          System.out.println("  -> FAILED: " + queryName + ". Error: " + errorMessage);
        }
        long endTime = System.nanoTime();
        double durationSec = (endTime - startTime) / 1e9;

        Map<String, String> metrics = listener.getMetrics();
        String metricsJson = "{}";
        try {
          metricsJson = mapper.writeValueAsString(metrics);
        } catch (Exception e) {
          System.err.println("Error serializing metrics to JSON: " + e.getMessage());
        }

        Map<String, Object> result = new HashMap<>();
        result.put("run_id", runId);
        result.put("schema_size", dbName);
        result.put("benchmark_type", benchmarkName);
        result.put("query_name", queryName);
        result.put("execution_time_sec", durationSec);
        result.put("status", status);
        result.put("error_message", errorMessage);
        result.put("metrics_json", metricsJson);
        result.put("analytics_core_enabled", analyticsCoreEnabled);
        result.put("client_type", clientType);
        result.put("planning_scan_time_ms", 0L); // Deprecated
        result.put(
            "total_batch_scan_time_ms", metrics.get("total_batch_scan_time_ms")); // Deprecated
        result.put("timestamp", Timestamp.from(Instant.now()));
        resultsBuffer.add(result);
        System.out.println(
            "  -> Buffered result for "
                + queryName
                + ": "
                + status
                + ", Metrics JSON: "
                + metricsJson);
      }
    } catch (IOException e) {
      System.err.println("Error listing SQL files: " + e.getMessage());
    }
  }

  private void flushResults(String tableName) {
    if (resultsBuffer.isEmpty()) {
      System.out.println("No results to flush.");
      return;
    }
    System.out.println("Flushing " + resultsBuffer.size() + " results to " + tableName);

    List<Row> rows =
        resultsBuffer.stream()
            .map(
                map ->
                    RowFactory.create(
                        map.get("run_id"),
                        map.get("schema_size"),
                        map.get("benchmark_type"),
                        map.get("query_name"),
                        map.get("execution_time_sec"),
                        map.get("status"),
                        map.get("error_message"),
                        map.get("metrics_json"),
                        map.get("analytics_core_enabled"),
                        map.get("client_type"),
                        map.get("planning_scan_time_ms"),
                        map.get("total_batch_scan_time_ms"),
                        map.get("timestamp")))
            .collect(Collectors.toList());

    Dataset<Row> resultsDF = spark.createDataFrame(rows, spark.table(tableName).schema());
    resultsDF.write().format("iceberg").mode(SaveMode.Append).save(tableName);

    System.out.println("  -> Flushed " + resultsBuffer.size() + " results.");
    resultsBuffer.clear();
  }
}
