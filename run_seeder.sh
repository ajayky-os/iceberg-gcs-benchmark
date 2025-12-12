#!/bin/bash

set -e # Exit on error

# --- Configuration ---
BENCHMARK_TYPE="tpcds"   # "tpcds" or "tpch"
SCALE_FACTOR="1"          # Scale factor (e.g., 1, 10, 100)
ICEBERG_CATALOG="my_iceberg" # Iceberg catalog name
DATABASE_NAME="${BENCHMARK_TYPE}_sf${SCALE_FACTOR}"
GCS_BUCKET="your-gcs-bucket-name" # !!! REPLACE with your GCS bucket !!!
PARTITION_DATA=true     # true or false
FILE_FORMAT="parquet"

# Tool paths
TOOLS_DIR="$(pwd)/tools"
TPCDS_DIR="${TOOLS_DIR}/TPC-DS"
TPCH_DIR="${TOOLS_DIR}/TPC-H"
DSDGEN_PATH="${TPCDS_DIR}/tools/dsdgen"
DBGEN_PATH="${TPCH_DIR}/dbgen"
LOCAL_TMP_DIR="/tmp/seeder_data_gen"

# Spark settings
SPARK_HOME="/path/to/your/spark"  # !!! REPLACE with your Spark installation directory !!!
SPARK_PACKAGES="org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.5.2,com.google.cloud.bigdataoss:gcs-connector:hadoop3-2.2.22"

# --- Helper Functions ---
log_info() {
    echo "[INFO] $(date +'%Y-%m-%d %H:%M:%S') $1"
}

install_tpcds() {
    # Install bison if yacc is not available
    if ! command -v yacc &> /dev/null
    then
        log_info "yacc not found, attempting to install bison..."
        if command -v apt-get &> /dev/null
        then
            sudo apt-get update && sudo apt-get install -y bison
        elif command -v dnf &> /dev/null
        then
            sudo dnf install -y bison
        elif command -v yum &> /dev/null
        then
            sudo yum install -y bison
        else
            log_info "Cannot find apt-get, dnf, or yum. Please install bison manually."
            exit 1
        fi
        if ! command -v yacc &> /dev/null; then # Still not found
             log_info "Installation of bison failed or yacc is not in PATH. Please install bison and ensure yacc is available."
             exit 1
        fi
    fi
    # Install flex if not available
    if ! command -v flex &> /dev/null
    then
        log_info "flex not found, attempting to install flex..."
        if command -v apt-get &> /dev/null
        then
            sudo apt-get update && sudo apt-get install -y flex
        elif command -v dnf &> /dev/null
        then
            sudo dnf install -y flex
        elif command -v yum &> /dev/null
        then
            sudo yum install -y flex
        else
            log_info "Cannot find apt-get, dnf, or yum. Please install flex manually."
            exit 1
        fi
        if ! command -v flex &> /dev/null; then # Still not found
             log_info "Installation of flex failed or flex is not in PATH. Please install flex and ensure flex is available."
             exit 1
        fi
    fi

    if [ ! -d "${TPCDS_DIR}" ]; then
        log_info "Cloning TPC-DS tools..."
        # !!! REPLACE with the correct Git repository for TPC-DS tools !!!
        git clone https://github.com/databricks/tpcds-kit.git "${TPCDS_DIR}"
    else
        log_info "TPC-DS directory already exists: ${TPCDS_DIR}"
    fi


    # Patch C files to fix build errors with modern compilers
    log_info "Patching TPC-DS C files..."
    sed -i 's/getDateWeightFromJulian(jDay, nDistribution)/getDateWeightFromJulian(int jDay, int nDistribution)/' "${TPCDS_DIR}/tools/date.c"
    sed -i 's/getTdefFunctionsByNumber(nTable)/getTdefFunctionsByNumber(int nTable)/' "${TPCDS_DIR}/tools/tdef_functions.c"
    # Multi-line sed command for getSimpleTdefsByNumber in tdefs.c
    sed -i -e ':a' -e 'N' -e '$!ba' -e 's/tdef \*\ngetSimpleTdefsByNumber(nTable)/tdef *\ngetSimpleTdefsByNumber(int nTable)/' "${TPCDS_DIR}/tools/tdefs.c"
    log_info "Finished patching."

    if [ ! -f "${DSDGEN_PATH}" ]; then
        log_info "Building TPC-DS tools (dsdgen)..."
        cd "${TPCDS_DIR}/tools"
        make OS=LINUX
        cd -
    else
        log_info "dsdgen already built: ${DSDGEN_PATH}"
    fi
}

install_tpch() {
    if [ ! -d "${TPCH_DIR}" ]; then
        log_info "Cloning TPC-H tools..."
        # !!! REPLACE with the correct Git repository for TPC-H tools !!!
        git clone https://github.com/databricks/tpch-dbgen.git "${TPCH_DIR}"
    else
        log_info "TPC-H directory already exists: ${TPCH_DIR}"
    fi

    if [ ! -f "${DBGEN_PATH}" ]; then
        log_info "Building TPC-H tools (dbgen)..."
        cd "${TPCH_DIR}"
        make
        cd -
    else
        log_info "dbgen already built: ${DBGEN_PATH}"
    fi
}

# --- Main Script ---
mkdir -p "${TOOLS_DIR}"
mkdir -p "${LOCAL_TMP_DIR}"

SEEDER_ARGS=()

if [ "${BENCHMARK_TYPE}" == "tpcds" ]; then
    install_tpcds
    SEEDER_ARGS+=(--dsdgen-path "${DSDGEN_PATH}")
elif [ "${BENCHMARK_TYPE}" == "tpch" ]; then
    install_tpch
    SEEDER_ARGS+=(--dbgen-path "${DBGEN_PATH}")
else
    echo "[ERROR] Invalid BENCHMARK_TYPE: ${BENCHMARK_TYPE}" >&2
    exit 1
fi

if [ "${PARTITION_DATA}" = true ]; then
    SEEDER_ARGS+=(--partition-data)
fi

log_info "Running seeder.py for ${BENCHMARK_TYPE} SF${SCALE_FACTOR}..."

if command -v spark-submit &> /dev/null
then
    SPARK_SUBMIT="spark-submit"
else
    SPARK_SUBMIT="${SPARK_HOME}/bin/spark-submit"
    if [ ! -f "${SPARK_SUBMIT}" ]; then
        log_info "spark-submit not found in PATH or at ${SPARK_SUBMIT}. Please install Spark or set SPARK_HOME."
        exit 1
    fi
fi

log_info "Using SPARK_SUBMIT: ${SPARK_SUBMIT}"

${SPARK_SUBMIT} \
    --packages "${SPARK_PACKAGES}" \
    seeder.py \
    --benchmark-type "${BENCHMARK_TYPE}" \
    --scale-factor "${SCALE_FACTOR}" \
    --iceberg-catalog "${ICEBERG_CATALOG}" \
    --database "${DATABASE_NAME}" \
    --bucket "${GCS_BUCKET}" \
    --file-format "${FILE_FORMAT}" \
    --local-tmp-dir "${LOCAL_TMP_DIR}" \
    "${SEEDER_ARGS[@]}"

log_info "Seeder script finished."

# Optional: Clean up local tmp dir
# rm -rf "${LOCAL_TMP_DIR}"
