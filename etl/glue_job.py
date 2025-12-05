import sys

from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    to_date,
    col,
    when,
    current_timestamp,
    count,
    avg,
    desc,
)
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType,
    DoubleType,
    DateType,
)

# Get job parameters
args = getResolvedOptions(sys.argv, ["JOB_NAME"])

# Initialize Spark and Glue contexts
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Get S3 bucket from environment or use default
s3_bucket = spark.conf.get(
    "spark.sql.catalog.glue_catalog.warehouse", "s3://default-bucket/warehouse/"
)

# Configure Iceberg
spark.conf.set(
    "spark.sql.extensions",
    "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
)
spark.conf.set("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog")
spark.conf.set(
    "spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog"
)
spark.conf.set(
    "spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO"
)


def create_sample_data() -> DataFrame:
    """Create sample data for demonstration."""
    schema = StructType(
        [
            StructField("id", IntegerType(), True),
            StructField("name", StringType(), True),
            StructField("age", IntegerType(), True),
            StructField("city", StringType(), True),
            StructField("salary", DoubleType(), True),
            StructField("created_date", DateType(), True),
        ]
    )

    data = [
        (1, "John Doe", 30, "New York", 75000.0, "2023-01-15"),
        (2, "Jane Smith", 25, "Los Angeles", 65000.0, "2023-02-20"),
        (3, "Mike Johnson", 35, "Chicago", 80000.0, "2023-03-10"),
        (4, "Sarah Wilson", 28, "Houston", 70000.0, "2023-04-05"),
        (5, "David Brown", 32, "Phoenix", 72000.0, "2023-05-12"),
    ]

    df = spark.createDataFrame(data, schema)
    df = df.withColumn("created_date", to_date(col("created_date")))
    return df


def transform_data(df: DataFrame) -> DataFrame:
    """Apply transformations to the data."""
    transformed_df = (
        df.withColumn(
            "salary_category",
            when(col("salary") >= 75000, "High")
            .when(col("salary") >= 65000, "Medium")
            .otherwise("Low"),
        )
        .withColumn(
            "age_group",
            when(col("age") < 30, "Young").when(col("age") < 35, "Mid").otherwise("Senior"),
        )
        .withColumn("processed_timestamp", current_timestamp())
    )

    return transformed_df


def write_to_iceberg(df: DataFrame, table_name: str, database_name: str) -> None:
    """Write DataFrame to Iceberg table."""
    # Create database if not exists
    spark.sql(f"CREATE DATABASE IF NOT EXISTS glue_catalog.{database_name}")

    # Write to Iceberg table
    df.writeTo(f"glue_catalog.{database_name}.{table_name}").using("iceberg").createOrReplace()

    print(f"Data written to Iceberg table: {database_name}.{table_name}")


def read_from_iceberg(table_name: str, database_name: str) -> DataFrame:
    """Read DataFrame from Iceberg table."""
    return spark.table(f"glue_catalog.{database_name}.{table_name}")


def perform_analytics(df: DataFrame) -> None:
    """Perform some analytics on the data."""
    print("=== Data Analytics ===")

    # Show basic statistics
    total_records = df.count()
    print("Total records:", total_records)

    # Group by salary category
    salary_stats = (
        df.groupBy("salary_category")
        .agg(count("*").alias("count"), avg("salary").alias("avg_salary"), avg("age").alias("avg_age"))
    )

    print("Salary Category Statistics:")
    salary_stats.show()

    # Group by city
    city_stats = (
        df.groupBy("city")
        .agg(count("*").alias("employee_count"), avg("salary").alias("avg_salary"))
        .orderBy(desc("avg_salary"))
    )

    print("City Statistics:")
    city_stats.show()


def main() -> None:
    """Main ETL process."""
    try:
        print("Starting Iceberg ETL Job...")

        # Step 1: Create or load source data
        print("Step 1: Creating sample data...")
        source_df = create_sample_data()
        source_df.show()

        # Step 2: Transform data
        print("Step 2: Transforming data...")
        transformed_df = transform_data(source_df)
        transformed_df.show()

        # Step 3: Write to Iceberg table
        print("Step 3: Writing to Iceberg table...")
        database_name = "dev_iceberg_db"  # This should match your Glue database
        table_name = "employee_data"

        write_to_iceberg(transformed_df, table_name, database_name)

        # Step 4: Read back from Iceberg and perform analytics
        print("Step 4: Reading from Iceberg and performing analytics...")
        iceberg_df = read_from_iceberg(table_name, database_name)
        perform_analytics(iceberg_df)

        # Step 5: Demonstrate Iceberg features
        print("Step 5: Demonstrating Iceberg time travel...")

        # Show table history
        history_df = spark.sql(f"SELECT * FROM glue_catalog.{database_name}.{table_name}.history")
        print("Table History:")
        history_df.show()

        # Show table snapshots
        snapshots_df = spark.sql(f"SELECT * FROM glue_catalog.{database_name}.{table_name}.snapshots")
        print("Table Snapshots:")
        snapshots_df.show()

        print("ETL Job completed successfully!")

    except Exception as exc:  # noqa: B902 - let exception bubble after logging
        print(f"Error in ETL job: {exc}")
        raise


if __name__ == "__main__":
    main()
    job.commit()
