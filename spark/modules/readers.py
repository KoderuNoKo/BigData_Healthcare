from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
from google.cloud import bigquery
from modules.topics_schema import patient_schema

def read_local(
    spark: SparkSession,
    path: str,
    fmt: str,
    schema: StructType | None = None
):
    reader = spark.read.format(fmt)
    if schema:
        reader = reader.schema(schema)
    return reader.load(path)


def read_bigquery(
    spark: SparkSession,
    query: str,
    project: str
):
    """
    Uses Spark BigQuery connector.
    """
    with bigquery.Client(project=project) as client:
        pdf = client.query(query).to_dataframe()
        pdf = pdf.where(pdf.notna(), None)  # convert all pandas null (e.g. NaT, NaN, etc...) to None (spark compebility)
        return spark.createDataFrame(pdf, schema=patient_schema)