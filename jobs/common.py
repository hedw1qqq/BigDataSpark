import os
import urllib.parse
import urllib.request

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    BooleanType,
    ByteType,
    DateType,
    DecimalType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    ShortType,
    StringType,
    TimestampType,
)


def env(name: str, default: str) -> str:
    return os.getenv(name, default)


def build_spark(app_name: str) -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        .master(env("SPARK_MASTER", "local[*]"))
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


def postgres_options() -> dict[str, str]:
    return {
        "url": (
            f"jdbc:postgresql://{env('POSTGRES_HOST', 'postgres')}:"
            f"{env('POSTGRES_PORT', '5432')}/{env('POSTGRES_DB', 'bdspark')}"
        ),
        "user": env("POSTGRES_USER", "postgres"),
        "password": env("POSTGRES_PASSWORD", "postgres"),
        "driver": "org.postgresql.Driver",
    }


def clickhouse_options() -> dict[str, str]:
    return {
        "url": (
            f"jdbc:clickhouse://{env('CLICKHOUSE_HOST', 'clickhouse')}:"
            f"{env('CLICKHOUSE_PORT', '8123')}/{env('CLICKHOUSE_DB', 'analytics')}"
            "?socket_timeout=300000"
        ),
        "user": env("CLICKHOUSE_USER", "etl"),
        "password": env("CLICKHOUSE_PASSWORD", "etl_password"),
        "driver": "com.clickhouse.jdbc.ClickHouseDriver",
    }


def clickhouse_http_url() -> str:
    query = urllib.parse.urlencode(
        {
            "database": env("CLICKHOUSE_DB", "analytics"),
            "user": env("CLICKHOUSE_USER", "etl"),
            "password": env("CLICKHOUSE_PASSWORD", "etl_password"),
        }
    )
    return (
        f"http://{env('CLICKHOUSE_HOST', 'clickhouse')}:"
        f"{env('CLICKHOUSE_PORT', '8123')}/?{query}"
    )


def clickhouse_execute(query: str) -> None:
    request = urllib.request.Request(
        clickhouse_http_url(),
        data=query.encode("utf-8"),
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=30):
        return


def clickhouse_type(field) -> str:
    data_type = field.dataType

    if isinstance(data_type, StringType):
        return "String"
    if isinstance(data_type, IntegerType):
        return "Int32"
    if isinstance(data_type, LongType):
        return "Int64"
    if isinstance(data_type, ShortType):
        return "Int16"
    if isinstance(data_type, ByteType):
        return "Int8"
    if isinstance(data_type, DoubleType):
        return "Float64"
    if isinstance(data_type, FloatType):
        return "Float32"
    if isinstance(data_type, DecimalType):
        return f"Decimal({data_type.precision}, {data_type.scale})"
    if isinstance(data_type, DateType):
        return "Date"
    if isinstance(data_type, TimestampType):
        return "DateTime"
    if isinstance(data_type, BooleanType):
        return "Bool"

    raise ValueError(f"Unsupported ClickHouse type for field {field.name}: {data_type}")


def parse_order_by_columns(order_by: str) -> set[str]:
    if order_by.strip() == "tuple()":
        return set()

    cleaned = order_by.strip().strip("()")
    columns = [part.strip() for part in cleaned.split(",") if part.strip()]
    return {column for column in columns if column.isidentifier()}


def ensure_clickhouse_table(df, table_name: str, order_by: str) -> None:
    database_name, relation_name = table_name.split(".", 1)
    order_by_columns = parse_order_by_columns(order_by)

    clickhouse_execute(f"CREATE DATABASE IF NOT EXISTS {database_name}")
    clickhouse_execute(f"DROP TABLE IF EXISTS {database_name}.{relation_name}")

    columns = []
    for field in df.schema.fields:
        base_type = clickhouse_type(field)
        column_type = base_type if field.name in order_by_columns else f"Nullable({base_type})"
        columns.append(f"{field.name} {column_type}")

    ddl = (
        f"CREATE TABLE {database_name}.{relation_name} "
        f"({', '.join(columns)}) "
        f"ENGINE = MergeTree() ORDER BY {order_by}"
    )
    clickhouse_execute(ddl)


def read_postgres_table(spark: SparkSession, table_name: str):
    return spark.read.format("jdbc").options(**postgres_options(), dbtable=table_name).load()


def write_postgres_table(df, table_name: str, mode: str = "overwrite") -> None:
    (
        df.write.format("jdbc")
        .mode(mode)
        .options(**postgres_options(), dbtable=table_name)
        .save()
    )


def write_clickhouse_table(
    df,
    table_name: str,
    mode: str = "overwrite",
    order_by: str = "tuple()",
) -> None:
    ensure_clickhouse_table(df, table_name, order_by)
    (
        df.write.format("jdbc")
        .mode("append")
        .options(**clickhouse_options(), dbtable=table_name)
        .save()
    )
