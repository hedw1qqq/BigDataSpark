import math

from pyspark.sql import Window
from pyspark.sql import functions as F

from common import build_spark, read_postgres_table, write_clickhouse_table


spark = build_spark("reports-to-clickhouse")

fact_sales = read_postgres_table(spark, "dwh.fact_sales")
dim_products = read_postgres_table(spark, "dwh.dim_products")
dim_customers = read_postgres_table(spark, "dwh.dim_customers")
dim_stores = read_postgres_table(spark, "dwh.dim_stores")
dim_suppliers = read_postgres_table(spark, "dwh.dim_suppliers")
dim_dates = read_postgres_table(spark, "dwh.dim_dates")

sales = (
    fact_sales.alias("f")
    .join(dim_products.alias("p"), "product_key")
    .join(dim_customers.alias("c"), "customer_key")
    .join(dim_stores.alias("s"), "store_key")
    .join(dim_suppliers.alias("sp"), "supplier_key")
    .join(dim_dates.alias("d"), "date_key")
    .select(
        "sale_key",
        "sale_date",
        "sale_quantity",
        "sale_total_price",
        "product_unit_price",
        "order_count",
        "product_key",
        "product_name",
        "product_category",
        "product_brand",
        "product_color",
        "product_size",
        "product_rating",
        "product_reviews",
        "product_price",
        "customer_key",
        "customer_full_name",
        "customer_country",
        "store_key",
        "store_name",
        "store_city",
        "store_country",
        "supplier_key",
        "supplier_name",
        "supplier_country",
        "calendar_year",
        "calendar_month",
        F.to_date(F.date_trunc("month", F.col("full_date"))).alias("month_start"),
    )
)

product_window = Window.partitionBy("product_category")
product_rank_window = Window.orderBy(F.desc("total_units_sold"), F.desc("total_revenue"))

product_sales = (
    sales.groupBy(
        "product_key",
        "product_name",
        "product_category",
        "product_brand",
        "product_color",
        "product_size",
    )
    .agg(
        F.round(F.sum("sale_total_price"), 2).alias("total_revenue"),
        F.sum("sale_quantity").cast("long").alias("total_units_sold"),
        F.countDistinct("sale_key").cast("long").alias("total_orders"),
        F.round(F.avg("product_rating"), 2).alias("avg_rating"),
        F.max("product_reviews").cast("long").alias("total_reviews"),
    )
    .withColumn("category_revenue", F.round(F.sum("total_revenue").over(product_window), 2))
    .withColumn("product_rank_by_units", F.dense_rank().over(product_rank_window))
)

customer_rank_window = Window.orderBy(F.desc("total_spent"), F.desc("total_orders"))
customer_country_window = Window.partitionBy("customer_country")

customer_sales = (
    sales.groupBy("customer_key", "customer_full_name", "customer_country")
    .agg(
        F.round(F.sum("sale_total_price"), 2).alias("total_spent"),
        F.countDistinct("sale_key").cast("long").alias("total_orders"),
    )
    .withColumn("avg_check", F.round(F.col("total_spent") / F.col("total_orders"), 2))
    .withColumn("country_customer_count", F.count("*").over(customer_country_window))
    .withColumn("customer_rank_by_spend", F.dense_rank().over(customer_rank_window))
)

time_rank_window = Window.orderBy("month_start")
time_year_window = Window.partitionBy("calendar_year")

time_sales = (
    sales.groupBy("calendar_year", "calendar_month", "month_start")
    .agg(
        F.round(F.sum("sale_total_price"), 2).alias("total_revenue"),
        F.countDistinct("sale_key").cast("long").alias("total_orders"),
    )
    .withColumn("avg_order_amount", F.round(F.col("total_revenue") / F.col("total_orders"), 2))
    .withColumn("previous_month_revenue", F.lag("total_revenue").over(time_rank_window))
    .withColumn(
        "revenue_delta",
        F.round(F.col("total_revenue") - F.coalesce(F.col("previous_month_revenue"), F.lit(0.0)), 2),
    )
    .withColumn(
        "revenue_delta_pct",
        F.when(
            F.col("previous_month_revenue").isNull() | (F.col("previous_month_revenue") == 0),
            None,
        ).otherwise(
            F.round(
                (F.col("total_revenue") - F.col("previous_month_revenue"))
                / F.col("previous_month_revenue")
                * 100,
                2,
            )
        ),
    )
    .withColumn("calendar_year_revenue", F.round(F.sum("total_revenue").over(time_year_window), 2))
)

store_rank_window = Window.orderBy(F.desc("total_revenue"), F.desc("total_orders"))
store_city_window = Window.partitionBy("store_city", "store_country")

store_sales = (
    sales.groupBy("store_key", "store_name", "store_city", "store_country")
    .agg(
        F.round(F.sum("sale_total_price"), 2).alias("total_revenue"),
        F.countDistinct("sale_key").cast("long").alias("total_orders"),
    )
    .withColumn("avg_check", F.round(F.col("total_revenue") / F.col("total_orders"), 2))
    .withColumn("city_country_revenue", F.round(F.sum("total_revenue").over(store_city_window), 2))
    .withColumn("store_rank_by_revenue", F.dense_rank().over(store_rank_window))
)

supplier_price = (
    sales.select("supplier_key", "product_key", "product_price")
    .dropDuplicates(["supplier_key", "product_key"])
    .groupBy("supplier_key")
    .agg(F.round(F.avg("product_price"), 2).alias("avg_product_price"))
)

supplier_rank_window = Window.orderBy(F.desc("total_revenue"), F.desc("total_orders"))
supplier_country_window = Window.partitionBy("supplier_country")

supplier_sales = (
    sales.groupBy("supplier_key", "supplier_name", "supplier_country")
    .agg(
        F.round(F.sum("sale_total_price"), 2).alias("total_revenue"),
        F.sum("sale_quantity").cast("long").alias("total_units_sold"),
        F.countDistinct("sale_key").cast("long").alias("total_orders"),
    )
    .join(supplier_price, "supplier_key", "left")
    .withColumn(
        "supplier_country_revenue",
        F.round(F.sum("total_revenue").over(supplier_country_window), 2),
    )
    .withColumn("supplier_rank_by_revenue", F.dense_rank().over(supplier_rank_window))
)

quality_desc_window = Window.orderBy(F.desc("avg_rating"), F.desc("total_units_sold"))
quality_asc_window = Window.orderBy(F.asc("avg_rating"), F.desc("total_units_sold"))
quality_reviews_window = Window.orderBy(F.desc("total_reviews"), F.desc("total_units_sold"))

quality_base = product_sales.select(
    "product_key",
    "product_name",
    "product_category",
    "product_brand",
    "avg_rating",
    "total_reviews",
    "total_units_sold",
    "total_revenue",
)

corr_value = quality_base.agg(F.corr("avg_rating", "total_units_sold").alias("corr_value")).collect()[0][
    "corr_value"
]

if corr_value is None or math.isnan(corr_value):
    corr_value = 0.0

product_quality = (
    quality_base.withColumn("rating_rank_desc", F.dense_rank().over(quality_desc_window))
    .withColumn("rating_rank_asc", F.dense_rank().over(quality_asc_window))
    .withColumn("reviews_rank", F.dense_rank().over(quality_reviews_window))
    .withColumn("rating_sales_correlation", F.lit(round(float(corr_value), 6)))
)

write_clickhouse_table(
    product_sales,
    "analytics.report_product_sales",
    order_by="(product_rank_by_units, product_key)",
)
write_clickhouse_table(
    customer_sales,
    "analytics.report_customer_sales",
    order_by="(customer_rank_by_spend, customer_key)",
)
write_clickhouse_table(
    time_sales,
    "analytics.report_time_sales",
    order_by="(calendar_year, calendar_month)",
)
write_clickhouse_table(
    store_sales,
    "analytics.report_store_sales",
    order_by="(store_rank_by_revenue, store_key)",
)
write_clickhouse_table(
    supplier_sales,
    "analytics.report_supplier_sales",
    order_by="(supplier_rank_by_revenue, supplier_key)",
)
write_clickhouse_table(
    product_quality,
    "analytics.report_product_quality",
    order_by="(rating_rank_desc, product_key)",
)

print(f"report_product_sales rows: {product_sales.count()}")
print(f"report_customer_sales rows: {customer_sales.count()}")
print(f"report_time_sales rows: {time_sales.count()}")
print(f"report_store_sales rows: {store_sales.count()}")
print(f"report_supplier_sales rows: {supplier_sales.count()}")
print(f"report_product_quality rows: {product_quality.count()}")

spark.stop()
