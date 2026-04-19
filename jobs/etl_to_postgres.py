from pyspark.sql import Window
from pyspark.sql import functions as F

from common import build_spark, read_postgres_table, write_postgres_table


def cleaned_text(column_name: str):
    value = F.trim(F.col(column_name))
    return F.when(value == "", None).otherwise(value)


def parsed_double(column_name: str):
    return cleaned_text(column_name).cast("double")


def parsed_int(column_name: str):
    return cleaned_text(column_name).cast("int")


def parsed_date(column_name: str):
    return F.to_date(cleaned_text(column_name), "M/d/yyyy")


def hashed_key(*column_names: str):
    parts = [F.coalesce(F.col(name).cast("string"), F.lit("")) for name in column_names]
    return F.sha2(F.concat_ws("||", *parts), 256)


def with_surrogate_key(df, key_column: str, surrogate_column: str):
    window = Window.orderBy(key_column)
    return df.withColumn(surrogate_column, F.row_number().over(window))


spark = build_spark("etl-to-postgres")

raw = read_postgres_table(spark, "raw.mock_data")

base = raw.select(
    F.col("source_row_id").cast("long").alias("source_row_id"),
    parsed_int("id").alias("raw_id"),
    cleaned_text("customer_first_name").alias("customer_first_name"),
    cleaned_text("customer_last_name").alias("customer_last_name"),
    parsed_int("customer_age").alias("customer_age"),
    cleaned_text("customer_email").alias("customer_email"),
    cleaned_text("customer_country").alias("customer_country"),
    cleaned_text("customer_postal_code").alias("customer_postal_code"),
    cleaned_text("customer_pet_type").alias("customer_pet_type"),
    cleaned_text("customer_pet_name").alias("customer_pet_name"),
    cleaned_text("customer_pet_breed").alias("customer_pet_breed"),
    cleaned_text("seller_first_name").alias("seller_first_name"),
    cleaned_text("seller_last_name").alias("seller_last_name"),
    cleaned_text("seller_email").alias("seller_email"),
    cleaned_text("seller_country").alias("seller_country"),
    cleaned_text("seller_postal_code").alias("seller_postal_code"),
    cleaned_text("product_name").alias("product_name"),
    cleaned_text("product_category").alias("product_category"),
    parsed_double("product_price").alias("product_price"),
    parsed_int("product_quantity").alias("product_available_quantity"),
    parsed_date("sale_date").alias("sale_date"),
    parsed_int("sale_quantity").alias("sale_quantity"),
    parsed_double("sale_total_price").alias("sale_total_price"),
    cleaned_text("store_name").alias("store_name"),
    cleaned_text("store_location").alias("store_location"),
    cleaned_text("store_city").alias("store_city"),
    cleaned_text("store_state").alias("store_state"),
    cleaned_text("store_country").alias("store_country"),
    cleaned_text("store_phone").alias("store_phone"),
    cleaned_text("store_email").alias("store_email"),
    cleaned_text("pet_category").alias("pet_category"),
    parsed_double("product_weight").alias("product_weight"),
    cleaned_text("product_color").alias("product_color"),
    cleaned_text("product_size").alias("product_size"),
    cleaned_text("product_brand").alias("product_brand"),
    cleaned_text("product_material").alias("product_material"),
    cleaned_text("product_description").alias("product_description"),
    parsed_double("product_rating").alias("product_rating"),
    parsed_int("product_reviews").alias("product_reviews"),
    parsed_date("product_release_date").alias("product_release_date"),
    parsed_date("product_expiry_date").alias("product_expiry_date"),
    cleaned_text("supplier_name").alias("supplier_name"),
    cleaned_text("supplier_contact").alias("supplier_contact"),
    cleaned_text("supplier_email").alias("supplier_email"),
    cleaned_text("supplier_phone").alias("supplier_phone"),
    cleaned_text("supplier_address").alias("supplier_address"),
    cleaned_text("supplier_city").alias("supplier_city"),
    cleaned_text("supplier_country").alias("supplier_country"),
)

enriched = (
    base.withColumn(
        "customer_nk",
        hashed_key("customer_email", "customer_first_name", "customer_last_name"),
    )
    .withColumn(
        "seller_nk",
        hashed_key("seller_email", "seller_first_name", "seller_last_name"),
    )
    .withColumn(
        "supplier_nk",
        hashed_key("supplier_email", "supplier_name", "supplier_phone"),
    )
    .withColumn(
        "store_nk",
        hashed_key(
            "store_name",
            "store_email",
            "store_phone",
            "store_city",
            "store_country",
            "store_location",
            "store_state",
        ),
    )
    .withColumn(
        "product_nk",
        hashed_key("product_name", "product_category", "product_brand"),
    )
    .withColumn("date_key", F.date_format("sale_date", "yyyyMMdd").cast("int"))
)

dim_customers = with_surrogate_key(
    enriched.select(
        "customer_nk",
        "customer_first_name",
        "customer_last_name",
        "customer_age",
        "customer_email",
        "customer_country",
        "customer_postal_code",
        "customer_pet_type",
        "customer_pet_name",
        "customer_pet_breed",
        "pet_category",
    ).dropDuplicates(["customer_nk"]),
    "customer_nk",
    "customer_key",
).select(
    "customer_key",
    "customer_nk",
    "customer_first_name",
    "customer_last_name",
    F.concat_ws(" ", "customer_first_name", "customer_last_name").alias("customer_full_name"),
    "customer_age",
    "customer_email",
    "customer_country",
    "customer_postal_code",
    "customer_pet_type",
    "customer_pet_name",
    "customer_pet_breed",
    "pet_category",
)

dim_sellers = with_surrogate_key(
    enriched.select(
        "seller_nk",
        "seller_first_name",
        "seller_last_name",
        "seller_email",
        "seller_country",
        "seller_postal_code",
    ).dropDuplicates(["seller_nk"]),
    "seller_nk",
    "seller_key",
).select(
    "seller_key",
    "seller_nk",
    "seller_first_name",
    "seller_last_name",
    F.concat_ws(" ", "seller_first_name", "seller_last_name").alias("seller_full_name"),
    "seller_email",
    "seller_country",
    "seller_postal_code",
)

dim_suppliers = with_surrogate_key(
    enriched.select(
        "supplier_nk",
        "supplier_name",
        "supplier_contact",
        "supplier_email",
        "supplier_phone",
        "supplier_address",
        "supplier_city",
        "supplier_country",
    ).dropDuplicates(["supplier_nk"]),
    "supplier_nk",
    "supplier_key",
).select(
    "supplier_key",
    "supplier_nk",
    "supplier_name",
    "supplier_contact",
    "supplier_email",
    "supplier_phone",
    "supplier_address",
    "supplier_city",
    "supplier_country",
)

dim_stores = with_surrogate_key(
    enriched.select(
        "store_nk",
        "store_name",
        "store_location",
        "store_city",
        "store_state",
        "store_country",
        "store_phone",
        "store_email",
    ).dropDuplicates(["store_nk"]),
    "store_nk",
    "store_key",
).select(
    "store_key",
    "store_nk",
    "store_name",
    "store_location",
    "store_city",
    "store_state",
    "store_country",
    "store_phone",
    "store_email",
)

dim_products = with_surrogate_key(
    enriched.groupBy("product_nk", "product_name", "product_category", "product_brand").agg(
        F.round(F.avg("product_price"), 2).alias("product_price"),
        F.max("product_available_quantity").alias("product_available_quantity"),
        F.first("pet_category", ignorenulls=True).alias("pet_category"),
        F.round(F.avg("product_weight"), 2).alias("product_weight"),
        F.first("product_color", ignorenulls=True).alias("product_color"),
        F.first("product_size", ignorenulls=True).alias("product_size"),
        F.first("product_material", ignorenulls=True).alias("product_material"),
        F.first("product_description", ignorenulls=True).alias("product_description"),
        F.round(F.avg("product_rating"), 2).alias("product_rating"),
        F.max("product_reviews").alias("product_reviews"),
        F.min("product_release_date").alias("product_release_date"),
        F.max("product_expiry_date").alias("product_expiry_date"),
    ),
    "product_nk",
    "product_key",
).select(
    "product_key",
    "product_nk",
    "product_name",
    "product_category",
    "product_price",
    "product_available_quantity",
    "pet_category",
    "product_weight",
    "product_color",
    "product_size",
    "product_brand",
    "product_material",
    "product_description",
    "product_rating",
    "product_reviews",
    "product_release_date",
    "product_expiry_date",
)

dim_dates = (
    enriched.select("sale_date", "date_key")
    .filter(F.col("sale_date").isNotNull())
    .dropDuplicates(["date_key"])
    .select(
        "date_key",
        F.col("sale_date").alias("full_date"),
        F.year("sale_date").alias("calendar_year"),
        F.quarter("sale_date").alias("calendar_quarter"),
        F.month("sale_date").alias("calendar_month"),
        F.date_format("sale_date", "MMMM").alias("month_name"),
        F.weekofyear("sale_date").alias("week_of_year"),
        F.dayofmonth("sale_date").alias("day_of_month"),
    )
)

customer_keys = dim_customers.select("customer_nk", "customer_key")
seller_keys = dim_sellers.select("seller_nk", "seller_key")
supplier_keys = dim_suppliers.select("supplier_nk", "supplier_key")
store_keys = dim_stores.select("store_nk", "store_key")
product_keys = dim_products.select("product_nk", "product_key")

fact_sales = (
    enriched.join(customer_keys, "customer_nk", "left")
    .join(seller_keys, "seller_nk", "left")
    .join(supplier_keys, "supplier_nk", "left")
    .join(store_keys, "store_nk", "left")
    .join(product_keys, "product_nk", "left")
    .select(
        F.col("source_row_id").alias("sale_key"),
        F.col("raw_id").alias("source_sale_id"),
        "date_key",
        "sale_date",
        "customer_key",
        "seller_key",
        "supplier_key",
        "store_key",
        "product_key",
        F.coalesce(F.col("sale_quantity"), F.lit(0)).alias("sale_quantity"),
        F.round(F.coalesce(F.col("sale_total_price"), F.lit(0.0)), 2).alias("sale_total_price"),
        F.round(F.coalesce(F.col("product_price"), F.lit(0.0)), 2).alias("product_unit_price"),
        F.lit(1).alias("order_count"),
    )
)

write_postgres_table(dim_customers, "dwh.dim_customers")
write_postgres_table(dim_sellers, "dwh.dim_sellers")
write_postgres_table(dim_suppliers, "dwh.dim_suppliers")
write_postgres_table(dim_stores, "dwh.dim_stores")
write_postgres_table(dim_products, "dwh.dim_products")
write_postgres_table(dim_dates, "dwh.dim_dates")
write_postgres_table(fact_sales, "dwh.fact_sales")

print(f"Loaded raw rows: {raw.count()}")
print(f"dim_customers rows: {dim_customers.count()}")
print(f"dim_sellers rows: {dim_sellers.count()}")
print(f"dim_suppliers rows: {dim_suppliers.count()}")
print(f"dim_stores rows: {dim_stores.count()}")
print(f"dim_products rows: {dim_products.count()}")
print(f"dim_dates rows: {dim_dates.count()}")
print(f"fact_sales rows: {fact_sales.count()}")

spark.stop()
