from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
ecom_df = spark.read.option("header", True).option("inferSchema", True).csv("dbfs:/Volumes/analytics_catalog/retail_schema/ecommerce/ecommerce_transactions.csv")
region_df = spark.read.option("header", True).option("inferSchema", True).csv("dbfs:/Volumes/analytics_catalog/retail_schema/ecommerce/country_region.csv")
ecom_df.show(5)
ecom_df.printSchema()
print(f"Total Records: {ecom_df.count()}")
from pyspark.sql.functions import col

clean_df = (
    ecom_df.dropDuplicates()
    .filter((col("customer_id").isNotNull()) & (col("quantity") > 0))
)
from pyspark.sql.functions import col, expr

# Step 1: Add 'order_value' column (e.g., unit_price * quantity if quantity exists)
enriched_df = enriched_df.withColumn("order_value", col("unit_price") * col("quantity"))

# Step 2: Then group by country and sum order_value
country_revenue_df = (
    enriched_df.groupBy("country")
    .sum("order_value")
    .withColumnRenamed("sum(order_value)", "total_revenue")
)

# Step 3: Display
country_revenue_df.show()
# Top customer per each country
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, sum as sum_

window_spec = Window.partitionBy("country").orderBy(col("total_spent").desc())

customer_spend_df = enriched_df.groupBy("country", "customer_id") \
                               .agg(sum_("order_value").alias("total_spent"))

top_customers_df = customer_spend_df.withColumn("rank", row_number().over(window_spec)) \
                                    .filter(col("rank") == 1)
top_customers_df.show()
# Region Join & Summary

joined_df = enriched_df.join(region_df, on="country", how="left")

region_sales_df = joined_df.groupBy("region").sum("order_value").withColumnRenamed("sum(order_value)", "region_sales")
region_sales_df.show()
# Monthly Categoery Pivot
monthly_category_df = enriched_df.groupBy("month", "category") \
                                 .agg(sum_("order_value").alias("monthly_sales")) \
                                 .groupBy("month").pivot("category").sum("monthly_sales")

monthly_category_df.show()
#price Band Counts
from pyspark.sql.functions import when

banded_df = enriched_df.withColumn("price_band", 
              when(col("order_value") < 50, "<50")
             .when(col("order_value").between(50, 100), "50-100")
             .when(col("order_value").between(100, 200), "100-200")
             .otherwise("200+"))

price_band_counts = banded_df.groupBy("price_band").count()
price_band_counts.show()