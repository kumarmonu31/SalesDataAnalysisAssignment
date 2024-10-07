# Load data from CSVs
from pyspark import StorageLevel
from pyspark.python.pyspark.shell import spark
from pyspark.sql import functions as F, Window


# Reading data from CSVs
df_jan = spark.read.csv('/Users/monu.kumar/PycharmProjects/Demo/AssignmentPyspark/data/Sales_February_2019.csv', header=True, inferSchema=True)
df_feb = spark.read.csv('/Users/monu.kumar/PycharmProjects/Demo/AssignmentPyspark/data/Sales_January_2019.csv', header=True, inferSchema=True)
df_mar = spark.read.csv('/Users/monu.kumar/PycharmProjects/Demo/AssignmentPyspark/data/Sales_March_2019.csv', header=True, inferSchema=True)

# Concatenate data from Jan, Feb, and Mar with column name refactoring.
df_combined = df_jan.union(df_feb).union(df_mar).withColumnRenamed('Order ID', 'Order_ID') \
    .withColumnRenamed('Product', 'Product') \
    .withColumnRenamed('Quantity Ordered', 'Quantity_Ordered') \
    .withColumnRenamed('Price Each', 'Price_Each') \
    .withColumnRenamed('Order Date', 'Order_Date') \
    .withColumnRenamed('Purchase Address', 'Purchase_Address')

# Q1. cleanse the data removing blank rows and duplicates
df_clean = df_combined.na.drop().withColumn('Order_timestamp', F.to_timestamp(F.col('Order_Date'), 'MM/dd/yy HH:mm')).withColumn('Order_date', F.to_date(F.col('Order_timestamp')))

#df_clean.show()


# create a Data column from .withColumn('Order_timestamp', F.to_timestamp(F.col('Order_Date'), 'MM/dd/yy HH:mm'))
df_clean = df_clean.withColumn('Date', F.to_date(F.col('Order_timestamp')))

# Create sales column by multiplying Quantity Ordered and Price Each
df_with_Sales = df_clean.withColumn('Sales', F.col('Quantity_Ordered') * F.col('Price_Each'))
# Because we are suing this DataFrame many times down the line we can persist it in memory So that it will not be recomputed every time we use it.
df_with_Sales = df_with_Sales.persist(StorageLevel.MEMORY_AND_DISK)

# Q2. get the date on which max sales was done by product
product_sales_by_date = df_with_Sales.groupBy("Product", "Order_Date") \
    .agg(F.sum("Sales").alias("Total_Sales"))

# Find the date of maximum sales for each product
windowSpec = Window.partitionBy("Product").orderBy(F.col("Total_Sales").desc())
max_sales_by_product = product_sales_by_date \
    .withColumn("Dense_rank", F.row_number().over(windowSpec)) \
    .filter(F.col("Dense_rank") == 1) \
    .select("Product", "Order_Date", "Total_Sales")

max_sales_by_product.show()


# Q3. Get the date on which max sales was done for all products
total_sales_by_date = df_with_Sales.groupBy("Order_Date") \
    .agg(F.sum("Sales").alias("Total_Sales"))

# Find the date with the maximum sales overall
max_sales_all = total_sales_by_date.orderBy(F.col("Total_Sales").desc()).first()
print(max_sales_all)


# Q4. Get the average sales value for each product in these 3 months
avg_sales_product = df_with_Sales.groupBy("Product") \
    .agg(F.avg("Sales").alias("Avg_Sales"))
avg_sales_product.show()



## Q.5  Create a combined dataset merging all these 3 datasets with order by date in desc order and add a new column which is “salesdiff”
# where this column will contain the difference of the sales in the current row (current date of that row) and the next row (previous date of that row, as the date columns are sorted by desc)
# grouped on the product For the last row, next row will be blank so consider the sales as 0

windowSpec = Window.partitionBy("Product").orderBy(F.col("Order_Date").desc())
df_with_lag = df_with_Sales.withColumn("Next_Sales", F.lag("Sales", 1).over(windowSpec))

# Calculate the sales difference (SalesDiff = Sales - Next_Sales)
# For the last row, if Next_Sales is null, we assume the next row's sales is 0
df_with_sales_diff = df_with_lag.withColumn(
    "SalesDiff",
    F.col("Sales") - F.coalesce(F.col("Next_Sales"), F.lit(0))
)
df_with_sales_diff.show()


# Q6. Get the orderId and purchase address details who made max sales in all the 3 months
max_sales_value = df_with_Sales.agg(F.max("Sales").alias("Max_Sales")).collect()[0]["Max_Sales"]
max_sales_order = df_with_Sales.filter(F.col("Sales") == max_sales_value) \
    .select("Order_ID", "Purchase_Address", "Sales")
max_sales_order.show()



# Q7. Extract city from 'Purchase Address' (2nd element in a comma-separated string)
df_with_city = df_with_Sales.withColumn("City",F.split(F.col("Purchase_Address"), ",")[1])\
                               .withColumn("City", F.trim(F.col("City")))

df_with_city_and_order_count = df_with_city.groupBy("City").agg(F.countDistinct("Order_ID").alias("OrderCount"))\
                               .orderBy(F.col("OrderCount").desc())

df_with_city_and_order_count.limit(1).show()

# Q7. Show the city with the most orders
df_with_city_and_order_count.show(truncate=False)

# To Free up the memory
df_with_Sales.unpersist()



