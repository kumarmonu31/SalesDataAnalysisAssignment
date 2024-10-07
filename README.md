# Problem Statement

Given three datasets related to online product sales in the year 2019 for the months of January, February, and March for a given store, the objective is to perform data engineering using PySpark or Spark (Scala/Java) on these datasets to achieve the following goals:
**All the Questions Are Answered with the help of PySpark.**
**And In comments Q1,Q2,Q3... are mentioned for each question.**

1. **Cleanse the data** by removing blank rows.
2. **Get the date** on which maximum sales occurred for each product during these three months.
3. **Get the date** on which maximum sales occurred for all products during these three months.
4. **Get the average sales value** for each product over these three months.
5. **Create a combined dataset** by merging all three datasets, ordering by date in descending order, and adding a new column named "SalesDiff". This column will contain the difference in sales between the current row (current date of that row) and the next row (previous date of that row, as the date columns are sorted by descending). For the last row, the next row will be considered blank, so the sales should be treated as 0.
6. **Get the order ID and purchase address** details for the customer who made the maximum sales during these three months.
7. **Extract the city** from the purchase address column, which is the second element in a comma-separated string, and determine which city received the most orders during these three months.
8. **Get the total order count details** for each city over these three months.

## Note



- Sales value is calculated by multiplying quantity by price.
- Order count can be determined based on the `Order_ID` (one `Order_ID` corresponds to one order).
- The datasets are provided in CSV format is ALready Availabe on data folder.
- The datasets are named as `Sales_January_2019.csv`, `Sales_February_2019.csv`, and `Sales_March_2019.csv`.
- The schema of the datasets is as follows Includes and created a new Columns:
    - `Order_ID`: The unique identifier for each order.
    - `Product_ID`: The unique identifier for each product.
    - `Quantity`: The quantity of the product sold in the order.
    - `Price`: The price of the product.
    - `Purchase_Address`: The address where the purchase was made.
    - `Purchase_Date`: The date on which the purchase was made.
    - `Purchase_Time`: The time at which the purchase was made.
    - `Purchase_Month`: The month in which the purchase was made.
    - `Sales`: The total sales value for the product (calculated by multiplying quantity by price).
    - `City`: The city where the purchase was made.


**How to run the code:**
Install Pyspark and Run pip install pyspark
Run requirements.txt file to install all the required libraries.
    
