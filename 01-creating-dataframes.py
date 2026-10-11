# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook 1: Creating DataFrames
# MAGIC
# MAGIC ## Goal
# MAGIC
# MAGIC The goal of this notebook is to learn how to create Spark DataFrames manually and from simple Python objects.
# MAGIC
# MAGIC In Spark, the DataFrame is one of the most important objects you will work with. A DataFrame is a distributed table with named columns. It feels similar to a pandas DataFrame or a SQL table, but it is designed to scale across a Spark cluster.
# MAGIC
# MAGIC In this notebook, we will practice creating DataFrames in several ways:
# MAGIC
# MAGIC 1. From a list of tuples
# MAGIC 2. With column names
# MAGIC 3. With inferred schemas
# MAGIC 4. With manually defined schemas
# MAGIC 5. With realistic retail transaction data
# MAGIC
# MAGIC By the end, you should be comfortable creating small Spark DataFrames for practice examples in Databricks.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## What You Will Learn
# MAGIC
# MAGIC | Topic | Practice |
# MAGIC |---|---|
# MAGIC | Creating a DataFrame from a list of tuples | Small toy datasets |
# MAGIC | Adding column names | Schema basics |
# MAGIC | Inferring schema | Let Spark detect column types |
# MAGIC | Defining schema manually | Control data types |
# MAGIC | Viewing data | `show()`, `display()` |
# MAGIC | Inspecting structure | `printSchema()`, `columns`, `dtypes` |
# MAGIC
# MAGIC DataFrames are the primary objects in Databricks PySpark workflows. They provide familiar operations like selecting columns, filtering rows, joining tables, and aggregating data.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Review: SparkSession
# MAGIC
# MAGIC In Databricks, the `SparkSession` object is usually created for you automatically and stored in a variable named `spark`.
# MAGIC
# MAGIC The `SparkSession` is your main entry point into Spark.
# MAGIC
# MAGIC You use it to:
# MAGIC
# MAGIC - Create DataFrames
# MAGIC - Read files
# MAGIC - Run SQL queries
# MAGIC - Access Spark configuration
# MAGIC - Work with tables
# MAGIC
# MAGIC Let's inspect the SparkSession again.
# MAGIC

# COMMAND ----------

# The SparkSession is usually available by default in Databricks.
# It is named spark.

spark


# COMMAND ----------

# Print the Spark version.
# This helps you know what Spark environment your notebook is running on.

print("Spark version:", spark.version)


# COMMAND ----------

# MAGIC %md
# MAGIC # Section 1: Creating a DataFrame from a List of Tuples

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Creating a DataFrame from a List of Tuples
# MAGIC
# MAGIC One of the simplest ways to create a Spark DataFrame is from a Python list of tuples.
# MAGIC
# MAGIC Each tuple represents one row.
# MAGIC
# MAGIC For example, this tuple:
# MAGIC
# MAGIC ```python
# MAGIC (1, "Alice", "Data Science", 95000)
# MAGIC ```
# MAGIC could represent one employee with:
# MAGIC ```
# MAGIC employee ID
# MAGIC name
# MAGIC department
# MAGIC salary
# MAGIC ```
# MAGIC This is useful for learning because we can create small toy datasets directly inside the notebook.
# MAGIC

# COMMAND ----------

# Create a small list of tuples.
# Each tuple represents one row of employee data.

employee_data = [
   (1, "Alice", "Data Science", 95000),
   (2, "Bob", "Engineering", 88000),
   (3, "Carlos", "Engineering", 72000),
   (4, "Diana", "Operations", 68000),
   (5, "Evan", "Data Science", 105000)
]

# Create a Spark DataFrame without specifying column names.
# Spark will create default column names like _1, _2, _3, and _4.

employees_no_columns_df = spark.createDataFrame(employee_data)


# COMMAND ----------

# show() prints the DataFrame in a text table.
# This is a Spark action, meaning it triggers execution.

employees_no_columns_df.show()

# COMMAND ----------

# printSchema() shows the column names and data types.
# Since we did not provide column names, Spark uses default names.

employees_no_columns_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC # Section 2: Adding Column Names

# COMMAND ----------

# MAGIC %md
# MAGIC ## Important Observation
# MAGIC
# MAGIC Spark created the DataFrame, but the column names are not meaningful.
# MAGIC
# MAGIC Instead of names like:
# MAGIC
# MAGIC - `employee_id`
# MAGIC - `name`
# MAGIC - `department`
# MAGIC - `salary`
# MAGIC
# MAGIC Spark used default names:
# MAGIC
# MAGIC - `_1`
# MAGIC - `_2`
# MAGIC - `_3`
# MAGIC - `_4`
# MAGIC
# MAGIC This is why we usually provide column names when creating a DataFrame.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Adding Column Names
# MAGIC
# MAGIC Now let's create the same DataFrame again, but this time we will provide meaningful column names.
# MAGIC
# MAGIC This makes the DataFrame easier to understand and easier to query.

# COMMAND ----------

# Define column names for the employee data.

employee_columns = ["employee_id", "name", "department", "salary"]

# Create a Spark DataFrame with column names.

employees_df = spark.createDataFrame(employee_data, employee_columns)

# COMMAND ----------

# display() is Databricks' interactive table display function.
# It is very useful inside notebooks.

display(employees_df)

# COMMAND ----------

# show() gives a simple text-based view of the DataFrame.

employees_df.show()

# COMMAND ----------

# Print the schema.
# Spark inferred the data types from the Python values.

employees_df.printSchema()

# COMMAND ----------

# The columns attribute returns a Python list of column names.

employees_df.columns

# COMMAND ----------

# dtypes returns the column names and Spark data types.

employees_df.dtypes

# COMMAND ----------

# MAGIC %md
# MAGIC # Section 3: Schema Basics

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. What Is a Schema?
# MAGIC
# MAGIC A schema describes the structure of a DataFrame.
# MAGIC
# MAGIC It tells Spark:
# MAGIC
# MAGIC - The column names
# MAGIC - The data type of each column
# MAGIC - Whether each column can contain null values
# MAGIC
# MAGIC For example, our employee DataFrame has a schema similar to this:
# MAGIC
# MAGIC | Column | Data Type |
# MAGIC |---|---|
# MAGIC | employee_id | integer or long |
# MAGIC | name | string |
# MAGIC | department | string |
# MAGIC | salary | integer or long |
# MAGIC
# MAGIC Schemas matter because Spark needs to understand the structure of the data before it can process it efficiently.
# MAGIC

# COMMAND ----------

# Let's inspect the schema object directly.

employees_df.schema


# COMMAND ----------

# Print each field in the schema one at a time.
# This makes it easier to see the column name, data type, and nullable setting.

for field in employees_df.schema.fields:
   print("Column name:", field.name)
   print("Data type:", field.dataType)
   print("Nullable:", field.nullable)
   print("-" * 30)

# COMMAND ----------

# MAGIC %md
# MAGIC # Section 4: Inferring Schema

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Inferring Schema
# MAGIC
# MAGIC When we created the employee DataFrame, Spark inferred the schema automatically.
# MAGIC
# MAGIC That means Spark looked at the Python values and guessed the appropriate Spark data types.
# MAGIC
# MAGIC For example:
# MAGIC
# MAGIC | Python Value | Spark Type |
# MAGIC |---|---|
# MAGIC | `1` | LongType |
# MAGIC | `"Alice"` | StringType |
# MAGIC | `95000` | LongType |
# MAGIC | `95.5` | DoubleType |
# MAGIC | `True` | BooleanType |
# MAGIC
# MAGIC Schema inference is convenient for small examples.
# MAGIC
# MAGIC However, in production workflows, you often want to define the schema manually to avoid surprises.
# MAGIC

# COMMAND ----------

# Create a small dataset with mixed data types.
# Spark will infer the schema based on these Python values.

product_data = [
   (101, "Laptop", "Electronics", 999.99, True),
   (102, "Desk Chair", "Furniture", 149.50, True),
   (103, "Notebook", "Office Supplies", 3.99, False),
   (104, "Monitor", "Electronics", 249.99, True)
]

product_columns = ["product_id", "product_name", "category", "price", "in_stock"]

products_df = spark.createDataFrame(product_data, product_columns)

display(products_df)

# COMMAND ----------

# Inspect the inferred schema.

products_df.printSchema()

# COMMAND ----------

# View the column names and data types as a list.

products_df.dtypes

# COMMAND ----------

# MAGIC %md
# MAGIC # Section 5: Defining Schema Manually

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Defining a Schema Manually
# MAGIC
# MAGIC Sometimes you do not want Spark to guess the data types.
# MAGIC
# MAGIC Instead, you can define the schema yourself.
# MAGIC
# MAGIC This gives you more control.
# MAGIC
# MAGIC A manually defined schema is useful when:
# MAGIC
# MAGIC - Reading messy data
# MAGIC - Working with dates and timestamps
# MAGIC - Making sure IDs are strings instead of numbers
# MAGIC - Controlling decimal, integer, or double types
# MAGIC - Building reliable data pipelines
# MAGIC
# MAGIC To manually define a schema, we use classes from `pyspark.sql.types`.
# MAGIC

# COMMAND ----------

# Import Spark SQL data types.
# These allow us to define a schema manually.

from pyspark.sql.types import (
   StructType,
   StructField,
   IntegerType,
   LongType,
   StringType,
   DoubleType,
   BooleanType
)

# COMMAND ----------

# Define a manual schema for the product data.
# StructType represents the full schema.
# StructField represents one column.

product_schema = StructType([
   StructField("product_id", IntegerType(), nullable=False),
   StructField("product_name", StringType(), nullable=False),
   StructField("category", StringType(), nullable=True),
   StructField("price", DoubleType(), nullable=True),
   StructField("in_stock", BooleanType(), nullable=True)
])

# COMMAND ----------

# Create a DataFrame using the manually defined schema.

products_manual_schema_df = spark.createDataFrame(
   product_data,
   schema=product_schema
)

display(products_manual_schema_df)


# COMMAND ----------

# Inspect the manually defined schema.
# Notice that product_id is now IntegerType instead of whatever Spark inferred.

products_manual_schema_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Inferred Schema vs Manual Schema
# MAGIC
# MAGIC Schema inference is fast and convenient for quick exploration.
# MAGIC
# MAGIC Manual schemas are better when correctness matters.
# MAGIC
# MAGIC | Approach | Best For |
# MAGIC |---|---|
# MAGIC | Inferred schema | Quick experiments and small toy examples |
# MAGIC | Manual schema | Reliable pipelines and controlled data types |
# MAGIC
# MAGIC For learning notebooks, inferred schemas are usually fine.
# MAGIC
# MAGIC For real data engineering work, manual schemas are often safer.

# COMMAND ----------

# MAGIC %md
# MAGIC # Section 6: Working with Null Values

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Creating a DataFrame with Null Values
# MAGIC
# MAGIC Real datasets often contain missing values.
# MAGIC
# MAGIC In Python, missing values are usually represented as `None`.
# MAGIC
# MAGIC When Spark sees `None`, it treats it as a null value.
# MAGIC
# MAGIC Let's create a small customer dataset with some missing values.

# COMMAND ----------

# Create customer data with some missing values.
# None becomes null in Spark.

customer_data = [
   (1, "Alice", "Gainesville", "FL"),
   (2, "Bob", None, "FL"),
   (3, "Carlos", "Atlanta", "GA"),
   (4, "Diana", "Orlando", None),
   (5, None, "Tampa", "FL")
]

customer_columns = ["customer_id", "customer_name", "city", "state"]

customers_df = spark.createDataFrame(customer_data, customer_columns)

display(customers_df)

# COMMAND ----------

# Inspect the schema.
# Spark marks these fields as nullable because null values may exist.

customers_df.printSchema()

# COMMAND ----------

# Count the number of rows in the DataFrame.
# count() is an action.

customers_df.count()

# COMMAND ----------

# MAGIC %md
# MAGIC # Section 7: Common DataFrame Inspection Methods

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Common DataFrame Inspection Methods
# MAGIC
# MAGIC When you create a DataFrame, you should inspect it before doing analysis.
# MAGIC
# MAGIC Useful methods include:
# MAGIC
# MAGIC | Method | Purpose |
# MAGIC |---|---|
# MAGIC | `display(df)` | Databricks interactive table view |
# MAGIC | `df.show()` | Text table output |
# MAGIC | `df.printSchema()` | View column names and data types |
# MAGIC | `df.columns` | Get column names |
# MAGIC | `df.dtypes` | Get column names and data types |
# MAGIC | `df.count()` | Count rows |
# MAGIC | `df.limit(n)` | Preview first n rows |

# COMMAND ----------

# Show the first few rows as a text table.

customers_df.show()

# COMMAND ----------

# Show only the first 3 rows.
# limit() is a transformation.
# show() is the action that triggers execution.

customers_df.limit(3).show()

# COMMAND ----------

# Get the column names.

customers_df.columns

# COMMAND ----------

# Get the data types.

customers_df.dtypes

# COMMAND ----------

# Count rows.

row_count = customers_df.count()

print("Number of customer rows:", row_count)


# COMMAND ----------

# MAGIC %md
# MAGIC # Section 8: Mini-Project — Retail Transaction DataFrame

# COMMAND ----------

# MAGIC %md
# MAGIC Now we will create a small retail transaction DataFrame.
# MAGIC
# MAGIC This dataset will include:
# MAGIC
# MAGIC | Column | Meaning |
# MAGIC |---|---|
# MAGIC | transaction_id | Unique transaction identifier |
# MAGIC | store_id | Store where the transaction occurred |
# MAGIC | customer_id | Customer identifier |
# MAGIC | amount | Transaction amount |
# MAGIC | category | Product category |
# MAGIC | timestamp | Time of transaction |
# MAGIC
# MAGIC This type of dataset is useful because it resembles the kind of structured data you might analyze in retail analytics, fraud detection, customer analytics, or loss prevention work.

# COMMAND ----------

# Import TimestampType for timestamp columns.

from pyspark.sql.types import TimestampType
from datetime import datetime

# COMMAND ----------

# Create a small list of retail transactions.
# Each tuple represents one transaction.

retail_transaction_data = [
   (1001, "Store_001", "Customer_001", 249.99, "Electronics", datetime(2026, 5, 1, 9, 15, 0)),
   (1002, "Store_001", "Customer_002", 34.50, "Grocery", datetime(2026, 5, 1, 10, 5, 0)),
   (1003, "Store_002", "Customer_003", 89.99, "Clothing", datetime(2026, 5, 1, 11, 30, 0)),
   (1004, "Store_002", "Customer_001", 599.99, "Electronics", datetime(2026, 5, 2, 14, 45, 0)),
   (1005, "Store_003", "Customer_004", 22.25, "Grocery", datetime(2026, 5, 2, 16, 20, 0)),
   (1006, "Store_003", "Customer_005", 129.99, "Home Goods", datetime(2026, 5, 3, 13, 10, 0)),
   (1007, "Store_001", "Customer_006", 14.99, "Grocery", datetime(2026, 5, 3, 17, 55, 0)),
   (1008, "Store_002", "Customer_007", 799.99, "Electronics", datetime(2026, 5, 4, 12, 0, 0))
]

# COMMAND ----------

# Define a manual schema for the retail transaction data.
# This gives us full control over the column names and data types.

retail_transaction_schema = StructType([
   StructField("transaction_id", IntegerType(), nullable=False),
   StructField("store_id", StringType(), nullable=False),
   StructField("customer_id", StringType(), nullable=False),
   StructField("amount", DoubleType(), nullable=False),
   StructField("category", StringType(), nullable=True),
   StructField("timestamp", TimestampType(), nullable=False)
])

# COMMAND ----------

# Create the retail transaction Spark DataFrame.

transactions_df = spark.createDataFrame(
   retail_transaction_data,
   schema=retail_transaction_schema
)

display(transactions_df)

# COMMAND ----------

# Inspect the schema.

transactions_df.printSchema()

# COMMAND ----------

# View column names.

transactions_df.columns

# COMMAND ----------

# View data types.

transactions_df.dtypes

# COMMAND ----------

# Count the number of transactions.
# This is an action.

transaction_count = transactions_df.count()

print("Number of transactions:", transaction_count)

# COMMAND ----------

# MAGIC %md
# MAGIC # Section 9: Quick Exploration of the Retail DataFrame

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Quick Exploration
# MAGIC
# MAGIC Even though this notebook is mainly about creating DataFrames, let's do a few simple checks.
# MAGIC
# MAGIC We will answer:
# MAGIC
# MAGIC 1. How many transactions are in the dataset?
# MAGIC 2. What categories are present?
# MAGIC 3. What are the highest-value transactions (and limit to top 3)?
# MAGIC 4. What does the schema look like?
# MAGIC
# MAGIC These are common first steps when working with any new DataFrame.

# COMMAND ----------

# Show the full transaction dataset.

transactions_df.show()

# COMMAND ----------

# Select only the category column and remove duplicates.
# distinct() returns unique rows.
# display() triggers execution.

display(transactions_df.select("category").distinct())

# COMMAND ----------

# Sort transactions by amount from highest to lowest.
# orderBy() is a transformation.
# display() is an action.

highest_value_transactions_df = transactions_df.orderBy("amount", ascending=False)

display(highest_value_transactions_df)


# COMMAND ----------

# Preview only the top 3 highest-value transactions.

display(highest_value_transactions_df.limit(3))

# COMMAND ----------

# Show the schema. 

highest_value_transactions_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC # Section 10: Create a DataFrame from Dictionaries

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Creating a DataFrame from a List of Dictionaries
# MAGIC
# MAGIC You can also create a Spark DataFrame from a list of Python dictionaries.
# MAGIC
# MAGIC Each dictionary represents one row.
# MAGIC
# MAGIC The dictionary keys become column names.
# MAGIC
# MAGIC This can be convenient, but for larger or more reliable workflows, tuples plus a schema are often cleaner.
# MAGIC

# COMMAND ----------

# Create a list of dictionaries.
# Each dictionary represents one row of store data.

store_data = [
   {"store_id": "Store_001", "city": "Gainesville", "state": "FL", "store_type": "Urban"},
   {"store_id": "Store_002", "city": "Orlando", "state": "FL", "store_type": "Suburban"},
   {"store_id": "Store_003", "city": "Atlanta", "state": "GA", "store_type": "Urban"}
]

stores_df = spark.createDataFrame(store_data)

display(stores_df)

# COMMAND ----------

# Inspect the inferred schema.

stores_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC # Section 11: Common Mistakes

# COMMAND ----------

# MAGIC %md
# MAGIC # Common Mistakes When Creating DataFrames
# MAGIC
# MAGIC ## Mistake 1: Forgetting Column Names
# MAGIC
# MAGIC If you create a DataFrame without column names, Spark will use default names like `_1`, `_2`, and `_3`.
# MAGIC
# MAGIC That makes your code harder to read.
# MAGIC
# MAGIC ## Mistake 2: Relying Too Much on Schema Inference
# MAGIC
# MAGIC Schema inference is convenient, but Spark may infer a type you did not expect.
# MAGIC
# MAGIC Manual schemas are safer when correctness matters.
# MAGIC
# MAGIC ## Mistake 3: Mixing Data Types in the Same Column
# MAGIC
# MAGIC Try to keep each column consistent.
# MAGIC
# MAGIC For example, do not mix numbers and strings in the same column unless you truly want the column to be treated as a string.
# MAGIC
# MAGIC ## Mistake 4: Forgetting That Spark DataFrames Are Distributed
# MAGIC
# MAGIC A Spark DataFrame may look like a local object, but it represents distributed data and a logical execution plan.
# MAGIC
# MAGIC Do not assume it behaves exactly like a pandas DataFrame.

# COMMAND ----------

# MAGIC %md
# MAGIC # Section 12: Practice Exercises

# COMMAND ----------

# MAGIC %md
# MAGIC # Practice Exercises
# MAGIC
# MAGIC Try these on your own.
# MAGIC
# MAGIC ## Exercise 1
# MAGIC
# MAGIC Create a Spark DataFrame called `products_practice_df` with these columns:
# MAGIC
# MAGIC - product_id
# MAGIC - product_name
# MAGIC - category
# MAGIC - price
# MAGIC - in_stock
# MAGIC
# MAGIC Add at least 6 rows.
# MAGIC
# MAGIC Use:
# MAGIC
# MAGIC - `display()`
# MAGIC - `show()`
# MAGIC - `printSchema()`
# MAGIC - `columns`
# MAGIC - `dtypes`
# MAGIC
# MAGIC ## Exercise 2
# MAGIC
# MAGIC Create the same product DataFrame again, but this time define the schema manually.
# MAGIC
# MAGIC Use:
# MAGIC
# MAGIC - `IntegerType`
# MAGIC - `StringType`
# MAGIC - `DoubleType`
# MAGIC - `BooleanType`
# MAGIC
# MAGIC ## Exercise 3
# MAGIC
# MAGIC Create a customer DataFrame with at least one null value.
# MAGIC
# MAGIC Columns:
# MAGIC
# MAGIC - customer_id
# MAGIC - customer_name
# MAGIC - loyalty_status
# MAGIC - state
# MAGIC
# MAGIC Inspect the schema.
# MAGIC
# MAGIC ## Exercise 4
# MAGIC
# MAGIC Create a small store DataFrame from a list of dictionaries.
# MAGIC
# MAGIC Columns:
# MAGIC
# MAGIC - store_id
# MAGIC - city
# MAGIC - state
# MAGIC - square_feet
# MAGIC
# MAGIC ## Exercise 5
# MAGIC
# MAGIC Create your own retail transaction DataFrame with at least 10 transactions.
# MAGIC
# MAGIC Include:
# MAGIC
# MAGIC - transaction_id
# MAGIC - store_id
# MAGIC - customer_id
# MAGIC - amount
# MAGIC - category
# MAGIC - timestamp

# COMMAND ----------

# MAGIC %md
# MAGIC # Section 14: Key Takeaways

# COMMAND ----------

# MAGIC %md
# MAGIC # Key Takeaways
# MAGIC
# MAGIC In this notebook, you learned how to create Spark DataFrames manually.
# MAGIC
# MAGIC The most important ideas are:
# MAGIC
# MAGIC 1. A Spark DataFrame is a distributed table-like object.
# MAGIC 2. You can create a DataFrame from a list of tuples.
# MAGIC 3. You should usually provide meaningful column names.
# MAGIC 4. Spark can infer schemas automatically.
# MAGIC 5. Manual schemas give you more control.
# MAGIC 6. `display()` and `show()` let you view data.
# MAGIC 7. `printSchema()`, `columns`, and `dtypes` help you inspect structure.
# MAGIC 8. Small toy datasets are useful for learning Spark concepts.
# MAGIC 9. Real Spark workflows often start by creating, reading, or transforming DataFrames.
# MAGIC
# MAGIC The core pattern is:
# MAGIC
# MAGIC ```text
# MAGIC Create data
# MAGIC    ↓
# MAGIC Define column names or schema
# MAGIC    ↓
# MAGIC Create Spark DataFrame
# MAGIC    ↓
# MAGIC Inspect the DataFrame
# MAGIC    ↓
# MAGIC Use the DataFrame for analysis
# MAGIC