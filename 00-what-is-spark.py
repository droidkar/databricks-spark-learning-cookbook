# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook 0: What is Spark?
# MAGIC
# MAGIC ## Goal
# MAGIC
# MAGIC The goal of this notebook is to understand Spark as a distributed data processing engine.
# MAGIC
# MAGIC Spark is not just a "faster pandas." Spark is designed to process data across many machines. In small examples, it may feel similar to pandas because both can work with rows and columns. The key difference is that Spark is built around distributed computation.
# MAGIC
# MAGIC In Spark, you write logical instructions such as:
# MAGIC
# MAGIC - Read this data
# MAGIC - Select these columns
# MAGIC - Filter these rows
# MAGIC - Group by this column
# MAGIC - Compute this summary
# MAGIC
# MAGIC Spark then decides how to execute those instructions across a cluster.
# MAGIC
# MAGIC In this notebook, we will learn the basic mental model of Spark and create our first Spark DataFrame.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Core Spark Concepts
# MAGIC
# MAGIC | Concept | What You Should Understand |
# MAGIC |---|---|
# MAGIC | Spark | A distributed engine for processing data at scale |
# MAGIC | Driver | The process that coordinates the Spark job |
# MAGIC | Executors | Worker processes that run tasks |
# MAGIC | Cluster | The collection of compute resources Spark uses |
# MAGIC | SparkSession | Your main entry point into Spark from PySpark |
# MAGIC | DataFrame | Spark's main tabular data abstraction |
# MAGIC | Lazy evaluation | Spark waits to execute until an action is called |
# MAGIC | Transformations | Operations that define a new DataFrame |
# MAGIC | Actions | Operations that trigger execution |
# MAGIC
# MAGIC Spark DataFrames are organized into named columns, similar to SQL tables or pandas DataFrames, but with distributed execution and query optimization underneath.
# MAGIC
# MAGIC Reference for deeper learning:
# MAGIC
# MAGIC https://spark.apache.org/docs/latest/sql-programming-guide.html
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. The Spark Mental Model
# MAGIC
# MAGIC A Spark application has a few major parts:
# MAGIC
# MAGIC ### Driver
# MAGIC
# MAGIC The driver is the coordinator. It runs your main program, keeps track of the operations you asked Spark to perform, and sends work to executors.
# MAGIC
# MAGIC In a Databricks notebook, your notebook code interacts with the Spark driver.
# MAGIC
# MAGIC ### Executors
# MAGIC
# MAGIC Executors are worker processes. They perform the actual computation on chunks of data.
# MAGIC
# MAGIC For example, if you ask Spark to filter a billion rows, Spark can split that work across multiple executors.
# MAGIC
# MAGIC ### Cluster
# MAGIC
# MAGIC A cluster is the collection of machines or compute resources available to Spark.
# MAGIC
# MAGIC In Databricks Free Edition, you do not need to manually manage all of this complexity right away. But the mental model still matters because Spark code is designed for distributed execution.
# MAGIC
# MAGIC ### SparkSession
# MAGIC
# MAGIC The `SparkSession` is your main entry point into Spark when using PySpark.
# MAGIC
# MAGIC In Databricks notebooks, a SparkSession named `spark` is usually created for you automatically.
# MAGIC

# COMMAND ----------

# In Databricks, the SparkSession is usually already available as a variable named spark.
# Let's inspect it.

spark


# COMMAND ----------

# We can check the type of the SparkSession object.
# This tells us what kind of Python object "spark" is.

type(spark)


# COMMAND ----------

# We can also print useful information about the Spark version.
# This is helpful because Spark features can vary slightly by version.

print("Spark version:", spark.version)


# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Creating Your First Spark DataFrame
# MAGIC
# MAGIC A Spark DataFrame is a distributed table-like object.
# MAGIC
# MAGIC It has:
# MAGIC
# MAGIC - Rows
# MAGIC - Columns
# MAGIC - Column names
# MAGIC - Column data types
# MAGIC - A schema
# MAGIC
# MAGIC This makes it feel similar to a pandas DataFrame or SQL table.
# MAGIC
# MAGIC However, the important difference is that a Spark DataFrame is designed to scale across a cluster.
# MAGIC

# COMMAND ----------

# Create a small Python list of tuples.
# Each tuple represents one row of data.

retail_data = [
   (1, "Store_A", "Electronics", 249.99),
   (2, "Store_A", "Grocery", 34.50),
   (3, "Store_B", "Clothing", 89.99),
   (4, "Store_B", "Electronics", 599.99),
   (5, "Store_C", "Grocery", 22.25)
]

# Define column names for the DataFrame.
columns = ["transaction_id", "store_id", "category", "amount"]

# Create a Spark DataFrame from the Python list.
# Spark converts the local Python data into a distributed Spark DataFrame.

transactions_df = spark.createDataFrame(retail_data, columns)
# transactions_df = spark.createDataFrame(retail_data)

# transactions_df.show()
transactions_df.printSchema()


# COMMAND ----------

# Display the DataFrame using Databricks' display() function.
# display() gives you a nice interactive table in Databricks.

display(transactions_df)


# COMMAND ----------

# show() is the standard Spark method for printing rows of a DataFrame.
# It works in Databricks and outside of Databricks.

transactions_df.show()


# COMMAND ----------

# printSchema() shows the structure of the DataFrame.
# The schema tells us the column names and data types.

transactions_df.printSchema()


# COMMAND ----------

# The columns attribute gives us a Python list of column names.

transactions_df.columns


# COMMAND ----------

# dtypes gives us the column names and their inferred Spark data types.

transactions_df.dtypes


# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Spark DataFrames vs pandas DataFrames
# MAGIC
# MAGIC If you have used pandas before, Spark DataFrames may look familiar.
# MAGIC
# MAGIC Both can represent tabular data.
# MAGIC
# MAGIC But there are major differences:
# MAGIC
# MAGIC | Topic | pandas | Spark |
# MAGIC |---|---|---|
# MAGIC | Execution | Runs locally on one machine | Can run across a cluster |
# MAGIC | Data size | Best for small to medium data | Designed for large-scale data |
# MAGIC | Evaluation | Usually eager | Lazy |
# MAGIC | Optimization | Mostly manual | Uses Spark's query optimizer |
# MAGIC | Common use | Analysis on local data | Distributed ETL, analytics, ML pipelines |
# MAGIC
# MAGIC The most important difference for now is lazy evaluation.
# MAGIC
# MAGIC In pandas, when you run a line of code, the result is usually computed immediately.
# MAGIC
# MAGIC In Spark, many operations do not execute immediately. Spark builds a plan and waits until you call an action.
# MAGIC

# COMMAND ----------

# Optional: Create a small pandas DataFrame for comparison.
# This is only for conceptual comparison.
# pandas runs locally in memory on the driver machine.

import pandas as pd

retail_data = [
   (1, "Store_A", "Electronics", 249.99),
   (2, "Store_A", "Grocery", 34.50),
   (3, "Store_B", "Clothing", 89.99),
   (4, "Store_B", "Electronics", 599.99),
   (5, "Store_C", "Grocery", 22.25)
]

columns = ["transaction_id", "store_id", "category", "amount"]

pandas_df = pd.DataFrame(
   retail_data,
   columns=columns
)

pandas_df

# COMMAND ----------

# Check the type of the pandas DataFrame.

type(pandas_df)

# COMMAND ----------

# Check the type of the Spark DataFrame.

type(transactions_df)


# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Transformations
# MAGIC
# MAGIC A transformation defines a new DataFrame from an existing DataFrame.
# MAGIC
# MAGIC Examples of transformations include:
# MAGIC
# MAGIC - `select()`
# MAGIC - `filter()`
# MAGIC - `where()`
# MAGIC - `withColumn()`
# MAGIC - `groupBy()`
# MAGIC - `orderBy()`
# MAGIC
# MAGIC Transformations are lazy.
# MAGIC
# MAGIC That means Spark does not immediately execute the operation. Instead, it records the operation as part of a logical plan.
# MAGIC

# COMMAND ----------

# This is a transformation.
# We are defining a new DataFrame containing only high-value transactions.
# Spark does NOT execute the filter immediately.

high_value_df = transactions_df.filter(transactions_df.amount > 100)

# Notice that this cell does not display any rows yet.
# We have only defined the transformation.


# COMMAND ----------

# Let's inspect the type of high_value_df.
# It is another Spark DataFrame.

type(high_value_df)


# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Actions
# MAGIC
# MAGIC An action triggers Spark to actually execute the computation.
# MAGIC
# MAGIC Examples of actions include:
# MAGIC
# MAGIC - `show()`
# MAGIC - `display()`
# MAGIC - `count()`
# MAGIC - `collect()`
# MAGIC - `first()`
# MAGIC - `take()`
# MAGIC - writing data to storage
# MAGIC
# MAGIC Until you call an action, Spark is mostly building a plan.
# MAGIC
# MAGIC When you call an action, Spark sends work to the executors and computes the result.
# MAGIC

# COMMAND ----------

# show() is an action.
# This triggers Spark to execute the filter transformation defined earlier.

high_value_df.show()


# COMMAND ----------

# count() is also an action.
# Spark must scan the DataFrame and count how many rows match.

num_high_value_transactions = high_value_df.count()

print("Number of high-value transactions:", num_high_value_transactions)


# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Chaining Transformations
# MAGIC
# MAGIC Spark code often chains multiple transformations together.
# MAGIC
# MAGIC This lets you express a logical data pipeline.
# MAGIC
# MAGIC In the example below, we will:
# MAGIC
# MAGIC 1. Filter for transactions over 50 dollars
# MAGIC 2. Select only a few columns
# MAGIC 3. Sort by amount in descending order
# MAGIC
# MAGIC These transformations define a plan.
# MAGIC
# MAGIC The plan executes when we call `show()` or `display()`.
# MAGIC

# COMMAND ----------

# Chain multiple transformations together.
# This still does not execute until an action is called.

filtered_sorted_df = (
   transactions_df
   .filter(transactions_df.amount > 50)
   .select("transaction_id", "store_id", "category", "amount")
   .orderBy("amount", ascending=False)
)

# The variable filtered_sorted_df now points to a new Spark DataFrame.
# The computation has not been triggered yet.


# COMMAND ----------

# display() is an action in Databricks.
# This triggers Spark to execute the chain of transformations.

display(filtered_sorted_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Inspecting the Query Plan with explain()
# MAGIC
# MAGIC Spark does not simply run your code line by line like normal Python.
# MAGIC
# MAGIC Instead, Spark builds a query plan.
# MAGIC
# MAGIC The query plan describes how Spark intends to execute your transformations.
# MAGIC
# MAGIC The `explain()` method lets us inspect that plan.
# MAGIC
# MAGIC You do not need to understand every detail yet. For now, the key idea is:
# MAGIC
# MAGIC > Spark converts your DataFrame operations into an execution plan.
# MAGIC

# COMMAND ----------

# explain() shows the physical plan Spark will use.
# This is useful later when learning performance optimization.

filtered_sorted_df.explain()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Aggregation Example
# MAGIC
# MAGIC Spark is often used to summarize large datasets.
# MAGIC
# MAGIC A common pattern is:
# MAGIC
# MAGIC 1. Group rows by one or more columns
# MAGIC 2. Compute summary statistics
# MAGIC
# MAGIC For example, we can calculate total sales by store.

# COMMAND ----------

from pyspark.sql import functions as F

# groupBy() defines grouped computation.
# agg() defines the summary calculation.
# This is a transformation until we call display() or show().

display(transactions_df)

sales_by_store_df = (
   transactions_df
   .groupBy("store_id")
   .agg(
       F.sum("amount").alias("total_sales"),
       F.count("*").alias("num_transactions")
   )
   .orderBy("total_sales", ascending=False)
)

display(sales_by_store_df)


# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Registering a Temporary View
# MAGIC
# MAGIC Spark lets us use both the DataFrame API and SQL.
# MAGIC
# MAGIC To query a DataFrame with SQL, we can register it as a temporary view.
# MAGIC
# MAGIC A temporary view exists only for the current Spark session.
# MAGIC

# COMMAND ----------

# Register the Spark DataFrame as a temporary SQL view.
# This allows us to query the DataFrame using SQL syntax.

transactions_df.createOrReplaceTempView("transactions")


# COMMAND ----------

# MAGIC %sql
# MAGIC -- select * from transactions;
# MAGIC
# MAGIC    SELECT
# MAGIC        store_id,
# MAGIC        SUM(amount) AS total_sales,
# MAGIC        COUNT(*) AS num_transactions
# MAGIC    FROM transactions
# MAGIC    GROUP BY store_id
# MAGIC    ORDER BY total_sales DESC

# COMMAND ----------

# Use Spark SQL to query the temporary view.
# The result of spark.sql() is also a Spark DataFrame.

sql_result_df = spark.sql("""
   SELECT
       store_id,
       SUM(amount) AS total_sales,
       COUNT(*) AS num_transactions
   FROM transactions
   GROUP BY store_id
   ORDER BY total_sales DESC
""")

display(sql_result_df)


# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. DataFrame API vs Spark SQL
# MAGIC
# MAGIC The DataFrame API and Spark SQL are two different ways to express similar logic.
# MAGIC
# MAGIC For example, these two approaches are equivalent:
# MAGIC
# MAGIC ### DataFrame API
# MAGIC
# MAGIC ```python
# MAGIC transactions_df.groupBy("store_id").agg(F.sum("amount"))
# MAGIC ```
# MAGIC ### Spark SQL
# MAGIC ```python
# MAGIC SELECT store_id, SUM(amount)
# MAGIC FROM transactions
# MAGIC GROUP BY store_id
# MAGIC ```
# MAGIC
# MAGIC In real projects, you may use both.
# MAGIC Python is often useful for building pipelines and reusable logic.
# MAGIC SQL is often useful for analytics, reporting, and communicating with business users.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## 11. Key Lesson
# MAGIC
# MAGIC Spark is not "faster pandas."
# MAGIC
# MAGIC Spark is a distributed computation engine.
# MAGIC
# MAGIC You write logical operations. Spark builds a plan. Then Spark decides how to execute that plan across available compute resources.
# MAGIC
# MAGIC The most important ideas from this notebook are:
# MAGIC
# MAGIC 1. Spark uses a driver and executors.
# MAGIC 2. The `SparkSession` is your entry point into Spark.
# MAGIC 3. A Spark DataFrame is a distributed table-like abstraction.
# MAGIC 4. Transformations define new DataFrames.
# MAGIC 5. Actions trigger execution.
# MAGIC 6. Spark uses lazy evaluation.
# MAGIC 7. Spark can be used with both Python syntax and SQL.

# COMMAND ----------

# MAGIC %md
# MAGIC # Practice Exercises

# COMMAND ----------

# MAGIC %md
# MAGIC # Practice Exercises
# MAGIC
# MAGIC Try these on your own before looking for help.
# MAGIC
# MAGIC ## Exercise 1
# MAGIC
# MAGIC Create a new Spark DataFrame called `employees_df` with the following columns:
# MAGIC
# MAGIC - employee_id
# MAGIC - name
# MAGIC - department
# MAGIC - salary
# MAGIC
# MAGIC Add at least 6 rows of data.
# MAGIC
# MAGIC Then use:
# MAGIC
# MAGIC - `display()`
# MAGIC - `show()`
# MAGIC - `printSchema()`
# MAGIC
# MAGIC ## Exercise 2
# MAGIC
# MAGIC Filter the employee DataFrame to show only employees with salary greater than 75,000.
# MAGIC
# MAGIC ## Exercise 3
# MAGIC
# MAGIC Group the employee DataFrame by department and calculate:
# MAGIC
# MAGIC - average salary
# MAGIC - number of employees
# MAGIC
# MAGIC ## Exercise 4
# MAGIC
# MAGIC Create a temporary view called `employees`.
# MAGIC
# MAGIC Then write a SQL query to calculate average salary by department.
# MAGIC
# MAGIC ## Exercise 5
# MAGIC
# MAGIC Use `explain()` on one of your filtered or grouped DataFrames.
# MAGIC

# COMMAND ----------

# Exercise 1
# Create a new Spark DataFrame called employees_df with the following columns:
# employee_id
# name
# department
# salary
# Add at least 6 rows of data.
# Then use:
# display()
# show()
# printSchema()
employees_df = spark.createDataFrame([
   (1, "Alice", "Data Science", 95000),
   (2, "Bob", "Engineering", 88000),
   (3, "Carlos", "Engineering", 72000),
   (4, "Diana", "Operations", 68000),
   (5, "Evan", "Data Science", 105000),
   (6, "Fatima", "Operations", 76000)
], ["employee_id", "name", "department", "salary"])
display(employees_df)
employees_df.show()
employees_df.printSchema()

# Exercise 2
# Filter the employee DataFrame to show only employees with salary greater than 75,000.
high_salary_df = employees_df.filter(employees_df.salary > 75000)
display(high_salary_df)

# Exercise 3
# Group the employee DataFrame by department and calculate:
# average salary
# number of employees
from pyspark.sql import functions as F
employees_by_dep_df = employees_df.groupBy("department").agg(F.count("*").alias("num_employees"), F.avg("salary").alias("avg_salary"))
display(employees_by_dep_df)

# Exercise 4
# Create a temporary view called employees.
employees_df.createOrReplaceTempView("employees")

# Then write a SQL query to calculate average salary by department.
display(spark.sql("""
          SELECT 
          department,
          AVG(salary) AS avg_salary
          FROM employees
          GROUP BY department
          """))

# Exercise 5
# Use explain() on one of your filtered or grouped DataFrames.
employees_by_dep_df.explain()

# COMMAND ----------

# MAGIC %md
# MAGIC # Optional Answer Starter for Exercises

# COMMAND ----------

# Exercise starter code
# Try modifying this with your own values.

employee_data = [
   (1, "Alice", "Data Science", 95000),
   (2, "Bob", "Engineering", 88000),
   (3, "Carlos", "Engineering", 72000),
   (4, "Diana", "Operations", 68000),
   (5, "Evan", "Data Science", 105000),
   (6, "Fatima", "Operations", 76000)
]

employee_columns = ["employee_id", "name", "department", "salary"]

employees_df = spark.createDataFrame(employee_data, employee_columns)

display(employees_df)

# Filter employees with salary greater than 75,000.
# This is a transformation until display() is called.

high_salary_df = employees_df.filter(employees_df.salary > 75000)

display(high_salary_df)

# Group by department and calculate average salary and employee count.

salary_by_department_df = (
   employees_df
   .groupBy("department")
   .agg(
       F.avg("salary").alias("avg_salary"),
       F.count("*").alias("num_employees")
   )
   .orderBy("avg_salary", ascending=False)
)

display(salary_by_department_df)

# Register employees_df as a temporary SQL view.

employees_df.createOrReplaceTempView("employees")

# Query the employees view using Spark SQL.

employee_sql_result_df = spark.sql("""
   SELECT
       department,
       AVG(salary) AS avg_salary,
       COUNT(*) AS num_employees
   FROM employees
   GROUP BY department
   ORDER BY avg_salary DESC
""")

display(employee_sql_result_df)

# Inspect the query plan.

employee_sql_result_df.explain()


# COMMAND ----------

# MAGIC %md
# MAGIC # Final Takeaways

# COMMAND ----------

# MAGIC %md
# MAGIC By the end of this first notebook, you should understand the basic Spark workflow:
# MAGIC ```
# MAGIC Create DataFrame
# MAGIC    ↓
# MAGIC Apply transformations
# MAGIC    ↓
# MAGIC Call an action
# MAGIC    ↓
# MAGIC Spark executes the plan
# MAGIC The core mental model is:
# MAGIC You define what should happen.
# MAGIC Spark decides how to execute it.
# MAGIC ```
# MAGIC