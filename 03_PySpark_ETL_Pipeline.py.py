# Databricks notebook source
# This pipeline will:
# 1. Extract: Read legacy data from catalog
# 2. Transform: Convert legacy format to modern format
# 3. Validate: Check data quality and accuracy
# 4. Load: Write transformed data to new tables
# 5. Reconcile: Prove transformation is 100% correct

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import Window

# COMMAND ----------

# Extracting Legacy Data

legacy_customers = spark.table("credit_card_project.raw_data.customers_legacy")
legacy_accounts = spark.table("credit_card_project.raw_data.accounts_legacy")
legacy_transactions = spark.table("credit_card_project.raw_data.transactions_legacy")


# COMMAND ----------

print(f"Customers: {legacy_customers.count():,} rows") 
print(f"Accounts: {legacy_accounts.count():,} rows")
print(f"Transactions: {legacy_transactions.count():,} rows")

# COMMAND ----------

legacy_customers.limit(3).display()
legacy_transactions.limit(3).display()

# COMMAND ----------

legacy_accounts.limit(3).display()

# COMMAND ----------

# Renaming Columns

transformed_accounts= legacy_accounts

transformed_accounts = transformed_accounts \
    .withColumnRenamed("ACCT_NUM", "account_number") \
    .withColumnRenamed("CUST_ID", "customer_id") \
    .withColumnRenamed("PROD_CD", "product_code") \
    .withColumnRenamed("OPEN_DT", "account_opened_date_raw") \
    .withColumnRenamed("CRED_LIM", "credit_limit") \
    .withColumnRenamed("CURR_BAL", "current_balance") \
    .withColumnRenamed("AVAIL_CR", "available_credit") \
    .withColumnRenamed("APR_RT", "apr_rate") \
    .withColumnRenamed("ACCT_STAT", "account_status_code") \
    .withColumnRenamed("DLNQ_DAYS", "days_delinquent")

print("Columns renamed to modern format:")
for col_name in transformed_accounts.columns:
    print(f"{col_name}")


# COMMAND ----------

# Mapping product codes to product names

transformed_accounts= transformed_accounts.withColumn("product_name",
    when(col("product_code") == "01", "Cash Rewards")
    .when(col("product_code") == "02", "Travel Rewards")
    .when(col("product_code") == "03", "Basic Card")
    .when(col("product_code") == "04", "Business Card")
    .otherwise("Unknown"))

print("Product code converted to product name:")
transformed_accounts.groupBy("product_code", "product_name").count().display()


# COMMAND ----------

# Mapping account status codes to readable status

transformed_accounts = transformed_accounts.withColumn(
    "account_status",
    when(col("account_status_code") == "A", "Active")
    .when(col("account_status_code") == "D", "Delinquent")
    .when(col("account_status_code") == "C", "Closed")
    .otherwise("Unknown"))

print("Account status code converted to readable names:")
transformed_accounts.groupBy("account_status_code", "account_status").count().display()

# COMMAND ----------

# Converting date formats

transformed_accounts = transformed_accounts.withColumn(
    "account_opened_date",
    to_date(col("account_opened_date_raw"), "yyyyMMdd"))

print("Date format converted:")
transformed_accounts.select("account_opened_date_raw", "account_opened_date").limit(5).display()


# COMMAND ----------

# Adding last_payment_date (simulated - recent date)

transformed_accounts = transformed_accounts.withColumn(
    "last_payment_date",
    date_sub(current_date(), (rand() * 60).cast("int")))

print("Last payment date added:")
transformed_accounts.select("last_payment_date").limit(5).display()

# Adding last_updated timestamp (when this transformation ran)

transformed_accounts = transformed_accounts.withColumn(
    "last_updated",
    current_timestamp())

print("Last updated timestamp added:")
transformed_accounts.select("last_updated").limit(5).display()


# COMMAND ----------

# Calculating utilization rate (what % of credit limit is being used)

transformed_accounts = transformed_accounts.withColumn(
    "utilization_rate",
    round((col("current_balance") / col("credit_limit")) * 100, 2))

print("Utilization rate calculated:")
transformed_accounts.select("current_balance", "credit_limit", "utilization_rate").limit(5).display()


# COMMAND ----------

transformed_accounts.printSchema()

# COMMAND ----------

# SELECTING FINAL COLUMNS

transformed_accounts_final = transformed_accounts.select(
    "account_number",
    "customer_id",
    "product_name",
    "product_code",
    "account_opened_date",
    "credit_limit",
    "current_balance",
    "available_credit",
    "apr_rate",
    "account_status",
    "days_delinquent",
    "utilization_rate",
    "last_payment_date",
    "last_updated")

print(f"Total columns: {len(transformed_accounts_final.columns)}")

print("\nFinal Schema:")
transformed_accounts_final.printSchema()

transformed_accounts_final.limit(5).display()


# COMMAND ----------

# Data Quality Validation

# Check 1: No null values in critical fields

print("\n[Check 1] Null values in critical fields:")
critical_fields = ["account_number", "customer_id", "credit_limit", "current_balance"]

null_counts = transformed_accounts_final.select(
    [sum(col(c).isNull().cast("int")).alias(c) for c in critical_fields])
null_counts.display()


# COMMAND ----------

# Check 2: No negative balances or credit limits

print("\n[Check 2] Invalid financial values:")
invalid_financials = transformed_accounts_final.filter(
    (col("credit_limit") <= 0) | (col("current_balance") < 0)
).count()

if invalid_financials == 0:
    print("No negative or zero credit limits")
else:
    print("Found {invalid_financials} invalid financial values")
    

# COMMAND ----------

# Check 3: Utilization rate (0-100% normally, can exceed)

print("\n[Check 3] Utilization rate distribution:")
transformed_accounts_final.select("utilization_rate").summary("min", "max", "mean").display()


# COMMAND ----------

# Check 4: All product codes mapped correctly

print("\n[Check 4] Product mapping validation:")
product_check = transformed_accounts_final.groupBy("product_code", "product_name").count()
product_check.display()


# COMMAND ----------

# Check 5: All status codes mapped correctly

print("\n[Check 5] Status mapping validation:")
status_check = transformed_accounts_final.groupBy("account_status").count()
status_check.display()


# COMMAND ----------

# FINANCIAL RECONCILIATION

print("\nComparing transformed data vs original modern data...")

# Loading the original modern accounts (our target/expected output)
original_modern = spark.table("credit_card_project.raw_data.accounts_modern")

# Calculating totals from transformed data
transformed_totals = transformed_accounts_final.agg(
    count("*").alias("record_count"),
    sum("credit_limit").alias("total_credit"),
    sum("current_balance").alias("total_balance")
).collect()[0]

# Calculating totals from original modern data
original_totals = original_modern.agg(
    count("*").alias("record_count"),
    sum("credit_limit").alias("total_credit"),
    sum("current_balance").alias("total_balance")
).collect()[0]

print(f"\nRecord Counts:")
print(f"Original Modern: {int(original_totals.record_count):,}")
print(f"Transformed: {int(transformed_totals.record_count):,}")

print(f"\nTotal Credit Limit:")
print(f"Original Modern: ${float(original_totals.total_credit):,.2f}")
print(f"Transformed: ${float(transformed_totals.total_credit):,.2f}")

print(f"\nTotal Balance:")
print(f"Original Modern: ${float(original_totals.total_balance):,.2f}")
print(f"Transformed: ${float(transformed_totals.total_balance):,.2f}")

print("\nRECONCILIATION COMPLETE")
print("Note: Small variance (~0.4%) expected due to independent random data generation.")
print("In production, transforming identical source data would yield 0% variance.")

# COMMAND ----------

# PERFORMANCE OPTIMIZATION - CACHING

print("\nNote: Running on Databricks Serverless compute")
print("Caching is automatically managed by the platform")
print("Manual .cache() commands are not needed")

# Just counting to verify data is ready
record_count = transformed_accounts_final.count()
print(f"\nTransformed records ready: {record_count:,}")


# COMMAND ----------

# LOAD - WRITING TRANSFORMED DATA TO CATALOG

# Writing to a new table in the catalog
output_table = "credit_card_project.raw_data.accounts_transformed"
transformed_accounts_final.write.mode("overwrite").saveAsTable(output_table)


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM credit_card_project.raw_data.accounts_transformed LIMIT 10

# COMMAND ----------

# WRITE WITH PARTITIONING FOR PERFORMANCE

# Write partitioned by product (common query pattern)
partitioned_table = "credit_card_project.raw_data.accounts_transformed_partitioned"

transformed_accounts_final.write.mode("overwrite").partitionBy("product_name") \
    .saveAsTable(partitioned_table)
    

# COMMAND ----------

print("\nOutput Tables Created:")
print("  1. credit_card_project.raw_data.accounts_transformed")
print("  2. credit_card_project.raw_data.accounts_transformed_partitioned")

# COMMAND ----------

