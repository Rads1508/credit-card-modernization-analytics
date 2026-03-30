# Databricks notebook source
# MAGIC %sql
# MAGIC -- count of active accounts and customers
# MAGIC
# MAGIC SELECT 
# MAGIC     COUNT(DISTINCT ACCT_NUM) as total_accounts,
# MAGIC     COUNT(DISTINCT CUST_ID) as total_customers
# MAGIC FROM credit_card_project.raw_data.accounts_legacy
# MAGIC WHERE ACCT_STAT != 'C'

# COMMAND ----------

# MAGIC %sql
# MAGIC -- credit limits, balances, and available credit
# MAGIC
# MAGIC SELECT 
# MAGIC     SUM(CRED_LIM) as total_credit_limit,
# MAGIC     SUM(CURR_BAL) as total_outstanding_balance,
# MAGIC     SUM(AVAIL_CR) as total_available_credit
# MAGIC FROM credit_card_project.raw_data.accounts_legacy
# MAGIC WHERE ACCT_STAT != 'C'

# COMMAND ----------

# MAGIC %sql
# MAGIC -- utilization rate
# MAGIC
# MAGIC SELECT 
# MAGIC     ROUND(AVG(CURR_BAL / CRED_LIM * 100), 2) as avg_utilization_rate_pct
# MAGIC FROM credit_card_project.raw_data.accounts_legacy
# MAGIC WHERE ACCT_STAT != 'C'

# COMMAND ----------

# MAGIC %sql
# MAGIC -- delinquent accounts
# MAGIC
# MAGIC SELECT 
# MAGIC     SUM(CASE WHEN ACCT_STAT = 'D' THEN 1 ELSE 0 END) as delinquent_accounts,
# MAGIC     ROUND(SUM(CASE WHEN ACCT_STAT = 'D' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as delinquency_rate_pct
# MAGIC FROM credit_card_project.raw_data.accounts_legacy
# MAGIC WHERE ACCT_STAT != 'C'

# COMMAND ----------

# MAGIC %sql
# MAGIC -- charge-off risk
# MAGIC
# MAGIC SELECT 
# MAGIC     SUM(CASE WHEN DLNQ_DAYS >= 90 THEN CURR_BAL ELSE 0 END) as balance_at_90plus_days,
# MAGIC     ROUND(SUM(CASE WHEN DLNQ_DAYS >= 90 THEN CURR_BAL ELSE 0 END) * 100.0 / SUM(CURR_BAL), 2) as charge_off_risk_pct
# MAGIC FROM credit_card_project.raw_data.accounts_legacy
# MAGIC WHERE ACCT_STAT != 'C'

# COMMAND ----------

# MAGIC %sql
# MAGIC -- product mix
# MAGIC
# MAGIC SELECT 
# MAGIC     CASE PROD_CD
# MAGIC         WHEN '01' THEN 'Cash Rewards'
# MAGIC         WHEN '02' THEN 'Travel Rewards'
# MAGIC         WHEN '03' THEN 'Basic Card'
# MAGIC         WHEN '04' THEN 'Business Card'
# MAGIC     END as product_name,
# MAGIC     COUNT(*) as account_count
# MAGIC FROM credit_card_project.raw_data.accounts_legacy
# MAGIC WHERE ACCT_STAT != 'C'
# MAGIC GROUP BY PROD_CD
# MAGIC ORDER BY account_count DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     CASE PROD_CD
# MAGIC         WHEN '01' THEN 'Cash Rewards'
# MAGIC         WHEN '02' THEN 'Travel Rewards'
# MAGIC         WHEN '03' THEN 'Basic Card'
# MAGIC         WHEN '04' THEN 'Business Card'
# MAGIC     END as product_name,
# MAGIC     SUM(CRED_LIM) as total_credit,
# MAGIC     SUM(CURR_BAL) as total_balance,
# MAGIC     ROUND(AVG(CURR_BAL / CRED_LIM * 100), 2) as avg_utilization_pct
# MAGIC FROM credit_card_project.raw_data.accounts_legacy
# MAGIC WHERE ACCT_STAT != 'C'
# MAGIC GROUP BY PROD_CD
# MAGIC ORDER BY total_balance DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- product with most delinquent accounts
# MAGIC
# MAGIC SELECT 
# MAGIC     CASE PROD_CD
# MAGIC         WHEN '01' THEN 'Cash Rewards'
# MAGIC         WHEN '02' THEN 'Travel Rewards'
# MAGIC         WHEN '03' THEN 'Basic Card'
# MAGIC         WHEN '04' THEN 'Business Card'
# MAGIC     END as product_name,
# MAGIC     SUM(CASE WHEN ACCT_STAT = 'D' THEN 1 ELSE 0 END) as delinquent_count,
# MAGIC     ROUND(SUM(CASE WHEN ACCT_STAT = 'D' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as delinquency_rate_pct
# MAGIC FROM credit_card_project.raw_data.accounts_legacy
# MAGIC WHERE ACCT_STAT != 'C'
# MAGIC GROUP BY PROD_CD
# MAGIC ORDER BY delinquency_rate_pct DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- customer count by credit tier
# MAGIC
# MAGIC SELECT 
# MAGIC     CASE CRED_TIER
# MAGIC         WHEN 'A' THEN 'Excellent'
# MAGIC         WHEN 'B' THEN 'Good'
# MAGIC         WHEN 'C' THEN 'Fair'
# MAGIC         WHEN 'D' THEN 'Poor'
# MAGIC     END as credit_tier,
# MAGIC     COUNT(DISTINCT CUST_ID) as customer_count
# MAGIC FROM credit_card_project.raw_data.customers_legacy
# MAGIC GROUP BY CRED_TIER
# MAGIC ORDER BY CRED_TIER

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     CASE c.CRED_TIER
# MAGIC         WHEN 'A' THEN 'Excellent'
# MAGIC         WHEN 'B' THEN 'Good'
# MAGIC         WHEN 'C' THEN 'Fair'
# MAGIC         WHEN 'D' THEN 'Poor'
# MAGIC     END as credit_tier,
# MAGIC     ROUND(AVG(a.CRED_LIM), 2) as avg_credit_limit,
# MAGIC     ROUND(AVG(a.CURR_BAL), 2) as avg_balance
# MAGIC FROM credit_card_project.raw_data.customers_legacy c
# MAGIC JOIN credit_card_project.raw_data.accounts_legacy a ON c.CUST_ID = a.CUST_ID
# MAGIC WHERE a.ACCT_STAT != 'C'
# MAGIC GROUP BY c.CRED_TIER
# MAGIC ORDER BY c.CRED_TIER

# COMMAND ----------

# MAGIC %sql
# MAGIC -- delinquency risk
# MAGIC
# MAGIC SELECT 
# MAGIC     CASE c.CRED_TIER
# MAGIC         WHEN 'A' THEN 'Excellent'
# MAGIC         WHEN 'B' THEN 'Good'
# MAGIC         WHEN 'C' THEN 'Fair'
# MAGIC         WHEN 'D' THEN 'Poor'
# MAGIC     END as credit_tier,
# MAGIC     COUNT(a.ACCT_NUM) as total_accounts,
# MAGIC     SUM(CASE WHEN a.ACCT_STAT = 'D' THEN 1 ELSE 0 END) as delinquent_accounts,
# MAGIC     ROUND(SUM(CASE WHEN a.ACCT_STAT = 'D' THEN 1 ELSE 0 END) * 100.0 / COUNT(a.ACCT_NUM), 2) as delinquency_rate_pct
# MAGIC FROM credit_card_project.raw_data.customers_legacy c
# MAGIC JOIN credit_card_project.raw_data.accounts_legacy a ON c.CUST_ID = a.CUST_ID
# MAGIC WHERE a.ACCT_STAT != 'C'
# MAGIC GROUP BY c.CRED_TIER
# MAGIC ORDER BY delinquency_rate_pct DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- transactions by merchant category
# MAGIC
# MAGIC SELECT 
# MAGIC     CASE MCC_CD
# MAGIC         WHEN '5411' THEN 'Grocery Stores'
# MAGIC         WHEN '5812' THEN 'Restaurants'
# MAGIC         WHEN '5541' THEN 'Gas Stations'
# MAGIC         WHEN '5912' THEN 'Drug Stores'
# MAGIC         WHEN '5311' THEN 'Department Stores'
# MAGIC         WHEN '5999' THEN 'Misc Retail'
# MAGIC         WHEN '4900' THEN 'Utilities'
# MAGIC         WHEN '7011' THEN 'Hotels'
# MAGIC     END as merchant_category,
# MAGIC     COUNT(*) as transaction_count,
# MAGIC     SUM(TXN_AMT) as total_volume
# MAGIC FROM credit_card_project.raw_data.transactions_legacy
# MAGIC WHERE TXN_TYP = 'P'
# MAGIC GROUP BY MCC_CD
# MAGIC ORDER BY total_volume DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- average spend
# MAGIC
# MAGIC SELECT 
# MAGIC     CASE MCC_CD
# MAGIC         WHEN '5411' THEN 'Grocery Stores'
# MAGIC         WHEN '5812' THEN 'Restaurants'
# MAGIC         WHEN '5541' THEN 'Gas Stations'
# MAGIC         WHEN '5912' THEN 'Drug Stores'
# MAGIC         WHEN '5311' THEN 'Department Stores'
# MAGIC         WHEN '5999' THEN 'Misc Retail'
# MAGIC         WHEN '4900' THEN 'Utilities'
# MAGIC         WHEN '7011' THEN 'Hotels'
# MAGIC     END as merchant_category,
# MAGIC     ROUND(AVG(TXN_AMT), 2) as avg_transaction_amount
# MAGIC FROM credit_card_project.raw_data.transactions_legacy
# MAGIC WHERE TXN_TYP = 'P'
# MAGIC GROUP BY MCC_CD
# MAGIC ORDER BY avg_transaction_amount DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- fraud by category
# MAGIC
# MAGIC SELECT 
# MAGIC     CASE MCC_CD
# MAGIC         WHEN '5411' THEN 'Grocery Stores'
# MAGIC         WHEN '5812' THEN 'Restaurants'
# MAGIC         WHEN '5541' THEN 'Gas Stations'
# MAGIC         WHEN '5912' THEN 'Drug Stores'
# MAGIC         WHEN '5311' THEN 'Department Stores'
# MAGIC         WHEN '5999' THEN 'Misc Retail'
# MAGIC         WHEN '4900' THEN 'Utilities'
# MAGIC         WHEN '7011' THEN 'Hotels'
# MAGIC     END as merchant_category,
# MAGIC     SUM(CASE WHEN FRAUD_FL = '1' THEN 1 ELSE 0 END) as fraud_count,
# MAGIC     SUM(CASE WHEN FRAUD_FL = '1' THEN TXN_AMT ELSE 0 END) as fraud_loss,
# MAGIC     ROUND(SUM(CASE WHEN FRAUD_FL = '1' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 4) as fraud_rate_pct
# MAGIC FROM credit_card_project.raw_data.transactions_legacy
# MAGIC WHERE TXN_TYP = 'P'
# MAGIC GROUP BY MCC_CD
# MAGIC ORDER BY fraud_rate_pct DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     CASE 
# MAGIC         WHEN DLNQ_DAYS = 0 THEN 'Current'
# MAGIC         WHEN DLNQ_DAYS BETWEEN 1 AND 29 THEN '1-29 Days'
# MAGIC         WHEN DLNQ_DAYS BETWEEN 30 AND 59 THEN '30-59 Days'
# MAGIC         WHEN DLNQ_DAYS BETWEEN 60 AND 89 THEN '60-89 Days'
# MAGIC         WHEN DLNQ_DAYS >= 90 THEN '90+ Days (High Risk)'
# MAGIC     END as delinquency_stage,
# MAGIC     COUNT(*) as account_count
# MAGIC FROM credit_card_project.raw_data.accounts_legacy
# MAGIC WHERE ACCT_STAT != 'C'
# MAGIC GROUP BY 
# MAGIC     CASE 
# MAGIC         WHEN DLNQ_DAYS = 0 THEN 'Current'
# MAGIC         WHEN DLNQ_DAYS BETWEEN 1 AND 29 THEN '1-29 Days'
# MAGIC         WHEN DLNQ_DAYS BETWEEN 30 AND 59 THEN '30-59 Days'
# MAGIC         WHEN DLNQ_DAYS BETWEEN 60 AND 89 THEN '60-89 Days'
# MAGIC         WHEN DLNQ_DAYS >= 90 THEN '90+ Days (High Risk)'
# MAGIC     END
# MAGIC ORDER BY delinquency_stage

# COMMAND ----------

# MAGIC %sql
# MAGIC -- balance at risk
# MAGIC
# MAGIC SELECT 
# MAGIC     CASE 
# MAGIC         WHEN DLNQ_DAYS = 0 THEN 'Current'
# MAGIC         WHEN DLNQ_DAYS BETWEEN 1 AND 29 THEN '1-29 Days'
# MAGIC         WHEN DLNQ_DAYS BETWEEN 30 AND 59 THEN '30-59 Days'
# MAGIC         WHEN DLNQ_DAYS BETWEEN 60 AND 89 THEN '60-89 Days'
# MAGIC         WHEN DLNQ_DAYS >= 90 THEN '90+ Days (High Risk)'
# MAGIC     END as delinquency_stage,
# MAGIC     SUM(CURR_BAL) as total_balance
# MAGIC FROM credit_card_project.raw_data.accounts_legacy
# MAGIC WHERE ACCT_STAT != 'C'
# MAGIC GROUP BY 
# MAGIC     CASE 
# MAGIC         WHEN DLNQ_DAYS = 0 THEN 'Current'
# MAGIC         WHEN DLNQ_DAYS BETWEEN 1 AND 29 THEN '1-29 Days'
# MAGIC         WHEN DLNQ_DAYS BETWEEN 30 AND 59 THEN '30-59 Days'
# MAGIC         WHEN DLNQ_DAYS BETWEEN 60 AND 89 THEN '60-89 Days'
# MAGIC         WHEN DLNQ_DAYS >= 90 THEN '90+ Days (High Risk)'
# MAGIC     END
# MAGIC ORDER BY total_balance DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- records matching
# MAGIC
# MAGIC SELECT 
# MAGIC     'Legacy' as source,
# MAGIC     COUNT(*) as total_records,
# MAGIC     SUM(CRED_LIM) as total_credit,
# MAGIC     SUM(CURR_BAL) as total_balance
# MAGIC FROM credit_card_project.raw_data.accounts_legacy
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 
# MAGIC     'Modern' as source,
# MAGIC     COUNT(*) as total_records,
# MAGIC     SUM(credit_limit) as total_credit,
# MAGIC     SUM(current_balance) as total_balance
# MAGIC FROM credit_card_project.raw_data.accounts_modern

# COMMAND ----------

# MAGIC %sql
# MAGIC -- vip customer analysis
# MAGIC SELECT 
# MAGIC     CASE WHEN VIP_FL = '1' THEN 'VIP' ELSE 'Standard' END as customer_type,
# MAGIC     COUNT(*) as customer_count
# MAGIC FROM credit_card_project.raw_data.customers_legacy
# MAGIC WHERE ACTIVE_FL = 'Y'
# MAGIC GROUP BY VIP_FL
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- vip customers contribution
# MAGIC
# MAGIC SELECT 
# MAGIC     CASE WHEN c.VIP_FL = '1' THEN 'VIP' ELSE 'Standard' END as customer_type,
# MAGIC     COUNT(a.ACCT_NUM) as total_accounts,
# MAGIC     SUM(a.CURR_BAL) as total_balance,
# MAGIC     ROUND(AVG(a.CURR_BAL), 2) as avg_balance_per_account
# MAGIC FROM credit_card_project.raw_data.customers_legacy c
# MAGIC JOIN credit_card_project.raw_data.accounts_legacy a ON c.CUST_ID = a.CUST_ID
# MAGIC WHERE c.ACTIVE_FL = 'Y' AND a.ACCT_STAT != 'C'
# MAGIC GROUP BY c.VIP_FL
# MAGIC ORDER BY total_balance DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- monthly trends
# MAGIC
# MAGIC SELECT 
# MAGIC     SUBSTR(TXN_DT, 1, 6) as year_month,
# MAGIC     COUNT(*) as transaction_count,
# MAGIC     SUM(CASE WHEN TXN_TYP = 'P' THEN TXN_AMT ELSE 0 END) as purchase_volume,
# MAGIC     ROUND(AVG(TXN_AMT), 2) as avg_amount
# MAGIC FROM credit_card_project.raw_data.transactions_legacy
# MAGIC GROUP BY SUBSTR(TXN_DT, 1, 6)
# MAGIC ORDER BY year_month

# COMMAND ----------

