# Databricks notebook source
# Generating data for credit card data simulation

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random

# COMMAND ----------

# CONFIGURATION

# Setting seeds for reproducibility (same data every time we run)
np.random.seed(42)
random.seed(42)

# Data volumes
NUM_CUSTOMERS = 50000      # 50K customers
NUM_ACCOUNTS = 65000       # 65K accounts (some customers have multiple cards)
NUM_TRANSACTIONS = 500000  # 500K transactions

# Date range for data
START_DATE = datetime(2023, 1, 1)
END_DATE = datetime(2024, 12, 31)

print("=" * 70)
print("CREDIT CARD DATA GENERATOR - LEGACY & MODERN SYSTEMS")
print("=" * 70)
print("\nConfiguration:")
print(f"  Customers: {NUM_CUSTOMERS:,}")
print(f"  Accounts: {NUM_ACCOUNTS:,}")
print(f"  Transactions: {NUM_TRANSACTIONS:,}")
print(f"  Date Range: {START_DATE.date()} to {END_DATE.date()}")
print("\n" + "=" * 70)


# COMMAND ----------

# HELPER FUNCTIONS

def generate_random_date(start, end):
    """Generate random datetime between start and end"""
    delta = end - start
    random_days = random.randint(0, delta.days)
    return start + timedelta(days=random_days)

# COMMAND ----------

# CUSTOMER DATA GENERATION
# Generating legacy customers data

def generate_legacy_customers(n):
    print(f"\n[1/6] Generating {n:,} Legacy Customers...")
    
    customers = []
    for i in range(n):
        cust_id = f"C{str(i+1).zfill(10)}"  # C0000000001 format
        ssn_last4 = str(random.randint(1000, 9999))
        
        # Credit tier: A=30%, B=40%, C=20%, D=10%
        credit_tier = random.choices(
            ['A', 'B', 'C', 'D'], 
            weights=[0.3, 0.4, 0.2, 0.1])[0]
        
        # State distribution (focus on PNC's footprint)
        state_code = random.choice([
            'PA', 'PA', 'PA',  # Pennsylvania (heavy weight)
            'OH', 'OH', 'KY', 'IN', 'IL',
            'FL', 'TX', 'CA', 'NY', 'NJ'])
        
        # Customer opened account sometime in date range
        open_date = generate_random_date(START_DATE, END_DATE)
        
        # 95% active, 5% inactive
        active_flag = random.choices(['Y', 'N'], weights=[0.95, 0.05])[0]
        
        # 15% VIP status
        vip_flag = random.choices(['1', '0'], weights=[0.15, 0.85])[0]
        
        customers.append({
            'CUST_ID': cust_id,
            'SSN_L4': ssn_last4,
            'CRED_TIER': credit_tier,
            'ST_CD': state_code,
            'OPEN_DT': open_date.strftime('%Y%m%d'),  # YYYYMMDD format
            'ACTIVE_FL': active_flag,
            'VIP_FL': vip_flag})
    
    df = pd.DataFrame(customers)
    print(f"Generated {len(df):,} records")
    print(f"Columns: {list(df.columns)}")
    
    return df

# COMMAND ----------

# Generating modern customers data

def generate_modern_customers(n):
    print(f"\n[2/6] Generating {n:,} Modern Customers...")

    customers = []

    # Mapping dictionaries
    credit_tiers= {
        'A': 'Excellent',
        'B': 'Good',
        'C': 'Fair',
        'D': 'Poor'
    }

    for i in range(n):
        cust_id = f"C{str(i+1).zfill(10)}"

        # Credit Tier and Score
        legacy_tier= random.choices(
            ['A', 'B', 'C', 'D'],
            weights=[0.3, 0.4, 0.2, 0.1])[0]
        
        # Credit score ranges by tier
        score_ranges = {
            'A': (750, 850),
            'B': (670, 749),
            'C': (580, 669),
            'D': (500, 579)}
        
        credit_score= random.randint(*score_ranges[legacy_tier])

        # State
        state = random.choice([
            'PA', 'PA', 'PA',
            'OH', 'OH', 'KY', 'IN', 'IL',
            'FL', 'TX', 'CA', 'NY', 'NJ'
        ])

        # Customer since date
        customer_since = generate_random_date(START_DATE, END_DATE)

        # Flags
        is_active = random.choices([True, False], weights=[0.95, 0.05])[0]
        is_vip = random.choices([True, False], weights=[0.15, 0.85])[0]
        marketing_consent = random.choices([True, False], weights=[0.7, 0.3])[0]

        customers.append({
            'customer_id': cust_id,
            'credit_score_band': credit_tiers[legacy_tier],
            'credit_score': credit_score,
            'state': state,
            'customer_since': customer_since.strftime('%Y-%m-%d'),  # ISO format
            'is_active': is_active,
            'is_vip': is_vip,
            'marketing_consent': marketing_consent,
            'last_updated': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        })

    df = pd.DataFrame(customers)
    print(f"Generated {len(df):,} records")
    print(f"Columns: {list(df.columns)}")

    return df



# COMMAND ----------

# ACCOUNT DATA GENERATION


def generate_legacy_accounts(customers_df, n):
    print(f"\n[3/6] Generating {n:,} Legacy Accounts...")

    accounts = []
    customer_ids = customers_df['CUST_ID'].tolist()

    # Product code definitions
    # 01 = Cash Rewards (40%)
    # 02 = Travel Rewards (25%)
    # 03 = Basic Card (25%)
    # 04 = Business Card (10%)

    for i in range(n):
        # Random customer (some will have multiple cards)
        cust_id = random.choice(customer_ids)

        # 16-digit card number
        acct_num = f"{random.randint(4000, 4999)}{str(i).zfill(12)}"

        # Product code with realistic distribution
        prod_cd = random.choices(
            ['01', '02', '03', '04'],
            weights=[0.4, 0.25, 0.25, 0.1]
        )[0]

        # Credit limit varies by product
        credit_limits = {
            '01': random.choice([5000, 10000, 15000, 25000]),      # Cash Rewards
            '02': random.choice([10000, 15000, 25000, 50000]),     # Travel
            '03': random.choice([1000, 2500, 5000, 7500]),         # Basic
            '04': random.choice([15000, 25000, 50000, 100000])     # Business
        }
        credit_limit = credit_limits[prod_cd]

        # Current balance (0% to 80% of limit)
        current_balance = round(random.uniform(0, credit_limit * 0.8), 2)

        # Account opened date
        open_date = generate_random_date(START_DATE, END_DATE)

        # APR varies by product and risk
        apr_range = {
            '01': (14.99, 21.99),
            '02': (16.99, 23.99),
            '03': (19.99, 26.99),
            '04': (13.99, 22.99)
        }
        apr_rate = round(random.uniform(*apr_range[prod_cd]), 2)

        # Account status: A=Active(88%), D=Delinquent(10%), C=Closed(2%)
        acct_stat = random.choices(
            ['A', 'D', 'C'],
            weights=[0.88, 0.10, 0.02]
        )[0]

        # Delinquency days (only if status is Delinquent)
        if acct_stat == 'D':
            dlnq_days = random.choice([30, 60, 90, 120])
        else:
            dlnq_days = 0

        accounts.append({
            'ACCT_NUM': acct_num,
            'CUST_ID': cust_id,
            'PROD_CD': prod_cd,
            'OPEN_DT': open_date.strftime('%Y%m%d'),
            'CRED_LIM': int(credit_limit),
            'CURR_BAL': current_balance,
            'AVAIL_CR': credit_limit - current_balance,
            'APR_RT': apr_rate,
            'ACCT_STAT': acct_stat,
            'DLNQ_DAYS': dlnq_days
        })

    df = pd.DataFrame(accounts)
    print(f"Generated {len(df):,} records")
    print(f"Total Credit Extended: ${df['CRED_LIM'].sum():,.2f}")
    print(f"Total Outstanding Balance: ${df['CURR_BAL'].sum():,.2f}")

    return df

# COMMAND ----------

def generate_modern_accounts(customers_df, n):
    print(f"\n[4/6] Generating {n:,} Modern Accounts...")

    accounts = []
    customer_ids = customers_df['customer_id'].tolist()

    # Product mappings
    product_mapping = {
        '01': 'Cash Rewards',
        '02': 'Travel Rewards',
        '03': 'Basic Card',
        '04': 'Business Card'
    }

    status_mapping = {
        'A': 'Active',
        'D': 'Delinquent',
        'C': 'Closed'
    }

    for i in range(n):
        cust_id = random.choice(customer_ids)
        acct_num = f"{random.randint(4000, 4999)}{str(i).zfill(12)}"

        # Product
        prod_cd = random.choices(
            ['01', '02', '03', '04'],
            weights=[0.4, 0.25, 0.25, 0.1]
        )[0]

        # Credit limit
        credit_limits = {
            '01': random.choice([5000, 10000, 15000, 25000]),
            '02': random.choice([10000, 15000, 25000, 50000]),
            '03': random.choice([1000, 2500, 5000, 7500]),
            '04': random.choice([15000, 25000, 50000, 100000])
        }
        credit_limit = credit_limits[prod_cd]
        current_balance = round(random.uniform(0, credit_limit * 0.8), 2)

        # Dates
        open_date = generate_random_date(START_DATE, END_DATE)
        last_payment = END_DATE - timedelta(days=random.randint(1, 60))

        # APR
        apr_range = {
            '01': (14.99, 21.99),
            '02': (16.99, 23.99),
            '03': (19.99, 26.99),
            '04': (13.99, 22.99)
        }
        apr_rate = round(random.uniform(*apr_range[prod_cd]), 2)

        # Status
        acct_stat = random.choices(['A', 'D', 'C'], weights=[0.88, 0.10, 0.02])[0]
        days_delinquent = random.choice([30, 60, 90, 120]) if acct_stat == 'D' else 0

        # Calculated field: utilization rate
        utilization_rate = round((current_balance / credit_limit) * 100, 2) if credit_limit > 0 else 0

        accounts.append({
            'account_number': acct_num,
            'customer_id': cust_id,
            'product_name': product_mapping[prod_cd],
            'product_code': prod_cd,
            'account_opened_date': open_date.strftime('%Y-%m-%d'),
            'credit_limit': int(credit_limit),
            'current_balance': current_balance,
            'available_credit': credit_limit - current_balance,
            'apr_rate': apr_rate,
            'account_status': status_mapping[acct_stat],
            'days_delinquent': days_delinquent,
            'utilization_rate': utilization_rate,
            'last_payment_date': last_payment.strftime('%Y-%m-%d'),
            'last_updated': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        })

    df = pd.DataFrame(accounts)
    print(f"Generated {len(df):,} records")
    print(f"Avg Utilization Rate: {df['utilization_rate'].mean():.2f}%")

    return df

# COMMAND ----------

# TRANSACTION DATA GENERATION

def generate_legacy_transactions(accounts_df, n):
    print(f"\n[5/6] Generating {n:,} Legacy Transactions...")

    transactions = []
    account_numbers = accounts_df['ACCT_NUM'].tolist()

    # Merchant Category Codes
    mcc_codes = {
        '5411': 'Grocery Stores',
        '5812': 'Restaurants',
        '5541': 'Gas Stations',
        '5912': 'Drug Stores',
        '5311': 'Department Stores',
        '5999': 'Misc Retail',
        '4900': 'Utilities',
        '7011': 'Hotels'
    }

    # Transaction amount ranges by MCC
    amount_ranges = {
        '5411': (20, 250),     # Grocery
        '5812': (15, 150),     # Restaurants
        '5541': (30, 100),     # Gas
        '5912': (10, 80),      # Drugstore
        '5311': (50, 500),     # Department store
        '5999': (10, 300),     # Misc retail
        '4900': (50, 200),     # Utilities
        '7011': (100, 500)     # Hotels
    }

    for i in range(n):
        acct_num = random.choice(account_numbers)

        # Random transaction datetime
        txn_datetime = START_DATE + timedelta(
            days=random.randint(0, (END_DATE - START_DATE).days),
            hours=random.randint(0, 23),
            minutes=random.randint(0, 59),
            seconds=random.randint(0, 59)
        )

        # MCC code
        mcc = random.choice(list(mcc_codes.keys()))

        # Amount based on MCC
        amount = round(random.uniform(*amount_ranges[mcc]), 2)

        # Transaction type: P=Purchase(90%), R=Return(8%), F=Fee(2%)
        txn_type = random.choices(
            ['P', 'R', 'F'],
            weights=[0.90, 0.08, 0.02]
        )[0]

        # Authorization code (6-char alphanumeric)
        auth_code = ''.join(random.choices('ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789', k=6))

        # Fraud flag: 0.2% fraud rate
        fraud_flag = '1' if random.random() < 0.002 else '0'

        transactions.append({
            'TXN_ID': f"T{str(i+1).zfill(12)}",
            'ACCT_NUM': acct_num,
            'TXN_DT': txn_datetime.strftime('%Y%m%d'),      # YYYYMMDD
            'TXN_TM': txn_datetime.strftime('%H%M%S'),      # HHMMSS
            'MCC_CD': mcc,
            'TXN_AMT': amount,
            'TXN_TYP': txn_type,
            'AUTH_CD': auth_code,
            'FRAUD_FL': fraud_flag
        })

    df = pd.DataFrame(transactions)


    # Calculate stats
    total_volume = df[df['TXN_TYP'] == 'P']['TXN_AMT'].sum()
    fraud_count = (df['FRAUD_FL'] == '1').sum()
    fraud_amount = df[df['FRAUD_FL'] == '1']['TXN_AMT'].sum()

    print(f"Generated {len(df):,} records")
    print(f"Total Purchase Volume: ${total_volume:,.2f}")
    print(f"Fraud Transactions: {fraud_count:,} (${fraud_amount:,.2f})")

    return df

# COMMAND ----------

def generate_modern_transactions(accounts_df, n):
    print(f"\n[6/6] Generating {n:,} Modern Transactions...")

    transactions = []
    account_numbers = accounts_df['account_number'].tolist()

    # MCC mappings
    mcc_mapping = {
        '5411': 'Grocery Stores',
        '5812': 'Restaurants',
        '5541': 'Gas Stations',
        '5912': 'Drug Stores',
        '5311': 'Department Stores',
        '5999': 'Misc Retail',
        '4900': 'Utilities',
        '7011': 'Hotels'
    }

    txn_type_mapping = {
        'P': 'Purchase',
        'R': 'Return',
        'F': 'Fee'
    }

    amount_ranges = {
        '5411': (20, 250),
        '5812': (15, 150),
        '5541': (30, 100),
        '5912': (10, 80),
        '5311': (50, 500),
        '5999': (10, 300),
        '4900': (50, 200),
        '7011': (100, 500)
    }

    for i in range(n):
        acct_num = random.choice(account_numbers)

        # Transaction datetime
        txn_datetime = START_DATE + timedelta(
            days=random.randint(0, (END_DATE - START_DATE).days),
            hours=random.randint(0, 23),
            minutes=random.randint(0, 59),
            seconds=random.randint(0, 59)
        )

        # MCC
        mcc = random.choice(list(mcc_mapping.keys()))
        amount = round(random.uniform(*amount_ranges[mcc]), 2)

        # Type
        txn_typ = random.choices(['P', 'R', 'F'], weights=[0.90, 0.08, 0.02])[0]

        # Auth code
        auth_code = ''.join(random.choices('ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789', k=6))

        # Fraud detection
        is_fraudulent = random.random() < 0.002

        # Risk score: 0-100 (higher = more suspicious)
        if is_fraudulent:
            risk_score = round(random.uniform(70, 100), 2)
        else:
            risk_score = round(random.uniform(0, 30), 2)

        transactions.append({
            'transaction_id': f"T{str(i+1).zfill(12)}",
            'account_number': acct_num,
            'transaction_datetime': txn_datetime.strftime('%Y-%m-%d %H:%M:%S'),
            'merchant_category_code': mcc,
            'merchant_category': mcc_mapping[mcc],
            'transaction_amount': amount,
            'transaction_type': txn_type_mapping[txn_typ],
            'authorization_code': auth_code,
            'is_fraudulent': is_fraudulent,
            'risk_score': risk_score,
            'processed_timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        })

    df = pd.DataFrame(transactions)

    fraud_count = df['is_fraudulent'].sum()
    fraud_rate = (fraud_count / len(df)) * 100

    print(f"Generated {len(df):,} records")
    print(f"Fraud Rate: {fraud_rate:.4f}%")

    return df

# COMMAND ----------

# MAIN EXECUTION

if __name__ == "__main__":

    # Generate all datasets
    print("\n" + "=" * 70)
    print("STARTING DATA GENERATION...")
    print("=" * 70)

    legacy_customers = generate_legacy_customers(NUM_CUSTOMERS)
    modern_customers = generate_modern_customers(NUM_CUSTOMERS)

    legacy_accounts = generate_legacy_accounts(legacy_customers, NUM_ACCOUNTS)
    modern_accounts = generate_modern_accounts(modern_customers, NUM_ACCOUNTS)

    legacy_transactions = generate_legacy_transactions(legacy_accounts, NUM_TRANSACTIONS)
    modern_transactions = generate_modern_transactions(modern_accounts, NUM_TRANSACTIONS)

    # Save to CSV
    print("\n" + "=" * 70)
    print("SAVING FILES TO CATALOG...")
    print("=" * 70)

    tables= {
        'customers_legacy': legacy_customers,
        'customers_modern': modern_customers,
        'accounts_legacy': legacy_accounts,
        'accounts_modern': modern_accounts,
        'transactions_legacy': legacy_transactions,
        'transactions_modern': modern_transactions
    }

    for table_name, df in tables.items():
        spark_df= spark.createDataFrame(df)
        spark_df.write.mode("overwrite").saveAsTable(f"credit_card_project.raw_data.{table_name}")
        print(f" Saved: {table_name}")

    print("\n All Tables saved to credit_card_project.raw_data")

    # Summary statistics
    print("\n" + "=" * 70)
    print("GENERATION COMPLETE - SUMMARY")
    print("=" * 70)
    print(f"\n Data Volumes:")
    print(f"Customers: {len(legacy_customers):,}")
    print(f"Accounts: {len(legacy_accounts):,}")
    print(f"Transactions: {len(legacy_transactions):,}")

    print(f"\n Financial Metrics:")
    print(f"Total Credit Extended: ${legacy_accounts['CRED_LIM'].sum():,.2f}")
    print(f"Total Outstanding Balance: ${legacy_accounts['CURR_BAL'].sum():,.2f}")
    print(f"Average Utilization: {(legacy_accounts['CURR_BAL'] / legacy_accounts['CRED_LIM'] * 100).mean():.2f}%")

    print(f"\n Risk Indicators:")
    delinquent = (legacy_accounts['ACCT_STAT'] == 'D').sum()
    delinquent_pct = (delinquent / len(legacy_accounts)) * 100
    print(f"Delinquent Accounts: {delinquent:,} ({delinquent_pct:.2f}%)")

    fraud_txns = (legacy_transactions['FRAUD_FL'] == '1').sum()
    fraud_pct = (fraud_txns / len(legacy_transactions)) * 100
    print(f"Fraud Transactions: {fraud_txns:,} ({fraud_pct:.4f}%)")

    print("\n" + "=" * 70)
    print("ALL FILES GENERATED SUCCESSFULLY!")
    print("=" * 70)
    

# COMMAND ----------

# Next Steps:
# 1. Review the CSV files to understand the data
# 2. Move to Step 2: SQL Analysis
# 3. Import data into SQLite/PostgreSQL for querying
