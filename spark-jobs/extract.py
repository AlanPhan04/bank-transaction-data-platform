import json
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.functions import count, sum, col, when, avg, month

DATA_PATH = "/opt/airflow/data/FinancialTransactionsDatasetAnalytics"
STAGING_PATH = "/opt/airflow/data/staging"

spark = SparkSession.builder\
    .appName("PySpark Example")\
    .master("local[*]")\
    .getOrCreate()

users_schema = StructType([
    StructField("id", IntegerType()),
    StructField("current_age", IntegerType()),
    StructField("retirement_age", IntegerType()),
    StructField("birth_year", IntegerType()),
    StructField("birth_month", IntegerType()),
    StructField("gender", StringType()),
    StructField("address", StringType()),
    StructField("latitude", IntegerType()),
    StructField("longitude", IntegerType()),
    StructField("per_capita_income", StringType()),
    StructField("yearly_income", StringType()),
    StructField("total_debt", StringType()),
    StructField("credit_score", IntegerType()),
    StructField("num_credit_cards", IntegerType())
])

transactions_schema = StructType([
    StructField("id", IntegerType()),
    StructField("date", TimestampType()),
    StructField("client_id", IntegerType()),
    StructField("card_id", IntegerType()),
    StructField("amount", StringType()),
    StructField("use_chip", StringType()),
    StructField("merchant_id", IntegerType()),
    StructField("merchant_city", StringType()),
    StructField("merchant_state", StringType()),
    StructField("zip", IntegerType()),
    StructField("mcc", IntegerType()),
    StructField("errors", StringType())
])

cards_schema = StructType([
    StructField("id", IntegerType()),
    StructField("client_id", IntegerType()),
    StructField("card_brand", StringType()),
    StructField("card_type", StringType()),
    StructField("card_number", IntegerType()),
    StructField("expires", StringType()),
    StructField("cvv", IntegerType()),
    StructField("has_chip", StringType()),
    StructField("num_cards_issued", IntegerType()),
    StructField("credit_limit", StringType()),
    StructField("acct_open_date", StringType()),
    StructField("year_pin_last_changed", IntegerType()),
    StructField("card_on_dark_web", StringType())
])

with open(f"{DATA_PATH}/mcc_codes.json", "r") as f:
    raw_mcc = json.load(f)

lst_mcc = [(k, v) for k, v in raw_mcc.items()]

mcc_schema = StructType([
    StructField("mcc_code", StringType(), True),
    StructField("description", StringType(), True)
])

with open(f"{DATA_PATH}/train_fraud_labels.json", "r") as f:
    raw_fraud_labels = json.load(f)

lst_fraud_labels = [(k, v) for k, v in raw_fraud_labels['target'].items()]

fraud_labels_schema = StructType([
    StructField("transaction_id", StringType()),
    StructField("label", StringType())
])

df_user = spark.read.csv(f"{DATA_PATH}/users_data.csv", header=True, schema=users_schema)
df_user.write.mode("overwrite").format("parquet").save(f"{STAGING_PATH}/users")

df_transaction = spark.read.csv(f"{DATA_PATH}/transactions_data.csv", header=True, schema=transactions_schema)
df_transaction.write.mode("overwrite").format("parquet").save(f"{STAGING_PATH}/transaction")


df_card = spark.read.csv(f"{DATA_PATH}/cards_data.csv", header=True, schema = cards_schema)
df_card.write.mode("overwrite").format("parquet").save(f"{STAGING_PATH}/cards")

df_mcc = spark.createDataFrame(lst_mcc, mcc_schema)
df_mcc.write.mode("overwrite").format("parquet").save(f"{STAGING_PATH}/mcc")

df_fraud_labels = spark.createDataFrame(lst_fraud_labels, fraud_labels_schema)
df_fraud_labels.write.mode("overwrite").format("parquet").save(f"{STAGING_PATH}/fraud_labels")

spark.stop()