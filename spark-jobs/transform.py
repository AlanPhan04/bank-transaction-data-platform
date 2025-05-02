import json
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.functions import count, sum, col, when, avg, month

DATA_PATH = "/opt/airflow/data/FinancialTransactionsDatasetAnalytics"
STAGING_PATH = "/opt/airflow/data/data_source"
# Tạo SparkSession
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

df_user = spark.read.csv(f"{DATA_PATH}/users_data.csv", header=True, inferSchema=True)
df_transaction = spark.read.csv(f"{DATA_PATH}/transactions_data.csv", header=True, inferSchema = True)
df_card = spark.read.csv(f"{DATA_PATH}/cards_data.csv", header=True, inferSchema = True)
df_mcc = spark.createDataFrame(lst_mcc, mcc_schema)
df_fraud_labels = spark.createDataFrame(lst_fraud_labels, fraud_labels_schema)

df_user = df_user.withColumn(
        "per_capita_income",
        regexp_replace(col("per_capita_income"), "[$]", "").cast("int"))\
                .withColumn(
        "yearly_income", 
        regexp_replace(col("yearly_income"), "[$]", "").cast("int"))\
                .withColumn(
        "total_debt", 
        regexp_replace(col("total_debt"), "[$]", "").cast("int"))

df_user = df_user.withColumn(
    "gender",
    when(col("gender")=="Male", "M")
    .when(col("gender")=="Female", "F")
    .otherwise(col("gender"))
)

df_card = df_card\
    .withColumn("month_expires", split(col("expires"), "/").getItem(0).cast("int"))\
    .withColumn("year_expires", split(col("expires"), "/").getItem(1).cast("int"))\
    .withColumn("month_acct_open", split(col("acct_open_date"), "/").getItem(1).cast("int"))\
    .withColumn("year_acct_open", split(col("acct_open_date"), "/").getItem(1).cast("int"))


df_card = df_card.withColumn(
    "credit_limit",
    regexp_replace(col("credit_limit"), "[$]", "")
)

df_card = df_card\
    .withColumn("has_chip", 
        when(col("has_chip")=="YES", True)
        .when(col("has_chip")=="NO", False)
        .otherwise(None)
        .cast("boolean")
    )\
    .withColumn("card_on_dark_web", 
        when(col("card_on_dark_web")=="Yes", True)
        .when(col("card_on_dark_web")=="No", False)
        .otherwise(None)
        .cast("boolean")
    )

df_mcc.withColumn("mcc_code", df_mcc["mcc_code"].cast("int"))
df_fraud_labels.withColumn("transaction_id", df_fraud_labels["transaction_id"].cast("int"))

df_transaction = df_transaction\
    .withColumn("amount", regexp_replace(col("amount"), "[$]", "").cast("int"))


### Predict the user's remaining asset accumulation potential 
### and recommend suitable savings and investment products.
df_user_retire_insight = df_user.withColumn("years_to_retirement", col("retirement_age") - col("current_age"))
df_user_retire_insight.select("id", "current_age", "retirement_age", "years_to_retirement").show(10)


### Identifying low credit score but high income segments
df_user.filter((col("credit_score") < 650) & (col("yearly_income") > 60000)).select(
    "id", "yearly_income", "credit_score", "total_debt"
).show(10)



### Total Spent of Male and Female in each MCC

df_transaction.join(df_mcc, df_transaction['mcc'] == df_mcc['mcc_code'], "left")\
    .join(df_user, df_transaction['client_id'] == df_user['id'], 'inner')\
    .groupBy("description", 'gender')\
    .agg(sum("amount").alias("total_spent"))\
    .orderBy(col("description"), col('gender'))\
    .show(10)

### Insights on age
df_user_label_age = df_user.withColumn(
    "age_group",
    when(col("current_age") < 30, "Under 30")
    .when((col("current_age") >= 30) & (col("current_age") < 50), "30-49")
    .when((col("current_age") >= 50) & (col("current_age") < 65), "50-64")
    .otherwise("65+")
)

df_user_label_age.groupBy("age_group").agg(
    count("*").alias("num_users"),
    avg("yearly_income").alias("avg_income"),
    avg("total_debt").alias("avg_debt"),
    avg("num_credit_cards").alias("avg_num_cards"),
    avg("credit_score").alias("avg_credit_score")
).orderBy("age_group").show(10)

### Categorize level of risk to users
df_user_risk = df_user.withColumn("debt_to_income_ratio", (col("total_debt") / col("yearly_income")))
df_user_risk = df_user_risk.withColumn(
    "risk_level",
    when(col("debt_to_income_ratio") > 0.6, "High Risk")
    .when(col("debt_to_income_ratio") > 0.4, "Medium Risk")
    .otherwise("Low Risk")
)

df_user_risk.groupBy("risk_level").agg(
    count("*").alias("num_users"),
    avg("credit_score").alias("avg_credit_score")
).show()
# df_user_risk.write.mode("overwrite").csv("/opt/airflow/data/insights/avg_transaction_by_gender.csv", header=True)


spark.stop()
