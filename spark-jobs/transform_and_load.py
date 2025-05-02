import json
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.functions import count, sum, col, when, avg, month

STAGING_PATH = "/opt/airflow/data/staging"
LOAD_PATH = "/opt/airflow/data/load"

spark = SparkSession.builder\
    .appName("PySpark Example")\
    .master("local[*]")\
    .getOrCreate()

df_user = spark.read.parquet(f"{STAGING_PATH}/users", header=True, inferSchema=True)
df_transaction = spark.read.parquet(f"{STAGING_PATH}/transaction", header=True, inferSchema = True)
df_card = spark.read.parquet(f"{STAGING_PATH}/cards", header=True, inferSchema = True)
df_mcc = spark.read.parquet(f"{STAGING_PATH}/mcc", header=True, inferSchema = True)
df_fraud_labels = spark.read.parquet(f"{STAGING_PATH}/fraud_labels", header=True, inferSchema = True)

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
load_retire_insight = df_user_retire_insight.select("id", "current_age", "retirement_age", "years_to_retirement")
load_retire_insight.write.mode("overwrite").format("parquet").save(f"{LOAD_PATH}/retire_insights")

### Identifying low credit score but high income segments
low_credit_score_x_high_income = df_user.filter((col("credit_score") < 650) & (col("yearly_income") > 60000)).select(
    "id", "yearly_income", "credit_score", "total_debt"
)
low_credit_score_x_high_income.write.mode("overwrite").format("parquet").save(f"{LOAD_PATH}/low_credit_score_x_high_income")


### Total Spent of Male and Female in each MCC

total_spent_sex_mcc = df_transaction.join(df_mcc, df_transaction['mcc'] == df_mcc['mcc_code'], "left")\
    .join(df_user, df_transaction['client_id'] == df_user['id'], 'inner')\
    .groupBy("description", 'gender')\
    .agg(sum("amount").alias("total_spent"))\
    .orderBy(col("description"), col('gender'))
total_spent_sex_mcc.write.mode("overwrite").format("parquet").save(f"{LOAD_PATH}/total_spent_sex_mcc")

### Insights on age
df_user_label_age = df_user.withColumn(
    "age_group",
    when(col("current_age") < 30, "Under 30")
    .when((col("current_age") >= 30) & (col("current_age") < 50), "30-49")
    .when((col("current_age") >= 50) & (col("current_age") < 65), "50-64")
    .otherwise("65+")
)

age_insights = df_user_label_age.groupBy("age_group").agg(
    count("*").alias("num_users"),
    avg("yearly_income").alias("avg_income"),
    avg("total_debt").alias("avg_debt"),
    avg("num_credit_cards").alias("avg_num_cards"),
    avg("credit_score").alias("avg_credit_score")
).orderBy("age_group")
age_insights.write.mode("overwrite").format("parquet").save(f"{LOAD_PATH}/age_insights")


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

df_user_risk.write.mode("overwrite").format("parquet").save(f"{LOAD_PATH}/user_level_risk")

spark.stop()
