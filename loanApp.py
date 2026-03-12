
from delta import configure_spark_with_delta_pip,DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, count, split, when , from_json, to_timestamp, floor # It is often useful to import other functions like 'col' as well.
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, LongType, TimestampType
from pyspark.sql.types import *
import psycopg2
import time
import random
import sys
import shutil


spark = ( SparkSession.builder \
.appName("DeltaLakePracticeNew") \
.master("local[*]") \
.config(
    "spark.jars.packages",
    ",".join([
        "io.delta:delta-spark_2.12:3.1.0",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1"
    ])
) \
.config("spark.executor.memory","512m") \
.config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
.config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
.getOrCreate()
        )

print("Spark Session Created Successfully!")
spark.sparkContext.setLogLevel("ERROR")

storage_path = "./streamFolder/bronze/requests"
silver_path = "./streamFolder/silver/requests"

gold_success_path = "./streamFolder/gold/requests/success"
gold_success_business_path = "./streamFolder/gold/requests/business_owner_success"
gold_failure_path = "./streamFolder/gold/requests/fail"

gold_success_path_checkpoint = "./streamFolder/gold/requests/success/_checkpoint"
gold_success_business_path_checkpoint = "./streamFolder/gold/requests/business_owner_success/__checkpoint"
gold_failure_path_checkpoint = "./streamFolder/gold/requests/fail/_checkpoint"
no_of_cars_condition = 5
no_of_houses_condition = 6


cdc_schema = StructType([
    StructField("before", StructType([
        StructField("id", IntegerType()),
        StructField("name", StringType()),
        StructField("profileid", IntegerType()),
        StructField("age", StringType()),
        StructField("assessment", StringType()),
        StructField("target_amount", StringType()),
        StructField("payment_completion_timeline", StringType()),
        StructField("no_of_kids", IntegerType()),
        StructField("marital_status", StringType()),
        StructField("no_of_cars", IntegerType()),
        StructField("no_of_houses", IntegerType()),
        StructField("residential_location", StringType()),
        StructField("occupation_type", StringType()),
        StructField("average_monthly_income", IntegerType()),
        StructField("average_monthly_spend", IntegerType()),
        StructField("business_valuation", IntegerType()),
        StructField("number_of_staffs", IntegerType()),
        StructField("mobility_index", IntegerType()),
        StructField("gender", StringType()),
        StructField("review_round", StringType())
    ])),
    StructField("after", StructType([
        StructField("id", IntegerType()),
        StructField("name", StringType()),
        StructField("profileid", IntegerType()),
        StructField("age", StringType()),
        StructField("assessment", StringType()),
        StructField("target_amount", StringType()),
        StructField("payment_completion_timeline", StringType()),
        StructField("no_of_kids", IntegerType()),
        StructField("marital_status", StringType()),
        StructField("no_of_cars", IntegerType()),
        StructField("no_of_houses", IntegerType()),
        StructField("residential_location", StringType()),
        StructField("occupation_type", StringType()),
        StructField("average_monthly_income", IntegerType()),
        StructField("average_monthly_spend", IntegerType()),
        StructField("business_valuation", IntegerType()),
        StructField("number_of_staffs", IntegerType()),
        StructField("mobility_index", IntegerType()),
        StructField("gender", StringType()),
        StructField("review_round", StringType())
    ])),
    StructField("op", StringType()),
    StructField("ts_ms", LongType())
])


def loan_verdict(average_monthly_income,average_monthly_spend,payment_completion_timeline,no_of_houses,no_of_cars,target):

    net_income = average_monthly_income - average_monthly_spend
    repayment_capacity = payment_completion_timeline * net_income

    sufficient_capacity = repayment_capacity > target
    moderate_capacity = (repayment_capacity < target) & (average_monthly_income >= 0.6 * target)
    asset_backup = (no_of_houses > no_of_houses_condition) & (no_of_cars >= no_of_cars_condition)

    return (
        when(sufficient_capacity, "Pass")
        .when(moderate_capacity, "Pass")
        .when(asset_backup, "Pass")
        .otherwise("Failed")
    )


def loan_verdict_business_owners(average_monthly_income,average_monthly_spend,payment_completion_timeline,no_of_houses,no_of_cars,business_valuation,number_of_staffs,mobility_index,target):

    net_income = average_monthly_income - average_monthly_spend
    repayment_capacity = payment_completion_timeline * net_income

    sufficient_capacity = repayment_capacity > target
    moderate_capacity = (repayment_capacity < target) & (average_monthly_income >= 0.6 * target)
    asset_backup = (no_of_houses > no_of_houses_condition) & (no_of_cars >= no_of_cars_condition)
    business_valuation_top_strength = business_valuation*0.5 > target
    next_level_qualifier = (business_valuation*0.2 > target) & (mobility_index>=2) & (number_of_staffs>=2)

    return (
        when(sufficient_capacity, "Pass")
        .when(moderate_capacity, "Pass")
        .when(asset_backup, "Pass")
        .when(business_valuation_top_strength, "Pass")
        .when(next_level_qualifier, "Pass")
        .otherwise("Failed")
    )

    

def breakTarget(column):
    return (
        when(
            column.contains("-"),
            floor( (
                split(column, "-")[0].cast("int") +
                split(column, "-")[1].cast("int")
            ) / 2)
        )
        .otherwise(
            split(column, r"\+")[0].cast("int")
        )
    )



def write_to_postgres_success(batch_df, batch_id):

    batch_df.write \
        .format("delta") \
        .mode("append") \
        .save(gold_success_path)
    batch_df.show()
    pdf = batch_df.toPandas()  # small batches only!

    for _, row in pdf.iterrows():
        cur.execute("""
            INSERT INTO success_reviewed_table (name,operation,profileid,age,assessment,target_amount,payment_completion_timeline,residential_location,
            event_time,no_of_kids,marital_status,no_of_cars,no_of_houses,occupation_type,average_monthly_income,average_monthly_spend,business_valuation,
            number_of_staffs,mobility_index,gender,review_round,applied_target_amount,loan_review,business_loan_review)
            VALUES (%s, %s, %s,%s, %s, %s,%s, %s, %s,%s, %s, %s,%s, %s, %s,%s, %s, %s,%s, %s, %s,%s,%s,%s)
        """, (row['name'],row['operation'],row['profileid'],row['age'],row['assessment'],row['target_amount'],row['payment_completion_timeline'],row['residential_location'],
            row['event_time'],row['no_of_kids'],row['marital_status'],row['no_of_cars'],row['no_of_houses'],row['occupation_type'],row['average_monthly_income'],row['average_monthly_spend'],row['business_valuation'],
            row['number_of_staffs'],row['mobility_index'],row['gender'],row['review_round'],row['applied_target_amount'],row['loan_review'],row['business_loan_review']))
        print('new loan approved')
    conn.commit()



def write_to_postgres_business_success(batch_df, batch_id):

    batch_df.write \
        .format("delta") \
        .mode("append") \
        .save(gold_success_business_path)
    batch_df.show()
    pdf = batch_df.toPandas()  # small batches only!

    for _, row in pdf.iterrows():
        cur.execute("""
            INSERT INTO success_reviewed_business_table (name,operation,profileid,age,assessment,target_amount,payment_completion_timeline,residential_location,
            event_time,no_of_kids,marital_status,no_of_cars,no_of_houses,occupation_type,average_monthly_income,average_monthly_spend,business_valuation,
            number_of_staffs,mobility_index,gender,review_round,applied_target_amount,loan_review,business_loan_review)
            VALUES (%s, %s, %s,%s, %s, %s,%s, %s, %s,%s, %s, %s,%s, %s, %s,%s, %s, %s,%s, %s, %s,%s,%s,%s)
        """, (row['name'],row['operation'],row['profileid'],row['age'],row['assessment'],row['target_amount'],row['payment_completion_timeline'],row['residential_location'],
            row['event_time'],row['no_of_kids'],row['marital_status'],row['no_of_cars'],row['no_of_houses'],row['occupation_type'],row['average_monthly_income'],row['average_monthly_spend'],row['business_valuation'],
            row['number_of_staffs'],row['mobility_index'],row['gender'],row['review_round'],row['applied_target_amount'],row['loan_review'],row['business_loan_review']))
        print('business owner loan')
    conn.commit()

    

def write_to_postgres_failures(batch_df, batch_id):

    batch_df.write \
        .format("delta") \
        .mode("append") \
        .save(gold_failure_path)
    pdf = batch_df.toPandas()  # small batches only!

    for _, row in pdf.iterrows():
        cur.execute("""
            INSERT INTO failure_reviewed_table (name,operation,profileid,age,assessment,target_amount,payment_completion_timeline,residential_location,
            event_time,no_of_kids,marital_status,no_of_cars,no_of_houses,occupation_type,average_monthly_income,average_monthly_spend,business_valuation,
            number_of_staffs,mobility_index,gender,review_round,applied_target_amount,loan_review,business_loan_review)
            VALUES (%s, %s, %s,%s, %s, %s,%s, %s, %s,%s, %s, %s,%s, %s, %s,%s, %s, %s,%s, %s, %s,%s,%s,%s)
        """, (row['name'],row['operation'],row['profileid'],row['age'],row['assessment'],row['target_amount'],row['payment_completion_timeline'],row['residential_location'],
            row['event_time'],row['no_of_kids'],row['marital_status'],row['no_of_cars'],row['no_of_houses'],row['occupation_type'],row['average_monthly_income'],row['average_monthly_spend'],row['business_valuation'],
            row['number_of_staffs'],row['mobility_index'],row['gender'],row['review_round'],row['applied_target_amount'],row['loan_review'],row['business_loan_review']))
        print('new loan rejected')

    conn.commit()


kafka_bootsrap_servers = 'localhost:9092'
#profile_topic = 'postgres.public.borrower_profile'
requests_topic = 'postgres.public.loan_requests'


kafka_config = {
    "kafka.bootstrap.servers": kafka_bootsrap_servers,
    "subscribe": requests_topic,
    "startingOffsets": "earliest",
    "failOnDataLoss": "false",
    "kafka.security.protocol": "PLAINTEXT"
}


kafka_stream = spark.readStream.format("kafka").options(**kafka_config).load()

parsed_stream = (
    kafka_stream
        .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)","topic", "timestamp")
        
)


if not DeltaTable.isDeltaTable(spark, storage_path):
    empty_df = spark.createDataFrame([], parsed_stream.schema)

    empty_df.write.format("delta").save(storage_path)
    #parsed_stream.limit(0).write.format("delta").save(storage_path)


query = (
    parsed_stream.writeStream
        .format("delta")
        .option("checkpointLocation", storage_path + "_checkpoint")
        .option("path", storage_path)
        .outputMode("append")
        .trigger(processingTime="10 seconds") 
        .start()
)

df = spark.readStream.format("delta").load(
   storage_path
)

#df.createOrReplaceTempView("mainhold_delta")

parsed_bronze = spark.readStream.format("delta").load(storage_path)

parsed_struct = (
    parsed_bronze
        .withColumn("parsed", from_json(col("value"), cdc_schema))
)


silver_df = (
    parsed_struct
        .filter(col("parsed.op").isin("c", "u"))  # create + update
        .select(
            col("parsed.after.id").alias("id"),
            col("parsed.after.name").alias("name"),

            when(col("parsed.op") == "c", "create")
            .when(col("parsed.op") == "u", "update")
            .otherwise("unknown")
            .alias("operation"),

            col("parsed.after.profileid").alias("profileid"),
            col("parsed.after.age").alias("age"),
            col("parsed.after.assessment").alias("assessment"),
            col("parsed.after.target_amount").alias("target_amount"),
            col("parsed.after.payment_completion_timeline").cast('int').alias("payment_completion_timeline"),
            col("parsed.after.residential_location").alias("residential_location"),
            to_timestamp(col("parsed.ts_ms") / 1000).alias("event_time"),
            col("parsed.after.no_of_kids").alias("no_of_kids"),
            col("parsed.after.marital_status").alias("marital_status"),
            col("parsed.after.no_of_cars").alias("no_of_cars"),

            col("parsed.after.no_of_houses").alias("no_of_houses"),
            col("parsed.after.occupation_type").alias("occupation_type"),
            col("parsed.after.average_monthly_income").alias("average_monthly_income"),
            col("parsed.after.average_monthly_spend").alias("average_monthly_spend"),
            col("parsed.after.business_valuation").alias("business_valuation"),

            col("parsed.after.number_of_staffs").alias("number_of_staffs"),
            col("parsed.after.mobility_index").alias("mobility_index"),
            col("parsed.after.gender").alias("gender"),
            col("parsed.after.review_round").alias("review_round"),
        )
     .withColumn(
            "applied_target_amount",
             breakTarget(col('target_amount'))
        )
     .withColumn(
            "loan_review",
            loan_verdict(col('average_monthly_income'),col('average_monthly_spend'),col('payment_completion_timeline'),col('no_of_houses'),col('no_of_cars'),col('applied_target_amount'))
        )
    .withColumn(
            "business_loan_review",
            loan_verdict_business_owners(col('average_monthly_income'),col('average_monthly_spend'),col('payment_completion_timeline'),col('no_of_houses'),col('no_of_cars'),col('business_valuation'),col('number_of_staffs'),col('mobility_index'),col('applied_target_amount'))
    )
)


if not DeltaTable.isDeltaTable(spark, silver_path):
    silver_df_two = spark.createDataFrame([], silver_df.schema)

    silver_df_two.write.format("delta").save(silver_path)
    #parsed_stream.limit(0).write.format("delta").save(storage_path)


queryNew = (
    silver_df.writeStream
        .format("delta")
        .option("checkpointLocation", silver_path + "_checkpoint")
        .option("path", silver_path)
        .outputMode("append")
        .trigger(processingTime="10 seconds") 
        .start()
)


read_silver_stream = spark.readStream.format("delta").load(silver_path)


successfulStreamData = read_silver_stream.filter(col("loan_review") == 'Pass')

failedStreamData = read_silver_stream.filter(col("loan_review") == 'Failed')

successfulStreamBusinessData = read_silver_stream.filter(
    (col("business_loan_review") == "Pass") | (col("loan_review") == "Pass")
)

#failedStreamBusinessData = read_silver_stream.filter(col("business_loan_review") == 'Failed')



#import pandas as pd

db_params = {
    "host"    : "localhost",
    "database": "ruleToData",
    "user"    : "ruleToData",
    "password": "rule2Data",
    "port"    : "5432"  # Default PostgreSQL port
}

conn = psycopg2.connect(**db_params)      
# Create a cursor object
cur = conn.cursor()


successfulStreamData.writeStream \
    .foreachBatch(write_to_postgres_success) \
    .option("checkpointLocation", gold_success_path_checkpoint) \
    .start()

successfulStreamBusinessData.writeStream \
    .foreachBatch(write_to_postgres_business_success) \
    .option("checkpointLocation", gold_success_business_path_checkpoint) \
    .start()


failedStreamData.writeStream \
    .foreachBatch(write_to_postgres_failures) \
    .option("checkpointLocation", gold_failure_path_checkpoint) \
    .start()
spark.streams.awaitAnyTermination()
#spark.streams.active - list of all active streams