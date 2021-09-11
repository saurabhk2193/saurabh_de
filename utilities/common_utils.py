from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType, IntegerType,BooleanType,DoubleType
import os
import shutil
from pyspark.sql.functions import col, expr, lit, udf, length, when, current_timestamp, concat

import logging

# Function to create Spark Session


def create_spark_session():
    spark = SparkSession.builder.master("local").appName("spark_de_test").getOrCreate()
    return spark

# Utility function to handle logs


def get_logger(prog_name):
    # create logger
    logger = logging.getLogger(prog_name)
    logger.setLevel(logging.DEBUG)
    # create console handler and set level to debug
    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)
    # create formatter
    formatter = logging.Formatter('%(asctime)s %(name)s %(levelname)s: %(message)s')
    # add formatter to ch
    ch.setFormatter(formatter)
    # add ch to logger
    logger.addHandler(ch)
    return logger

# JSON schema definition


def write_to_csv(output_path, df_final, filename="output"):
    df_final.repartition(1).write.csv(output_path, sep='|')
    list1 = os.listdir(output_path)
    for name in list1:
        if name.endswith(".csv"):
            src = output_path+"\\"+name
            os.rename(src, "output\\task_output\\"+filename+".csv")
            print(src)
    shutil.rmtree(output_path)


j_schema = StructType([
    StructField("name", StringType()),
    StructField("ingredients", StringType()),
    StructField("url", StringType()),
    StructField("image", StringType()),
    StructField("cookTime", StringType()),
    StructField("recipeYield", StringType()),
    StructField("datePublished", StringType()),
    StructField("prepTime", StringType()),
    StructField("description", StringType())
])
