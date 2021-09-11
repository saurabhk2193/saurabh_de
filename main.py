from pyspark.sql.functions import regexp_replace, col, lower, expr, avg, round
from utilities.common_utils import *
import pyspark.sql.functions as F
import os
import shutil




in_path = "input\\"
out_filename = "output"
out_path = "output\\task_output\\out.csv"

if __name__ == '__main__':
    sp_util = PysparkUtils()

    # Initializing logger for logging
    logger = sp_util.get_logger('MainProg')

    logger.info('main program starts....')
    print('PyCharm')



    spark = sp_util.create_spark_session()

    src_input = spark.read.schema(sp_util.j_schema).json(in_path+"recipes*.json")

    prc_src = src_input.withColumn("ingredients", regexp_replace("ingredients", "[\\r\\n]", ". "))\
        .withColumn("recipeYield", regexp_replace("recipeYield", "[\\r\\n]", ". "))\
        .withColumn("description", regexp_replace("description", "[\\r\\n]", ". "))

    logger.info('count before filter : '+str(prc_src.count()))

    # filter using ingredients substring
    df_filter = prc_src.filter(lower(col("ingredients")).contains("beef"))
    logger.info('count after filter : '+str(df_filter.count()))

    # find prepTime and cookTime
    df_time = df_filter.withColumn("cooktime_in_mins", F.coalesce(F.regexp_extract("cookTime", r'(\d+)H', 1).cast('int'), F.lit(0)) * 60 + F.coalesce(F.regexp_extract('cookTime', r'(\d+)M', 1).cast('int'), F.lit(0)))\
        .withColumn("preptime_in_mins", F.coalesce(F.regexp_extract("prepTime", r'(\d+)H', 1).cast('int'), F.lit(0)) * 60 + F.coalesce(F.regexp_extract('prepTime', r'(\d+)M', 1).cast('int'), F.lit(0)))

    # find total cook time
    df_total_cook_time = df_time.withColumn("total_cook_time", F.coalesce(col("cooktime_in_mins") + col("preptime_in_mins"), F.lit(0)))

    # Find difficulty as per cooking time
    df_difficulty = df_total_cook_time.withColumn("difficulty", expr("CASE WHEN total_cook_time < 30 THEN 'easy' WHEN (total_cook_time >= 30 and total_cook_time <= 60) THEN 'medium' WHEN total_cook_time > 60 THEN 'hard' END"))

    # df final with avg time as per difficulty
    df_final = df_difficulty.groupby("difficulty").agg(round(avg("total_cook_time"), 2).alias("avg_total_cooking_time"))

    # write to csv
    logger.info('Calling function write_to_csv....')
    sp_util.write_to_csv(out_path, df_final, out_filename)

    logger.info('main program ends....')


