'''
This module contains all the functions for data transformation and filtering required for the etl job.
'''
from pyspark.sql.functions import regexp_replace, col, lower, expr, avg, round
from utilities.common_utils import *
from utilities.etl_job import *
import pyspark.sql.functions as F


def pre_process_data(src_input):
    '''
    This function is used to pre-process input dataframe and remove newline characters from receipeYeild and description column
    :param src_input: dataframe which needs to pre-processed
    :return: final pre-processed dataframe
    '''
    prc_src = src_input.withColumn("ingredients", regexp_replace("ingredients", "[\\r\\n]", ". ")) \
        .withColumn("recipeYield", regexp_replace("recipeYield", "[\\r\\n]", ". ")) \
        .withColumn("description", regexp_replace("description", "[\\r\\n]", ". "))
    return prc_src


def filter_data(prc_src):
    '''
    Filter receipes that have beef in it as one of the ingredients
    :param prc_src: dataframe that needs to be filtered
    :return: filtered dataframe
    '''
    df_filter = prc_src.filter(lower(col("ingredients")).contains("beef"))
    return df_filter


def transform_data(df_filter):
    '''
    This function extracts duration from cookTime and prepTime to calculate total cooking time for the receipe.
    Difficulty(easy, medium, hard) for the receipe is calculated using total cooking time for the receipe.
    Average cooking time for each difficulty level is calculated finally
    :param df_filter: input dataframe to apply transformation
    :return: final datafram containing difficulty and average cook time for it
    '''
    df_time = df_filter.withColumn("cooktime_in_mins",
                                   F.coalesce(F.regexp_extract("cookTime", r'(\d+)H', 1).cast('int'),
                                              F.lit(0)) * 60 + F.coalesce(
                                       F.regexp_extract('cookTime', r'(\d+)M', 1).cast('int'), F.lit(0))) \
        .withColumn("preptime_in_mins",
                    F.coalesce(F.regexp_extract("prepTime", r'(\d+)H', 1).cast('int'), F.lit(0)) * 60 + F.coalesce(
                        F.regexp_extract('prepTime', r'(\d+)M', 1).cast('int'), F.lit(0)))

    # find total cook time
    df_total_cook_time = df_time.withColumn("total_cook_time",
                                            F.coalesce(col("cooktime_in_mins") + col("preptime_in_mins"), F.lit(0)))

    # Find difficulty as per cooking time
    df_difficulty = df_total_cook_time.withColumn("difficulty", expr(
        "CASE WHEN total_cook_time < 30 THEN 'easy' WHEN (total_cook_time >= 30 and total_cook_time <= 60) THEN 'medium' WHEN total_cook_time > 60 THEN 'hard' END"))

    # df final with avg time as per difficulty
    df_final = df_difficulty.groupby("difficulty").agg(round(avg("total_cook_time"), 2).alias("avg_total_cooking_time"))
    return df_final
