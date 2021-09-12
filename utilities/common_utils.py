'''
This module contains all the utilities commonly used in the program
'''

import logging
import os
import shutil

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType


def create_spark_session():
    '''
    this function is used to create spark session which would be used to communicate with all spark components
    '''
    spark = SparkSession.builder.master("local").appName("spark_de_test").getOrCreate()
    return spark


def get_logger(prg_name):
    '''
    This program is used for logging in Spark application.
    :param prg_name: name of program which would be used to display in the logs
    '''
    # create logger
    logger = logging.getLogger(prg_name)
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
    '''
    This program is used to save dataframe as csv with given filename.
    This function renames part file with the given input and delete other files in that folder.
    :param output_path: path where csv file needs to be saved
    :param df_final: dataframe which needs to be saved as csv
    :param filename: input filename with which csv needs to be named
    '''
    df_final.repartition(1).write.csv(output_path, sep='|')
    list1 = os.listdir(output_path)
    for name in list1:
        if name.endswith(".csv"):
            src = output_path + "\\" + name
            os.rename(src, "output\\task_output\\" + filename + ".csv")
            print(src)
    shutil.rmtree(output_path)


def extract_data(spark, schema, in_path):
    '''
    This function extracts data from json file input path and create dataframe on top of it.
    :param spark: spark session
    :param schema: schema to be enforced on json structure
    :param in_path: input file path for json file
    :return: dataframe created on top of json input
    '''
    src_input = spark.read.schema(schema).json(in_path + "recipes*.json")
    return src_input

'''
Schema to be enforced on JSON input having 9 columns 
and have been all set to String type so that all the data could be saved in our source tables.
'''
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
