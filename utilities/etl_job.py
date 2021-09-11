from pyspark.sql.functions import regexp_replace, col, lower, expr, avg, round
from utilities.common_utils import *
from utilities.etl_job import *
import pyspark.sql.functions as F


def extract_data(self,spark,schema, in_path):
    src_input = spark.read.schema(schema).json(in_path + "recipes*.json")
    return src_input

def