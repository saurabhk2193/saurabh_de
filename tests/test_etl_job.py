'''
This module contains unit tests for the transformation steps of the ETL
job defined in etl_job.py & common_utils.py. It makes use of a local version of PySpark
that is bundled with the PySpark package.
'''
import unittest
from utilities.common_utils import *
from utilities.etl_job import *
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType

class TestETL(unittest.TestCase):
    """Test suite for transformation in etl_job.py
        """
    def setUp(self):
        '''Start spark'''
        spark = SparkSession.builder.master("local").appName("spark_de_unit_test").getOrCreate()
        self.spark = spark
        self.in_path = "input\\"
        self.out_filename = "output"
        self.out_path = "output\\task_output\\out.csv"
        self.j_schema = StructType([
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

    def tearDown(self) -> None:
        '''Stop spark'''
        self.spark.stop()

    def test_extract_data(self):
        ''' This test case is used to source input data count and number of columns in it'''
        expected_data = self.spark.read.schema(self.j_schema).json(self.in_path + "recipes*.json")
        input_data = extract_data(self.spark, self.j_schema, self.in_path)
        expected_cols = len(expected_data.columns)
        expected_rows = expected_data.count()

        cols = len(input_data.columns)
        rows = input_data.count()
        self.assertEqual(expected_cols, cols)
        self.assertEqual(expected_rows, rows)

    def test_all_data(self):
        ''' This test case is used to test filtered & transformed data count and columns in it'''
        expected_data = self.spark.read.schema(self.j_schema).json(self.in_path + "recipes*.json")
        input_data = extract_data(self.spark, self.j_schema, self.in_path)
        input_prc_src = pre_process_data(input_data)
        input_df_filter = filter_data(input_prc_src)
        print(len(input_df_filter.columns))
        expected_rows_after_filter = 47
        expected_cols_filter = 9
        rows_filter = input_df_filter.count()
        cols_filter = len(input_df_filter.columns)
        input_df_final = transform_data(input_df_filter)
        expected_rows_after_transform = 3
        expected_cols_transform = 2
        rows_transform = input_df_final.count()
        cols_transform = len(input_df_final.columns)

        self.assertEqual(expected_rows_after_filter, rows_filter)
        self.assertEqual(expected_rows_after_transform, rows_transform)
        self.assertEqual(expected_cols_filter, cols_filter)
        self.assertEqual(expected_cols_transform, cols_transform)


if __name__ == '__main__':
    unittest.main()
