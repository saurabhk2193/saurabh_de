from pyspark.sql.functions import regexp_replace, col, lower, expr, avg, round
from utilities.common_utils import *
from utilities.etl_job import *
import pyspark.sql.functions as F
import os
import shutil

# Initializing logger for logging
logger = get_logger('MainProg')

in_path = "input\\"
out_filename = "output"
out_path = "output\\task_output\\out.csv"

if __name__ == '__main__':
    logger.info('main program starts....')
    # print('PyCharm')
    logger.info('creating spark session....')
    spark = create_spark_session()
    logger.info('reading from source path....')
    src_input = extract_data(spark, j_schema, in_path)
    logger.info('pre-processing source data....')
    prc_src = pre_process_data(src_input)

    logger.info('count before filter : ' + str(prc_src.count()))
    logger.info('filtering data....')
    df_filter = filter_data(prc_src)

    logger.info('count after filter : ' + str(df_filter.count()))
    logger.info('data transform starts....')
    df_final = transform_data(df_filter)
    logger.info('data transform ends....')
    df_final.show()

    # write to csv
    logger.info('Calling function write_to_csv....')
    write_to_csv(out_path, df_final, out_filename)

    logger.info('main program ends....')
