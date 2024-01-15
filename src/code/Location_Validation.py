"""
4 - Location Validation Module
Validate the location field for correctness by doing a lookup against the Areas_in_blore.csv file.
Records that do not validate should be outputted in a separate file.
"""

import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from datetime import datetime


spark = SparkSession.builder.appName("abc").getOrCreate()


def data_validation(data_file_path='../data/src_data_daily/data_file_20210528182554.csv',
                    ref_file_path ='../data/Areas_in_blore.xlsx'):
    """
    Creating two files from input data file using reference look up file.
    keeping only matching areas (from 'ref_file_path') with location( from 'data_file_path')
    :param data_file_path: data file (e.g - '../data/src_data_daily/data_file_20210528182554.csv')
    :param ref_file_path: reference  lookup file (e.g. - '../data/Areas_in_blore.xlsx')
    :return: None
    """
    print(ref_file_path)
    df = pd.read_excel(ref_file_path, sheet_name='Sheet1')
    sp_df = spark.createDataFrame(df)

    ref_df = sp_df.withColumn('Area',trim(lower('Area')))


    data_df = spark.read.csv(data_file_path,header = True,inferSchema = True)
    data = data_df.withColumn('location',trim(lower('location')))
    output_data = ref_df.join(data,ref_df.Area==data.location,how='inner').select(data['*'])
    subtracted_data = data.subtract(output_data)
    output_data.write.csv(f"../data/correct_output/a.out/")
    subtracted_data.write.csv(f"../data/bad_data_dir/a.bad/")



