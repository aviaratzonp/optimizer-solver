from joblib import Parallel, delayed
from pulp import *
import time
import json
import sys
import os
import glob
import pandas as pd
from optparse import OptionParser
import math
import logging
from pyspark.sql import SparkSession


from pulp import solvers

start = time.time()
t = time.gmtime().tm_hour
log_file = '/mnt/solvertmp/solver_logger_{}.log'.format(t)
logging.basicConfig(filename=log_file,
                    format='%(asctime)-32s  %(levelname)s  %(filename)22s %(funcName)14s :  %(message)10s',
                    level=logging.INFO)
logger = logging.getLogger()
logger.setLevel(20)

schema = ['hour', 'zone_id', 'region',
          'country', 'domain', 'forecast',
          'kpi_forecast', 'banner_id', 'campaign_id',
          'limitation', 'lifetime_impressions', 'today_impressions',
          'needed_daily_quota','skip_daily_goal', 'weight', 'priority',
          'units', 'experiment_precentage', 'lifetime_metric_numerator',
          'today_metric_numerator', 'kpi_type', 'kpi_goal','is_programmatic'
          ]


def get_data(spark, input_path):
    dfs = spark.read.csv(input_path + '/*', sep='\t')
    oldColumns = dfs.schema.names
    dfs = reduce(lambda dfs, idx: dfs.withColumnRenamed(oldColumns[idx], schema[idx]),
                     xrange(len(oldColumns)), dfs)

    df = dfs.toPandas()
    return df


def preprocessing(df):
    # step #1- split the default slices

    #step #2 -split the programmitc slices

    #step #3-

    pass

if __name__ == "__main__":
    logger.info("Runnig solver, going to fetch the data...")
    input_path = sys.argv[1]
    spark = SparkSession.builder.appName('solver').getOrCreate()
    df = get_data(spark,input_path)

    logger.info("total slices (variables)- {}".format(df.shape[0]))
    logger.info("going to preprocess the slices...")




