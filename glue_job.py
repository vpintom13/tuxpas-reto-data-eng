import os
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark import SparkConf
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

from src.pipeline import run_pipeline

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'INPUT_PATH', 'DELTA_LAKE_PATH', 'OUTPUT_CSV_PATH'])

conf = SparkConf()
conf.set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
conf.set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
conf.set("spark.delta.logStore.class", "org.apache.spark.sql.delta.storage.S3SingleDriverLogStore")

sc = SparkContext(conf=conf)
glueContext = GlueContext(sc)

spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

os.environ["INPUT_PATH"] = args['INPUT_PATH']
os.environ["DELTA_LAKE_PATH"] = args['DELTA_LAKE_PATH']
os.environ["OUTPUT_CSV_PATH"] = args['OUTPUT_CSV_PATH']

run_pipeline(spark)

job.commit()
