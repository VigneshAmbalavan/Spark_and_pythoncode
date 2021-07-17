
from common_etl_util import *
from pyspark.sql import SparkSession
from datetime import datetime
from configparser import ConfigParser
import pyspark.sql.functions as f
from pyspark.sql.window import Window
from pyspark.sql.functions import udf
import isodate

class AnalysisTask2(CommonEtl):

    def transform(self,df):
        df = df.where(f.lower(f.col('ingredients')).like("%beef%"))

        df.createOrReplaceTempView('beefrecipe_total_cooking_time')

        df = spark.sql("select total_cooking_time,case when total_cooking_time<30 then 'easy'\
                                                       when total_cooking_time between 30 and 60 then 'medium'\
                                                       when total_cooking_time > 60 then 'hard' end as difficulty\
                                        from(select cal_total_cooktime(cooktime,preptime) as total_cooking_time\
                                        from beefrecipe_total_cooking_time)")

        w = (Window().partitionBy(f.col("difficulty")).orderBy(f.col("difficulty")))

        return df.withColumn('avg_total_cooking_time', f.avg("total_cooking_time").over(w)).select("difficulty","avg_total_cooking_time")


if __name__ == '__main__':
    print('Enter the config file path:')
    config_file_path = input()
    logger = logging.getLogger('py4j')
    spark = SparkSession.builder.appName('task1').getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    spark.conf.set('spark.sql.sources.partitionOverwriteMode','dynamic')
    config = ConfigParser()
    start_time = str(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

    def cal_total_cooktime(cooktime,preptime):
        return int((isodate.parse_duration(cooktime).seconds + isodate.parse_duration(preptime).seconds) / 60)
    spark.udf.register("cal_total_cooktime", cal_total_cooktime)

    try:
        if len(config_file_path) > 0:
            py_config_file = str(config_file_path)
            config.read(py_config_file)
        else:
            logger.error('issue with config file parameter')
            raise Exception
    except Exception as e:
        logger.error('exception occurred as no Config mentioned')
        logger.error(str(datetime.now().strftime("%Y-%m-%d %H:%M:%S")) + " " + str(e))
        raise e

    job_config = JobConfig(config)
    etl = AnalysisTask2(spark, job_config)
    etl.execute()
