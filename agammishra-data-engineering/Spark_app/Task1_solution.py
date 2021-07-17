
from common_etl_util import *
from pyspark.sql import SparkSession
from datetime import datetime
from configparser import ConfigParser
from pyspark.sql.functions import *

class AnalysisTask1(CommonEtl):

    def transform(self, df):
        df=df.withColumn('name',translate(col('name'),", !)('",""))
        return df


if __name__ == '__main__':
    print('Enter the config file path:')
    config_file_path = input()
    logger = logging.getLogger('py4j')
    spark = SparkSession.builder.appName('task1').getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    config = ConfigParser()
    start_time = str(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

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
    etl = AnalysisTask1(spark, job_config)
    etl.execute()
