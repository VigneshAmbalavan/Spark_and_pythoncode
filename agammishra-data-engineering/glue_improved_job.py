import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.window import Window
from pyspark.sql import functions as F
from awsglue.dynamicframe import DynamicFrame


## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME','lower_bound_date','upper_bound_date'])
target_format="csv"
lower_bound_date=args['lower_bound_date']
upper_bound_date=args['upper_bound_date']

s3outputPath='s3://output-covid-data/output-moving-average/'


sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)


datasource0 = glueContext.create_dynamic_frame.from_catalog(database = "covid_case", table_name = "daily_covid_data")

new_df=datasource0.toDF()
new_df.createOrReplaceTempView('daily_covid_data')


###reading the data based on upper and lower bound dates
new=spark.sql("select * from  daily_covid_data where province_state is not null and country_region is not null and to_date(last_update) between '{}' and '{}'".format(lower_bound_date,upper_bound_date))

####logic of having a average
new_df = new.withColumn('last_update_timestamp', F.col('last_update').cast('timestamp'))

w = (Window()
     .partitionBy(F.col("province_state"),F.col('country_region'))
     .orderBy(F.col("last_update_timestamp").cast('long')))
     

spark_df = new_df.withColumn('average_date',F.lit(lower_bound_date)).withColumn('confirmed_MA', F.avg("confirmed").over(w)).withColumn('deaths_MA', F.avg("deaths").over(w))\
.withColumn('active_MA', F.avg("active").over(w)).withColumn('recovered_MA', F.avg("recovered").over(w)).repartition(1)

###writing back to the s3 output location
spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")
spark_df.drop(F.col('last_update')).write.mode("overwrite").format("csv").option('header',True).partitionBy('average_date').save(s3outputPath)
  


job.commit()

