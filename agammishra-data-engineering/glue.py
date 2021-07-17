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
args = getResolvedOptions(sys.argv, ['JOB_NAME','new_file_name','partition_value'])
target_format="csv"
s3inpputfilename=args['new_file_name']
partition_col=args['partition_value']
s3outputPath='s3://output-covid-data/output-moving-average/'


sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)


# datasource0 = glueContext.create_dynamic_frame.from_catalog(database = "covid_case", table_name = "input_covid_data")
new = spark.read.format('csv').option("header","true").load(s3inpputfilename)
# new=datasource0.toDF()

####creating a window of 3, we can take the window length as we want, and partitioning is done on state and country
w = (Window.partitionBy('province_state','country_region').orderBy(F.col('last_update').cast('timestamp')).rowsBetween(-1, 1))
#####calculation of average on this window will give us the moving average for length 3(-1,0,1) on confirmed cases,deaths,recovery
###and active
spark_df = new.withColumn('file_load_date',F.lit(partition_col)).withColumn('confirmed_MA', F.avg("confirmed").over(w)).withColumn('deaths_MA', F.avg("deaths").over(w))\
.withColumn('active_MA', F.avg("active").over(w)).withColumn('recovered_MA', F.avg("recovered").over(w)).repartition(1)

spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")
spark_df.write.mode("overwrite").format("csv").option('header',True).partitionBy('file_load_date').save(s3outputPath)
  

# resultdata=DynamicFrame.fromDF(spark_df, glueContext,"nested")

# glueContext.write_dynamic_frame.from_options(frame = resultdata, connection_type = "s3", connection_options = {"path": s3Path}, format = target_format)

job.commit()



##below are the open questions"
#1.what is the length of the interval for calculating moving average(currently we have taken 3)
#2.what is writing scenario to the target destination, means do we need to write daily in a partitioned date folder or we need 
##to only append to the existing previous day file
#3. do we need to read the whole data daily(means previous days also), to calculate MA or we need to calculate only on new file.